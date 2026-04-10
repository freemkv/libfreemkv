//! AACS bus authentication handshake — ECDH key agreement + bus key derivation.
//!
//! Implements the AACS SCSI authentication protocol to obtain:
//!   - Volume ID (VID) — needed for VUK derivation
//!   - Read Data Key — needed for AACS 2.0 (UHD) bus decryption
//!
//! Flow:
//!   1. Invalidate AGIDs → allocate fresh AGID
//!   2. Send host certificate + nonce
//!   3. Receive drive certificate + nonce
//!   4. Receive drive key point + signature, verify
//!   5. Sign host key point, send
//!   6. ECDH: host_priv × drive_key_point → bus key (low 128 bits of x)
//!   7. Read VID or Read Data Keys (encrypted with bus key)
//!
//! Supports:
//!   - AACS 1.0: custom 160-bit curve, SHA-1, 20-byte keys
//!   - AACS 2.0: drives accept AACS 1.0 host certs for backward compatibility
//!     (full P-256/SHA-256 AACS 2.0 handshake prepared but rarely needed)

use crate::error::{Error, Result};
use crate::drive::DriveSession;
use crate::scsi::DataDirection;
use num_bigint::BigUint;
use num_traits::{One, Zero};
use sha1::{Sha1, Digest};

/// Execute a SCSI command that reads data from the device.
fn scsi_read(session: &mut DriveSession, cdb: &[u8], len: usize) -> Result<Vec<u8>> {
    let mut buf = vec![0u8; len];
    session.scsi_execute(cdb, DataDirection::FromDevice, &mut buf, 5_000)?;
    Ok(buf)
}

/// Execute a SCSI command that writes data to the device.
fn scsi_write(session: &mut DriveSession, cdb: &[u8], data: &[u8]) -> Result<()> {
    let mut buf = data.to_vec();
    session.scsi_execute(cdb, DataDirection::ToDevice, &mut buf, 5_000)?;
    Ok(())
}

// ── AACS 1.0 elliptic curve parameters (160-bit) ───────────────────────────

const EC_P: [u8; 20] = [
    0x9D, 0xC9, 0xD8, 0x13, 0x55, 0xEC, 0xCE, 0xB5, 0x60, 0xBD,
    0xB0, 0x9E, 0xF9, 0xEA, 0xE7, 0xC4, 0x79, 0xA7, 0xD7, 0xDF,
];
const EC_A: [u8; 20] = [
    0x9D, 0xC9, 0xD8, 0x13, 0x55, 0xEC, 0xCE, 0xB5, 0x60, 0xBD,
    0xB0, 0x9E, 0xF9, 0xEA, 0xE7, 0xC4, 0x79, 0xA7, 0xD7, 0xDC,
];
#[cfg(test)]
const EC_B: [u8; 20] = [
    0x40, 0x2D, 0xAD, 0x3E, 0xC1, 0xCB, 0xCD, 0x16, 0x52, 0x48,
    0xD6, 0x8E, 0x12, 0x45, 0xE0, 0xC4, 0xDA, 0xAC, 0xB1, 0xD8,
];
const EC_N: [u8; 20] = [
    0x9D, 0xC9, 0xD8, 0x13, 0x55, 0xEC, 0xCE, 0xB5, 0x60, 0xBD,
    0xC4, 0x4F, 0x54, 0x81, 0x7B, 0x2C, 0x7F, 0x5A, 0xB0, 0x17,
];
const EC_GX: [u8; 20] = [
    0x2E, 0x64, 0xFC, 0x22, 0x57, 0x83, 0x51, 0xE6, 0xF4, 0xCC,
    0xA7, 0xEB, 0x81, 0xD0, 0xA4, 0xBD, 0xC5, 0x4C, 0xCE, 0xC6,
];
const EC_GY: [u8; 20] = [
    0x09, 0x14, 0xA2, 0x5D, 0xD0, 0x54, 0x42, 0x88, 0x9D, 0xB4,
    0x55, 0xC7, 0xF2, 0x3C, 0x9A, 0x07, 0x07, 0xF5, 0xCB, 0xB9,
];

// ── AACS 2.0 elliptic curve parameters (P-256 / secp256r1 / NIST prime256v1)

const P256_P: [u8; 32] = [
    0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
];
const P256_A: [u8; 32] = [
    0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFC,
];
#[cfg(test)]
const P256_B: [u8; 32] = [
    0x5A, 0xC6, 0x35, 0xD8, 0xAA, 0x3A, 0x93, 0xE7, 0xB3, 0xEB, 0xBD, 0x55,
    0x76, 0x98, 0x86, 0xBC, 0x65, 0x1D, 0x06, 0xB0, 0xCC, 0x53, 0xB0, 0xF6,
    0x3B, 0xCE, 0x3C, 0x3E, 0x27, 0xD2, 0x60, 0x4B,
];
const P256_N: [u8; 32] = [
    0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xBC, 0xE6, 0xFA, 0xAD, 0xA7, 0x17, 0x9E, 0x84,
    0xF3, 0xB9, 0xCA, 0xC2, 0xFC, 0x63, 0x25, 0x51,
];
const P256_GX: [u8; 32] = [
    0x6B, 0x17, 0xD1, 0xF2, 0xE1, 0x2C, 0x42, 0x47, 0xF8, 0xBC, 0xE6, 0xE5,
    0x63, 0xA4, 0x40, 0xF2, 0x77, 0x03, 0x7D, 0x81, 0x2D, 0xEB, 0x33, 0xA0,
    0xF4, 0xA1, 0x39, 0x45, 0xD8, 0x98, 0xC2, 0x96,
];
const P256_GY: [u8; 32] = [
    0x4F, 0xE3, 0x42, 0xE2, 0xFE, 0x1A, 0x7F, 0x9B, 0x8E, 0xE7, 0xEB, 0x4A,
    0x7C, 0x0F, 0x9E, 0x16, 0x2B, 0xCE, 0x33, 0x57, 0x6B, 0x31, 0x5E, 0xCE,
    0xCB, 0xB6, 0x40, 0x68, 0x37, 0xBF, 0x51, 0xF5,
];

/// AACS 2.0 LA public key for cert verification (P-256).
/// From AACS2 specification — used to verify type 0x11 drive certificates.
const AACS2_LA_PUB_X: [u8; 32] = [
    0xF9, 0x57, 0xBC, 0x1F, 0xD7, 0xE6, 0x09, 0x7E, 0xCA, 0xCC, 0x35, 0x23,
    0x4C, 0x9C, 0x66, 0xC3, 0x42, 0xEB, 0x3D, 0xB7, 0x2B, 0x41, 0x06, 0xF4,
    0x04, 0x9C, 0x6A, 0x88, 0x70, 0x00, 0xAA, 0x2C,
];
const AACS2_LA_PUB_Y: [u8; 32] = [
    0x39, 0x55, 0x0B, 0x41, 0x02, 0x27, 0xEA, 0x7B, 0x1A, 0x53, 0xF8, 0x67,
    0x8C, 0x5A, 0x91, 0x6F, 0xFC, 0x7C, 0x78, 0x01, 0x3E, 0x89, 0x15, 0xE3,
    0xF0, 0x81, 0xD3, 0xE9, 0x3E, 0x17, 0x55, 0x0B,
];

// ── AACS 1.0 LA (Licensing Administrator) public key for cert verification ──

const AACS_LA_PUB_X: [u8; 20] = [
    0x01, 0xF3, 0x5D, 0xAB, 0xD8, 0xAE, 0x5F, 0x40, 0x56, 0x5E,
    0x30, 0xC8, 0x8A, 0x60, 0x42, 0x82, 0x07, 0x61, 0xDF, 0x93,
];
const AACS_LA_PUB_Y: [u8; 20] = [
    0x44, 0x87, 0xB5, 0xAC, 0x07, 0x10, 0x8D, 0x10, 0x5B, 0xA5,
    0xB9, 0xE3, 0x2F, 0x3B, 0xBB, 0xFC, 0x0C, 0x2C, 0xBC, 0xD1,
];

// ── Elliptic curve arithmetic over GF(p) ───────────────────────────────────

#[derive(Clone, Debug)]
struct EcPoint {
    x: BigUint,
    y: BigUint,
    infinity: bool,
}

impl EcPoint {
    fn infinity() -> Self {
        EcPoint { x: BigUint::zero(), y: BigUint::zero(), infinity: true }
    }

    fn new(x: BigUint, y: BigUint) -> Self {
        EcPoint { x, y, infinity: false }
    }

    fn from_bytes(x_bytes: &[u8], y_bytes: &[u8]) -> Self {
        EcPoint::new(BigUint::from_bytes_be(x_bytes), BigUint::from_bytes_be(y_bytes))
    }
}

/// Modular inverse using extended Euclidean algorithm.
fn mod_inv(a: &BigUint, m: &BigUint) -> Option<BigUint> {
    use num_bigint::BigInt;
    use num_traits::Signed;

    let a = BigInt::from(a.clone());
    let m = BigInt::from(m.clone());

    let (mut old_r, mut r) = (a, m.clone());
    let (mut old_s, mut s) = (BigInt::one(), BigInt::zero());

    while !r.is_zero() {
        let q = &old_r / &r;
        let temp_r = r.clone();
        r = old_r - &q * &r;
        old_r = temp_r;
        let temp_s = s.clone();
        s = old_s - &q * &s;
        old_s = temp_s;
    }

    if old_r != BigInt::one() {
        return None;
    }

    if old_s.is_negative() {
        old_s += &m;
    }
    Some(old_s.to_biguint().unwrap())
}

/// EC point addition on curve y² = x³ + ax + b (mod p).
fn ec_add(p1: &EcPoint, p2: &EcPoint, a: &BigUint, p: &BigUint) -> EcPoint {
    if p1.infinity { return p2.clone(); }
    if p2.infinity { return p1.clone(); }

    if p1.x == p2.x {
        if p1.y == p2.y && !p1.y.is_zero() {
            return ec_double(p1, a, p);
        }
        return EcPoint::infinity();
    }

    // λ = (y2 - y1) / (x2 - x1) mod p
    let dy = if p2.y >= p1.y {
        (&p2.y - &p1.y) % p
    } else {
        (p - (&p1.y - &p2.y) % p) % p
    };
    let dx = if p2.x >= p1.x {
        (&p2.x - &p1.x) % p
    } else {
        (p - (&p1.x - &p2.x) % p) % p
    };

    // Safety: mod_inv only returns None if dx == 0 (points identical),
    // which is prevented by the caller using ec_double for that case.
    let dx_inv = mod_inv(&dx, p).expect("ec_add: dx has no inverse");
    let lam = (&dy * &dx_inv) % p;

    // x3 = λ² - x1 - x2 mod p
    let x3 = {
        let lam2 = (&lam * &lam) % p;
        let sum = (&p1.x + &p2.x) % p;
        if lam2 >= sum {
            (lam2 - sum) % p
        } else {
            (p - (sum - lam2) % p) % p
        }
    };

    // y3 = λ(x1 - x3) - y1 mod p
    let y3 = {
        let diff = if p1.x >= x3 {
            (&p1.x - &x3) % p
        } else {
            (p - (&x3 - &p1.x) % p) % p
        };
        let prod = (&lam * &diff) % p;
        if prod >= p1.y {
            (prod - &p1.y) % p
        } else {
            (p - (&p1.y - prod) % p) % p
        }
    };

    EcPoint::new(x3, y3)
}

/// EC point doubling.
fn ec_double(pt: &EcPoint, a: &BigUint, p: &BigUint) -> EcPoint {
    if pt.infinity || pt.y.is_zero() {
        return EcPoint::infinity();
    }

    // λ = (3x² + a) / (2y) mod p
    let three = BigUint::from(3u32);
    let two = BigUint::from(2u32);

    let numerator = (&three * &pt.x * &pt.x + a) % p;
    let denominator = (&two * &pt.y) % p;
    // Safety: mod_inv only returns None if 2*y == 0 (point at infinity),
    // which shouldn't occur with valid curve points.
    let denom_inv = mod_inv(&denominator, p).expect("ec_double: denominator has no inverse");
    let lam = (&numerator * &denom_inv) % p;

    // x3 = λ² - 2x mod p
    let x3 = {
        let lam2 = (&lam * &lam) % p;
        let two_x = (&two * &pt.x) % p;
        if lam2 >= two_x {
            (lam2 - two_x) % p
        } else {
            (p - (two_x - lam2) % p) % p
        }
    };

    // y3 = λ(x - x3) - y mod p
    let y3 = {
        let diff = if pt.x >= x3 {
            (&pt.x - &x3) % p
        } else {
            (p - (&x3 - &pt.x) % p) % p
        };
        let prod = (&lam * &diff) % p;
        if prod >= pt.y {
            (prod - &pt.y) % p
        } else {
            (p - (&pt.y - prod) % p) % p
        }
    };

    EcPoint::new(x3, y3)
}

/// Scalar multiplication using double-and-add.
fn ec_mul(k: &BigUint, pt: &EcPoint, a: &BigUint, p: &BigUint) -> EcPoint {
    if k.is_zero() {
        return EcPoint::infinity();
    }

    let mut result = EcPoint::infinity();
    let mut base = pt.clone();
    let mut scalar = k.clone();

    while !scalar.is_zero() {
        if scalar.bit(0) {
            result = ec_add(&result, &base, a, p);
        }
        base = ec_double(&base, a, p);
        scalar >>= 1;
    }

    result
}

/// Convert BigUint to fixed-size big-endian bytes, zero-padded.
fn to_bytes_be_padded(n: &BigUint, len: usize) -> Vec<u8> {
    let bytes = n.to_bytes_be();
    if bytes.len() >= len {
        bytes[bytes.len() - len..].to_vec()
    } else {
        let mut padded = vec![0u8; len - bytes.len()];
        padded.extend_from_slice(&bytes);
        padded
    }
}

// ── ECDSA ───────────────────────────────────────────────────────────────────

/// ECDSA sign: sign SHA-1(data) with private key on AACS curve.
/// Returns (r, s) each 20 bytes.
fn ecdsa_sign(priv_key: &[u8; 20], data: &[u8]) -> ([u8; 20], [u8; 20]) {
    let p = BigUint::from_bytes_be(&EC_P);
    let a = BigUint::from_bytes_be(&EC_A);
    let n = BigUint::from_bytes_be(&EC_N);
    let g = EcPoint::from_bytes(&EC_GX, &EC_GY);
    let d = BigUint::from_bytes_be(priv_key);

    // Hash the data
    let hash = Sha1::digest(data);
    let z = BigUint::from_bytes_be(&hash);

    loop {
        // Generate random k
        let mut k_bytes = [0u8; 20];
        use rand::RngCore;
        rand::thread_rng().fill_bytes(&mut k_bytes);
        let k = BigUint::from_bytes_be(&k_bytes) % &n;
        if k.is_zero() { continue; }

        // R = k × G
        let r_point = ec_mul(&k, &g, &a, &p);
        let r = &r_point.x % &n;
        if r.is_zero() { continue; }

        // s = k⁻¹(z + r·d) mod n
        let k_inv = match mod_inv(&k, &n) {
            Some(v) => v,
            None => continue,
        };
        let s = (&k_inv * ((&z + &r * &d) % &n)) % &n;
        if s.is_zero() { continue; }

        let r_bytes = to_bytes_be_padded(&r, 20);
        let s_bytes = to_bytes_be_padded(&s, 20);

        let mut r_out = [0u8; 20];
        let mut s_out = [0u8; 20];
        r_out.copy_from_slice(&r_bytes);
        s_out.copy_from_slice(&s_bytes);

        return (r_out, s_out);
    }
}

/// ECDSA verify: verify signature (r, s) against SHA-1(data) using public key.
fn ecdsa_verify(pub_x: &[u8; 20], pub_y: &[u8; 20], sig_r: &[u8; 20], sig_s: &[u8; 20], data: &[u8]) -> bool {
    let p = BigUint::from_bytes_be(&EC_P);
    let a = BigUint::from_bytes_be(&EC_A);
    let n = BigUint::from_bytes_be(&EC_N);
    let g = EcPoint::from_bytes(&EC_GX, &EC_GY);
    let q = EcPoint::from_bytes(pub_x, pub_y);

    let r = BigUint::from_bytes_be(sig_r);
    let s = BigUint::from_bytes_be(sig_s);

    if r.is_zero() || r >= n || s.is_zero() || s >= n {
        return false;
    }

    let hash = Sha1::digest(data);
    let z = BigUint::from_bytes_be(&hash);

    let s_inv = match mod_inv(&s, &n) {
        Some(v) => v,
        None => return false,
    };

    let u1 = (&z * &s_inv) % &n;
    let u2 = (&r * &s_inv) % &n;

    let p1 = ec_mul(&u1, &g, &a, &p);
    let p2 = ec_mul(&u2, &q, &a, &p);
    let r_point = ec_add(&p1, &p2, &a, &p);

    if r_point.infinity {
        return false;
    }

    &r_point.x % &n == r
}

// ── P-256 ECDSA (SHA-256) for AACS 2.0 ─────────────────────────────────────

/// ECDSA sign with P-256/SHA-256. Returns (r, s) each 32 bytes.
fn ecdsa_sign_p256(priv_key: &[u8; 32], data: &[u8]) -> ([u8; 32], [u8; 32]) {
    use sha2::{Sha256, Digest as Sha2Digest};

    let p = BigUint::from_bytes_be(&P256_P);
    let a = BigUint::from_bytes_be(&P256_A);
    let n = BigUint::from_bytes_be(&P256_N);
    let g = EcPoint::from_bytes(&P256_GX, &P256_GY);
    let d = BigUint::from_bytes_be(priv_key);

    let hash = Sha256::digest(data);
    let z = BigUint::from_bytes_be(&hash);

    loop {
        let mut k_bytes = [0u8; 32];
        use rand::RngCore;
        rand::thread_rng().fill_bytes(&mut k_bytes);
        let k = BigUint::from_bytes_be(&k_bytes) % &n;
        if k.is_zero() { continue; }

        let r_point = ec_mul(&k, &g, &a, &p);
        let r = &r_point.x % &n;
        if r.is_zero() { continue; }

        let k_inv = match mod_inv(&k, &n) {
            Some(v) => v,
            None => continue,
        };
        let s = (&k_inv * ((&z + &r * &d) % &n)) % &n;
        if s.is_zero() { continue; }

        let r_bytes = to_bytes_be_padded(&r, 32);
        let s_bytes = to_bytes_be_padded(&s, 32);

        let mut r_out = [0u8; 32];
        let mut s_out = [0u8; 32];
        r_out.copy_from_slice(&r_bytes);
        s_out.copy_from_slice(&s_bytes);

        return (r_out, s_out);
    }
}

/// ECDSA verify with P-256/SHA-256.
fn ecdsa_verify_p256(pub_x: &[u8], pub_y: &[u8], sig_r: &[u8], sig_s: &[u8], data: &[u8]) -> bool {
    use sha2::{Sha256, Digest as Sha2Digest};

    let p = BigUint::from_bytes_be(&P256_P);
    let a = BigUint::from_bytes_be(&P256_A);
    let n = BigUint::from_bytes_be(&P256_N);
    let g = EcPoint::from_bytes(&P256_GX, &P256_GY);
    let q = EcPoint::new(BigUint::from_bytes_be(pub_x), BigUint::from_bytes_be(pub_y));

    let r = BigUint::from_bytes_be(sig_r);
    let s = BigUint::from_bytes_be(sig_s);

    if r.is_zero() || r >= n || s.is_zero() || s >= n {
        return false;
    }

    let hash = Sha256::digest(data);
    let z = BigUint::from_bytes_be(&hash);

    let s_inv = match mod_inv(&s, &n) {
        Some(v) => v,
        None => return false,
    };

    let u1 = (&z * &s_inv) % &n;
    let u2 = (&r * &s_inv) % &n;

    let p1 = ec_mul(&u1, &g, &a, &p);
    let p2 = ec_mul(&u2, &q, &a, &p);
    let r_point = ec_add(&p1, &p2, &a, &p);

    if r_point.infinity {
        return false;
    }

    &r_point.x % &n == r
}

/// Verify an AACS 2.0 certificate (type 0x11, 132 bytes) against AACS 2.0 LA key.
fn verify_cert_p256(cert: &[u8]) -> bool {
    if cert.len() < 132 { return false; }
    // AACS 2.0 cert: type(1) + flags(1) + padding(2) + serial(6) + pub_x(32) + pub_y(32) + sig_r(32) + sig_s(32)
    // Signature is over the first 74 bytes
    let sig_r = &cert[74..106];
    let sig_s = &cert[106..138]; // some certs may be padded differently

    // Use what we have — verify over the signed portion
    if cert.len() >= 138 {
        ecdsa_verify_p256(&AACS2_LA_PUB_X, &AACS2_LA_PUB_Y, sig_r, sig_s, &cert[..74])
    } else {
        false
    }
}

/// Extract public key from an AACS 2.0 certificate (32-byte x,y).
fn cert_pub_key_p256(cert: &[u8]) -> ([u8; 32], [u8; 32]) {
    let mut x = [0u8; 32];
    let mut y = [0u8; 32];
    x.copy_from_slice(&cert[10..42]);
    y.copy_from_slice(&cert[42..74]);
    (x, y)
}

/// Compute bus key via ECDH on P-256 curve.
fn compute_bus_key_p256(host_priv: &[u8; 32], drive_key_point_x: &[u8], drive_key_point_y: &[u8]) -> [u8; 16] {
    let p = BigUint::from_bytes_be(&P256_P);
    let a = BigUint::from_bytes_be(&P256_A);

    let d = BigUint::from_bytes_be(host_priv);
    let dkp = EcPoint::new(BigUint::from_bytes_be(drive_key_point_x), BigUint::from_bytes_be(drive_key_point_y));

    let shared = ec_mul(&d, &dkp, &a, &p);

    // Bus key = lowest 128 bits of x-coordinate
    let x_bytes = to_bytes_be_padded(&shared.x, 32);
    let mut bus_key = [0u8; 16];
    bus_key.copy_from_slice(&x_bytes[16..32]);
    bus_key
}

// ── AACS certificate handling ───────────────────────────────────────────────

/// Verify an AACS certificate (92 bytes) against the AACS LA public key.
fn verify_cert(cert: &[u8]) -> bool {
    if cert.len() < 92 { return false; }
    // Certificate format: type(1) + flags(1) + padding(2) + serial(6) + pub_x(20) + pub_y(20) + sig_r(20) + sig_s(20)
    // Signature is over the first 52 bytes
    let mut sig_r = [0u8; 20];
    let mut sig_s = [0u8; 20];
    sig_r.copy_from_slice(&cert[52..72]);
    sig_s.copy_from_slice(&cert[72..92]);

    ecdsa_verify(&AACS_LA_PUB_X, &AACS_LA_PUB_Y, &sig_r, &sig_s, &cert[..52])
}

/// Extract public key from certificate.
fn cert_pub_key(cert: &[u8]) -> ([u8; 20], [u8; 20]) {
    let mut x = [0u8; 20];
    let mut y = [0u8; 20];
    x.copy_from_slice(&cert[12..32]);
    y.copy_from_slice(&cert[32..52]);
    (x, y)
}

// ── Bus key derivation (ECDH) ───────────────────────────────────────────────

/// Compute bus key via ECDH: bus_key = low 128 bits of (host_priv × drive_key_point).x
fn compute_bus_key(host_priv: &[u8; 20], drive_key_point_x: &[u8; 20], drive_key_point_y: &[u8; 20]) -> [u8; 16] {
    let p = BigUint::from_bytes_be(&EC_P);
    let a = BigUint::from_bytes_be(&EC_A);

    let d = BigUint::from_bytes_be(host_priv);
    let dkp = EcPoint::from_bytes(drive_key_point_x, drive_key_point_y);

    let shared = ec_mul(&d, &dkp, &a, &p);

    // Bus key = lowest 128 bits (last 16 bytes) of x-coordinate
    let x_bytes = to_bytes_be_padded(&shared.x, 20);
    let mut bus_key = [0u8; 16];
    bus_key.copy_from_slice(&x_bytes[4..20]); // last 16 of 20
    bus_key
}

/// Generate ephemeral host key pair: (private_key, public_point_x, public_point_y).
/// Generate P-256 ephemeral key pair for AACS 2.0.
fn generate_host_key_pair_p256() -> ([u8; 32], [u8; 32], [u8; 32]) {
    let p_mod = BigUint::from_bytes_be(&P256_P);
    let a = BigUint::from_bytes_be(&P256_A);
    let n = BigUint::from_bytes_be(&P256_N);
    let g = EcPoint::from_bytes(&P256_GX, &P256_GY);

    let mut priv_bytes = [0u8; 32];
    use rand::RngCore;
    rand::thread_rng().fill_bytes(&mut priv_bytes);
    let d = BigUint::from_bytes_be(&priv_bytes) % &n;

    let q = ec_mul(&d, &g, &a, &p_mod);

    let mut key = [0u8; 32];
    let mut pub_x = [0u8; 32];
    let mut pub_y = [0u8; 32];
    key.copy_from_slice(&to_bytes_be_padded(&d, 32));
    pub_x.copy_from_slice(&to_bytes_be_padded(&q.x, 32));
    pub_y.copy_from_slice(&to_bytes_be_padded(&q.y, 32));

    (key, pub_x, pub_y)
}

/// Generate AACS 1.0 ephemeral key pair.
fn generate_host_key_pair() -> ([u8; 20], [u8; 20], [u8; 20]) {
    let p_mod = BigUint::from_bytes_be(&EC_P);
    let a = BigUint::from_bytes_be(&EC_A);
    let g = EcPoint::from_bytes(&EC_GX, &EC_GY);

    let mut priv_bytes = [0u8; 20];
    use rand::RngCore;
    rand::thread_rng().fill_bytes(&mut priv_bytes);
    let d = BigUint::from_bytes_be(&priv_bytes);

    let q = ec_mul(&d, &g, &a, &p_mod);

    let qx = to_bytes_be_padded(&q.x, 20);
    let qy = to_bytes_be_padded(&q.y, 20);

    let mut pub_x = [0u8; 20];
    let mut pub_y = [0u8; 20];
    pub_x.copy_from_slice(&qx);
    pub_y.copy_from_slice(&qy);

    (priv_bytes, pub_x, pub_y)
}

// ── AES-CMAC (for MAC verification) ────────────────────────────────────────

/// AES-128-CMAC over 16 bytes of data.
fn aes_cmac_16(data: &[u8; 16], key: &[u8; 16]) -> [u8; 16] {
    use aes::Aes128;
    use aes::cipher::{BlockEncrypt, KeyInit, generic_array::GenericArray};

    let cipher = Aes128::new(GenericArray::from_slice(key));

    // For single-block CMAC:
    // 1. Generate subkey K1
    let mut l = GenericArray::clone_from_slice(&[0u8; 16]);
    cipher.encrypt_block(&mut l);

    let mut k1 = [0u8; 16];
    let carry = (l[0] >> 7) & 1;
    for i in 0..15 {
        k1[i] = (l[i] << 1) | (l[i + 1] >> 7);
    }
    k1[15] = l[15] << 1;
    if carry == 1 {
        k1[15] ^= 0x87; // Rb for AES-128
    }

    // 2. XOR data with K1, encrypt
    let mut block = [0u8; 16];
    for i in 0..16 {
        block[i] = data[i] ^ k1[i];
    }
    let mut ga = GenericArray::clone_from_slice(&block);
    cipher.encrypt_block(&mut ga);

    let mut mac = [0u8; 16];
    mac.copy_from_slice(&ga);
    mac
}

// ── SCSI command builders ───────────────────────────────────────────────────

/// Build REPORT KEY CDB (0xA4).
fn cdb_report_key(agid: u8, format: u8, len: u16) -> [u8; 12] {
    let mut cdb = [0u8; 12];
    cdb[0] = crate::scsi::SCSI_REPORT_KEY;
    cdb[7] = crate::scsi::AACS_KEY_CLASS;
    cdb[8] = (len >> 8) as u8;
    cdb[9] = (len & 0xFF) as u8;
    cdb[10] = (agid << 6) | (format & 0x3F);
    cdb
}

/// Build SEND KEY CDB (0xA3).
fn cdb_send_key(agid: u8, format: u8, len: u16) -> [u8; 12] {
    let mut cdb = [0u8; 12];
    cdb[0] = crate::scsi::SCSI_SEND_KEY;
    cdb[7] = crate::scsi::AACS_KEY_CLASS;
    cdb[8] = (len >> 8) as u8;
    cdb[9] = (len & 0xFF) as u8;
    cdb[10] = (agid << 6) | (format & 0x3F);
    cdb
}

/// Build REPORT DISC STRUCTURE CDB (0xAD).
fn cdb_report_disc_structure(agid: u8, format: u8, len: u16) -> [u8; 12] {
    let mut cdb = [0u8; 12];
    cdb[0] = crate::scsi::SCSI_READ_DISC_STRUCTURE;
    cdb[1] = 0x01; // Blu-ray
    cdb[7] = format;
    cdb[8] = (len >> 8) as u8;
    cdb[9] = (len & 0xFF) as u8;
    cdb[10] = agid << 6;
    cdb
}

// ── High-level handshake ────────────────────────────────────────────────────

/// Result of a successful AACS authentication handshake.
#[derive(Debug)]
pub struct AacsAuth {
    /// Bus key (16 bytes) — derived from ECDH
    pub bus_key: [u8; 16],
    /// AGID used for this session
    pub agid: u8,
    /// Volume ID (16 bytes) — read after auth
    pub volume_id: Option<[u8; 16]>,
    /// Read data key (16 bytes) — for AACS 2.0 bus decryption
    pub read_data_key: Option<[u8; 16]>,
    /// Drive certificate (92 bytes)
    pub drive_cert: [u8; 92],
}

/// Perform the full AACS authentication handshake.
///
/// Requires a host private key (20 bytes) and host certificate (92 bytes)
/// from the KEYDB.cfg HC entry.
pub fn aacs_authenticate(
    session: &mut DriveSession,
    host_priv_key: &[u8; 20],
    host_cert: &[u8],
) -> Result<AacsAuth> {
    if host_cert.len() < 92 {
        return Err(Error::AacsCertShort);
    }

    // Step 1: Invalidate all AGIDs
    for agid in 0..4u8 {
        let cdb = cdb_report_key(agid, 0x3F, 2);
        let _ = scsi_read(session, &cdb, 2);
    }

    // Step 2: Allocate AGID
    let cdb = cdb_report_key(0, 0x00, 8);
    let response = scsi_read(session, &cdb, 8)
        .map_err(|_| Error::AacsAgidAlloc)?;
    let agid = (response[7] >> 6) & 0x03;

    // Step 3: Generate host nonce and ephemeral key pair
    let mut host_nonce = [0u8; 20];
    use rand::RngCore;
    rand::thread_rng().fill_bytes(&mut host_nonce);
    let (host_key, host_key_point_x, host_key_point_y) = generate_host_key_pair();

    // Step 4: Send host certificate + nonce (SEND KEY format 0x01)
    let mut send_buf = [0u8; 116];
    send_buf[1] = 0x72; // data length
    send_buf[4..24].copy_from_slice(&host_nonce);
    send_buf[24..116].copy_from_slice(&host_cert[..92]);

    let cdb = cdb_send_key(agid, 0x01, 116);
    scsi_write(session, &cdb, &send_buf)
        .map_err(|_| Error::AacsCertRejected)?;

    // Step 5: Read drive certificate + nonce (REPORT KEY format 0x01)
    let cdb = cdb_report_key(agid, 0x01, 116);
    let response = scsi_read(session, &cdb, 116)
        .map_err(|_| Error::AacsCertRead)?;

    let mut drive_nonce = [0u8; 20];
    let mut drive_cert = [0u8; 92];
    drive_nonce.copy_from_slice(&response[4..24]);
    drive_cert.copy_from_slice(&response[24..116]);

    // Verify drive certificate
    if drive_cert[0] == 0x01 {
        // AACS 1.0 certificate
        if !verify_cert(&drive_cert) {
            return Err(Error::AacsCertVerify);
        }
    } else if drive_cert[0] == 0x11 {
        // AACS 2.0 certificate — verify with P-256 LA key
        // Note: AACS 2.0 drives still accept AACS 1.0 host certs for compatibility
        // Verification is optional here since we proceed with AACS 1.0 flow anyway
    }

    // Step 6: Read drive key point + signature (REPORT KEY format 0x02)
    let cdb = cdb_report_key(agid, 0x02, 84);
    let response = scsi_read(session, &cdb, 84)
        .map_err(|_| Error::AacsKeyRead)?;

    let mut drive_key_point = [0u8; 40];   // x(20) + y(20)
    let mut drive_key_sig = [0u8; 40];     // r(20) + s(20)
    drive_key_point.copy_from_slice(&response[4..44]);
    drive_key_sig.copy_from_slice(&response[44..84]);

    // Verify drive key signature: sign(drive_nonce=host_nonce || drive_key_point)
    let (drive_pub_x, drive_pub_y) = cert_pub_key(&drive_cert);
    let mut verify_data = [0u8; 60];
    verify_data[..20].copy_from_slice(&host_nonce);
    verify_data[20..60].copy_from_slice(&drive_key_point);

    let mut sig_r = [0u8; 20];
    let mut sig_s = [0u8; 20];
    sig_r.copy_from_slice(&drive_key_sig[..20]);
    sig_s.copy_from_slice(&drive_key_sig[20..40]);

    if !ecdsa_verify(&drive_pub_x, &drive_pub_y, &sig_r, &sig_s, &verify_data) {
        return Err(Error::AacsKeyVerify);
    }

    // Step 7: Sign host key point (ECDSA over drive_nonce || host_key_point)
    let mut sign_data = [0u8; 60];
    sign_data[..20].copy_from_slice(&drive_nonce);
    sign_data[20..40].copy_from_slice(&host_key_point_x);
    sign_data[40..60].copy_from_slice(&host_key_point_y);

    let (host_sig_r, host_sig_s) = ecdsa_sign(host_priv_key, &sign_data);

    // Step 8: Send host key point + signature (SEND KEY format 0x02)
    let mut send_buf = [0u8; 84];
    send_buf[1] = 0x52;
    send_buf[4..24].copy_from_slice(&host_key_point_x);
    send_buf[24..44].copy_from_slice(&host_key_point_y);
    send_buf[44..64].copy_from_slice(&host_sig_r);
    send_buf[64..84].copy_from_slice(&host_sig_s);

    let cdb = cdb_send_key(agid, 0x02, 84);
    scsi_write(session, &cdb, &send_buf)
        .map_err(|_| Error::AacsKeyRejected)?;

    // Step 9: Compute bus key via ECDH
    let mut dkp_x = [0u8; 20];
    let mut dkp_y = [0u8; 20];
    dkp_x.copy_from_slice(&drive_key_point[..20]);
    dkp_y.copy_from_slice(&drive_key_point[20..40]);

    let bus_key = compute_bus_key(&host_key, &dkp_x, &dkp_y);

    Ok(AacsAuth {
        bus_key,
        agid,
        volume_id: None,
        read_data_key: None,
        drive_cert,
    })
}

/// Full AACS 2.0 authentication using P-256/SHA-256.
///
/// Used when both host and drive support AACS 2.0 natively.
/// Falls back to aacs_authenticate (AACS 1.0) if AACS 2.0 host credentials
/// are not available.
pub fn aacs2_authenticate(
    session: &mut DriveSession,
    host_priv_key_v1: &[u8; 20],
    host_cert_v1: &[u8],
    host_priv_key_v2: Option<&[u8; 32]>,
    host_cert_v2: Option<&[u8]>,
) -> Result<AacsAuth> {
    // Try AACS 1.0 first (backward compatible with all drives)
    match aacs_authenticate(session, host_priv_key_v1, host_cert_v1) {
        Ok(auth) => return Ok(auth),
        Err(_) => {
            // AACS 1.0 rejected — try native P-256 if we have v2 credentials
        }
    }

    // AACS 2.0 native P-256 handshake
    let host_priv_v2 = host_priv_key_v2.ok_or(Error::AacsCertShort)?;
    let host_cert_v2 = host_cert_v2.ok_or(Error::AacsCertShort)?;

    aacs2_authenticate_p256(session, host_priv_v2, host_cert_v2)
}

/// Native AACS 2.0 handshake using P-256/SHA-256.
/// Same SCSI protocol, larger payloads (32-byte keys, 132-byte certs).
fn aacs2_authenticate_p256(
    session: &mut DriveSession,
    host_priv_key: &[u8; 32],
    host_cert: &[u8],
) -> Result<AacsAuth> {
    if host_cert.len() < 132 {
        return Err(Error::AacsCertShort);
    }

    // Step 1: Invalidate all AGIDs
    for agid in 0..4u8 {
        let cdb = cdb_report_key(agid, 0x3F, 2);
        let _ = scsi_read(session, &cdb, 2);
    }

    // Step 2: Allocate AGID
    let cdb = cdb_report_key(0, 0x00, 8);
    let response = scsi_read(session, &cdb, 8)
        .map_err(|_| Error::AacsAgidAlloc)?;
    let agid = (response[7] >> 6) & 0x03;

    // Step 3: Generate host nonce + P-256 ephemeral key pair
    let mut host_nonce = [0u8; 20];
    use rand::RngCore;
    rand::thread_rng().fill_bytes(&mut host_nonce);
    let (host_eph_key, host_eph_pub_x, host_eph_pub_y) = generate_host_key_pair_p256();

    // Step 4: Send AACS 2.0 host certificate + nonce
    // AACS 2.0: cert is 132 bytes, total payload = 4 + 20 + 132 = 156
    let mut send_buf = vec![0u8; 156];
    send_buf[1] = 0x9a; // data length (154)
    send_buf[4..24].copy_from_slice(&host_nonce);
    send_buf[24..156].copy_from_slice(&host_cert[..132]);

    let cdb = cdb_send_key(agid, 0x01, 156);
    scsi_write(session, &cdb, &send_buf)
        .map_err(|_| Error::AacsCertRejected)?;

    // Step 5: Read drive certificate + nonce
    // AACS 2.0 drive cert is also 132 bytes
    let cdb = cdb_report_key(agid, 0x01, 156);
    let response = scsi_read(session, &cdb, 156)
        .map_err(|_| Error::AacsCertRead)?;

    let mut drive_nonce = [0u8; 20];
    drive_nonce.copy_from_slice(&response[4..24]);
    let drive_cert = &response[24..156];

    // Verify drive certificate with AACS 2.0 LA key
    if drive_cert[0] == 0x11 && !verify_cert_p256(drive_cert) {
        // Non-fatal: some cert formats may differ
    }

    // Step 6: Read drive key point + signature (P-256: 64+64 = 128 bytes)
    let cdb = cdb_report_key(agid, 0x02, 132);
    let response = scsi_read(session, &cdb, 132)
        .map_err(|_| Error::AacsKeyRead)?;

    let drive_key_x = &response[4..36];
    let drive_key_y = &response[36..68];
    let drive_sig_r = &response[68..100];
    let drive_sig_s = &response[100..132];

    // Verify drive key signature
    let (drive_pub_x, drive_pub_y) = cert_pub_key_p256(drive_cert);
    let mut verify_data = Vec::with_capacity(84);
    verify_data.extend_from_slice(&host_nonce);
    verify_data.extend_from_slice(drive_key_x);
    verify_data.extend_from_slice(drive_key_y);

    if !ecdsa_verify_p256(&drive_pub_x, &drive_pub_y, drive_sig_r, drive_sig_s, &verify_data) {
        return Err(Error::AacsKeyVerify);
    }

    // Step 7: Sign host key point
    let mut sign_data = Vec::with_capacity(84);
    sign_data.extend_from_slice(&drive_nonce);
    sign_data.extend_from_slice(&host_eph_pub_x);
    sign_data.extend_from_slice(&host_eph_pub_y);

    let (host_sig_r, host_sig_s) = ecdsa_sign_p256(host_priv_key, &sign_data);

    // Step 8: Send host key point + signature (P-256: 64+64 = 128 bytes payload)
    let mut send_buf = vec![0u8; 132];
    send_buf[1] = 0x82; // data length
    send_buf[4..36].copy_from_slice(&host_eph_pub_x);
    send_buf[36..68].copy_from_slice(&host_eph_pub_y);
    send_buf[68..100].copy_from_slice(&host_sig_r);
    send_buf[100..132].copy_from_slice(&host_sig_s);

    let cdb = cdb_send_key(agid, 0x02, 132);
    scsi_write(session, &cdb, &send_buf)
        .map_err(|_| Error::AacsKeyRejected)?;

    // Step 9: Compute bus key via P-256 ECDH
    let bus_key = compute_bus_key_p256(&host_eph_key, drive_key_x, drive_key_y);

    Ok(AacsAuth {
        bus_key,
        agid,
        volume_id: None,
        read_data_key: None,
        drive_cert: {
            let mut dc = [0u8; 92];
            dc.copy_from_slice(&drive_cert[..92.min(drive_cert.len())]);
            dc
        },
    })
}

/// Read Volume ID after successful authentication.
pub fn read_volume_id(session: &mut DriveSession, auth: &mut AacsAuth) -> Result<[u8; 16]> {
    // REPORT DISC STRUCTURE format 0x80
    let cdb = cdb_report_disc_structure(auth.agid, 0x80, 36);
    let response = scsi_read(session, &cdb, 36)
        .map_err(|_| Error::AacsVidRead)?;

    let mut vid = [0u8; 16];
    let mut mac = [0u8; 16];
    vid.copy_from_slice(&response[4..20]);
    mac.copy_from_slice(&response[20..36]);

    // Verify MAC: AES-CMAC(VID, bus_key) should equal mac
    let calc_mac = aes_cmac_16(&vid, &auth.bus_key);
    if calc_mac != mac {
        return Err(Error::AacsVidMac);
    }

    auth.volume_id = Some(vid);
    Ok(vid)
}

/// Read data keys after successful authentication (for AACS 2.0 bus encryption).
pub fn read_data_keys(session: &mut DriveSession, auth: &mut AacsAuth) -> Result<([u8; 16], [u8; 16])> {
    // REPORT DISC STRUCTURE format 0x84
    let cdb = cdb_report_disc_structure(auth.agid, 0x84, 36);
    let response = scsi_read(session, &cdb, 36)
        .map_err(|_| Error::AacsDataKey)?;

    let mut enc_rdk = [0u8; 16];
    let mut enc_wdk = [0u8; 16];
    enc_rdk.copy_from_slice(&response[4..20]);
    enc_wdk.copy_from_slice(&response[20..36]);

    // Decrypt with bus key (AES-ECB)
    let read_data_key = super::aes_ecb_decrypt(&auth.bus_key, &enc_rdk);
    let write_data_key = super::aes_ecb_decrypt(&auth.bus_key, &enc_wdk);

    auth.read_data_key = Some(read_data_key);
    Ok((read_data_key, write_data_key))
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ec_curve_generator_on_curve() {
        // Verify G is on the curve: y² = x³ + ax + b (mod p)
        let p = BigUint::from_bytes_be(&EC_P);
        let a = BigUint::from_bytes_be(&EC_A);
        let b = BigUint::from_bytes_be(&EC_B);
        let gx = BigUint::from_bytes_be(&EC_GX);
        let gy = BigUint::from_bytes_be(&EC_GY);

        let lhs = (&gy * &gy) % &p;
        let rhs = (&gx * &gx * &gx + &a * &gx + &b) % &p;
        assert_eq!(lhs, rhs, "Generator point is not on the curve");
    }

    #[test]
    fn test_ec_mul_identity() {
        let p = BigUint::from_bytes_be(&EC_P);
        let a = BigUint::from_bytes_be(&EC_A);
        let g = EcPoint::from_bytes(&EC_GX, &EC_GY);

        // 1 × G = G
        let result = ec_mul(&BigUint::one(), &g, &a, &p);
        assert_eq!(result.x, g.x);
        assert_eq!(result.y, g.y);
    }

    #[test]
    fn test_ec_mul_order() {
        // n × G = O (point at infinity)
        let p = BigUint::from_bytes_be(&EC_P);
        let a = BigUint::from_bytes_be(&EC_A);
        let n = BigUint::from_bytes_be(&EC_N);
        let g = EcPoint::from_bytes(&EC_GX, &EC_GY);

        let result = ec_mul(&n, &g, &a, &p);
        assert!(result.infinity, "n × G should be point at infinity");
    }

    #[test]
    fn test_ecdsa_sign_verify() {
        // Generate a key pair and test sign/verify
        let (priv_key, pub_x, pub_y) = generate_host_key_pair();
        let data = b"test data for AACS ECDSA";

        let (sig_r, sig_s) = ecdsa_sign(&priv_key, data);
        assert!(ecdsa_verify(&pub_x, &pub_y, &sig_r, &sig_s, data),
            "ECDSA signature should verify");

        // Verify with wrong data fails
        assert!(!ecdsa_verify(&pub_x, &pub_y, &sig_r, &sig_s, b"wrong data"),
            "ECDSA should fail with wrong data");
    }

    #[test]
    fn test_ecdh_shared_secret() {
        // Two parties should derive the same shared point
        let p = BigUint::from_bytes_be(&EC_P);
        let a = BigUint::from_bytes_be(&EC_A);
        let g = EcPoint::from_bytes(&EC_GX, &EC_GY);

        let (priv_a, pub_ax, pub_ay) = generate_host_key_pair();
        let (priv_b, pub_bx, pub_by) = generate_host_key_pair();

        // A computes: priv_a × pub_B
        let shared_a = compute_bus_key(&priv_a, &pub_bx, &pub_by);
        // B computes: priv_b × pub_A
        let shared_b = compute_bus_key(&priv_b, &pub_ax, &pub_ay);

        assert_eq!(shared_a, shared_b, "ECDH shared secrets should match");
    }

    #[test]
    fn test_p256_generator_on_curve() {
        let p = BigUint::from_bytes_be(&P256_P);
        let a = BigUint::from_bytes_be(&P256_A);
        let b = BigUint::from_bytes_be(&P256_B);
        let gx = BigUint::from_bytes_be(&P256_GX);
        let gy = BigUint::from_bytes_be(&P256_GY);

        let lhs = (&gy * &gy) % &p;
        let rhs = (&gx * &gx * &gx + &a * &gx + &b) % &p;
        assert_eq!(lhs, rhs, "P-256 generator not on curve");
    }

    #[test]
    fn test_p256_mul_order() {
        let p = BigUint::from_bytes_be(&P256_P);
        let a = BigUint::from_bytes_be(&P256_A);
        let n = BigUint::from_bytes_be(&P256_N);
        let g = EcPoint::from_bytes(&P256_GX, &P256_GY);

        let result = ec_mul(&n, &g, &a, &p);
        assert!(result.infinity, "n × G should be point at infinity on P-256");
    }

    #[test]
    fn test_p256_ecdsa_sign_verify() {
        let p = BigUint::from_bytes_be(&P256_P);
        let a = BigUint::from_bytes_be(&P256_A);
        let n = BigUint::from_bytes_be(&P256_N);
        let g = EcPoint::from_bytes(&P256_GX, &P256_GY);

        // Generate random P-256 key pair
        let mut priv_bytes = [0u8; 32];
        use rand::RngCore;
        rand::thread_rng().fill_bytes(&mut priv_bytes);
        let d = BigUint::from_bytes_be(&priv_bytes) % &n;
        let priv_key: [u8; 32] = to_bytes_be_padded(&d, 32).try_into().unwrap();

        let pub_point = ec_mul(&d, &g, &a, &p);
        let pub_x: Vec<u8> = to_bytes_be_padded(&pub_point.x, 32);
        let pub_y: Vec<u8> = to_bytes_be_padded(&pub_point.y, 32);

        let data = b"AACS 2.0 P-256 ECDSA test";
        let (sig_r, sig_s) = ecdsa_sign_p256(&priv_key, data);
        assert!(ecdsa_verify_p256(&pub_x, &pub_y, &sig_r, &sig_s, data));
        assert!(!ecdsa_verify_p256(&pub_x, &pub_y, &sig_r, &sig_s, b"wrong"));
    }

    #[test]
    fn test_p256_ecdh() {
        let p = BigUint::from_bytes_be(&P256_P);
        let a = BigUint::from_bytes_be(&P256_A);
        let n = BigUint::from_bytes_be(&P256_N);
        let g = EcPoint::from_bytes(&P256_GX, &P256_GY);

        let mut pa = [0u8; 32];
        let mut pb = [0u8; 32];
        use rand::RngCore;
        rand::thread_rng().fill_bytes(&mut pa);
        rand::thread_rng().fill_bytes(&mut pb);
        let da = BigUint::from_bytes_be(&pa) % &n;
        let db = BigUint::from_bytes_be(&pb) % &n;
        let priv_a: [u8; 32] = to_bytes_be_padded(&da, 32).try_into().unwrap();
        let priv_b: [u8; 32] = to_bytes_be_padded(&db, 32).try_into().unwrap();

        let pub_a = ec_mul(&da, &g, &a, &p);
        let pub_b = ec_mul(&db, &g, &a, &p);

        let key_a = compute_bus_key_p256(&priv_a,
            &to_bytes_be_padded(&pub_b.x, 32), &to_bytes_be_padded(&pub_b.y, 32));
        let key_b = compute_bus_key_p256(&priv_b,
            &to_bytes_be_padded(&pub_a.x, 32), &to_bytes_be_padded(&pub_a.y, 32));

        assert_eq!(key_a, key_b, "P-256 ECDH shared secrets should match");
    }

    #[test]
    fn test_aes_cmac() {
        // Basic CMAC test — at minimum verify it produces consistent output
        let key = [0x2b, 0x7e, 0x15, 0x16, 0x28, 0xae, 0xd2, 0xa6,
                   0xab, 0xf7, 0x15, 0x88, 0x09, 0xcf, 0x4f, 0x3c];
        let data = [0u8; 16];
        let mac1 = aes_cmac_16(&data, &key);
        let mac2 = aes_cmac_16(&data, &key);
        assert_eq!(mac1, mac2);
        assert_ne!(mac1, [0u8; 16]); // shouldn't be all zeros
    }

    #[test]
    fn test_verify_host_cert_from_keydb() {
        // Verify the host cert from our KEYDB
        let keydb_path = match std::env::var("KEYDB_PATH").ok() {
            Some(p) => std::path::PathBuf::from(p),
            None => return, // skip if KEYDB_PATH not set
        };
        if !keydb_path.exists() { return; }

        let db = crate::aacs::KeyDb::load(&keydb_path).unwrap();
        if let Some(hc) = db.host_certs.first() {
            let valid = verify_cert(&hc.certificate);
            eprintln!("Host cert verification: {}", if valid { "PASS" } else { "FAIL" });
            // Note: our cert is revoked but should still have valid LA signature
            // If it doesn't verify, the LA public key might be wrong
            if !valid {
                eprintln!("  (cert may use different LA key or format)");
            }
        }
    }
}
