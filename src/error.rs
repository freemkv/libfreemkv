//! Error types for libfreemkv.
//!
//! Every error is a code with structured data. No English text.
//! Applications map codes to localized messages.
//!
//! # Error Code Ranges
//!
//! | Range | Category |
//! |-------|----------|
//! | E1xxx | Device errors |
//! | E2xxx | Profile errors |
//! | E3xxx | Unlock errors |
//! | E4xxx | SCSI errors |
//! | E5xxx | I/O errors |
//! | E6xxx | Disc format errors |
//! | E7xxx | AACS errors |
//! | E8xxx | Keydb errors |
//! | E9xxx | Mux errors |

// ── Error codes ─────────────────────────────────────────────────────────────

pub const E_DEVICE_NOT_FOUND: u16 = 1000;
pub const E_DEVICE_PERMISSION: u16 = 1001;
pub const E_UNSUPPORTED_DRIVE: u16 = 2000;
pub const E_PROFILE_NOT_FOUND: u16 = 2001;
pub const E_PROFILE_PARSE: u16 = 2002;
pub const E_UNLOCK_FAILED: u16 = 3000;
pub const E_SIGNATURE_MISMATCH: u16 = 3001;
pub const E_NOT_UNLOCKED: u16 = 3002;
pub const E_NOT_CALIBRATED: u16 = 3003;
pub const E_SCSI_ERROR: u16 = 4000;
pub const E_SCSI_TIMEOUT: u16 = 4001;
pub const E_IO_ERROR: u16 = 5000;
pub const E_WRITE_ERROR: u16 = 5001;
// Disc format (6xxx)
pub const E_DISC_READ: u16 = 6000;
pub const E_MPLS_PARSE: u16 = 6001;
pub const E_CLPI_PARSE: u16 = 6002;
pub const E_UDF_NOT_FOUND: u16 = 6003;
pub const E_DISC_NO_TITLES: u16 = 6004;
pub const E_DISC_TITLE_RANGE: u16 = 6005;
pub const E_DISC_NO_EXTENTS: u16 = 6006;
pub const E_IFO_PARSE: u16 = 6007;
// AACS (7xxx)
pub const E_AACS_NO_KEYS: u16 = 7000;
pub const E_AACS_CERT_SHORT: u16 = 7001;
pub const E_AACS_AGID_ALLOC: u16 = 7002;
pub const E_AACS_CERT_REJECTED: u16 = 7003;
pub const E_AACS_CERT_READ: u16 = 7004;
pub const E_AACS_CERT_VERIFY: u16 = 7005;
pub const E_AACS_KEY_READ: u16 = 7006;
pub const E_AACS_KEY_REJECTED: u16 = 7007;
pub const E_AACS_KEY_VERIFY: u16 = 7008;
pub const E_AACS_VID_READ: u16 = 7009;
pub const E_AACS_VID_MAC: u16 = 7010;
pub const E_AACS_DATA_KEY: u16 = 7011;
pub const E_AACS_VUK_DERIVE: u16 = 7012;
// Keydb (8xxx)
pub const E_KEYDB_CONNECT: u16 = 8000;
pub const E_KEYDB_HTTP: u16 = 8001;
pub const E_KEYDB_INVALID: u16 = 8002;
pub const E_KEYDB_WRITE: u16 = 8003;
pub const E_KEYDB_PARSE: u16 = 8004;
pub const E_KEYDB_LOAD: u16 = 8005;
// Mux (9xxx)
pub const E_MUX_LOOKAHEAD: u16 = 9000;
pub const E_MUX_WRITE: u16 = 9001;

// ── Error enum ──────────────────────────────────────────────────────────────

/// Structured error with numeric code and context data. No English text.
#[derive(Debug)]
pub enum Error {
    // Device
    DeviceNotFound {
        path: String,
    },
    DevicePermission {
        path: String,
    },

    // Profile
    UnsupportedDrive {
        vendor_id: String,
        product_id: String,
        product_revision: String,
    },
    ProfileNotFound {
        vendor_id: String,
        product_revision: String,
        vendor_specific: String,
    },
    ProfileParse,

    // Unlock
    UnlockFailed,
    SignatureMismatch {
        expected: [u8; 4],
        got: [u8; 4],
    },
    NotUnlocked,
    NotCalibrated,

    // SCSI
    ScsiError {
        opcode: u8,
        status: u8,
        sense_key: u8,
    },
    ScsiTimeout {
        opcode: u8,
    },

    // I/O
    IoError {
        source: std::io::Error,
    },
    WriteError,

    // Disc format
    DiscRead {
        sector: u64,
    },
    MplsParse,
    ClpiParse,
    UdfNotFound {
        path: String,
    },
    DiscNoTitles,
    DiscTitleRange {
        index: usize,
        count: usize,
    },
    DiscNoExtents,
    IfoParse,

    // AACS
    AacsNoKeys,
    AacsCertShort,
    AacsAgidAlloc,
    AacsCertRejected,
    AacsCertRead,
    AacsCertVerify,
    AacsKeyRead,
    AacsKeyRejected,
    AacsKeyVerify,
    AacsVidRead,
    AacsVidMac,
    AacsDataKey,
    AacsVukDerive,

    // Keydb
    KeydbConnect {
        host: String,
    },
    KeydbHttp {
        status: u16,
    },
    KeydbInvalid,
    KeydbWrite {
        path: String,
    },
    KeydbParse,
    KeydbLoad {
        path: String,
    },

    // Mux
    MuxLookahead,
    MuxWrite,
}

impl Error {
    pub fn code(&self) -> u16 {
        match self {
            Error::DeviceNotFound { .. } => E_DEVICE_NOT_FOUND,
            Error::DevicePermission { .. } => E_DEVICE_PERMISSION,
            Error::UnsupportedDrive { .. } => E_UNSUPPORTED_DRIVE,
            Error::ProfileNotFound { .. } => E_PROFILE_NOT_FOUND,
            Error::ProfileParse => E_PROFILE_PARSE,
            Error::UnlockFailed => E_UNLOCK_FAILED,
            Error::SignatureMismatch { .. } => E_SIGNATURE_MISMATCH,
            Error::NotUnlocked => E_NOT_UNLOCKED,
            Error::NotCalibrated => E_NOT_CALIBRATED,
            Error::ScsiError { .. } => E_SCSI_ERROR,
            Error::ScsiTimeout { .. } => E_SCSI_TIMEOUT,
            Error::IoError { .. } => E_IO_ERROR,
            Error::WriteError => E_WRITE_ERROR,
            Error::DiscRead { .. } => E_DISC_READ,
            Error::MplsParse => E_MPLS_PARSE,
            Error::ClpiParse => E_CLPI_PARSE,
            Error::UdfNotFound { .. } => E_UDF_NOT_FOUND,
            Error::DiscNoTitles => E_DISC_NO_TITLES,
            Error::DiscTitleRange { .. } => E_DISC_TITLE_RANGE,
            Error::DiscNoExtents => E_DISC_NO_EXTENTS,
            Error::IfoParse => E_IFO_PARSE,
            Error::AacsNoKeys => E_AACS_NO_KEYS,
            Error::AacsCertShort => E_AACS_CERT_SHORT,
            Error::AacsAgidAlloc => E_AACS_AGID_ALLOC,
            Error::AacsCertRejected => E_AACS_CERT_REJECTED,
            Error::AacsCertRead => E_AACS_CERT_READ,
            Error::AacsCertVerify => E_AACS_CERT_VERIFY,
            Error::AacsKeyRead => E_AACS_KEY_READ,
            Error::AacsKeyRejected => E_AACS_KEY_REJECTED,
            Error::AacsKeyVerify => E_AACS_KEY_VERIFY,
            Error::AacsVidRead => E_AACS_VID_READ,
            Error::AacsVidMac => E_AACS_VID_MAC,
            Error::AacsDataKey => E_AACS_DATA_KEY,
            Error::AacsVukDerive => E_AACS_VUK_DERIVE,
            Error::KeydbConnect { .. } => E_KEYDB_CONNECT,
            Error::KeydbHttp { .. } => E_KEYDB_HTTP,
            Error::KeydbInvalid => E_KEYDB_INVALID,
            Error::KeydbWrite { .. } => E_KEYDB_WRITE,
            Error::KeydbParse => E_KEYDB_PARSE,
            Error::KeydbLoad { .. } => E_KEYDB_LOAD,
            Error::MuxLookahead => E_MUX_LOOKAHEAD,
            Error::MuxWrite => E_MUX_WRITE,
        }
    }
}

/// Display: "E{code}" with structured data. No English words.
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::DeviceNotFound { path } => write!(f, "E{}: {}", self.code(), path),
            Error::DevicePermission { path } => write!(f, "E{}: {}", self.code(), path),
            Error::UnsupportedDrive {
                vendor_id,
                product_id,
                product_revision,
            } => write!(
                f,
                "E{}: {} {} {}",
                self.code(),
                vendor_id.trim(),
                product_id.trim(),
                product_revision.trim()
            ),
            Error::ProfileNotFound {
                vendor_id,
                product_revision,
                vendor_specific,
            } => write!(
                f,
                "E{}: {} {} {}",
                self.code(),
                vendor_id.trim(),
                product_revision.trim(),
                vendor_specific.trim()
            ),
            Error::SignatureMismatch { expected, got } => write!(
                f,
                "E{}: {:02x}{:02x}{:02x}{:02x}!={:02x}{:02x}{:02x}{:02x}",
                self.code(),
                expected[0],
                expected[1],
                expected[2],
                expected[3],
                got[0],
                got[1],
                got[2],
                got[3]
            ),
            Error::ScsiError {
                opcode,
                status,
                sense_key,
            } => write!(
                f,
                "E{}: 0x{:02x}/0x{:02x}/0x{:02x}",
                self.code(),
                opcode,
                status,
                sense_key
            ),
            Error::ScsiTimeout { opcode } => write!(f, "E{}: 0x{:02x}", self.code(), opcode),
            Error::IoError { source } => write!(f, "E{}: {}", self.code(), source),
            Error::DiscRead { sector } => write!(f, "E{}: {}", self.code(), sector),
            Error::UdfNotFound { path } => write!(f, "E{}: {}", self.code(), path),
            Error::DiscTitleRange { index, count } => {
                write!(f, "E{}: {}/{}", self.code(), index, count)
            }
            Error::KeydbConnect { host } => write!(f, "E{}: {}", self.code(), host),
            Error::KeydbHttp { status } => write!(f, "E{}: {}", self.code(), status),
            Error::KeydbWrite { path } => write!(f, "E{}: {}", self.code(), path),
            Error::KeydbLoad { path } => write!(f, "E{}: {}", self.code(), path),
            // Simple codes — no extra data
            _ => write!(f, "E{}", self.code()),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::IoError { source } => Some(source),
            _ => None,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::IoError { source: e }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
