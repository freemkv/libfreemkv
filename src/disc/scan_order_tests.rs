//! Live-scan command order: UDF and the AACS files are read before any AACS
//! CDB, and the handshake's failures land where they belong.

use super::*;
use crate::scsi::{DataDirection, ScsiResult, ScsiSense};
use crate::udf::fixture::*;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

// A real AACS1 host keypair (pubkey = d·G on the AACS1 curve); T0 guards it.
fn test_hc() -> crate::aacs::types::HostCert {
    let mut cert = vec![0u8; 92];
    cert[0] = 0x02;
    cert[3] = 92;
    cert[4..10].copy_from_slice(&[0xF7, 0xEE, 0x00, 0x00, 0x00, 0x01]);
    cert[12..32].copy_from_slice(&hex20("603c6514caab3f999bdd5790624434741fb875cf"));
    cert[32..52].copy_from_slice(&hex20("1cc0b11e2c894c62ba253f7e268eac2ca9a1f30b"));
    crate::aacs::types::HostCert {
        private_key: *b"freemkv-test-hc-key!",
        certificate: cert,
        private_key_v2: None,
        certificate_v2: None,
    }
}

fn hex20(s: &str) -> [u8; 20] {
    let mut out = [0u8; 20];
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).unwrap();
    }
    out
}

type Log = Arc<Mutex<Vec<Vec<u8>>>>;
type Pred = fn(&[u8]) -> bool;
type ReadFail = Box<dyn Fn(u32, u16, &[Vec<u8>]) -> Option<Error> + Send>;
type HaltOn = Box<dyn Fn(&[u8]) -> bool + Send>;

fn is_aacs_cdb(c: &[u8]) -> bool {
    matches!(c[0], 0xA3 | 0xA4 | 0xAD)
}
fn is_read10(c: &[u8]) -> bool {
    c[0] == crate::scsi::SCSI_READ_10
}
fn is_aacs_agid_alloc(c: &[u8]) -> bool {
    c[0] == 0xA4 && c[7] == 0x02 && c[10] & 0x3F == 0x00
}
fn is_send_host_cert(c: &[u8]) -> bool {
    c[0] == 0xA3 && c[10] & 0x3F == 0x01
}
fn is_css_agid_alloc(c: &[u8]) -> bool {
    c[0] == 0xA4 && c[7] == 0x00 && c[10] & 0x3F == 0x00
}
fn is_drive_mkb_read(c: &[u8]) -> bool {
    c[0] == 0xAD && c[7] == 0x83
}
fn read_range(c: &[u8]) -> (u32, u32) {
    let lba = u32::from_be_bytes([c[2], c[3], c[4], c[5]]);
    (lba, u16::from_be_bytes([c[7], c[8]]) as u32)
}
fn check(sense_key: u8, asc: u8, ascq: u8, opcode: u8) -> Error {
    Error::ScsiError {
        opcode,
        status: 2,
        sense: Some(ScsiSense {
            sense_key,
            asc,
            ascq,
        }),
    }
}
fn transport_fault(opcode: u8) -> Error {
    Error::ScsiError {
        opcode,
        status: crate::scsi::SCSI_STATUS_TRANSPORT_FAILURE,
        sense: None,
    }
}

// A CDB-logging drive over a MemDisc: SEND KEY host cert → 05/6F/00, so the AKE
// really issues REPORT/SEND KEY but can never complete.
struct AkeMemTransport {
    mem: MemDisc,
    log: Log,
    profile: u16,
    fail_read: Option<ReadFail>,
    fault_on: Option<Pred>,
    halt_on: Option<HaltOn>,
    halt: Arc<Mutex<Option<Arc<AtomicBool>>>>,
    mkb_pack: Option<Vec<u8>>,
}

impl crate::scsi::ScsiTransport for AkeMemTransport {
    fn execute(
        &mut self,
        cdb: &[u8],
        _dir: DataDirection,
        data: &mut [u8],
        _timeout_ms: u32,
    ) -> Result<ScsiResult> {
        let history = self.log.lock().unwrap().clone();
        self.log.lock().unwrap().push(cdb.to_vec());
        if self.halt_on.as_ref().is_some_and(|p| p(cdb))
            && let Some(h) = self.halt.lock().unwrap().as_ref()
        {
            h.store(true, Ordering::Relaxed);
        }
        if self.fault_on.is_some_and(|p| p(cdb)) {
            return Err(transport_fault(cdb[0]));
        }
        data.fill(0);
        match cdb[0] {
            crate::scsi::SCSI_READ_10 => {
                let (lba, count) = read_range(cdb);
                if let Some(f) = &self.fail_read
                    && let Some(e) = f(lba, count as u16, &history)
                {
                    return Err(e);
                }
                self.mem.read_sectors(lba, count as u16, data, false)?;
            }
            crate::scsi::SCSI_READ_CAPACITY => {
                data[..4].copy_from_slice(&9_999u32.to_be_bytes());
                data[4..8].copy_from_slice(&2048u32.to_be_bytes());
            }
            0x46 if data.len() >= 8 => data[6..8].copy_from_slice(&self.profile.to_be_bytes()),
            0xA3 if is_send_host_cert(cdb) => return Err(check(5, 0x6F, 0, 0xA3)),
            0xAD if cdb[7] == 0x83 => {
                if let Some(m) = &self.mkb_pack {
                    data[..2].copy_from_slice(&((m.len() + 2) as u16).to_be_bytes());
                    data[3] = 1;
                    data[4..4 + m.len()].copy_from_slice(m);
                }
            }
            _ => {}
        }
        Ok(ScsiResult {
            status: 0,
            bytes_transferred: data.len(),
            sense: [0u8; 32],
        })
    }
}

fn mkb_gen(version: u32) -> Vec<u8> {
    let mut v = vec![0x10, 0x00, 0x00, 0x10, 0, 0, 0, 0];
    v.extend_from_slice(&version.to_be_bytes());
    v.extend_from_slice(&[0u8; 4]);
    v
}

fn cert(bee: bool) -> Vec<u8> {
    let mut c = vec![0u8; 20];
    c[0] = 0x10;
    c[1] = if bee { 0x80 } else { 0x00 };
    c
}

fn mpls_2h() -> Vec<u8> {
    let mut b = b"MPLS0200".to_vec();
    b.extend_from_slice(&40u32.to_be_bytes());
    b.extend_from_slice(&[0u8; 28]);
    let pl = b.len();
    b.extend_from_slice(&[0u8; 4]);
    b.extend_from_slice(&[0, 0, 0, 1, 0, 0]);
    let mut item = b"00001M2TS".to_vec();
    item.extend_from_slice(&[0, 0, 0]);
    item.extend_from_slice(&0u32.to_be_bytes());
    item.extend_from_slice(&(7200u32 * 45_000).to_be_bytes());
    item.extend_from_slice(&[0u8; 12]);
    item.extend_from_slice(&[0, 14, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
    b.extend_from_slice(&(item.len() as u16).to_be_bytes());
    b.extend_from_slice(&item);
    let len = (b.len() - pl - 4) as u32;
    b[pl..pl + 4].copy_from_slice(&len.to_be_bytes());
    let marks = b.len() as u32;
    b[12..16].copy_from_slice(&marks.to_be_bytes());
    b.extend_from_slice(&2u32.to_be_bytes());
    b.extend_from_slice(&0u16.to_be_bytes());
    b
}

fn clpi() -> Vec<u8> {
    let mut d = vec![0u8; 60];
    d[0..4].copy_from_slice(b"HDMV");
    d[4..8].copy_from_slice(b"0200");
    d[56..60].copy_from_slice(&4000u32.to_be_bytes());
    d
}

const UK_LBA: u32 = PART_START + 600;
const CERT_LBA: u32 = PART_START + 610;
const MKB_LBA: u32 = PART_START + 620;

// A BD with one 2 h title; `aacs` adds /AACS (Unit_Key_RO.inf, Content000.cer, MKB_RO.inf).
fn bd_disc(aacs: Option<bool>) -> MemDisc {
    let bdmv = DirSpec {
        name: "BDMV".into(),
        icb_lba: 22,
        dir_data_lba: 23,
        files: vec![file_with(
            "index.bdmv",
            43,
            630,
            b"INDX0200".to_vec(),
            false,
        )],
        subdirs: vec![
            DirSpec {
                name: "PLAYLIST".into(),
                icb_lba: 24,
                dir_data_lba: 25,
                files: vec![file_with("00800.mpls", 44, 700, mpls_2h(), false)],
                subdirs: vec![],
            },
            DirSpec {
                name: "CLIPINF".into(),
                icb_lba: 26,
                dir_data_lba: 27,
                files: vec![file_with("00001.clpi", 45, 720, clpi(), false)],
                subdirs: vec![],
            },
            DirSpec {
                name: "STREAM".into(),
                icb_lba: 28,
                dir_data_lba: 29,
                files: vec![file("00001.m2ts", 46, 5_000, 1_000 * 2048, true)],
                subdirs: vec![],
            },
        ],
    };
    let mut subdirs = vec![bdmv];
    if let Some(bee) = aacs {
        subdirs.push(DirSpec {
            name: "AACS".into(),
            icb_lba: 20,
            dir_data_lba: 21,
            files: vec![
                file_with("Unit_Key_RO.inf", 40, 600, vec![0xAB; 64], false),
                file_with("Content000.cer", 41, 610, cert(bee), false),
                file_with("MKB_RO.inf", 42, 620, mkb_gen(70), false),
            ],
            subdirs: vec![],
        });
    }
    let root = DirSpec {
        name: String::new(),
        icb_lba: 10,
        dir_data_lba: 11,
        files: Vec::new(),
        subdirs,
    };
    let mut mem = MemDisc::new();
    build_udf_skeleton(&mut mem, 10);
    lay_dir(&mut mem, &root);
    mem
}

// A drive over an unencrypted BD that scans successfully.
pub(crate) fn scannable_drive() -> Drive {
    Rig::new(bd_disc(None), |_| {}).drive
}

struct Rig {
    drive: Drive,
    log: Log,
}

impl Rig {
    fn new(mem: MemDisc, f: impl FnOnce(&mut AkeMemTransport)) -> Self {
        let log: Log = Arc::default();
        let halt: Arc<Mutex<Option<Arc<AtomicBool>>>> = Arc::default();
        let mut t = AkeMemTransport {
            mem,
            log: log.clone(),
            profile: 0x0040,
            fail_read: None,
            fault_on: None,
            halt_on: None,
            halt: halt.clone(),
            mkb_pack: Some(mkb_gen(70)),
        };
        f(&mut t);
        let drive = Drive::from_transport_for_test(Box::new(t));
        *halt.lock().unwrap() = Some(drive.halt_flag());
        Rig { drive, log }
    }
    fn cdbs(&self) -> Vec<Vec<u8>> {
        self.log.lock().unwrap().clone()
    }
    fn count(&self, p: Pred) -> usize {
        self.cdbs().iter().filter(|c| p(c)).count()
    }
}

fn with_hc() -> ScanOptions {
    ScanOptions {
        credentials: Some(crate::DriveCredentials {
            host_certs: vec![test_hc()],
        }),
        ..Default::default()
    }
}

fn touches(c: &[u8], lbas: &[u32]) -> bool {
    is_read10(c) && {
        let (lba, n) = read_range(c);
        lbas.iter().any(|x| (lba..lba + n).contains(x))
    }
}

// AACS-path WARN+ events (the drive's per-CDB read diagnostics are out of scope).
fn aacs_warns(ev: &[crate::testlog::CapturedEvent]) -> Vec<&crate::testlog::CapturedEvent> {
    ev.iter()
        .filter(|e| e.level <= tracing::Level::WARN && e.target != "freemkv::drive")
        .collect()
}

fn verdicts(ev: &[crate::testlog::CapturedEvent]) -> Vec<&crate::testlog::CapturedEvent> {
    ev.iter()
        .filter(|e| e.field("phase") == Some("aacs_verdict"))
        .collect()
}

const KEY_FILE_CODE: u16 = crate::error::E_AACS_KEY_FILE_UNREADABLE;

#[test]
fn test_hc_is_a_valid_aacs1_pairing() {
    let hc = test_hc();
    assert!(freemkv_unlock::aacs1_keypair_matches(
        &hc.private_key,
        &hc.certificate
    ));
}

#[test]
fn scan_reads_udf_and_aacs_files_before_any_aacs_scsi() {
    let mut rig = Rig::new(bd_disc(Some(true)), |_| {});
    let _ = Disc::scan(&mut rig.drive, &with_hc());
    let cdbs = rig.cdbs();
    let first_aacs = cdbs.iter().position(|c| is_aacs_cdb(c)).expect("AACS CDBs");
    let last_file = cdbs
        .iter()
        .rposition(|c| touches(c, &[UK_LBA, CERT_LBA, MKB_LBA]))
        .expect("AACS file reads");
    assert!(
        first_aacs > last_file,
        "first AACS CDB #{first_aacs} precedes the last AACS file read #{last_file}"
    );
    assert!(rig.count(is_aacs_agid_alloc) >= 1 && rig.count(is_send_host_cert) >= 1);
}

#[test]
fn unencrypted_bd_issues_no_aacs_scsi() {
    let mut rig = Rig::new(bd_disc(None), |_| {});
    let d = Disc::scan(&mut rig.drive, &with_hc()).expect("scan");
    assert!(d.aacs.is_none() && d.aacs_error.is_none());
    assert_eq!(rig.count(is_aacs_cdb), 0, "{:02x?}", rig.cdbs());
}

#[test]
fn live_unreadable_uk_ro_fails_scan_before_any_aacs_scsi() {
    for bee in [true, false] {
        // Both copies absent: the file is simply not on the disc.
        let mut mem = bd_disc(Some(bee));
        lay_dir(
            &mut mem,
            &DirSpec {
                name: "AACS".into(),
                icb_lba: 20,
                dir_data_lba: 21,
                files: vec![
                    file_with("Content000.cer", 41, 610, cert(bee), false),
                    file_with("MKB_RO.inf", 42, 620, mkb_gen(70), false),
                ],
                subdirs: vec![],
            },
        );
        let mut rig = Rig::new(mem, |_| {});
        let r = Disc::scan(&mut rig.drive, &with_hc());
        assert_eq!(
            r.err().map(|e| e.code()),
            Some(KEY_FILE_CODE),
            "absent, bee={bee}"
        );
        assert_eq!(rig.count(is_aacs_cdb), 0, "absent, bee={bee}");

        // Present but unreadable.
        let mut rig = Rig::new(bd_disc(Some(bee)), |t| {
            t.fail_read = Some(Box::new(|lba, n, _| {
                (lba..lba + n as u32)
                    .contains(&UK_LBA)
                    .then(|| check(3, 0x11, 0, 0x28))
            }));
        });
        let r = Disc::scan(&mut rig.drive, &with_hc());
        assert_eq!(
            r.err().map(|e| e.code()),
            Some(KEY_FILE_CODE),
            "read error, bee={bee}"
        );
        assert_eq!(rig.count(is_aacs_cdb), 0, "read error, bee={bee}");
    }
}

#[test]
fn raw_copy_scan_records_unreadable_uk_ro() {
    let mut rig = Rig::new(bd_disc(Some(false)), |t| {
        t.fail_read = Some(Box::new(|lba, n, _| {
            (lba..lba + n as u32)
                .contains(&UK_LBA)
                .then(|| check(3, 0x11, 0, 0x28))
        }));
    });
    let opts = ScanOptions {
        raw_copy: true,
        ..with_hc()
    };
    let d = Disc::scan(&mut rig.drive, &opts).expect("raw copy scans on");
    assert!(d.aacs.is_none() && !d.titles.is_empty());
    assert_eq!(d.aacs_error.as_ref().map(|e| e.code()), Some(KEY_FILE_CODE));
}

#[test]
fn failed_handshake_cannot_poison_uk_ro_read() {
    let mut rig = Rig::new(bd_disc(Some(false)), |t| {
        t.fail_read = Some(Box::new(|lba, n, hist| {
            (hist.iter().any(|c| is_aacs_agid_alloc(c)) && (lba..lba + n as u32).contains(&UK_LBA))
                .then(|| check(3, 0x11, 0, 0x28))
        }));
    });
    let d = Disc::scan(&mut rig.drive, &with_hc()).expect("scan");
    assert!(d.aacs.is_some(), "{:?}", d.aacs_error);
    assert!(d.aacs_error.is_none(), "{:?}", d.aacs_error);
}

#[test]
fn handshake_issues_no_drive_mkb_read() {
    let mut rig = Rig::new(bd_disc(Some(true)), |_| {});
    let _ = Disc::scan(&mut rig.drive, &with_hc());
    assert_eq!(rig.count(is_drive_mkb_read), 0);
}

#[test]
fn non_bus_failed_handshake_is_info_only() {
    let mut rig = Rig::new(bd_disc(Some(false)), |_| {});
    let (r, ev) = crate::testlog::capture(|| Disc::scan(&mut rig.drive, &with_hc()));
    let d = r.expect("scan");
    assert!(d.aacs_error.is_none(), "{:?}", d.aacs_error);
    assert!(aacs_warns(&ev).is_empty(), "{:?}", aacs_warns(&ev));
    let v = verdicts(&ev);
    assert_eq!(v.len(), 1, "{ev:?}");
    assert_eq!(v[0].level, tracing::Level::INFO);
}

#[test]
fn bus_disc_failed_handshake_lists_titles_one_error_refuses_keys() {
    let mut rig = Rig::new(bd_disc(Some(true)), |_| {});
    let (r, ev) = crate::testlog::capture(|| Disc::scan(&mut rig.drive, &with_hc()));
    let d = r.expect("scan");
    assert!(!d.titles.is_empty());
    assert!(
        matches!(d.aacs_error, Some(Error::AacsHostCertRejected)),
        "{:?}",
        d.aacs_error
    );
    let w = aacs_warns(&ev);
    assert_eq!(w.len(), 1, "{w:?}");
    assert_eq!(w[0].field("phase"), Some("aacs_verdict"));
    assert!(matches!(
        crate::keys::check_decryptable(&d, false, None, &crate::keys::KeyScope::WholeDisc),
        Err(Error::AacsHostCertRejected)
    ));
}

#[test]
fn transport_fault_mid_ake_aborts_scan() {
    for bee in [true, false] {
        let mut rig = Rig::new(bd_disc(Some(bee)), |t| t.fault_on = Some(is_send_host_cert));
        let r = Disc::scan(&mut rig.drive, &with_hc());
        assert!(
            matches!(
                r,
                Err(Error::ScsiError {
                    status: 0xFF,
                    sense: None,
                    ..
                })
            ),
            "bee={bee}: {:?}",
            r.map(|d| d.aacs_error)
        );
        let cdbs = rig.cdbs();
        let fault = cdbs
            .iter()
            .position(|c| is_send_host_cert(c))
            .expect("fault");
        assert!(!cdbs[fault..].iter().any(|c| is_read10(c)), "bee={bee}");
    }
}

fn dvd_disc() -> MemDisc {
    let root = DirSpec {
        name: String::new(),
        icb_lba: 10,
        dir_data_lba: 11,
        files: Vec::new(),
        subdirs: vec![DirSpec {
            name: "VIDEO_TS".into(),
            icb_lba: 20,
            dir_data_lba: 21,
            files: Vec::new(),
            subdirs: vec![],
        }],
    };
    let mut mem = MemDisc::new();
    build_udf_skeleton(&mut mem, 10);
    lay_dir(&mut mem, &root);
    mem
}

#[test]
fn css_transport_fault_aborts_scan() {
    let mut rig = Rig::new(dvd_disc(), |t| {
        t.profile = 0x0010;
        t.fault_on = Some(is_css_agid_alloc);
    });
    let r = Disc::scan(&mut rig.drive, &ScanOptions::default());
    assert!(
        matches!(
            r,
            Err(Error::ScsiError {
                status: 0xFF,
                sense: None,
                ..
            })
        ),
        "{:?}",
        r.map(|d| d.titles.len())
    );
    assert_eq!(rig.count(is_read10), 0);
}

#[test]
fn dvd_css_bus_auth_precedes_the_first_udf_read() {
    let mut rig = Rig::new(dvd_disc(), |t| t.profile = 0x0010);
    let _ = Disc::scan(&mut rig.drive, &ScanOptions::default());
    let cdbs = rig.cdbs();
    let css = cdbs.iter().position(|c| is_css_agid_alloc(c)).expect("CSS");
    let read = cdbs.iter().position(|c| is_read10(c)).expect("READ");
    assert!(css < read);
}

// A DVD carrying /AACS gets no AACS handshake, so its missing key file is recorded.
#[test]
fn dvd_with_aacs_dir_records_missing_key_file_without_aacs_scsi() {
    let mut mem = dvd_disc();
    lay_dir(
        &mut mem,
        &DirSpec {
            name: String::new(),
            icb_lba: 10,
            dir_data_lba: 11,
            files: Vec::new(),
            subdirs: vec![
                DirSpec {
                    name: "VIDEO_TS".into(),
                    icb_lba: 20,
                    dir_data_lba: 21,
                    files: Vec::new(),
                    subdirs: vec![],
                },
                DirSpec {
                    name: "AACS".into(),
                    icb_lba: 22,
                    dir_data_lba: 23,
                    files: vec![file_with("Content000.cer", 41, 610, cert(true), false)],
                    subdirs: vec![],
                },
            ],
        },
    );
    let mut rig = Rig::new(mem, |t| t.profile = 0x0010);
    let d = Disc::scan(&mut rig.drive, &with_hc()).expect("scan");
    assert!(d.aacs.is_none());
    assert!(
        matches!(d.aacs_error, Some(Error::AacsNoKeys)),
        "{:?}",
        d.aacs_error
    );
    let aacs_class = |c: &[u8]| c[0] == 0xAD || (matches!(c[0], 0xA3 | 0xA4) && c[7] == 0x02);
    assert_eq!(rig.count(aacs_class), 0, "{:02x?}", rig.cdbs());
}

#[test]
fn stop_during_ake_returns_halted() {
    let mut rig = Rig::new(bd_disc(Some(true)), |t| {
        t.halt_on = Some(Box::new(is_aacs_agid_alloc))
    });
    let r = Disc::scan(&mut rig.drive, &with_hc());
    assert!(
        matches!(r, Err(Error::Halted)),
        "{:?}",
        r.map(|d| d.aacs_error)
    );
}

#[derive(Clone, Default)]
struct SpySource(Arc<Mutex<Vec<Option<u32>>>>);
impl crate::KeySource for SpySource {
    fn get_unit_keys(
        &self,
        _ctx: &dyn crate::keysource::ResolveCtx,
    ) -> Result<Vec<crate::aacs::types::UnitKey>> {
        Ok(Vec::new())
    }
    fn host_certs(&self, mkb: Option<u32>) -> Vec<crate::aacs::types::HostCert> {
        self.0.lock().unwrap().push(mkb);
        vec![test_hc()]
    }
}

#[test]
fn cert_sources_receive_no_mkb_hint() {
    let spy = SpySource::default();
    let mut rig = Rig::new(bd_disc(Some(true)), |_| {});
    let opts = ScanOptions {
        key_sources: vec![Box::new(spy.clone())],
        ..Default::default()
    };
    let _ = Disc::scan(&mut rig.drive, &opts);
    let seen = spy.0.lock().unwrap().clone();
    assert!(!seen.is_empty());
    assert!(seen.iter().all(Option::is_none), "{seen:?}");
}

#[test]
fn pre_ake_read_refusal_is_a_plain_scan_error() {
    let mut rig = Rig::new(bd_disc(Some(true)), |t| {
        t.fail_read = Some(Box::new(|_, _, hist| {
            (!hist.iter().any(|c| c[0] == 0xA4)).then(|| check(5, 0x21, 0, 0x28))
        }));
    });
    let r = Disc::scan(&mut rig.drive, &with_hc());
    let Err(e) = r else { panic!("scan must fail") };
    let s = e.scsi_sense().copied();
    assert!(
        s.is_some_and(|s| s.sense_key == 5 && s.asc == 0x21),
        "{e:?}"
    );
    assert_eq!(rig.count(|c| matches!(c[0], 0xA3 | 0xA4)), 0);
}

fn hddvd_disc() -> MemDisc {
    let root = DirSpec {
        name: String::new(),
        icb_lba: 10,
        dir_data_lba: 11,
        files: Vec::new(),
        subdirs: vec![
            DirSpec {
                name: "HVDVD_TS".into(),
                icb_lba: 20,
                dir_data_lba: 21,
                files: vec![file("MAIN.EVO", 100, 5_000, 3_000_000, true)],
                subdirs: vec![],
            },
            DirSpec {
                name: "AACS!".into(),
                icb_lba: 22,
                dir_data_lba: 23,
                files: vec![
                    file_with("MKBROM.AACS", 101, 900, mkb_gen(70), false),
                    file_with("VTKF000.AACS", 102, 910, vec![0xAB; 64], false),
                ],
                subdirs: vec![],
            },
        ],
    };
    let mut mem = MemDisc::new();
    build_udf_skeleton(&mut mem, 10);
    lay_dir(&mut mem, &root);
    mem
}

const VTKF_LBA: u32 = PART_START + 910;

// An HD DVD's `X!` AACS dir is an AACS dir: identified as encrypted, its VTKF captured
// before the handshake, and a keyless rip refused instead of writing ciphertext.
#[test]
fn hddvd_aacs_dir_is_captured_and_handshaked() {
    let mut rig = Rig::new(hddvd_disc(), |_| {});
    assert!(Disc::identify(&mut rig.drive).expect("identify").encrypted);
    let d = Disc::scan(&mut rig.drive, &with_hc()).expect("scan");
    assert!(d.encrypted && !d.titles.is_empty());
    let a = d.aacs.as_ref().expect("HD DVD AACS state");
    assert!(
        !a.bus_encryption && d.aacs_error.is_none(),
        "{:?}",
        d.aacs_error
    );
    assert!(matches!(
        crate::keys::check_decryptable(&d, false, None, &crate::keys::KeyScope::WholeDisc),
        Err(Error::NoDiscKey { .. })
    ));
    let cdbs = rig.cdbs();
    let first_aacs = cdbs.iter().position(|c| is_aacs_cdb(c)).expect("AACS CDBs");
    let vtkf = cdbs.iter().rposition(|c| touches(c, &[VTKF_LBA]));
    assert!(vtkf.is_some_and(|i| i < first_aacs), "{cdbs:02x?}");
}

#[test]
fn hddvd_image_aacs_dir_is_captured() {
    let mut mem = hddvd_disc();
    let d = Disc::scan_image(&mut mem, 9_999, &ScanOptions::default()).expect("scan");
    assert!(d.encrypted && d.aacs.is_some() && d.aacs_error.is_none());
}

#[test]
fn halted_during_capture_propagates() {
    for lba in [CERT_LBA, MKB_LBA] {
        let mut rig = Rig::new(bd_disc(Some(true)), |t| {
            t.halt_on = Some(Box::new(move |c: &[u8]| touches(c, &[lba])));
        });
        let r = Disc::scan(&mut rig.drive, &with_hc());
        assert!(
            matches!(r, Err(Error::Halted)),
            "lba {lba}: {:?}",
            r.map(|d| d.aacs_error)
        );
    }
}

// A source whose reads covering `lba` fail with `Halted`, as a stopped drive's do.
struct HaltAt {
    mem: MemDisc,
    lba: u32,
}
impl crate::sector::SectorSource for HaltAt {
    fn read_sectors(&mut self, lba: u32, n: u16, buf: &mut [u8], rec: bool) -> Result<usize> {
        if (lba..lba + n as u32).contains(&self.lba) {
            return Err(Error::Halted);
        }
        self.mem.read_sectors(lba, n, buf, rec)
    }
}

// No later check may mask a Stop swallowed by one of capture's own reads.
#[test]
fn capture_propagates_a_stop_from_every_key_file_read() {
    use super::encrypt::{CaptureFrom, capture};
    for lba in [UK_LBA, CERT_LBA, MKB_LBA] {
        for from in [
            CaptureFrom::Image,
            CaptureFrom::Live { raw_copy: true },
            CaptureFrom::Live { raw_copy: false },
        ] {
            let mut src = HaltAt {
                mem: bd_disc(Some(true)),
                lba,
            };
            let udf = crate::udf::read_filesystem(&mut src).expect("udf");
            let r = capture(&mut src, &udf, from);
            assert!(matches!(r, Err(Error::Halted)), "lba {lba}");
        }
    }
}

#[test]
fn no_unlocker_runs_cert_route_and_issues_ake() {
    let mut rig = Rig::new(bd_disc(Some(true)), |_| {});
    let d = Disc::scan(&mut rig.drive, &with_hc()).expect("scan");
    assert!(rig.count(is_aacs_agid_alloc) >= 1 && rig.count(is_send_host_cert) >= 1);
    assert_eq!(rig.count(|c| c[0] == 0xAD), 0);
    assert!(matches!(d.aacs_error, Some(Error::AacsHostCertRejected)));
}

#[test]
fn image_unreadable_uk_ro_is_recorded_not_fatal() {
    let mut mem = bd_disc(Some(false));
    lay_dir(
        &mut mem,
        &DirSpec {
            name: "AACS".into(),
            icb_lba: 20,
            dir_data_lba: 21,
            files: vec![file_with("Content000.cer", 41, 610, cert(false), false)],
            subdirs: vec![],
        },
    );
    let d = Disc::scan_image(&mut mem, 9_999, &ScanOptions::default()).expect("scan");
    assert!(d.aacs.is_none());
    assert!(
        matches!(d.aacs_error, Some(Error::AacsNoKeys)),
        "{:?}",
        d.aacs_error
    );
}

// ── Stop design §5.1 "Scan" (LS1–LS6), over `test_util::FakeTransport` ──

fn ake(mem: MemDisc, profile: u16) -> AkeMemTransport {
    AkeMemTransport {
        mem,
        log: Arc::default(),
        profile,
        fail_read: None,
        fault_on: None,
        halt_on: None,
        halt: Arc::default(),
        mkb_pack: Some(mkb_gen(70)),
    }
}

fn with_halt(h: &crate::halt::Halt) -> ScanOptions {
    ScanOptions {
        halt: Some(h.clone()),
        ..with_hc()
    }
}

// The `n`th (1-based) CDB matching `p`.
fn nth(n: usize, p: Pred) -> impl Fn(&[u8]) -> bool + Send + 'static {
    let seen = std::sync::atomic::AtomicUsize::new(0);
    move |c| p(c) && seen.fetch_add(1, Ordering::Relaxed) + 1 == n
}

/// LS1 (D5): a Stop during a recovery-timeout UDF metadata READ waits for that CDB,
/// issues nothing after it, and ends the scan `Halted`.
#[test]
fn stop_during_udf_metadata_read_waits_for_inflight_cdb() {
    use crate::test_util::{FakeMode, FakeTransport};
    let h = crate::halt::Halt::new();
    let (t, fake) = FakeTransport::new();
    let t = t
        .with_inner(Box::new(ake(bd_disc(Some(true)), 0x0040)))
        .rule_n(nth(1, is_read10), FakeMode::Stall, 1)
        .scale(100)
        .watch(&h);
    let mut drive = Drive::from_transport(Box::new(t));
    let (f2, h2) = (fake.clone(), h.clone());
    let stopper = std::thread::spawn(move || {
        assert!(f2.wait_for(1, is_read10, std::time::Duration::from_secs(5)));
        h2.cancel();
        std::thread::sleep(std::time::Duration::from_millis(20));
        f2.release();
    });
    let r = Disc::scan(&mut drive, &with_halt(&h));
    stopper.join().unwrap();
    assert!(
        matches!(r, Err(Error::Halted)),
        "{:?}",
        r.map(|d| d.titles.len())
    );
    let log = fake.log();
    let at = log.iter().position(|c| is_read10(&c.cdb)).unwrap();
    assert_eq!(
        log[at].timeout_ms,
        crate::scsi::READ_RECOVERY_TIMEOUT_MS,
        "D5: recovery READ"
    );
    assert_eq!(
        at + 1,
        log.len(),
        "zero CDBs after the in-flight READ: {:02x?}",
        fake.cdbs()
    );
}

/// LS2 / G1 (GUARD, D5): a metadata READ slower than the fast 10 s timeout but inside
/// the 60 s recovery one (30 s, scaled) still scans.
#[test]
fn scan_survives_metadata_read_slower_than_fast_timeout() {
    use crate::test_util::{FakeMode, FakeTransport};
    let (t, fake) = FakeTransport::new();
    // Scale 1000: fast READ times out at 10 ms, recovery at 60 ms; this one takes 30.
    let t = t.with_inner(Box::new(ake(bd_disc(None), 0x0040))).rule_n(
        nth(1, is_read10),
        FakeMode::Complete(std::time::Duration::from_millis(30)),
        1,
    );
    let mut drive = Drive::from_transport(Box::new(t));
    let d = Disc::scan(&mut drive, &with_hc()).expect("a slow recovery READ is not a failure");
    assert!(!d.titles.is_empty());
    assert!(fake.count(is_read10) > 1);
}

/// LS4: AACS and CSS bus steps both observe the op token (the `ScanOptions.halt`
/// alias, not the drive's own) through `bus_step_guard`.
#[test]
fn bus_step_stop_and_transport_share_bus_step_guard() {
    use crate::test_util::FakeTransport;
    for (mem, profile, alloc) in [
        (bd_disc(Some(true)), 0x0040, is_aacs_agid_alloc as Pred),
        (dvd_disc(), 0x0010, is_css_agid_alloc as Pred),
    ] {
        let h = crate::halt::Halt::new();
        let (t, fake) = FakeTransport::new();
        let t = t
            .with_inner(Box::new(ake(mem, profile)))
            .cancel_on(nth(1, alloc), &h)
            .watch(&h);
        let mut drive = Drive::from_transport(Box::new(t));
        let r = Disc::scan(&mut drive, &with_halt(&h));
        assert!(
            matches!(r, Err(Error::Halted)),
            "{profile:#x}: {:?}",
            r.map(|d| d.titles.len())
        );
        assert!(
            !drive.is_halted(),
            "the drive's own token was never cancelled"
        );
        // After the Stop only the §2.4 clean-up: the allocated AGID's one release.
        let cdbs = fake.cdbs();
        let at = cdbs.iter().position(|c| alloc(c)).unwrap();
        let after = &cdbs[at + 1..];
        let release = |c: &Vec<u8>| crate::drive::allow::invalidated_agid(c).is_some();
        assert!(
            after.len() <= 1 && after.iter().all(release),
            "{profile:#x}: {after:02x?}"
        );
    }
    let enc = include_str!("encrypt.rs");
    let body = |src: &'static str, f: &str| {
        let i = src.find(f).unwrap();
        &src[i..i + src[i..]
            .find("\n}\n")
            .or_else(|| src[i..].find("\n    }\n"))
            .unwrap()]
    };
    assert!(body(enc, "pub(super) fn aacs_bus_step(").contains("bus_step_guard("));
    assert!(body(include_str!("mod.rs"), "fn css_bus_step(").contains("bus_step_guard("));
}

/// LS5: a Stop during the metadata prefetch ends `prefetch_ranges` with `Halted`; no
/// READ follows, and no title parse reads on from the cache.
#[test]
fn stop_between_prefetch_ranges_and_title_parses() {
    use crate::test_util::FakeTransport;
    let mpls = PART_START + 700;
    let h = crate::halt::Halt::new();
    let (t, fake) = FakeTransport::new();
    let t = t
        .with_inner(Box::new(ake(bd_disc(None), 0x0040)))
        .cancel_on(move |c| touches(c, &[mpls]), &h)
        .watch(&h);
    let mut drive = Drive::from_transport(Box::new(t));
    let r = Disc::scan(&mut drive, &with_halt(&h));
    assert!(
        matches!(r, Err(Error::Halted)),
        "{:?}",
        r.map(|d| d.titles.len())
    );
    let cdbs = fake.cdbs();
    let at = cdbs.iter().position(|c| touches(c, &[mpls])).unwrap();
    assert_eq!(at + 1, cdbs.len(), "no READ after the Stop: {cdbs:02x?}");
}

/// LS6: a Stop after the scan's last CDB, before it returns, still ends it `Halted`
/// with no `Disc`.
#[test]
fn scan_with_final_check() {
    use crate::test_util::FakeTransport;
    let (t, fake) = FakeTransport::new();
    let mut drive =
        Drive::from_transport(Box::new(t.with_inner(Box::new(ake(bd_disc(None), 0x0040)))));
    Disc::scan(&mut drive, &with_hc()).expect("dry run scans");
    let last = fake.log().len();
    let h = crate::halt::Halt::new();
    let (t, fake) = FakeTransport::new();
    let t = t
        .with_inner(Box::new(ake(bd_disc(None), 0x0040)))
        .cancel_on(nth(last, |_| true), &h);
    let mut drive = Drive::from_transport(Box::new(t));
    let r = Disc::scan(&mut drive, &with_halt(&h));
    assert!(
        matches!(r, Err(Error::Halted)),
        "{:?}",
        r.map(|d| d.titles.len())
    );
    assert_eq!(fake.log().len(), last);
}

// A key source that records whether the drive's Progress was busy while it ran.
struct BusyProbe(crate::halt::Progress, Arc<Mutex<Vec<bool>>>);
impl crate::KeySource for BusyProbe {
    fn get_unit_keys(
        &self,
        _ctx: &dyn crate::keysource::ResolveCtx,
    ) -> Result<Vec<crate::aacs::types::UnitKey>> {
        Ok(Vec::new())
    }
    fn host_certs(&self, _mkb: Option<u32>) -> Vec<crate::aacs::types::HostCert> {
        self.1.lock().unwrap().push(self.0.is_busy());
        vec![test_hc()]
    }
}

/// ST4-2: `bus_step_guard` holds `busy()` on the Drive-attached `Progress` for the
/// whole bus step, so the first keydb parse (in `host_certs`) is never idle time.
#[test]
fn bus_step_guard_holds_busy_across_host_certs() {
    let p = crate::halt::Progress::new();
    let seen: Arc<Mutex<Vec<bool>>> = Arc::default();
    let mut rig = Rig::new(bd_disc(Some(true)), |_| {});
    rig.drive.attach_progress(&p);
    let opts = ScanOptions {
        key_sources: vec![Box::new(BusyProbe(p.clone(), seen.clone()))],
        ..Default::default()
    };
    let _ = Disc::scan(&mut rig.drive, &opts);
    let seen = seen.lock().unwrap().clone();
    assert!(!seen.is_empty() && seen.iter().all(|b| *b), "{seen:?}");
    assert!(!p.is_busy(), "released after the step");
}

// ── Stop design §5.1 "Scan" LS7 and the session alias rule (ST-L3) ──

// A `DiscSession` brought up over `t`: under `halt` as `open_with` does, else as `open`.
fn session_over(
    t: crate::test_util::FakeTransport,
    halt: Option<&crate::halt::Halt>,
) -> crate::session::DiscSession {
    let drive = match halt {
        Some(h) => Drive::from_transport_with(Box::new(t), h),
        None => Drive::from_transport(Box::new(t)),
    };
    crate::session::DiscSession::bring_up(drive, Default::default(), halt.cloned())
        .expect("brought up")
}

/// LS7 (the scan half; KU's LK18 covers the library half): a Stop during
/// `scan_with` returns `Halted` and stores no `Disc`, and a key resolution on that
/// session then returns `Halted` too and builds no set.
#[test]
fn cancelled_scan_then_resolve_returns_halted_and_builds_no_set() {
    use crate::test_util::FakeTransport;
    let h = crate::halt::Halt::new();
    let (t, fake) = FakeTransport::new();
    let t = t
        .with_inner(Box::new(ake(bd_disc(None), 0x0040)))
        .cancel_on(nth(1, is_read10), &h)
        .watch(&h);
    let mut s = session_over(t, Some(&h));
    let r = s.scan_with(with_hc()).map(|d| d.titles.len());
    assert!(matches!(r, Err(Error::Halted)), "{r:?}");
    assert!(s.disc().is_none(), "a stopped scan stores no Disc");
    let sources: crate::session::KeySourceFactory = Arc::new(Vec::new);
    let r = s.resolve_key_set(
        crate::keys::KeyScope::WholeDisc,
        &sources,
        Default::default(),
    );
    assert!(matches!(r, Err(Error::Halted)), "{:?}", r.err());
    let reads = fake.count(is_read10);
    assert_eq!(
        reads,
        1,
        "nothing read after the Stop: {:02x?}",
        fake.cdbs()
    );
}

/// Guard (LD8/LD9 at the session layer, §2.2 alias rule): a session from `open` (own
/// token) scans under the `ScanOptions.halt` alias and gets its own token back; a
/// session from `open_with` keeps its attached token, which wins over the alias.
#[test]
fn session_scan_with_follows_the_alias_rule() {
    use crate::test_util::FakeTransport;
    let alias = crate::halt::Halt::new();
    let (t, _) = FakeTransport::new();
    let t = t
        .with_inner(Box::new(ake(bd_disc(None), 0x0040)))
        .cancel_on(nth(1, is_read10), &alias);
    let mut own = session_over(t, None);
    let r = own.scan_with(with_halt(&alias)).map(|d| d.titles.len());
    assert!(matches!(r, Err(Error::Halted)), "the alias stops it: {r:?}");
    let drive = own.into_drive().expect("drive");
    let tok = drive.token().expect("own token restored");
    assert!(!Arc::ptr_eq(tok.as_arc(), alias.as_arc()) && !tok.is_cancelled());

    let (op, alias) = (crate::halt::Halt::new(), crate::halt::Halt::new());
    let (t, _) = FakeTransport::new();
    let t = t
        .with_inner(Box::new(ake(bd_disc(None), 0x0040)))
        .cancel_on(nth(1, is_read10), &alias);
    let mut attached = session_over(t, Some(&op));
    let d = attached.scan_with(with_halt(&alias));
    assert!(d.is_ok(), "the attached op token wins over the alias");
}
