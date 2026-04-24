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
//! | E9xxx | Stream/mux errors |

// ── Error codes ─────────────────────────────────────────────────────────────

// Device (1xxx)
pub const E_DEVICE_NOT_FOUND: u16 = 1000;
pub const E_DEVICE_PERMISSION: u16 = 1001;
pub const E_DEVICE_NOT_READY: u16 = 1002;
pub const E_DEVICE_RESET_FAILED: u16 = 1003;
pub const E_SCSI_INTERFACE_UNAVAILABLE: u16 = 1004;
pub const E_DEVICE_LOCKED: u16 = 1005;
pub const E_IOKIT_PLUGIN_FAILED: u16 = 1006;

// Profile (2xxx)
pub const E_UNSUPPORTED_DRIVE: u16 = 2000;
pub const E_PROFILE_PARSE: u16 = 2002;
pub const E_UNSUPPORTED_PLATFORM: u16 = 2003;
pub const E_PLATFORM_NOT_IMPLEMENTED: u16 = 2004;

// Unlock (3xxx)
pub const E_UNLOCK_FAILED: u16 = 3000;
pub const E_SIGNATURE_MISMATCH: u16 = 3001;

// SCSI (4xxx)
pub const E_SCSI_ERROR: u16 = 4000;

// I/O (5xxx)
pub const E_IO_ERROR: u16 = 5000;

// Disc format (6xxx)
pub const E_DISC_READ: u16 = 6000;
pub const E_HALTED: u16 = 6010;
pub const E_MPLS_PARSE: u16 = 6001;
pub const E_CLPI_PARSE: u16 = 6002;
pub const E_UDF_NOT_FOUND: u16 = 6003;
pub const E_DISC_TITLE_RANGE: u16 = 6005;
pub const E_IFO_PARSE: u16 = 6007;
pub const E_MKV_INVALID: u16 = 6008;
pub const E_NO_STREAMS: u16 = 6009;
pub const E_MAPFILE_INVALID: u16 = 6011;

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
pub const E_DECRYPT_FAILED: u16 = 7013;
pub const E_CSS_AUTH_FAILED: u16 = 7014;

// Keydb (8xxx)
pub const E_KEYDB_CONNECT: u16 = 8000;
pub const E_KEYDB_HTTP: u16 = 8001;
pub const E_KEYDB_INVALID: u16 = 8002;
pub const E_KEYDB_WRITE: u16 = 8003;
pub const E_KEYDB_PARSE: u16 = 8004;
pub const E_KEYDB_LOAD: u16 = 8005;

// Stream/mux (9xxx)
pub const E_STREAM_READ_ONLY: u16 = 9000;
pub const E_STREAM_WRITE_ONLY: u16 = 9001;
pub const E_STREAM_URL_INVALID: u16 = 9002;
pub const E_STREAM_URL_MISSING_PATH: u16 = 9003;
pub const E_STREAM_URL_MISSING_PORT: u16 = 9004;
pub const E_PES_FRAME_TOO_LARGE: u16 = 9005;
pub const E_PES_INVALID_MAGIC: u16 = 9006;
pub const E_ISO_TOO_LARGE: u16 = 9007;
pub const E_NO_METADATA: u16 = 9008;
pub const E_DISC_URL_NOT_DIRECT: u16 = 9009;

// ── Error enum ──────────────────────────────────────────────────────────────

/// Structured error with numeric code and context data. No English text.
#[derive(Debug)]
pub enum Error {
    // Device (1xxx)
    DeviceNotFound {
        path: String,
    },
    DevicePermission {
        path: String,
    },
    DeviceNotReady {
        path: String,
    },
    DeviceResetFailed {
        path: String,
    },
    /// Platform-specific SCSI interface couldn't be obtained from the OS
    /// (macOS: `SCSITaskDeviceInterface` unavailable). The `path` field
    /// carries the device path; no English commentary on the failure mode.
    ScsiInterfaceUnavailable {
        path: String,
    },
    /// Device is held by another process / kernel state. `kr` is the
    /// platform return code (macOS IOReturn, Linux errno-equivalent).
    DeviceLocked {
        path: String,
        kr: u32,
    },
    /// macOS IOKit plugin couldn't be created for this device. `kr` is
    /// the IOReturn code from `IOCreatePlugInInterfaceForService`.
    IoKitPluginFailed {
        path: String,
        kr: u32,
    },

    // Profile (2xxx)
    UnsupportedDrive {
        vendor_id: String,
        product_id: String,
        product_revision: String,
    },
    ProfileParse,
    /// SCSI transport was requested on an OS without a backend
    /// implementation. `target` is the `std::env::consts::OS` value.
    UnsupportedPlatform {
        target: String,
    },
    /// Drive matched a known platform that we haven't implemented yet
    /// (e.g. Renesas firmware). `platform` is a stable identifier.
    PlatformNotImplemented {
        platform: String,
    },

    // Unlock (3xxx)
    UnlockFailed,
    SignatureMismatch {
        expected: [u8; 4],
        got: [u8; 4],
    },

    // SCSI (4xxx)
    ScsiError {
        opcode: u8,
        status: u8,
        sense_key: u8,
    },

    // I/O (5xxx)
    IoError {
        source: std::io::Error,
    },

    // Disc format (6xxx)
    DiscRead {
        sector: u64,
    },
    /// Drive was halted by caller.
    Halted,
    MplsParse,
    ClpiParse,
    UdfNotFound {
        path: String,
    },
    DiscTitleRange {
        index: usize,
        count: usize,
    },
    IfoParse,
    MkvInvalid,
    NoStreams,
    /// ddrescue mapfile parse failed. `kind` is a stable, language-neutral
    /// identifier (e.g. `"status_char"`, `"hex"`); not a translatable
    /// English message.
    MapfileInvalid {
        kind: &'static str,
    },

    // AACS (7xxx)
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
    DecryptFailed,
    CssAuthFailed,

    // Keydb (8xxx)
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

    // Stream/mux (9xxx)
    StreamReadOnly,
    StreamWriteOnly,
    StreamUrlInvalid {
        url: String,
    },
    StreamUrlMissingPath {
        scheme: String,
    },
    StreamUrlMissingPort {
        addr: String,
    },
    PesFrameTooLarge {
        size: usize,
    },
    PesInvalidMagic,
    IsoTooLarge {
        path: String,
    },
    NoMetadata,
    /// `disc://` URLs aren't openable through `input()` — callers must use
    /// `Drive::open() + Disc::scan() + DiscStream::new()` directly. This
    /// is a structural API constraint, not a parse failure.
    DiscUrlNotDirect,
}

impl Error {
    pub fn code(&self) -> u16 {
        match self {
            Error::DeviceNotFound { .. } => E_DEVICE_NOT_FOUND,
            Error::DevicePermission { .. } => E_DEVICE_PERMISSION,
            Error::DeviceNotReady { .. } => E_DEVICE_NOT_READY,
            Error::DeviceResetFailed { .. } => E_DEVICE_RESET_FAILED,
            Error::ScsiInterfaceUnavailable { .. } => E_SCSI_INTERFACE_UNAVAILABLE,
            Error::DeviceLocked { .. } => E_DEVICE_LOCKED,
            Error::IoKitPluginFailed { .. } => E_IOKIT_PLUGIN_FAILED,
            Error::UnsupportedDrive { .. } => E_UNSUPPORTED_DRIVE,
            Error::ProfileParse => E_PROFILE_PARSE,
            Error::UnsupportedPlatform { .. } => E_UNSUPPORTED_PLATFORM,
            Error::PlatformNotImplemented { .. } => E_PLATFORM_NOT_IMPLEMENTED,
            Error::UnlockFailed => E_UNLOCK_FAILED,
            Error::SignatureMismatch { .. } => E_SIGNATURE_MISMATCH,
            Error::ScsiError { .. } => E_SCSI_ERROR,
            Error::IoError { .. } => E_IO_ERROR,
            Error::DiscRead { .. } => E_DISC_READ,
            Error::Halted => E_HALTED,
            Error::MplsParse => E_MPLS_PARSE,
            Error::ClpiParse => E_CLPI_PARSE,
            Error::UdfNotFound { .. } => E_UDF_NOT_FOUND,
            Error::DiscTitleRange { .. } => E_DISC_TITLE_RANGE,
            Error::IfoParse => E_IFO_PARSE,
            Error::MkvInvalid => E_MKV_INVALID,
            Error::NoStreams => E_NO_STREAMS,
            Error::MapfileInvalid { .. } => E_MAPFILE_INVALID,
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
            Error::DecryptFailed => E_DECRYPT_FAILED,
            Error::CssAuthFailed => E_CSS_AUTH_FAILED,
            Error::KeydbConnect { .. } => E_KEYDB_CONNECT,
            Error::KeydbHttp { .. } => E_KEYDB_HTTP,
            Error::KeydbInvalid => E_KEYDB_INVALID,
            Error::KeydbWrite { .. } => E_KEYDB_WRITE,
            Error::KeydbParse => E_KEYDB_PARSE,
            Error::KeydbLoad { .. } => E_KEYDB_LOAD,
            Error::StreamReadOnly => E_STREAM_READ_ONLY,
            Error::StreamWriteOnly => E_STREAM_WRITE_ONLY,
            Error::StreamUrlInvalid { .. } => E_STREAM_URL_INVALID,
            Error::StreamUrlMissingPath { .. } => E_STREAM_URL_MISSING_PATH,
            Error::StreamUrlMissingPort { .. } => E_STREAM_URL_MISSING_PORT,
            Error::PesFrameTooLarge { .. } => E_PES_FRAME_TOO_LARGE,
            Error::PesInvalidMagic => E_PES_INVALID_MAGIC,
            Error::IsoTooLarge { .. } => E_ISO_TOO_LARGE,
            Error::NoMetadata => E_NO_METADATA,
            Error::DiscUrlNotDirect => E_DISC_URL_NOT_DIRECT,
        }
    }
}

/// Display: "E{code}" with structured data. No English words.
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::DeviceNotFound { path } => write!(f, "E{}: {}", self.code(), path),
            Error::DevicePermission { path } => write!(f, "E{}: {}", self.code(), path),
            Error::DeviceNotReady { path } => write!(f, "E{}: {}", self.code(), path),
            Error::DeviceResetFailed { path } => write!(f, "E{}: {}", self.code(), path),
            Error::ScsiInterfaceUnavailable { path } => write!(f, "E{}: {}", self.code(), path),
            Error::DeviceLocked { path, kr } => {
                write!(f, "E{}: {} 0x{:08x}", self.code(), path, kr)
            }
            Error::IoKitPluginFailed { path, kr } => {
                write!(f, "E{}: {} 0x{:08x}", self.code(), path, kr)
            }
            Error::UnsupportedPlatform { target } => {
                write!(f, "E{}: {}", self.code(), target)
            }
            Error::PlatformNotImplemented { platform } => {
                write!(f, "E{}: {}", self.code(), platform)
            }
            Error::MapfileInvalid { kind } => {
                write!(f, "E{}: {}", self.code(), kind)
            }
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
            } => {
                write!(
                    f,
                    "E{}: 0x{:02x}/0x{:02x}/0x{:02x}",
                    self.code(),
                    opcode,
                    status,
                    sense_key
                )
            }
            Error::IoError { source } => write!(f, "E{}: {}", self.code(), source),
            Error::DiscRead { sector } => write!(f, "E{}: {}", self.code(), sector),
            Error::Halted => write!(f, "E{}", self.code()),
            Error::UdfNotFound { path } => write!(f, "E{}: {}", self.code(), path),
            Error::DiscTitleRange { index, count } => {
                write!(f, "E{}: {}/{}", self.code(), index, count)
            }
            Error::KeydbConnect { host } => write!(f, "E{}: {}", self.code(), host),
            Error::KeydbHttp { status } => write!(f, "E{}: {}", self.code(), status),
            Error::KeydbWrite { path } => write!(f, "E{}: {}", self.code(), path),
            Error::KeydbLoad { path } => write!(f, "E{}: {}", self.code(), path),
            Error::StreamUrlInvalid { url } => write!(f, "E{}: {}", self.code(), url),
            Error::StreamUrlMissingPath { scheme } => write!(f, "E{}: {}", self.code(), scheme),
            Error::StreamUrlMissingPort { addr } => write!(f, "E{}: {}", self.code(), addr),
            Error::PesFrameTooLarge { size } => write!(f, "E{}: {}", self.code(), size),
            Error::IsoTooLarge { path } => write!(f, "E{}: {}", self.code(), path),
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

impl From<Error> for std::io::Error {
    fn from(e: Error) -> Self {
        let code = e.code();
        let msg = e.to_string();
        // Map our error categories to io::ErrorKind
        let kind = match code {
            1000..=1999 => std::io::ErrorKind::NotFound,
            2000..=2999 => std::io::ErrorKind::Unsupported,
            3000..=3999 => std::io::ErrorKind::PermissionDenied,
            4000..=4999 => std::io::ErrorKind::Other,
            5000..=5999 => std::io::ErrorKind::Other,
            6000..=6999 => std::io::ErrorKind::InvalidData,
            7000..=7999 => std::io::ErrorKind::PermissionDenied,
            8000..=8999 => std::io::ErrorKind::Other,
            9000..=9001 => std::io::ErrorKind::Unsupported,
            9002..=9008 => std::io::ErrorKind::InvalidInput,
            // 9009 DiscUrlNotDirect: structurally unsupported entry point,
            // not a parse failure — caller used the wrong API.
            9009 => std::io::ErrorKind::Unsupported,
            _ => std::io::ErrorKind::Other,
        };
        std::io::Error::new(kind, msg)
    }
}

/// Convenience alias for `Result<T, Error>`.
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    //! Smoke tests for the error code → variant mapping. Each new variant
    //! added in 0.13.0 (English-elimination work) gets a code() check + a
    //! Display sanity-check (no English words) + an io::ErrorKind mapping
    //! check. Without these, future drift between the const codes and the
    //! match arms in `code()` / the From impl could silently miscategorize.
    use super::*;

    #[test]
    fn new_variants_have_distinct_codes() {
        let codes = [
            Error::ScsiInterfaceUnavailable { path: "p".into() }.code(),
            Error::DeviceLocked {
                path: "p".into(),
                kr: 0,
            }
            .code(),
            Error::IoKitPluginFailed {
                path: "p".into(),
                kr: 0,
            }
            .code(),
            Error::UnsupportedPlatform { target: "x".into() }.code(),
            Error::PlatformNotImplemented {
                platform: "renesas".into(),
            }
            .code(),
            Error::MapfileInvalid { kind: "hex" }.code(),
            Error::DiscUrlNotDirect.code(),
        ];
        let mut sorted = codes.to_vec();
        sorted.sort();
        sorted.dedup();
        assert_eq!(
            sorted.len(),
            codes.len(),
            "two new variants share a code — check error.rs constants"
        );
    }

    #[test]
    fn display_emits_no_english_words() {
        // Every variant's Display must be `E{code}: {data}` — no English.
        // Sample a few of the new variants and a few existing ones to
        // catch accidental string-stuffing in future edits.
        let cases: &[(Error, u16)] = &[
            (
                Error::ScsiInterfaceUnavailable {
                    path: "/dev/sg4".into(),
                },
                E_SCSI_INTERFACE_UNAVAILABLE,
            ),
            (
                Error::DeviceLocked {
                    path: "/dev/sg4".into(),
                    kr: 0xE00002C5,
                },
                E_DEVICE_LOCKED,
            ),
            (
                Error::UnsupportedPlatform {
                    target: "freebsd".into(),
                },
                E_UNSUPPORTED_PLATFORM,
            ),
            (
                Error::PlatformNotImplemented {
                    platform: "renesas".into(),
                },
                E_PLATFORM_NOT_IMPLEMENTED,
            ),
            (Error::MapfileInvalid { kind: "hex" }, E_MAPFILE_INVALID),
            (Error::DiscUrlNotDirect, E_DISC_URL_NOT_DIRECT),
        ];
        for (e, want_code) in cases {
            let s = e.to_string();
            assert!(
                s.starts_with(&format!("E{}", want_code)),
                "{:?} display does not lead with code: {}",
                e,
                s
            );
            // Crude English filter — `Display` should never emit ASCII words
            // longer than 4 chars (codes/paths/identifiers like `/dev/sg4`,
            // `renesas`, `freebsd` all pass; "exclusive access denied" would
            // not).
            for word in s.split(|c: char| !c.is_ascii_alphabetic()) {
                assert!(
                    word.len() <= 8
                        || word.eq_ignore_ascii_case("renesas")
                        || word.eq_ignore_ascii_case("freebsd"),
                    "Display contains suspicious English-looking word `{word}` in `{s}`"
                );
            }
        }
    }

    #[test]
    fn iokind_mapping_for_new_variants() {
        use std::io::ErrorKind;
        let mapped = |e: Error| -> ErrorKind {
            let io: std::io::Error = e.into();
            io.kind()
        };
        // 1xxx range → NotFound
        assert_eq!(
            mapped(Error::ScsiInterfaceUnavailable { path: "p".into() }),
            ErrorKind::NotFound
        );
        assert_eq!(
            mapped(Error::DeviceLocked {
                path: "p".into(),
                kr: 0
            }),
            ErrorKind::NotFound
        );
        // 2xxx range → Unsupported
        assert_eq!(
            mapped(Error::UnsupportedPlatform { target: "x".into() }),
            ErrorKind::Unsupported
        );
        assert_eq!(
            mapped(Error::PlatformNotImplemented {
                platform: "x".into()
            }),
            ErrorKind::Unsupported
        );
        // 6xxx range → InvalidData
        assert_eq!(
            mapped(Error::MapfileInvalid { kind: "hex" }),
            ErrorKind::InvalidData
        );
        // 9009 special-cased to Unsupported
        assert_eq!(mapped(Error::DiscUrlNotDirect), ErrorKind::Unsupported);
    }
}
