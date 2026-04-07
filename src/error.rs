//! Error types for libfreemkv.
//!
//! Every error carries a numeric code for programmatic handling.
//! No user-facing English text — applications format their own messages.
//! This keeps the library locale-independent and testable.
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

// ── Error codes (single source of truth) ────────────────────────────────────

pub const E_DEVICE_NOT_FOUND: u16     = 1000;
pub const E_DEVICE_PERMISSION: u16    = 1001;
pub const E_UNSUPPORTED_DRIVE: u16    = 2000;
pub const E_PROFILE_NOT_FOUND: u16    = 2001;
pub const E_PROFILE_PARSE: u16        = 2002;
pub const E_UNLOCK_FAILED: u16        = 3000;
pub const E_SIGNATURE_MISMATCH: u16   = 3001;
pub const E_NOT_UNLOCKED: u16         = 3002;
pub const E_NOT_CALIBRATED: u16       = 3003;
pub const E_SCSI_ERROR: u16           = 4000;
pub const E_SCSI_TIMEOUT: u16         = 4001;
pub const E_IO_ERROR: u16             = 5000;
pub const E_DISC_ERROR: u16           = 6000;
pub const E_AACS_ERROR: u16           = 7000;

// ── Error enum ──────────────────────────────────────────────────────────────

/// Structured error with numeric code and context data.
#[derive(Debug)]
pub enum Error {
    DeviceNotFound { path: String },
    DevicePermission { path: String },
    UnsupportedDrive { vendor_id: String, product_id: String, product_revision: String },
    ProfileNotFound { vendor_id: String, product_revision: String, vendor_specific: String },
    ProfileParse { detail: String },
    UnlockFailed { detail: String },
    SignatureMismatch { expected: [u8; 4], got: [u8; 4] },
    NotUnlocked,
    NotCalibrated,
    ScsiError { opcode: u8, status: u8, sense_key: u8 },
    ScsiTimeout { opcode: u8 },
    IoError { source: std::io::Error },
    DiscError { detail: String },
    AacsError { detail: String },
}

impl Error {
    /// Numeric error code.
    pub fn code(&self) -> u16 {
        match self {
            Error::DeviceNotFound { .. }    => E_DEVICE_NOT_FOUND,
            Error::DevicePermission { .. }  => E_DEVICE_PERMISSION,
            Error::UnsupportedDrive { .. }  => E_UNSUPPORTED_DRIVE,
            Error::ProfileNotFound { .. }   => E_PROFILE_NOT_FOUND,
            Error::ProfileParse { .. }      => E_PROFILE_PARSE,
            Error::UnlockFailed { .. }      => E_UNLOCK_FAILED,
            Error::SignatureMismatch { .. } => E_SIGNATURE_MISMATCH,
            Error::NotUnlocked              => E_NOT_UNLOCKED,
            Error::NotCalibrated            => E_NOT_CALIBRATED,
            Error::ScsiError { .. }         => E_SCSI_ERROR,
            Error::ScsiTimeout { .. }       => E_SCSI_TIMEOUT,
            Error::IoError { .. }           => E_IO_ERROR,
            Error::DiscError { .. }         => E_DISC_ERROR,
            Error::AacsError { .. }         => E_AACS_ERROR,
        }
    }
}

/// Display format: "E{code}: {context}" — terse, for logs.
/// Applications should format their own user-facing messages using code() and fields.
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::DeviceNotFound { path } =>
                write!(f, "E{}: {}", E_DEVICE_NOT_FOUND, path),
            Error::DevicePermission { path } =>
                write!(f, "E{}: {}", E_DEVICE_PERMISSION, path),
            Error::UnsupportedDrive { vendor_id, product_id, product_revision } =>
                write!(f, "E{}: {} {} {}", E_UNSUPPORTED_DRIVE,
                    vendor_id.trim(), product_id.trim(), product_revision.trim()),
            Error::ProfileNotFound { vendor_id, product_revision, vendor_specific } =>
                write!(f, "E{}: {} {} {}", E_PROFILE_NOT_FOUND,
                    vendor_id.trim(), product_revision.trim(), vendor_specific.trim()),
            Error::ProfileParse { detail } =>
                write!(f, "E{}: {}", E_PROFILE_PARSE, detail),
            Error::UnlockFailed { detail } =>
                write!(f, "E{}: {}", E_UNLOCK_FAILED, detail),
            Error::SignatureMismatch { expected, got } =>
                write!(f, "E{}: expected {:02x}{:02x}{:02x}{:02x} got {:02x}{:02x}{:02x}{:02x}",
                    E_SIGNATURE_MISMATCH,
                    expected[0], expected[1], expected[2], expected[3],
                    got[0], got[1], got[2], got[3]),
            Error::NotUnlocked =>
                write!(f, "E{}", E_NOT_UNLOCKED),
            Error::NotCalibrated =>
                write!(f, "E{}", E_NOT_CALIBRATED),
            Error::ScsiError { opcode, status, sense_key } =>
                write!(f, "E{}: opcode=0x{:02x} status=0x{:02x} sense=0x{:02x}",
                    E_SCSI_ERROR, opcode, status, sense_key),
            Error::ScsiTimeout { opcode } =>
                write!(f, "E{}: opcode=0x{:02x}", E_SCSI_TIMEOUT, opcode),
            Error::IoError { source } =>
                write!(f, "E{}: {}", E_IO_ERROR, source),
            Error::DiscError { detail } =>
                write!(f, "E{}: {}", E_DISC_ERROR, detail),
            Error::AacsError { detail } =>
                write!(f, "E{}: {}", E_AACS_ERROR, detail),
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
