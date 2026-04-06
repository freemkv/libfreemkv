/// libfreemkv error codes.
///
/// The library returns structured error codes with context data.
/// Applications are responsible for formatting user-facing messages.
/// This keeps the library locale-independent and testable.

/// Error code table.
///
/// | Code | Name | Meaning |
/// |------|------|---------|
/// | 1000 | DeviceNotFound | Device path doesn't exist or can't be opened |
/// | 1001 | DevicePermission | Device exists but permission denied |
/// | 2000 | UnsupportedDrive | Drive not in profile database |
/// | 2001 | ProfileNotFound | Specific firmware version not in database |
/// | 2002 | ProfileParse | Profile database is malformed |
/// | 3000 | UnlockFailed | Drive rejected unlock command |
/// | 3001 | SignatureMismatch | Wrong signature returned by drive |
/// | 3002 | NotUnlocked | Raw read attempted before unlock |
/// | 3003 | NotCalibrated | Raw read attempted before calibrate |
/// | 4000 | ScsiError | SCSI command failed |
/// | 4001 | ScsiTimeout | SCSI command timed out |
/// | 5000 | IoError | OS-level I/O error |
#[derive(Debug)]
pub enum Error {
    // 1xxx — Device errors
    DeviceNotFound { path: String },
    DevicePermission { path: String },

    // 2xxx — Profile errors
    UnsupportedDrive { vendor_id: String, product_id: String, product_revision: String },
    ProfileNotFound { vendor_id: String, product_revision: String, vendor_specific: String },
    ProfileParse { detail: String },

    // 3xxx — Unlock errors
    UnlockFailed { detail: String },
    SignatureMismatch { expected: [u8; 4], got: [u8; 4] },
    NotUnlocked,
    NotCalibrated,

    // 4xxx — SCSI errors
    ScsiError { opcode: u8, status: u8, sense_key: u8 },
    ScsiTimeout { opcode: u8 },

    // 5xxx — I/O errors
    IoError { source: std::io::Error },

    // 6xxx — Disc format errors
    DiscError { detail: String },
}

impl Error {
    /// Numeric error code for programmatic handling.
    pub fn code(&self) -> u16 {
        match self {
            Error::DeviceNotFound { .. }    => 1000,
            Error::DevicePermission { .. }  => 1001,
            Error::UnsupportedDrive { .. }  => 2000,
            Error::ProfileNotFound { .. }   => 2001,
            Error::ProfileParse { .. }      => 2002,
            Error::UnlockFailed { .. }      => 3000,
            Error::SignatureMismatch { .. } => 3001,
            Error::NotUnlocked              => 3002,
            Error::NotCalibrated            => 3003,
            Error::ScsiError { .. }         => 4000,
            Error::ScsiTimeout { .. }       => 4001,
            Error::IoError { .. }           => 5000,
            Error::DiscError { .. }         => 6000,
        }
    }
}

/// Default Display — terse, for logs. Applications should format their own messages.
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::DeviceNotFound { path } => write!(f, "E1000: device not found: {path}"),
            Error::DevicePermission { path } => write!(f, "E1001: permission denied: {path}"),
            Error::UnsupportedDrive { vendor_id, product_id, product_revision } =>
                write!(f, "E2000: unsupported drive: {} {} {}", vendor_id.trim(), product_id.trim(), product_revision.trim()),
            Error::ProfileNotFound { vendor_id, product_revision, vendor_specific } =>
                write!(f, "E2001: no profile: {} {} {}", vendor_id.trim(), product_revision.trim(), vendor_specific.trim()),
            Error::ProfileParse { detail } => write!(f, "E2002: profile parse: {detail}"),
            Error::UnlockFailed { detail } => write!(f, "E3000: unlock failed: {detail}"),
            Error::SignatureMismatch { expected, got } =>
                write!(f, "E3001: signature mismatch: expected {:02x}{:02x}{:02x}{:02x} got {:02x}{:02x}{:02x}{:02x}",
                    expected[0], expected[1], expected[2], expected[3],
                    got[0], got[1], got[2], got[3]),
            Error::NotUnlocked => write!(f, "E3002: not unlocked"),
            Error::NotCalibrated => write!(f, "E3003: not calibrated"),
            Error::ScsiError { opcode, status, sense_key } =>
                write!(f, "E4000: SCSI 0x{opcode:02x} failed: status=0x{status:02x} sense=0x{sense_key:02x}"),
            Error::ScsiTimeout { opcode } => write!(f, "E4001: SCSI 0x{opcode:02x} timeout"),
            Error::IoError { source } => write!(f, "E5000: {source}"),
            Error::DiscError { detail } => write!(f, "E6000: disc: {detail}"),
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
