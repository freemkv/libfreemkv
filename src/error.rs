use std::fmt;

#[derive(Debug)]
pub enum Error {
    DeviceNotFound(String),
    UnsupportedDrive(String),
    ScsiError { cdb: Vec<u8>, status: u8, sense: Vec<u8> },
    UnlockFailed(String),
    NotUnlocked,
    NotCalibrated,
    ProfileNotFound(String),
    ProfileParse(String),
    SignatureMismatch { expected: [u8; 4], got: [u8; 4] },
    Io(std::io::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::DeviceNotFound(s) => write!(f, "device not found: {s}"),
            Error::UnsupportedDrive(s) => write!(f, "unsupported drive: {s}"),
            Error::ScsiError { status, .. } => write!(f, "SCSI error: status 0x{status:02x}"),
            Error::UnlockFailed(s) => write!(f, "unlock failed: {s}"),
            Error::NotUnlocked => write!(f, "drive not unlocked, call unlock() first"),
            Error::NotCalibrated => write!(f, "speed not calibrated, call calibrate() first"),
            Error::ProfileNotFound(s) => write!(f, "no profile for: {s}"),
            Error::ProfileParse(s) => write!(f, "profile parse error: {s}"),
            Error::SignatureMismatch { expected, got } => {
                write!(f, "signature mismatch: expected {:02x}{:02x}{:02x}{:02x}, got {:02x}{:02x}{:02x}{:02x}",
                    expected[0], expected[1], expected[2], expected[3],
                    got[0], got[1], got[2], got[3])
            }
            Error::Io(e) => write!(f, "I/O error: {e}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}

pub type Result<T> = std::result::Result<T, Error>;
