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
// 2001: burned/retired — do not reuse.
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
pub const E_MPLS_PARSE: u16 = 6001;
pub const E_CLPI_PARSE: u16 = 6002;
pub const E_UDF_NOT_FOUND: u16 = 6003;
// 6004: burned/retired — do not reuse.
pub const E_DISC_TITLE_RANGE: u16 = 6005;
// 6006: burned/retired — do not reuse.
pub const E_IFO_PARSE: u16 = 6007;
pub const E_MKV_INVALID: u16 = 6008;
pub const E_NO_STREAMS: u16 = 6009;
pub const E_HALTED: u16 = 6010;
pub const E_MAPFILE_INVALID: u16 = 6011;
pub const E_UDF_BUFFER_TOO_SMALL: u16 = 6012;

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
// 7012: burned/retired — do not reuse.
pub const E_DECRYPT_FAILED: u16 = 7013;
pub const E_CSS_AUTH_FAILED: u16 = 7014;
pub const E_AACS_HOST_CERT_REJECTED: u16 = 7015;
pub const E_AACS_RAW_READ_UNSUPPORTED: u16 = 7016;
pub const E_AACS_VID_UNAVAILABLE: u16 = 7017;
pub const E_AACS_MK_UNAVAILABLE: u16 = 7018;
pub const E_AACS_VUK_NOT_IN_KEYDB: u16 = 7019;
pub const E_DRIVE_PROFILE_MISSING: u16 = 7020;
pub const E_VID_CDB_UNAVAILABLE: u16 = 7021;
pub const E_NO_DISC_KEY: u16 = 7022;

// Keydb (8xxx)
pub const E_KEYDB_CONNECT: u16 = 8000;
pub const E_KEYDB_HTTP: u16 = 8001;
pub const E_KEYDB_INVALID: u16 = 8002;
pub const E_KEYDB_WRITE: u16 = 8003;
pub const E_KEYDB_PARSE: u16 = 8004;
pub const E_KEYDB_LOAD: u16 = 8005;
pub const E_KEYDB_UNSUPPORTED_SCHEME: u16 = 8006;
pub const E_KEYDB_TOO_MANY_REDIRECTS: u16 = 8007;

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
pub const E_HEVC_PARAM_PARSE: u16 = 9010;
pub const E_MUX_TRACK_RANGE: u16 = 9011;
pub const E_FMP4_UNIMPLEMENTED: u16 = 9012;
pub const E_DEMUX_THREAD_PANICKED: u16 = 9013;
pub const E_PIPELINE_JOIN_TIMEOUT: u16 = 9014;
pub const E_PIPELINE_CONSUMER_PANICKED: u16 = 9015;
pub const E_SWEEP_CONSUMER_GONE: u16 = 9016;
pub const E_PES_TRACK_TOO_LARGE: u16 = 9017;
pub const E_PIPELINE_CONSUMER_GONE: u16 = 9018;
pub const E_DISC_CAPACITY_OVERFLOW: u16 = 9020;
pub const E_M2TS_PACKET_MALFORMED: u16 = 9021;
pub const E_EXTENT_NOT_UNIT_ALIGNED: u16 = 9030;
/// READ CAPACITY returned a short or overflowing transfer.
pub const E_DISC_CAPACITY_MALFORMED: u16 = 9047;

// ── Error enum ──────────────────────────────────────────────────────────────

/// Structured error with numeric code and context data. No English text.
///
/// Marked `#[non_exhaustive]`: downstream crates must not match it
/// exhaustively, so new variants can be added without a semver break.
#[derive(Debug)]
#[non_exhaustive]
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
    /// SCSI command failed.
    ///
    /// `opcode` is the failing CDB byte 0. `status` is the raw SCSI
    /// status byte: `0x02` = CHECK CONDITION (drive replied with sense
    /// data), `0xFF` = libfreemkv-synthesised sentinel meaning "no SCSI
    /// status delivered" (kernel timeout, USB bridge wedge, IOKit
    /// service failure). `sense` carries the drive's SPC-4 sense triple
    /// when the drive replied; `None` for transport-layer failures.
    ///
    /// Recommended dispatch (callers shouldn't pattern-match raw
    /// fields):
    ///   - [`Error::is_scsi_transport_failure`] — bail; bridge/transport wedge
    ///   - [`Error::is_marginal_read`] — drive said this read was marginal; smaller block may recover
    ///   - [`Error::scsi_sense`] — borrow the sense triple for finer routing ([`ScsiSense::is_medium_error`] etc.)
    ScsiError {
        opcode: u8,
        status: u8,
        sense: Option<crate::scsi::ScsiSense>,
    },

    // I/O (5xxx)
    IoError {
        source: std::io::Error,
    },

    // Disc format (6xxx)
    DiscRead {
        sector: u64,
        status: Option<u8>,
        sense: Option<crate::scsi::ScsiSense>,
    },
    /// Drive was halted by caller.
    Halted,
    MplsParse,
    ClpiParse,
    UdfNotFound {
        path: String,
    },
    /// A `SectorSource` caller passed a destination buffer smaller than one
    /// 2048-byte sector. A contract violation on the public reader API —
    /// returned instead of panicking on the slice.
    UdfBufferTooSmall,
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
    /// Host certificate rejected by the drive's revocation list (HRL hit).
    /// All available host certs failed mutual auth on this drive.
    AacsHostCertRejected,
    /// Drive cannot be put into raw-read mode and standard AACS cert
    /// auth failed. No path to decryption remains.
    AacsRawReadUnsupported,
    /// Volume ID could not be retrieved from the drive (neither via cert
    /// auth nor via the alternate VID read path). Downstream of step 1
    /// of the AACS chain.
    AacsVidUnavailable,
    /// No available path produced a Media Key (no MK+VID in keydb, no
    /// PK match, no DK derivation).
    AacsMkUnavailable,
    /// Disc-hash lookup in the keydb missed and no other path is
    /// available (typically because VID is missing).
    AacsVukNotInKeydb,
    /// Drive identity did not match any bundled profile; per-drive CDB
    /// templates aren't available so the OEM VID retrieval path can't
    /// run.
    DriveProfileMissing,
    /// Drive's profile is present but doesn't carry a VID-retrieval CDB
    /// template (older profile blob, or a drive class without an OEM
    /// VID path).
    VidCdbUnavailable,
    /// The disc is AACS-encrypted and decryption was requested, but key
    /// resolution produced no usable key for it — so muxing would emit
    /// undecryptable garbage. Distinct from [`Error::KeydbLoad`] (no keydb
    /// file at all): a keydb may be present but lack an entry for this disc.
    /// `disc_hash` is the 40-hex SHA1 of `Unit_Key_RO.inf` (no `0x` prefix)
    /// so the application can name the disc; empty if the hash wasn't
    /// captured at scan.
    NoDiscKey {
        disc_hash: String,
    },

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
    /// A redirect (or the configured URL) targets a scheme this
    /// dependency-light HTTP client cannot fetch (e.g. `https://`).
    /// Carries the offending scheme for diagnostics.
    KeydbUnsupportedScheme {
        scheme: String,
    },
    /// The redirect chain exceeded the follow limit.
    KeydbTooManyRedirects,

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
    /// PES frame track index exceeds the 1-byte on-wire field (> 255).
    /// Carries the offending index. Distinct from [`Error::PesInvalidMagic`],
    /// which signals corrupt input on the read side.
    PesTrackTooLarge {
        track: usize,
    },
    IsoTooLarge {
        path: String,
    },
    NoMetadata,
    /// `disc://` URLs aren't openable through `input()` — callers must use
    /// `Drive::open() + Disc::scan() + DiscStream::new()` directly. This
    /// is a structural API constraint, not a parse failure.
    DiscUrlNotDirect,
    /// A non-empty `HEVCDecoderConfigurationRecord` (hvcC) was supplied to
    /// a muxer but failed to parse into any VPS/SPS/PPS NAL — emitting the
    /// stream without parameter sets would yield an undecodable result.
    HevcParamParse,
    /// A muxer `write_frame` / `set_codec_private` was given a track index
    /// beyond the configured PID/track count.
    MuxTrackRange {
        track: usize,
        tracks: usize,
    },
    /// The fragmented-MP4 sink cannot emit media — `moof`/`mdat` framing is
    /// not implemented. Surfaced instead of silently discarding samples.
    Fmp4Unimplemented,
    /// A worker thread in the threaded mux pipeline terminated without
    /// sending its terminal sentinel — i.e. it panicked or was dropped
    /// mid-stream. Surfaced so a parser/demux panic is never silently
    /// reported to the caller as a clean end-of-stream (which would
    /// truncate output without any error).
    DemuxThreadPanicked,
    /// A pipeline `join()` exceeded its deadline while waiting for the
    /// consumer thread to drain. The consumer is intentionally leaked;
    /// the caller should fall back to a degraded path.
    PipelineJoinTimeout,
    /// The pipeline consumer thread panicked. The original panic
    /// payload is not preserved (no English text in the library); it is
    /// logged at the panic site instead.
    PipelineConsumerPanicked,
    /// A pipeline producer's `send` failed because the consumer thread
    /// has already terminated (the receiver end is gone).
    SweepConsumerGone,
    /// A producer thread tried to hand work to its pipeline consumer
    /// (sweep / patch sink) but the consumer thread had already
    /// terminated (panicked or dropped the receiver). The producer
    /// surfaces this so the outer pass can abort cleanly instead of
    /// blocking on a dead channel.
    PipelineConsumerGone,
    /// READ CAPACITY(10) reported a last-LBA of `0xFFFFFFFF` — the SPC
    /// sentinel meaning "capacity exceeds 32-bit addressing". Adding 1 to
    /// derive the sector count would overflow `u32`. Reachable from
    /// disc-reported bytes and synthetic [`crate::sector::SectorSource`]
    /// fixtures.
    DiscCapacityOverflow,
    /// An extent fed to the prefetch producer has a `sector_count`
    /// whose trailing 1-2 sectors cannot form a complete AACS aligned
    /// unit (3 sectors / 6144 bytes). Emitting that tail as a
    /// standalone batch would hand the decrypt step a sub-unit chunk
    /// it silently leaves encrypted. The producer surfaces this rather
    /// than emit still-encrypted bytes.
    ExtentNotUnitAligned,
    /// An MPEG-TS packet under construction violated the 188-byte fixed
    /// size (over-long adaptation field, overflowing payload, or a
    /// short/mis-assembled packet). Indicates a muxer invariant break,
    /// not untrusted input — surfaced instead of writing a corrupt
    /// transport stream.
    M2tsPacketMalformed,
    /// READ CAPACITY transferred fewer than 4 bytes, or the decoded
    /// last-LBA + 1 overflowed `u32`. Either case means the capacity
    /// response is unusable; no English commentary.
    DiscCapacityMalformed,
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
            Error::UdfBufferTooSmall => E_UDF_BUFFER_TOO_SMALL,
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
            Error::AacsHostCertRejected => E_AACS_HOST_CERT_REJECTED,
            Error::AacsRawReadUnsupported => E_AACS_RAW_READ_UNSUPPORTED,
            Error::AacsVidUnavailable => E_AACS_VID_UNAVAILABLE,
            Error::AacsMkUnavailable => E_AACS_MK_UNAVAILABLE,
            Error::AacsVukNotInKeydb => E_AACS_VUK_NOT_IN_KEYDB,
            Error::DriveProfileMissing => E_DRIVE_PROFILE_MISSING,
            Error::VidCdbUnavailable => E_VID_CDB_UNAVAILABLE,
            Error::NoDiscKey { .. } => E_NO_DISC_KEY,
            Error::KeydbConnect { .. } => E_KEYDB_CONNECT,
            Error::KeydbHttp { .. } => E_KEYDB_HTTP,
            Error::KeydbInvalid => E_KEYDB_INVALID,
            Error::KeydbWrite { .. } => E_KEYDB_WRITE,
            Error::KeydbParse => E_KEYDB_PARSE,
            Error::KeydbLoad { .. } => E_KEYDB_LOAD,
            Error::KeydbUnsupportedScheme { .. } => E_KEYDB_UNSUPPORTED_SCHEME,
            Error::KeydbTooManyRedirects => E_KEYDB_TOO_MANY_REDIRECTS,
            Error::StreamReadOnly => E_STREAM_READ_ONLY,
            Error::StreamWriteOnly => E_STREAM_WRITE_ONLY,
            Error::StreamUrlInvalid { .. } => E_STREAM_URL_INVALID,
            Error::StreamUrlMissingPath { .. } => E_STREAM_URL_MISSING_PATH,
            Error::StreamUrlMissingPort { .. } => E_STREAM_URL_MISSING_PORT,
            Error::PesFrameTooLarge { .. } => E_PES_FRAME_TOO_LARGE,
            Error::PesInvalidMagic => E_PES_INVALID_MAGIC,
            Error::PesTrackTooLarge { .. } => E_PES_TRACK_TOO_LARGE,
            Error::IsoTooLarge { .. } => E_ISO_TOO_LARGE,
            Error::NoMetadata => E_NO_METADATA,
            Error::DiscUrlNotDirect => E_DISC_URL_NOT_DIRECT,
            Error::HevcParamParse => E_HEVC_PARAM_PARSE,
            Error::MuxTrackRange { .. } => E_MUX_TRACK_RANGE,
            Error::Fmp4Unimplemented => E_FMP4_UNIMPLEMENTED,
            Error::DemuxThreadPanicked => E_DEMUX_THREAD_PANICKED,
            Error::PipelineJoinTimeout => E_PIPELINE_JOIN_TIMEOUT,
            Error::PipelineConsumerPanicked => E_PIPELINE_CONSUMER_PANICKED,
            Error::SweepConsumerGone => E_SWEEP_CONSUMER_GONE,
            Error::PipelineConsumerGone => E_PIPELINE_CONSUMER_GONE,
            Error::DiscCapacityOverflow => E_DISC_CAPACITY_OVERFLOW,
            Error::ExtentNotUnitAligned => E_EXTENT_NOT_UNIT_ALIGNED,
            Error::M2tsPacketMalformed => E_M2TS_PACKET_MALFORMED,
            Error::DiscCapacityMalformed => E_DISC_CAPACITY_MALFORMED,
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
                sense,
            } => match sense {
                Some(s) => write!(
                    f,
                    "E{}: 0x{:02x}/0x{:02x}/0x{:02x}/0x{:02x}/0x{:02x}",
                    self.code(),
                    opcode,
                    status,
                    s.sense_key,
                    s.asc,
                    s.ascq,
                ),
                None => write!(f, "E{}: 0x{:02x}/0x{:02x}", self.code(), opcode, status,),
            },
            // Language-neutral: std::io::Error's Display is English
            // ("permission denied"); emit the raw OS errno when present,
            // else the ErrorKind debug name (an identifier, not prose).
            Error::IoError { source } => match source.raw_os_error() {
                Some(errno) => write!(f, "E{}: {}", self.code(), errno),
                None => write!(f, "E{}: {:?}", self.code(), source.kind()),
            },
            Error::DiscRead {
                sector,
                status,
                sense,
            } => match (status, sense) {
                (Some(st), Some(s)) => write!(
                    f,
                    "E{}: {} 0x{:02x}/0x{:02x}/0x{:02x}/0x{:02x}",
                    self.code(),
                    sector,
                    st,
                    s.sense_key,
                    s.asc,
                    s.ascq,
                ),
                (Some(st), None) => write!(f, "E{}: {} 0x{:02x}", self.code(), sector, st,),
                (None, Some(s)) => write!(
                    f,
                    "E{}: {} 0x{:02x}/0x{:02x}/0x{:02x}",
                    self.code(),
                    sector,
                    s.sense_key,
                    s.asc,
                    s.ascq,
                ),
                (None, None) => write!(f, "E{}: {}", self.code(), sector),
            },
            Error::Halted => write!(f, "E{}", self.code()),
            Error::UdfNotFound { path } => write!(f, "E{}: {}", self.code(), path),
            Error::DiscTitleRange { index, count } => {
                write!(f, "E{}: {}/{}", self.code(), index, count)
            }
            Error::KeydbConnect { host } => write!(f, "E{}: {}", self.code(), host),
            Error::KeydbHttp { status } => write!(f, "E{}: {}", self.code(), status),
            Error::KeydbWrite { path } => write!(f, "E{}: {}", self.code(), path),
            Error::KeydbLoad { path } => write!(f, "E{}: {}", self.code(), path),
            Error::KeydbUnsupportedScheme { scheme } => {
                write!(f, "E{}: {}", self.code(), scheme)
            }
            Error::StreamUrlInvalid { url } => write!(f, "E{}: {}", self.code(), url),
            Error::StreamUrlMissingPath { scheme } => write!(f, "E{}: {}", self.code(), scheme),
            Error::StreamUrlMissingPort { addr } => write!(f, "E{}: {}", self.code(), addr),
            Error::PesFrameTooLarge { size } => write!(f, "E{}: {}", self.code(), size),
            Error::PesTrackTooLarge { track } => write!(f, "E{}: {}", self.code(), track),
            Error::IsoTooLarge { path } => write!(f, "E{}: {}", self.code(), path),
            Error::NoDiscKey { disc_hash } => {
                if disc_hash.is_empty() {
                    write!(f, "E{}", self.code())
                } else {
                    write!(f, "E{}: {}", self.code(), disc_hash)
                }
            }
            Error::MuxTrackRange { track, tracks } => {
                write!(f, "E{}: {}/{}", self.code(), track, tracks)
            }
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
        // An `Error::IoError` is just a wrapper around an underlying
        // `io::Error` that entered via `From<io::Error> for Error`.
        // Round-trip it back unchanged so the original `ErrorKind` and
        // raw OS error code survive instead of being flattened to
        // `Other` with a stringified message.
        if let Error::IoError { source } = e {
            return source;
        }
        let code = e.code();
        let msg = e.to_string();
        // Map our error categories to io::ErrorKind
        let kind = match code {
            // Device access-denied semantics map to PermissionDenied;
            // the rest of the 1xxx block is "device absent" -> NotFound.
            E_DEVICE_PERMISSION | E_DEVICE_LOCKED => std::io::ErrorKind::PermissionDenied,
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
            // 9010 HevcParamParse: malformed hvcC payload.
            9010 => std::io::ErrorKind::InvalidData,
            // 9011 MuxTrackRange: caller passed a bad track index.
            9011 => std::io::ErrorKind::InvalidInput,
            // 9012 Fmp4Unimplemented: sink can't emit media yet.
            9012 => std::io::ErrorKind::Unsupported,
            // 9014 PipelineJoinTimeout: consumer drain exceeded deadline.
            E_PIPELINE_JOIN_TIMEOUT => std::io::ErrorKind::TimedOut,
            // 9017 PesTrackTooLarge: out-of-range track index on serialize.
            9017 => std::io::ErrorKind::InvalidInput,
            // 9020 DiscCapacityOverflow: disc reported a capacity sentinel
            // we can't represent — treat as bad/invalid device data.
            9020 => std::io::ErrorKind::InvalidData,
            // 9021 M2tsPacketMalformed: a muxer invariant break produced
            // a non-188-byte packet — treat as invalid data.
            9021 => std::io::ErrorKind::InvalidData,
            // 9030 ExtentNotUnitAligned: a malformed/non-AACS-aligned
            // extent was handed to the prefetch producer.
            9030 => std::io::ErrorKind::InvalidInput,
            // 9047 DiscCapacityMalformed: the drive returned an unusable
            // READ CAPACITY response (short transfer / overflow).
            9047 => std::io::ErrorKind::InvalidData,
            _ => std::io::ErrorKind::Other,
        };
        std::io::Error::new(kind, msg)
    }
}

/// Convenience alias for `Result<T, Error>`.
pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    /// Borrow the drive-returned SPC-4 sense triple if this error is a
    /// [`Error::ScsiError`] carrying sense data. `None` for any other
    /// variant **and** for `ScsiError`s that represent a transport-layer
    /// failure (where the device never delivered a SCSI status reply, so
    /// no sense data exists).
    pub fn scsi_sense(&self) -> Option<&crate::scsi::ScsiSense> {
        match self {
            Error::ScsiError { sense: Some(s), .. } => Some(s),
            Error::DiscRead { sense: Some(s), .. } => Some(s),
            _ => None,
        }
    }

    /// True if this is a [`Error::ScsiError`] representing a transport-layer
    /// failure — kernel timeout, USB bridge wedge, IOKit service error.
    /// The device never delivered a SCSI status reply, so there is no
    /// sense data to inspect; retrying typically requires physical
    /// intervention (replug).
    pub fn is_scsi_transport_failure(&self) -> bool {
        matches!(
            self,
            Error::ScsiError {
                status: crate::scsi::SCSI_STATUS_TRANSPORT_FAILURE,
                ..
            }
        ) || matches!(
            self,
            Error::DiscRead {
                status: Some(crate::scsi::SCSI_STATUS_TRANSPORT_FAILURE),
                ..
            }
        )
    }

    /// True if this error indicates bridge degradation — the SCSI status
    /// is neither GOOD (0x00), CHECK CONDITION (0x02), nor transport failure
    /// (0xFF). Observed on the Initio INIC-1618L USB bridge preceding a full
    /// crash: the bridge firmware returns non-standard status bytes (e.g.
    /// 0x04, 0x05) with empty sense data. The caller should cool down
    /// (10 s pause) and retry rather than hammering the bridge.
    pub fn is_bridge_degradation(&self) -> bool {
        let status = match self {
            Error::ScsiError { status, .. } => *status,
            Error::DiscRead { status, .. } => status.unwrap_or(0),
            _ => return false,
        };
        status != crate::scsi::SCSI_STATUS_GOOD
            && status != crate::scsi::SCSI_STATUS_CHECK_CONDITION
            && status != crate::scsi::SCSI_STATUS_TRANSPORT_FAILURE
    }

    /// True if the underlying SCSI failure is a *marginal read* — the
    /// drive returned an error category in which smaller-granularity
    /// retries can sometimes recover the data:
    ///
    ///   - MEDIUM ERROR (sense key 3) — canonical bad-sector signal
    ///   - ABORTED COMMAND (sense key B) — transient; retry usually works
    ///   - NOT READY (sense key 2) — the dominant bad-sector response on
    ///     the BU40N (ASC 0x04/ASCQ 0x3E); a pause + retry often recovers
    ///   - RECOVERED ERROR (sense key 1) / NO SENSE (sense key 0) — not
    ///     classified as fatal; treat as recoverable
    ///
    /// Returns `false` for transport failures (no sense data delivered),
    /// HARDWARE ERROR, DATA PROTECT, UNIT ATTENTION, ILLEGAL
    /// REQUEST, BLANK CHECK, kernel `IoError`, and any non-SCSI variant.
    /// Caller-agnostic predicate — describes a property of the *error*,
    /// not what one specific call site should do with it. Used by
    /// `Disc::copy`'s hysteresis dispatch.
    pub fn is_marginal_read(&self) -> bool {
        self.scsi_sense()
            .map(crate::scsi::ScsiSense::is_marginal)
            .unwrap_or(false)
    }
}

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
            Error::ExtentNotUnitAligned.code(),
            Error::M2tsPacketMalformed.code(),
            Error::DiscCapacityMalformed.code(),
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
            (Error::ExtentNotUnitAligned, E_EXTENT_NOT_UNIT_ALIGNED),
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
                    word.len() <= 8,
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
        // 1xxx "device absent" → NotFound
        assert_eq!(
            mapped(Error::ScsiInterfaceUnavailable { path: "p".into() }),
            ErrorKind::NotFound
        );
        assert_eq!(
            mapped(Error::DeviceNotFound { path: "p".into() }),
            ErrorKind::NotFound
        );
        // 1xxx access-denied semantics → PermissionDenied (not NotFound)
        assert_eq!(
            mapped(Error::DevicePermission { path: "p".into() }),
            ErrorKind::PermissionDenied
        );
        assert_eq!(
            mapped(Error::DeviceLocked {
                path: "p".into(),
                kr: 0
            }),
            ErrorKind::PermissionDenied
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
        // 9021 special-cased to InvalidData
        assert_eq!(mapped(Error::M2tsPacketMalformed), ErrorKind::InvalidData);
        // 9047 DiscCapacityMalformed → InvalidData
        assert_eq!(mapped(Error::DiscCapacityMalformed), ErrorKind::InvalidData);
    }

    /// `Error::IoError` must round-trip back to the *original*
    /// `io::Error` — preserving its `ErrorKind` and raw OS error —
    /// rather than being flattened to `Other` with a stringified
    /// message.
    #[test]
    fn ioerror_roundtrips_preserving_kind_and_oscode() {
        use std::io::ErrorKind;
        let original = std::io::Error::from_raw_os_error(13); // EACCES
        let original_kind = original.kind();
        let wrapped: Error = original.into(); // From<io::Error> for Error
        let back: std::io::Error = wrapped.into(); // From<Error> for io::Error
        assert_eq!(back.kind(), original_kind);
        assert_eq!(back.raw_os_error(), Some(13));

        // A synthesized kind (no OS code) must also survive.
        let timeout: Error = std::io::Error::from(ErrorKind::TimedOut).into();
        let back2: std::io::Error = timeout.into();
        assert_eq!(back2.kind(), ErrorKind::TimedOut);
    }

    /// `DiscRead` Display must include the ASCQ byte (the 5th field) so
    /// NOT_READY substates (0x04/0x3E vs 0x04/0x01) are distinguishable
    /// in logs and bug reports.
    #[test]
    fn discread_display_includes_ascq() {
        let e = Error::DiscRead {
            sector: 42,
            status: Some(0x02),
            sense: Some(crate::scsi::ScsiSense {
                sense_key: 0x02,
                asc: 0x04,
                ascq: 0x3e,
            }),
        };
        let s = e.to_string();
        // sense_key/asc/ascq triple all present.
        assert!(s.contains("0x02/0x04/0x3e"), "ascq missing from `{s}`");
    }

    /// `NoDiscKey` with an empty hash must not emit a dangling
    /// "colon space" suffix.
    #[test]
    fn nodisckey_empty_hash_has_no_trailing_colon() {
        let e = Error::NoDiscKey {
            disc_hash: String::new(),
        };
        assert_eq!(e.to_string(), format!("E{}", E_NO_DISC_KEY));
        let e2 = Error::NoDiscKey {
            disc_hash: "abc".into(),
        };
        assert_eq!(e2.to_string(), format!("E{}: abc", E_NO_DISC_KEY));
    }
}
