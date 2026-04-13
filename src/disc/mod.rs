//! Disc structure -- scan titles, streams, and sector ranges from a Blu-ray disc.
//!
//! This is the high-level API for disc content. The CLI calls this,
//! never parses MPLS/CLPI/UDF directly.
//!
//! Usage:
//!   let disc = Disc::scan(&mut session)?;
//!   for title in disc.titles() { ... }
//!   for stream in title.streams() { ... }

mod bluray;
mod dvd;
mod encrypt;

use crate::drive::Drive;
use crate::error::{Error, Result};
use crate::sector::SectorReader;
use crate::speed::DriveSpeed;
use crate::udf;

use encrypt::HandshakeResult;

// ─── Public types ───────────────────────────────────────────────────────────

/// A scanned Blu-ray disc.
#[derive(Debug)]
pub struct Disc {
    /// UDF Volume Identifier from Primary Volume Descriptor (always present)
    pub volume_id: String,
    /// Disc title from META/DL/bdmt_eng.xml (None if disc has no metadata)
    pub meta_title: Option<String>,
    /// Disc format (BD, UHD, DVD)
    pub format: DiscFormat,
    /// Disc capacity in sectors
    pub capacity_sectors: u32,
    /// Disc capacity in bytes
    pub capacity_bytes: u64,
    /// Number of layers (1 = single, 2 = dual)
    pub layers: u8,
    /// Titles sorted by duration (longest first), then playlist name
    pub titles: Vec<DiscTitle>,
    /// Disc region
    pub region: DiscRegion,
    /// AACS state -- None if disc is unencrypted or keys unavailable
    pub aacs: Option<AacsState>,
    /// CSS state -- None if not a CSS-encrypted DVD
    pub css: Option<crate::css::CssState>,
    /// Whether this disc requires decryption (AACS or CSS)
    pub encrypted: bool,
    /// Content format (BD transport stream vs DVD program stream)
    pub content_format: ContentFormat,
}

/// Content format — determines how sectors are interpreted downstream.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ContentFormat {
    /// Blu-ray BD Transport Stream (192-byte packets)
    BdTs,
    /// DVD MPEG-2 Program Stream (VOB)
    MpegPs,
}

/// Disc format.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DiscFormat {
    /// 4K UHD Blu-ray (HEVC 2160p)
    Uhd,
    /// Standard Blu-ray (1080p/1080i)
    BluRay,
    /// DVD
    Dvd,
    /// Unknown
    Unknown,
}

/// Disc playback region.
#[derive(Debug, Clone, PartialEq)]
pub enum DiscRegion {
    /// Region-free (all UHD discs, some BD/DVD)
    Free,
    /// Blu-ray regions (A/B/C or combination)
    BluRay(Vec<BdRegion>),
    /// DVD regions (1-8 or combination)
    Dvd(Vec<u8>),
}

/// Blu-ray region codes.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BdRegion {
    /// Region A/1 -- Americas, East Asia (Japan, Korea, Southeast Asia)
    A,
    /// Region B/2 -- Europe, Africa, Australia, Middle East
    B,
    /// Region C/3 -- Central/South Asia, China, Russia
    C,
}

/// A title (one MPLS playlist).
#[derive(Debug, Clone)]
pub struct DiscTitle {
    /// Playlist filename (e.g. "00800.mpls")
    pub playlist: String,
    /// Playlist number (e.g. 800)
    pub playlist_id: u16,
    /// Duration in seconds
    pub duration_secs: f64,
    /// Total size in bytes
    pub size_bytes: u64,
    /// Clip references in playback order
    pub clips: Vec<Clip>,
    /// All streams (video, audio, subtitle, etc.)
    pub streams: Vec<Stream>,
    /// Chapter points
    pub chapters: Vec<Chapter>,
    /// Sector extents for ripping (clip LBA ranges)
    pub extents: Vec<Extent>,
    /// Content format for this title
    pub content_format: ContentFormat,
}

/// A clip reference within a title.
#[derive(Debug, Clone)]
pub struct Clip {
    /// Clip filename without extension (e.g. "00001")
    pub clip_id: String,
    /// In-time in 45kHz ticks
    pub in_time: u32,
    /// Out-time in 45kHz ticks
    pub out_time: u32,
    /// Duration in seconds
    pub duration_secs: f64,
    /// Source packet count (from CLPI, 0 if unavailable)
    pub source_packets: u32,
}

/// A stream within a title.
#[derive(Debug, Clone)]
pub enum Stream {
    Video(VideoStream),
    Audio(AudioStream),
    Subtitle(SubtitleStream),
}

/// A video stream.
#[derive(Debug, Clone)]
pub struct VideoStream {
    /// MPEG-TS packet ID
    pub pid: u16,
    /// Codec (HEVC, H.264, VC-1, MPEG-2)
    pub codec: Codec,
    /// Resolution (e.g. "2160p", "1080p", "1080i")
    pub resolution: String,
    /// Frame rate (e.g. "23.976", "25")
    pub frame_rate: String,
    /// HDR format
    pub hdr: HdrFormat,
    /// Color space
    pub color_space: ColorSpace,
    /// Whether this is a secondary stream (PiP, Dolby Vision EL)
    pub secondary: bool,
    /// Extra label (e.g. "Dolby Vision EL")
    pub label: String,
}

/// An audio stream.
#[derive(Debug, Clone)]
pub struct AudioStream {
    /// MPEG-TS packet ID
    pub pid: u16,
    /// Codec (TrueHD, DTS-HD MA, DD, LPCM, etc.)
    pub codec: Codec,
    /// Channel layout (e.g. "5.1", "7.1", "stereo", "mono")
    pub channels: String,
    /// ISO 639-2 language code (e.g. "eng", "fra")
    pub language: String,
    /// Sample rate (e.g. "48kHz", "96kHz")
    pub sample_rate: String,
    /// Whether this is a secondary stream (commentary)
    pub secondary: bool,
    /// Extra label
    pub label: String,
}

/// A subtitle stream.
#[derive(Debug, Clone)]
pub struct SubtitleStream {
    /// MPEG-TS packet ID
    pub pid: u16,
    /// Codec (PGS)
    pub codec: Codec,
    /// ISO 639-2 language code (e.g. "eng", "fra")
    pub language: String,
    /// Whether this is a forced subtitle
    pub forced: bool,
    /// Pre-formatted codec private data (e.g. VobSub .idx palette header)
    pub codec_data: Option<Vec<u8>>,
}

/// Video/audio codec.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Codec {
    // Video
    Hevc,
    H264,
    Vc1,
    Mpeg2,
    // Audio
    TrueHd,
    DtsHdMa,
    DtsHdHr,
    Dts,
    Ac3,
    Ac3Plus,
    Lpcm,
    // Subtitle
    Pgs,
    DvdSub,
    // Unknown
    Unknown(u8),
}

/// HDR format.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum HdrFormat {
    Sdr,
    Hdr10,
    DolbyVision,
}

/// Color space.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ColorSpace {
    Bt709,
    Bt2020,
    Unknown,
}

/// A chapter point within a title.
#[derive(Debug, Clone)]
pub struct Chapter {
    /// Chapter start time in seconds
    pub time_secs: f64,
    /// Chapter name (e.g. "Chapter 1", "Chapter 2")
    pub name: String,
}

/// A contiguous range of sectors on disc.
#[derive(Debug, Clone, Copy)]
pub struct Extent {
    pub start_lba: u32,
    pub sector_count: u32,
}

// ─── Display helpers ────────────────────────────────────────────────────────

impl Codec {
    pub fn name(&self) -> &'static str {
        match self {
            Codec::Hevc => "HEVC",
            Codec::H264 => "H.264",
            Codec::Vc1 => "VC-1",
            Codec::Mpeg2 => "MPEG-2",
            Codec::TrueHd => "TrueHD",
            Codec::DtsHdMa => "DTS-HD MA",
            Codec::DtsHdHr => "DTS-HD HR",
            Codec::Dts => "DTS",
            Codec::Ac3 => "AC-3",
            Codec::Ac3Plus => "AC-3+",
            Codec::Lpcm => "LPCM",
            Codec::Pgs => "PGS",
            Codec::DvdSub => "DVD Subtitle",
            Codec::Unknown(_) => "Unknown",
        }
    }

    fn from_coding_type(ct: u8) -> Self {
        match ct {
            0x24 => Codec::Hevc,
            0x1B => Codec::H264,
            0xEA => Codec::Vc1,
            0x02 => Codec::Mpeg2,
            0x83 => Codec::TrueHd,
            0x86 => Codec::DtsHdMa,
            0x85 => Codec::DtsHdHr,
            0x82 => Codec::Dts,
            0x81 => Codec::Ac3,
            0x84 | 0xA1 => Codec::Ac3Plus,
            0x80 => Codec::Lpcm,
            0xA2 => Codec::DtsHdHr,
            0x90 | 0x91 => Codec::Pgs,
            ct => Codec::Unknown(ct),
        }
    }
}

impl HdrFormat {
    pub fn name(&self) -> &'static str {
        match self {
            HdrFormat::Sdr => "SDR",
            HdrFormat::Hdr10 => "HDR10",
            HdrFormat::DolbyVision => "Dolby Vision",
        }
    }
}

impl ColorSpace {
    pub fn name(&self) -> &'static str {
        match self {
            ColorSpace::Bt709 => "BT.709",
            ColorSpace::Bt2020 => "BT.2020",
            ColorSpace::Unknown => "",
        }
    }
}

impl DiscTitle {
    /// Empty DiscTitle with no streams.
    pub fn empty() -> Self {
        Self {
            playlist: String::new(),
            playlist_id: 0,
            duration_secs: 0.0,
            size_bytes: 0,
            clips: Vec::new(),
            streams: Vec::new(),
            chapters: Vec::new(),
            extents: Vec::new(),
            content_format: ContentFormat::BdTs,
        }
    }

    /// Duration formatted as "Xh Ym"
    pub fn duration_display(&self) -> String {
        let hrs = (self.duration_secs / 3600.0) as u32;
        let mins = ((self.duration_secs % 3600.0) / 60.0) as u32;
        format!("{hrs}h {mins:02}m")
    }

    /// Size in GB
    pub fn size_gb(&self) -> f64 {
        self.size_bytes as f64 / (1024.0 * 1024.0 * 1024.0)
    }

    /// Total sectors across all extents
    pub fn total_sectors(&self) -> u64 {
        self.extents.iter().map(|e| e.sector_count as u64).sum()
    }
}

// ─── Encryption ─────────────────────────────────────────────────────────────

/// AACS decryption state for a disc.
#[derive(Debug)]
pub struct AacsState {
    /// AACS version (1 or 2)
    pub version: u8,
    /// Whether bus encryption is enabled (always true for AACS 2.0 / UHD)
    pub bus_encryption: bool,
    /// MKB version from disc (e.g. 68, 77)
    pub mkb_version: Option<u32>,
    /// Disc hash (SHA1 of Unit_Key_RO.inf) -- hex string with 0x prefix
    pub disc_hash: String,
    /// How keys were resolved
    pub key_source: KeySource,
    /// Volume Unique Key (16 bytes)
    pub vuk: [u8; 16],
    /// Decrypted unit keys (CPS unit number, key)
    pub unit_keys: Vec<(u32, [u8; 16])>,
    /// Read data key for AACS 2.0 bus decryption -- None for AACS 1.0
    pub read_data_key: Option<[u8; 16]>,
    /// Volume ID (16 bytes) -- from SCSI handshake
    pub volume_id: [u8; 16],
}

/// How AACS keys were resolved.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum KeySource {
    /// VUK found directly in KEYDB by disc hash
    KeyDb,
    /// Media key + Volume ID from KEYDB → derived VUK
    KeyDbDerived,
    /// MKB + processing keys → media key → VUK
    ProcessingKey,
    /// MKB + device keys → subset-difference tree → VUK
    DeviceKey,
}

impl KeySource {
    pub fn name(&self) -> &'static str {
        match self {
            KeySource::KeyDb => "KEYDB",
            KeySource::KeyDbDerived => "KEYDB (derived)",
            KeySource::ProcessingKey => "MKB + processing key",
            KeySource::DeviceKey => "MKB + device key",
        }
    }
}

// ─── Disc scanning ──────────────────────────────────────────────────────────

/// Standard KEYDB.cfg search locations (compatible with libaacs).
const KEYDB_SEARCH_PATHS: &[&str] = &[
    ".config/aacs/KEYDB.cfg", // relative to $HOME
];
const KEYDB_SYSTEM_PATH: &str = "/etc/aacs/KEYDB.cfg";

/// Options for disc scanning.
#[derive(Default)]
pub struct ScanOptions {
    /// Path to KEYDB.cfg for AACS key lookup.
    /// If None, searches standard locations ($HOME/.config/aacs/ and /etc/aacs/).
    pub keydb_path: Option<std::path::PathBuf>,
}

impl ScanOptions {
    /// Create options with a specific KEYDB path.
    pub fn with_keydb(path: impl Into<std::path::PathBuf>) -> Self {
        ScanOptions {
            keydb_path: Some(path.into()),
        }
    }

    /// Resolve KEYDB path: explicit path first, then standard locations.
    fn resolve_keydb(&self) -> Option<std::path::PathBuf> {
        if let Some(p) = &self.keydb_path {
            if p.exists() {
                return Some(p.clone());
            }
        }
        if let Some(home) = std::env::var_os("HOME").or_else(|| std::env::var_os("USERPROFILE")) {
            for relative in KEYDB_SEARCH_PATHS {
                let p = std::path::PathBuf::from(&home).join(relative);
                if p.exists() {
                    return Some(p);
                }
            }
        }
        let p = std::path::PathBuf::from(KEYDB_SYSTEM_PATH);
        if p.exists() {
            return Some(p);
        }
        None
    }
}

/// A disc with an active drive session -- the main API.
///
/// Owns both the disc metadata and the drive connection.
/// Created by `Disc::open()`. Provides `rip()` to read title data.
pub struct OpenDisc {
    pub disc: Disc,
    pub session: Drive,
}

impl OpenDisc {
    /// Open a drive, wait for disc, initialize, probe, and scan.
    /// This is the single entry point -- one call does everything.
    ///
    pub fn open(device: &str, keydb_path: Option<&str>) -> Result<Self> {
        use std::path::Path;

        let mut session = Drive::open(Path::new(device))?;
        session.wait_ready()?;

        // Init (unlock + firmware) -- non-fatal if fails
        let _ = session.init();
        let _ = session.probe_disc();

        let opts = if let Some(kp) = keydb_path {
            ScanOptions::with_keydb(kp)
        } else {
            ScanOptions::default()
        };

        let disc = Disc::scan(&mut session, &opts)?;
        Ok(Self { disc, session })
    }

    /// Rip a title to any output stream.
    ///
    /// Reads sectors from disc, decrypts AACS, handles errors/retries,
    /// and writes decrypted BD-TS bytes to the output.
    /// Knows nothing about the output format -- just calls `write_all()`.
    ///
    pub fn rip(&mut self, title_idx: usize, mut output: impl std::io::Write) -> Result<()> {
        let mut reader = self.disc.open_title(&mut self.session, title_idx)?;

        loop {
            match reader.read_batch() {
                Ok(Some(batch)) => {
                    output.write_all(batch).map_err(|_| Error::WriteError)?;
                }
                Ok(None) => break,
                Err(_) => {
                    // ContentReader handles retries internally
                }
            }
        }

        Ok(())
    }

    /// Total bytes for a title (for progress tracking).
    pub fn title_size(&self, title_idx: usize) -> u64 {
        self.disc
            .titles
            .get(title_idx)
            .map(|t| t.size_bytes)
            .unwrap_or(0)
    }
}

impl Disc {
    /// Disc capacity in GB
    pub fn capacity_gb(&self) -> f64 {
        self.capacity_sectors as f64 * 2048.0 / (1024.0 * 1024.0 * 1024.0)
    }

    /// Scan a disc -- parse filesystem, playlists, streams, and set up AACS decryption.
    ///
    /// This is the main entry point. After scan(), the Disc is ready:
    ///   - titles are populated with streams
    ///   - AACS keys are derived (if KEYDB available)
    ///   - content can be read and decrypted transparently
    ///
    /// Scan a disc. One pipeline, one order:
    ///   1. Read capacity
    ///   2. Read UDF filesystem
    ///   3. Resolve AACS keys (all via UDF, no SCSI commands)
    ///   4. Parse playlists + streams
    ///   5. Apply labels
    ///
    /// The session must be open and unlocked (Drive::open handles this).
    /// All disc reads use standard READ(10) via UDF -- no vendor SCSI commands.
    pub fn scan(session: &mut Drive, opts: &ScanOptions) -> Result<Self> {
        // READ CAPACITY may fail in LibreDrive mode — proceed with 0 and estimate later
        let capacity = Self::read_capacity(session).unwrap_or(0);
        let handshake = Self::do_handshake(session, opts);
        Self::scan_with(session, capacity, handshake, opts)
    }

    /// Scan a disc image (ISO or any SectorReader). No SCSI, no handshake.
    /// AACS resolution uses KEYDB VUK lookup only.
    pub fn scan_image(
        reader: &mut dyn SectorReader,
        capacity: u32,
        opts: &ScanOptions,
    ) -> Result<Self> {
        Self::scan_with(reader, capacity, None, opts)
    }

    /// Core scan pipeline — works with any SectorReader.
    fn scan_with(
        reader: &mut dyn SectorReader,
        capacity: u32,
        handshake: Option<HandshakeResult>,
        opts: &ScanOptions,
    ) -> Result<Self> {
        // 1. UDF filesystem
        let udf_fs = udf::read_filesystem(reader)?;

        // 2. Resolve encryption (AACS, CSS, or none)
        let encrypted =
            udf_fs.find_dir("/AACS").is_some() || udf_fs.find_dir("/BDMV/AACS").is_some();

        let aacs = if encrypted {
            if let Some(keydb_path) = opts.resolve_keydb() {
                Self::resolve_encryption(&udf_fs, reader, &keydb_path, handshake.as_ref()).ok()
            } else {
                None
            }
        } else {
            None
        };

        // 3. Titles — BD (MPLS playlists) or DVD (IFO title sets)
        let (mut titles, content_format) = if udf_fs.find_dir("/BDMV").is_some() {
            (
                Self::scan_bluray_titles(reader, &udf_fs),
                ContentFormat::BdTs,
            )
        } else if udf_fs.find_dir("/VIDEO_TS").is_some() {
            (
                Self::scan_dvd_titles(reader, &udf_fs),
                ContentFormat::MpegPs,
            )
        } else {
            (Vec::new(), ContentFormat::BdTs)
        };
        titles.sort_by(|a, b| {
            b.duration_secs
                .partial_cmp(&a.duration_secs)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // 4. Metadata + labels
        let meta_title = Self::read_meta_title(reader, &udf_fs);
        crate::labels::apply(reader, &udf_fs, &mut titles);

        // 5. Derive format, layers, region
        let format = Self::detect_format(&titles);
        let layers = if capacity > 24_000_000 { 2 } else { 1 };
        let region = DiscRegion::Free;

        // 6. CSS detection for DVDs
        let css = if content_format == ContentFormat::MpegPs && !titles.is_empty() {
            crate::css::crack_key(reader, &titles[0].extents)
        } else {
            None
        };
        let encrypted = encrypted || css.is_some();

        Ok(Disc {
            volume_id: udf_fs.volume_id.clone(),
            meta_title,
            format,
            capacity_sectors: capacity,
            capacity_bytes: capacity as u64 * 2048,
            layers,
            titles,
            region,
            aacs,
            css,
            encrypted,
            content_format,
        })
    }

    // ── Internal helpers ────────────────────────────────────────────────────

    /// Detect disc format from the main title's video streams.
    fn detect_format(titles: &[DiscTitle]) -> DiscFormat {
        for title in titles.iter().take(3) {
            for stream in &title.streams {
                if let Stream::Video(v) = stream {
                    if v.resolution.contains("2160") {
                        return DiscFormat::Uhd;
                    }
                    if v.resolution.contains("1080") || v.resolution.contains("720") {
                        return DiscFormat::BluRay;
                    }
                    if v.resolution.contains("480") || v.resolution.contains("576") {
                        return DiscFormat::Dvd;
                    }
                }
            }
        }
        DiscFormat::Unknown
    }

    fn read_capacity(session: &mut Drive) -> Result<u32> {
        let cdb = [
            crate::scsi::SCSI_READ_CAPACITY,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
        ];
        let mut buf = [0u8; 8];
        session.scsi_execute(
            &cdb,
            crate::scsi::DataDirection::FromDevice,
            &mut buf,
            5_000,
        )?;
        let lba = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        Ok(lba + 1)
    }
}

// ─── Decrypted reader ──────────────────────────────────────────────────────

/// A reader that reads m2ts content, decrypting transparently if needed.
///
/// Adaptive read strategy:
///   - Starts at max batch size (510 sectors ≈ 1MB) and full disc speed
///   - On read error: halves batch size, brief pause for drive recovery
///   - On repeated errors: reduces disc spin speed (scratched region)
///   - On success streak: ramps batch back up, then restores disc speed
///   - At minimum batch + still failing: retries once, then skips + zero-fills
pub struct ContentReader<'a> {
    session: &'a mut Drive,
    decrypt_keys: crate::decrypt::DecryptKeys,
    extents: Vec<Extent>,
    current_extent: usize,
    current_offset: u32,
    unit_key_idx: usize,
    read_buf: Vec<u8>,
    buf_pos: usize,
    buf_len: usize,
    /// Current batch size in sectors (adapts on errors)
    batch_sectors: u16,
    /// Maximum batch size detected from kernel limits
    max_batch_sectors: u16,
    /// Consecutive successful batch reads
    ok_streak: u32,
    /// Consecutive errors at current position
    error_streak: u32,
    /// Current speed tier index (0 = max, higher = slower)
    /// Last time maintain_speed was called
    /// Total read errors encountered
    pub errors: u32,
}

impl Disc {
    /// Get the resolved decryption keys for this disc.
    /// Used by disc-to-ISO and other full-disc operations.
    pub fn decrypt_keys(&self) -> crate::decrypt::DecryptKeys {
        if let Some(ref aacs) = self.aacs {
            crate::decrypt::DecryptKeys::Aacs {
                unit_keys: aacs.unit_keys.clone(),
                read_data_key: aacs.read_data_key,
            }
        } else if let Some(ref css) = self.css {
            crate::decrypt::DecryptKeys::Css {
                title_key: css.title_key,
            }
        } else {
            crate::decrypt::DecryptKeys::None
        }
    }

    /// Open a title for reading. Decryption is automatic -- if the disc
    /// is encrypted and keys were found during scan(), content is decrypted
    /// on the fly. Unencrypted discs pass through unchanged.
    ///
    pub fn open_title<'a>(
        &'a self,
        session: &'a mut Drive,
        title_idx: usize,
    ) -> Result<ContentReader<'a>> {
        let title = self.titles.get(title_idx).ok_or(Error::DiscTitleRange {
            index: title_idx,
            count: self.titles.len(),
        })?;

        // Let the drive manage its own read speed after init.
        // SET_CD_SPEED is only used reactively by the error handler to slow
        // down on read errors, then let the drive recover.

        // Detect kernel max transfer size for this device
        let max_batch = detect_max_batch_sectors(session.device_path());

        let decrypt_keys = if let Some(ref aacs) = self.aacs {
            crate::decrypt::DecryptKeys::Aacs {
                unit_keys: aacs.unit_keys.clone(),
                read_data_key: aacs.read_data_key,
            }
        } else if let Some(ref css) = self.css {
            crate::decrypt::DecryptKeys::Css {
                title_key: css.title_key,
            }
        } else {
            crate::decrypt::DecryptKeys::None
        };

        Ok(ContentReader {
            session,
            decrypt_keys,
            extents: title.extents.clone(),
            current_extent: 0,
            current_offset: 0,
            unit_key_idx: 0,
            read_buf: Vec::with_capacity(max_batch as usize * 2048),
            buf_pos: 0,
            buf_len: 0,
            batch_sectors: max_batch,
            max_batch_sectors: max_batch,
            ok_streak: 0,
            error_streak: 0,
            errors: 0,
        })
    }
}

/// Detect the maximum transfer size in sectors for a device.
/// Reads /sys/block/<dev>/queue/max_hw_sectors_kb on Linux.
/// For sg devices, resolves the corresponding block device via sysfs.
/// Returns a value aligned to 3 sectors (one aligned unit).
pub(crate) fn detect_max_batch_sectors(device_path: &str) -> u16 {
    let dev_name = device_path.rsplit('/').next().unwrap_or("");
    if dev_name.is_empty() {
        return DEFAULT_BATCH_SECTORS;
    }

    // For sg devices, find the corresponding block device name
    let block_name = if dev_name.starts_with("sg") {
        let block_dir = format!("/sys/class/scsi_generic/{dev_name}/device/block");
        std::fs::read_dir(&block_dir)
            .ok()
            .and_then(|mut entries| entries.next())
            .and_then(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().to_string())
    } else {
        Some(dev_name.to_string())
    };

    if let Some(bname) = block_name {
        let sysfs_path = format!("/sys/block/{bname}/queue/max_hw_sectors_kb");
        if let Ok(content) = std::fs::read_to_string(&sysfs_path) {
            if let Ok(kb) = content.trim().parse::<u32>() {
                // Convert KB to sectors (1 sector = 2 KB = 2048 bytes)
                let sectors = (kb / 2) as u16;
                // Align down to 3 (one aligned unit)
                let aligned = (sectors / 3) * 3;
                if aligned >= MIN_BATCH_SECTORS {
                    return aligned.min(MAX_BATCH_SECTORS);
                }
            }
        }
    }
    // Fallback: safe default well under typical kernel limits
    DEFAULT_BATCH_SECTORS
}

/// Read strategy constants
pub(crate) const MAX_BATCH_SECTORS: u16 = 510; // absolute max (170 aligned units ≈ 1MB)
pub(crate) const DEFAULT_BATCH_SECTORS: u16 = 60; // fallback: typical kernel limit (120KB = 60 sectors)
pub(crate) const MIN_BATCH_SECTORS: u16 = 3; // 1 aligned unit = 6KB (error recovery)
pub(crate) const RAMP_BATCH_AFTER: u32 = 5; // successes before doubling batch size
pub(crate) const RAMP_SPEED_AFTER: u32 = 50; // successes at max batch before restoring speed
pub(crate) const SLOW_SPEED_AFTER: u32 = 3; // consecutive errors before reducing disc speed

impl<'a> ContentReader<'a> {
    /// Total bytes across all extents (for progress display).
    pub fn total_bytes(&self) -> u64 {
        self.extents
            .iter()
            .map(|e| e.sector_count as u64 * 2048)
            .sum()
    }

    /// Read the next aligned unit (6144 bytes).
    /// Automatically decrypted if AACS keys are available.
    /// Returns None when all extents are exhausted.
    pub fn read_unit(&mut self) -> Result<Option<Vec<u8>>> {
        // Refill buffer if empty
        if self.buf_pos >= self.buf_len && !self.fill_buffer()? {
            return Ok(None);
        }

        // Extract one aligned unit from buffer
        let start = self.buf_pos * crate::aacs::ALIGNED_UNIT_LEN;
        let end = start + crate::aacs::ALIGNED_UNIT_LEN;
        let mut unit = self.read_buf[start..end].to_vec();

        // Decrypt if needed
        self.decrypt_unit(&mut unit);
        self.buf_pos += 1;
        Ok(Some(unit))
    }

    /// Read the next batch of aligned units, decrypted in-place.
    /// Returns the decrypted data as a single contiguous slice.
    /// More efficient than read_unit() -- one write_all() per batch instead of per unit.
    /// Returns None when all extents are exhausted.
    pub fn read_batch(&mut self) -> Result<Option<&[u8]>> {
        if !self.fill_buffer()? {
            return Ok(None);
        }

        // Decrypt all units in the buffer in-place
        let unit_len = crate::aacs::ALIGNED_UNIT_LEN;
        let total_bytes = self.buf_len * unit_len;
        crate::decrypt::decrypt_sectors(
            &mut self.read_buf[..total_bytes],
            &self.decrypt_keys,
            self.unit_key_idx,
        );
        self.buf_pos = self.buf_len;
        Ok(Some(&self.read_buf[..total_bytes]))
    }

    /// Decrypt a single aligned unit in-place if needed.
    fn decrypt_unit(&self, unit: &mut [u8]) {
        crate::decrypt::decrypt_sectors(unit, &self.decrypt_keys, self.unit_key_idx);
    }

    /// Read sectors via standard READ(10) 0x00.
    /// calibration primers. Standard reads are faster on most drives.
    fn read_sectors(&mut self, lba: u32, count: u16) -> Result<()> {
        self.session.read(lba, count, &mut self.read_buf)?;
        Ok(())
    }

    /// Read a batch of sectors into the internal buffer.
    ///
    /// Error handling:
    ///   - First error: re-init drive (may have re-locked), halve batch
    ///   - Repeated errors: reduce speed, keep halving batch
    ///   - At minimum batch: retry once, then skip + zero-fill
    ///   - After sustained success: ramp batch back up, restore max speed
    fn fill_buffer(&mut self) -> Result<bool> {
        loop {
            if self.current_extent >= self.extents.len() {
                return Ok(false);
            }

            let ext_start = self.extents[self.current_extent].start_lba;
            let ext_sectors = self.extents[self.current_extent].sector_count;
            let remaining = ext_sectors.saturating_sub(self.current_offset);

            // Align to 3 sectors (one aligned unit)
            let sectors_to_read = remaining.min(self.batch_sectors as u32) as u16;
            let sectors_to_read = sectors_to_read - (sectors_to_read % 3);
            if sectors_to_read == 0 {
                self.current_extent += 1;
                self.current_offset = 0;
                continue;
            }

            let lba = ext_start + self.current_offset;
            let byte_count = sectors_to_read as usize * 2048;
            self.read_buf.resize(byte_count, 0);

            match self.read_sectors(lba, sectors_to_read) {
                Ok(_) => {
                    self.buf_len = sectors_to_read as usize / 3;
                    self.buf_pos = 0;
                    self.current_offset += sectors_to_read as u32;
                    self.error_streak = 0;

                    if self.current_offset >= ext_sectors {
                        self.current_extent += 1;
                        self.current_offset = 0;
                    }

                    // Ramp up batch size after consecutive successes
                    self.ok_streak += 1;
                    if self.batch_sectors < self.max_batch_sectors
                        && self.ok_streak >= RAMP_BATCH_AFTER
                    {
                        self.batch_sectors = (self.batch_sectors * 2).min(self.max_batch_sectors);
                        self.ok_streak = 0;
                    }

                    // Restore max speed after sustained success at full batch
                    if self.batch_sectors == self.max_batch_sectors
                        && self.ok_streak >= RAMP_SPEED_AFTER
                    {
                        self.session.set_speed(0xFFFF);
                        self.ok_streak = 0;
                    }

                    return Ok(true);
                }
                Err(_) => {
                    self.errors += 1;
                    self.error_streak += 1;
                    self.ok_streak = 0;

                    // First error: re-init (drive may have re-locked)
                    if self.error_streak == 1 {
                        let _ = self.session.init();
                        let _ = self.session.probe_disc();
                    }

                    // Repeated errors: slow down
                    if self.error_streak >= SLOW_SPEED_AFTER {
                        self.session.set_speed(DriveSpeed::BD2x.to_kbps());
                        self.error_streak = 0;
                    }

                    if self.batch_sectors > MIN_BATCH_SECTORS {
                        self.batch_sectors = (self.batch_sectors / 2).max(MIN_BATCH_SECTORS);
                        std::thread::sleep(std::time::Duration::from_millis(100));
                    } else {
                        // At minimum batch -- retry once with longer pause
                        std::thread::sleep(std::time::Duration::from_millis(500));
                        self.read_buf.resize(MIN_BATCH_SECTORS as usize * 2048, 0);
                        if self.read_sectors(lba, MIN_BATCH_SECTORS).is_ok() {
                            self.buf_len = 1;
                            self.buf_pos = 0;
                            self.error_streak = 0;
                            self.current_offset += MIN_BATCH_SECTORS as u32;
                            if self.current_offset >= ext_sectors {
                                self.current_extent += 1;
                                self.current_offset = 0;
                            }
                            return Ok(true);
                        }
                        // Still failing -- skip this unit (zero-fill)
                        self.current_offset += 3;
                        if self.current_offset >= ext_sectors {
                            self.current_extent += 1;
                            self.current_offset = 0;
                        }
                        self.read_buf.resize(crate::aacs::ALIGNED_UNIT_LEN, 0);
                        self.read_buf.fill(0);
                        self.buf_len = 1;
                        self.buf_pos = 0;
                        return Ok(true);
                    }
                }
            }
        }
    }
}

// ─── Format helpers ────────────────────────────────────────────────────────

fn format_resolution(video_format: u8, _video_rate: u8) -> String {
    match video_format {
        1 => "480i".into(),
        2 => "576i".into(),
        3 => "480p".into(),
        4 => "1080i".into(),
        5 => "720p".into(),
        6 => "1080p".into(),
        7 => "576p".into(),
        8 => "2160p".into(),
        _ => String::new(),
    }
}

fn format_framerate(video_rate: u8) -> String {
    match video_rate {
        1 => "23.976".into(),
        2 => "24".into(),
        3 => "25".into(),
        4 => "29.97".into(),
        6 => "50".into(),
        7 => "59.94".into(),
        _ => String::new(),
    }
}

fn format_channels(audio_format: u8) -> String {
    match audio_format {
        1 => "mono".into(),
        3 => "stereo".into(),
        6 => "5.1".into(),
        12 => "7.1".into(),
        _ if audio_format > 0 => format!("{audio_format}ch"),
        _ => String::new(),
    }
}

fn format_samplerate(audio_rate: u8) -> String {
    match audio_rate {
        1 => "48kHz".into(),
        4 => "96kHz".into(),
        5 => "192kHz".into(),
        12 => "48/192kHz".into(),
        14 => "48/96kHz".into(),
        _ => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: build a DiscTitle with a single video stream at the given resolution.
    fn title_with_video(codec: Codec, resolution: &str) -> DiscTitle {
        DiscTitle {
            playlist: "00800.mpls".into(),
            playlist_id: 800,
            duration_secs: 7200.0,
            size_bytes: 0,
            clips: Vec::new(),
            streams: vec![Stream::Video(VideoStream {
                pid: 0x1011,
                codec,
                resolution: resolution.into(),
                frame_rate: "23.976".into(),
                hdr: HdrFormat::Sdr,
                color_space: ColorSpace::Bt709,
                secondary: false,
                label: String::new(),
            })],
            chapters: Vec::new(),
            extents: Vec::new(),
            content_format: ContentFormat::BdTs,
        }
    }

    #[test]
    fn detect_format_uhd() {
        let titles = vec![title_with_video(Codec::Hevc, "2160p")];
        assert_eq!(Disc::detect_format(&titles), DiscFormat::Uhd);
    }

    #[test]
    fn detect_format_bluray() {
        let titles = vec![title_with_video(Codec::H264, "1080p")];
        assert_eq!(Disc::detect_format(&titles), DiscFormat::BluRay);
    }

    #[test]
    fn detect_format_dvd() {
        let titles = vec![title_with_video(Codec::Mpeg2, "480i")];
        assert_eq!(Disc::detect_format(&titles), DiscFormat::Dvd);
    }

    #[test]
    fn detect_format_empty() {
        let titles: Vec<DiscTitle> = Vec::new();
        assert_eq!(Disc::detect_format(&titles), DiscFormat::Unknown);
    }

    #[test]
    fn content_format_default_bdts() {
        let t = title_with_video(Codec::H264, "1080p");
        assert_eq!(t.content_format, ContentFormat::BdTs);
    }

    #[test]
    fn content_format_dvd_mpegps() {
        let t = DiscTitle {
            content_format: ContentFormat::MpegPs,
            ..title_with_video(Codec::Mpeg2, "480i")
        };
        assert_eq!(t.content_format, ContentFormat::MpegPs);
    }

    #[test]
    fn disc_capacity_gb() {
        // Single-layer BD-25: ~12,219,392 sectors
        let disc = Disc {
            volume_id: String::new(),
            meta_title: None,
            format: DiscFormat::BluRay,
            capacity_sectors: 12_219_392,
            capacity_bytes: 12_219_392u64 * 2048,
            layers: 1,
            titles: Vec::new(),
            region: DiscRegion::Free,
            aacs: None,
            css: None,
            encrypted: false,
            content_format: ContentFormat::BdTs,
        };
        let gb = disc.capacity_gb();
        // 12,219,392 * 2048 / 1073741824 = ~23.3 GB
        assert!((gb - 23.3).abs() < 0.1, "expected ~23.3 GB, got {}", gb);

        // Zero sectors
        let disc_zero = Disc {
            capacity_sectors: 0,
            capacity_bytes: 0,
            ..disc
        };
        assert_eq!(disc_zero.capacity_gb(), 0.0);
    }

    #[test]
    fn disc_title_duration_display_edge_cases() {
        let mut t = DiscTitle::empty();

        // 0 seconds
        t.duration_secs = 0.0;
        assert_eq!(t.duration_display(), "0h 00m");

        // 1 second
        t.duration_secs = 1.0;
        assert_eq!(t.duration_display(), "0h 00m");

        // 59 minutes
        t.duration_secs = 59.0 * 60.0;
        assert_eq!(t.duration_display(), "0h 59m");

        // 24 hours
        t.duration_secs = 24.0 * 3600.0;
        assert_eq!(t.duration_display(), "24h 00m");
    }
}
