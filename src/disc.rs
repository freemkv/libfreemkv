//! Disc structure — scan titles, streams, and sector ranges from a Blu-ray disc.
//!
//! This is the high-level API for disc content. The CLI calls this,
//! never parses MPLS/CLPI/UDF directly.
//!
//! Usage:
//!   let disc = Disc::scan(&mut session)?;
//!   for title in disc.titles() { ... }
//!   for stream in title.streams() { ... }

use crate::error::{Error, Result};
use crate::drive::DriveSession;
use crate::udf;
use crate::mpls;
use crate::clpi;


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
    pub titles: Vec<Title>,
    /// JAR track labels (audio/subtitle names from BD-J menus)
    pub jar_labels: crate::jar::JarLabels,
    /// AACS state — None if disc is unencrypted or keys unavailable
    pub aacs: Option<AacsState>,
    /// Whether this disc requires AACS decryption
    pub encrypted: bool,
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

/// A title (one MPLS playlist).
#[derive(Debug, Clone)]
pub struct Title {
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
    /// Sector extents for ripping (clip LBA ranges)
    pub extents: Vec<Extent>,
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

impl Title {
    /// Duration formatted as "Xh Ym"
    pub fn duration_display(&self) -> String {
        let hrs = (self.duration_secs / 3600.0) as u32;
        let mins = ((self.duration_secs % 3600.0) / 60.0) as u32;
        format!("{}h {:02}m", hrs, mins)
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


// ─── AACS state ─────────────────────────────────────────────────────────────

/// AACS decryption state for a disc.
#[derive(Debug)]
pub struct AacsState {
    /// AACS version (1 or 2)
    pub version: u8,
    /// Whether bus encryption is enabled (always true for AACS 2.0 / UHD)
    pub bus_encryption: bool,
    /// MKB version from disc (e.g. 68, 77)
    pub mkb_version: Option<u32>,
    /// Disc hash (SHA1 of Unit_Key_RO.inf) — hex string with 0x prefix
    pub disc_hash: String,
    /// How keys were resolved
    pub key_source: KeySource,
    /// Volume Unique Key (16 bytes)
    pub vuk: [u8; 16],
    /// Decrypted unit keys (CPS unit number, key)
    pub unit_keys: Vec<(u32, [u8; 16])>,
    /// Read data key for AACS 2.0 bus decryption — None for AACS 1.0
    pub read_data_key: Option<[u8; 16]>,
    /// Volume ID (16 bytes) — from SCSI handshake
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
    ".config/aacs/KEYDB.cfg",  // relative to $HOME
];
const KEYDB_SYSTEM_PATH: &str = "/etc/aacs/KEYDB.cfg";

/// Options for disc scanning.
pub struct ScanOptions {
    /// Path to KEYDB.cfg for AACS key lookup.
    /// If None, searches standard locations ($HOME/.config/aacs/ and /etc/aacs/).
    pub keydb_path: Option<std::path::PathBuf>,
}

impl Default for ScanOptions {
    fn default() -> Self {
        ScanOptions { keydb_path: None }
    }
}

impl ScanOptions {
    /// Create options with a specific KEYDB path.
    pub fn with_keydb(path: impl Into<std::path::PathBuf>) -> Self {
        ScanOptions { keydb_path: Some(path.into()) }
    }

    /// Resolve KEYDB path: explicit path first, then standard locations.
    fn resolve_keydb(&self) -> Option<std::path::PathBuf> {
        if let Some(p) = &self.keydb_path {
            if p.exists() { return Some(p.clone()); }
        }
        if let Some(home) = std::env::var_os("HOME") {
            for relative in KEYDB_SEARCH_PATHS {
                let p = std::path::PathBuf::from(&home).join(relative);
                if p.exists() { return Some(p); }
            }
        }
        let p = std::path::PathBuf::from(KEYDB_SYSTEM_PATH);
        if p.exists() { return Some(p); }
        None
    }
}

impl Disc {
    /// Disc capacity in GB
    pub fn capacity_gb(&self) -> f64 {
        self.capacity_sectors as f64 * 2048.0 / (1024.0 * 1024.0 * 1024.0)
    }

    /// Scan a disc — parse filesystem, playlists, streams, and set up AACS decryption.
    ///
    /// This is the main entry point. After scan(), the Disc is ready:
    ///   - titles are populated with streams
    ///   - AACS keys are derived (if KEYDB available)
    ///   - content can be read and decrypted transparently
    ///
    /// ```no_run
    /// use libfreemkv::{DriveSession, Disc};
    /// use libfreemkv::disc::ScanOptions;
    /// use std::path::Path;
    ///
    /// let mut session = DriveSession::open(Path::new("/dev/sr0")).unwrap();
    /// let disc = Disc::scan(&mut session, &ScanOptions::default()).unwrap();
    /// for title in &disc.titles {
    ///     println!("{} — {} streams", title.duration_display(), title.streams.len());
    /// }
    /// ```
    pub fn scan(session: &mut DriveSession, opts: &ScanOptions) -> Result<Self> {
        // Step 1: Read capacity
        let capacity = Self::read_capacity(session)?;

        // Step 2: Parse UDF filesystem
        let udf_fs = udf::read_filesystem(session)?;

        // Step 3: Find and parse MPLS playlists
        let mut titles = Vec::new();
        if let Some(playlist_dir) = udf_fs.find_dir("/BDMV/PLAYLIST") {
            for entry in &playlist_dir.entries {
                if !entry.is_dir && entry.name.to_lowercase().ends_with(".mpls") {
                    let path = format!("/BDMV/PLAYLIST/{}", entry.name);
                    if let Ok(mpls_data) = udf_fs.read_file(session, &path) {
                        if let Some(title) = Self::parse_playlist(session, &udf_fs, &entry.name, &mpls_data) {
                            titles.push(title);
                        }
                    }
                }
            }
        }

        // Sort: longest first
        titles.sort_by(|a, b| b.duration_secs.partial_cmp(&a.duration_secs).unwrap_or(std::cmp::Ordering::Equal));

        // Step 4: Read disc title from META/DL/bdmt_eng.xml
        let meta_title = Self::read_meta_title(session, &udf_fs);

        // Step 5: Extract JAR track labels
        let jar_labels = Self::read_jar_labels(session, &udf_fs);

        // Step 6: Detect AACS encryption
        let encrypted = udf_fs.find_dir("/AACS").is_some()
            || udf_fs.find_dir("/BDMV/AACS").is_some();

        // Step 7: If encrypted and KEYDB available, authenticate and derive keys
        let aacs = if encrypted {
            if let Some(keydb_path) = opts.resolve_keydb() {
                match Self::setup_aacs(session, &keydb_path) {
                    Ok(state) => Some(state),
                    Err(_) => None, // keys not found, continue without decryption
                }
            } else {
                None
            }
        } else {
            None
        };

        // Derive disc format from main title video codec
        let format = Self::detect_format(&titles);

        // Derive layer count from capacity
        // BD-25 single layer: up to ~12M sectors (~25GB)
        // BD-50 dual layer: ~12M-25M sectors (~50GB)
        // BD-66/100 UHD: 25M+ sectors
        let layers = if capacity > 24_000_000 { 2 } else { 1 };

        Ok(Disc {
            volume_id: udf_fs.volume_id.clone(),
            meta_title: meta_title,
            format,
            capacity_sectors: capacity,
            capacity_bytes: capacity as u64 * 2048,
            layers,
            titles,
            jar_labels,
            aacs,
            encrypted,
        })
    }

    /// Set up AACS decryption for this disc.
    /// Call after scan() to enable transparent content decryption.
    pub fn setup_aacs(
        session: &mut DriveSession,
        keydb_path: &std::path::Path,
    ) -> Result<AacsState> {
        use crate::aacs::{self, KeyDb};
        use crate::aacs::handshake;

        // Load KEYDB
        let keydb = KeyDb::load(keydb_path).map_err(|e| Error::AacsError {
            detail: format!("failed to load KEYDB: {}", e),
        })?;

        // Step 1: Try SCSI handshake for Volume ID + read_data_key
        // Open a separate transport (AACS auth must happen before raw mode).
        // If handshake fails (drive doesn't support AACS layer, e.g. raw-mode drives),
        // fall back to disc-hash-only KEYDB lookup.
        let device_path = session.device_path().to_string();
        let mut vid: Option<[u8; 16]> = None;
        let mut read_data_key: Option<[u8; 16]> = None;

        if !device_path.is_empty() {
            if let Ok(mut aacs_session) = DriveSession::open_no_unlock(std::path::Path::new(&device_path)) {
                if let Ok(hc) = keydb.host_cert.as_ref().ok_or(()) {
                    if let Ok(mut auth) = handshake::aacs_authenticate(
                        &mut aacs_session, &hc.private_key, &hc.certificate,
                    ) {
                        vid = handshake::read_volume_id(&mut aacs_session, &mut auth).ok();
                        read_data_key = handshake::read_data_keys(&mut aacs_session, &mut auth)
                            .ok().map(|(rdk, _)| rdk);
                    }
                }
            }
            // Handshake failure is not fatal — we can still resolve via disc hash
        }

        // Step 2: Read Unit_Key_RO.inf from disc via UDF (uses the unlocked main session)
        let udf_fs = udf::read_filesystem(session)?;
        let uk_ro_data = udf_fs.read_file(session, "/AACS/Unit_Key_RO.inf")
            .or_else(|_| udf_fs.read_file(session, "/AACS/DUPLICATE/Unit_Key_RO.inf"))
            .map_err(|_| Error::AacsError {
                detail: "failed to read Unit_Key_RO.inf from disc".into(),
            })?;

        // Step 3: Read Content Certificate (optional — for AACS version detection)
        let cc_data = udf_fs.read_file(session, "/AACS/Content000.cer")
            .or_else(|_| udf_fs.read_file(session, "/AACS/Content001.cer"))
            .ok();

        // Step 4: Resolve keys
        // If we have VID from handshake, use full 4-path chain.
        // If no VID (handshake failed), use disc-hash-only KEYDB lookup.
        let mkb_data = aacs::read_mkb_from_drive(session).ok();
        let mkb_ver = mkb_data.as_deref().and_then(aacs::mkb_version);

        // Use a zero VID placeholder if handshake failed — resolve_keys
        // will still work via disc hash (path 1)
        let vid_for_resolve = vid.unwrap_or([0u8; 16]);

        let resolved = aacs::resolve_keys(
            &uk_ro_data,
            cc_data.as_deref(),
            &vid_for_resolve,
            &keydb,
            mkb_data.as_deref(),
        ).ok_or_else(|| Error::AacsError {
            detail: "failed to resolve AACS keys — disc not in KEYDB".into(),
        })?;

        let key_source = match resolved.key_source {
            1 => KeySource::KeyDb,
            2 => KeySource::KeyDbDerived,
            3 => KeySource::ProcessingKey,
            4 => KeySource::DeviceKey,
            _ => KeySource::KeyDb,
        };

        Ok(AacsState {
            version: if resolved.aacs2 { 2 } else { 1 },
            bus_encryption: resolved.bus_encryption,
            mkb_version: mkb_ver,
            disc_hash: aacs::disc_hash_hex(&resolved.disc_hash),
            key_source,
            vuk: resolved.vuk,
            unit_keys: resolved.unit_keys,
            read_data_key,
            volume_id: vid.unwrap_or([0u8; 16]),
        })
    }

    // ── Internal helpers ────────────────────────────────────────────────────

    /// Detect disc format from the main title's video streams.
    fn detect_format(titles: &[Title]) -> DiscFormat {
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

    /// Read disc title from META/DL/bdmt_eng.xml (Blu-ray Disc Meta Table).
    /// Prefers English, falls back to first available language.
    /// Returns None if META directory is empty or XML has no usable title.
    fn read_meta_title(session: &mut DriveSession, udf_fs: &udf::UdfFs) -> Option<String> {
        let meta_dir = udf_fs.find_dir("/BDMV/META")?;
        for sub in &meta_dir.entries {
            if !sub.is_dir { continue; }
            let dl_path = format!("/BDMV/META/{}", sub.name);
            if let Some(dl_dir) = udf_fs.find_dir(&dl_path) {
                let xml_files: Vec<_> = dl_dir.entries.iter()
                    .filter(|e| !e.is_dir && e.name.to_lowercase().ends_with(".xml"))
                    .collect();

                let eng = xml_files.iter().find(|e| e.name.to_lowercase().contains("eng"));
                let target = eng.or_else(|| xml_files.first());

                if let Some(entry) = target {
                    let path = format!("{}/{}", dl_path, entry.name);
                    if let Ok(data) = udf_fs.read_file(session, &path) {
                        let xml = String::from_utf8_lossy(&data);
                        if let Some(start) = xml.find("<di:name>") {
                            let s = start + "<di:name>".len();
                            if let Some(end) = xml[s..].find("</di:name>") {
                                let title = xml[s..s + end].trim().to_string();
                                if !title.is_empty() && title != "Blu-ray" {
                                    return Some(title);
                                }
                            }
                        }
                    }
                }
            }
        }
        None
    }

    /// Extract track labels from BD-J JAR files.
    fn read_jar_labels(session: &mut DriveSession, udf_fs: &udf::UdfFs) -> crate::jar::JarLabels {
        if let Some(jar_dir) = udf_fs.find_dir("/BDMV/JAR") {
            for entry in &jar_dir.entries {
                if !entry.is_dir && entry.name.to_lowercase().ends_with(".jar") {
                    let path = format!("/BDMV/JAR/{}", entry.name);
                    if let Ok(jar_data) = udf_fs.read_file(session, &path) {
                        if let Some(labels) = crate::jar::extract_labels(&jar_data) {
                            return labels;
                        }
                    }
                }
            }
        }
        crate::jar::JarLabels::default()
    }

    fn read_capacity(session: &mut DriveSession) -> Result<u32> {
        let cdb = [0x25, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        let mut buf = [0u8; 8];
        session.scsi_execute(&cdb, crate::scsi::DataDirection::FromDevice, &mut buf, 5_000)?;
        let lba = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        Ok(lba + 1)
    }

    fn parse_playlist(
        session: &mut DriveSession,
        udf_fs: &udf::UdfFs,
        filename: &str,
        data: &[u8],
    ) -> Option<Title> {
        let parsed = mpls::parse(data).ok()?;

        // Calculate duration from play items
        let duration_ticks: u64 = parsed.play_items.iter()
            .map(|pi| (pi.out_time.saturating_sub(pi.in_time)) as u64)
            .sum();
        let duration_secs = duration_ticks as f64 / 45000.0;

        // Skip very short playlists (< 30 seconds)
        if duration_secs < 30.0 {
            return None;
        }

        // Parse each clip for size, duration, and sector extents
        let mut extents = Vec::new();
        let mut total_size: u64 = 0;
        let mut clips = Vec::with_capacity(parsed.play_items.len());

        for play_item in &parsed.play_items {
            let clip_dur = play_item.out_time.saturating_sub(play_item.in_time) as f64 / 45000.0;
            let mut pkt_count: u32 = 0;

            let clpi_path = format!("/BDMV/CLIPINF/{}.clpi", play_item.clip_id);
            if let Ok(clpi_data) = udf_fs.read_file(session, &clpi_path) {
                if let Ok(clip_info) = clpi::parse(&clpi_data) {
                    pkt_count = clip_info.source_packet_count;
                    total_size += pkt_count as u64 * 192;

                    let clip_extents = clip_info.get_extents(play_item.in_time, play_item.out_time);
                    extents.extend(clip_extents);
                }
            }

            clips.push(Clip {
                clip_id: play_item.clip_id.clone(),
                in_time: play_item.in_time,
                out_time: play_item.out_time,
                duration_secs: clip_dur,
                source_packets: pkt_count,
            });
        }

        // Build streams from STN table
        let streams: Vec<Stream> = parsed.streams.iter().map(|s| {
            let codec = Codec::from_coding_type(s.coding_type);
            match s.stream_type {
                1 | 6 | 7 => Stream::Video(VideoStream {
                    pid: s.pid,
                    codec,
                    resolution: format_resolution(s.video_format, s.video_rate),
                    frame_rate: format_framerate(s.video_rate),
                    hdr: match s.dynamic_range {
                        1 => HdrFormat::Hdr10,
                        2 => HdrFormat::DolbyVision,
                        _ => HdrFormat::Sdr,
                    },
                    color_space: match s.color_space {
                        1 => ColorSpace::Bt709,
                        2 => ColorSpace::Bt2020,
                        _ => ColorSpace::Unknown,
                    },
                    secondary: s.secondary,
                    label: match s.stream_type {
                        7 => "Dolby Vision EL".to_string(),
                        _ => String::new(),
                    },
                }),
                2 | 5 => Stream::Audio(AudioStream {
                    pid: s.pid,
                    codec,
                    channels: format_channels(s.audio_format),
                    language: s.language.clone(),
                    sample_rate: format_samplerate(s.audio_rate),
                    secondary: s.stream_type == 5,
                    label: String::new(),
                }),
                3 => Stream::Subtitle(SubtitleStream {
                    pid: s.pid,
                    codec,
                    language: s.language.clone(),
                    forced: false, // TODO: parse from MPLS stream attributes
                }),
                _ => Stream::Video(VideoStream {
                    pid: s.pid,
                    codec,
                    resolution: String::new(),
                    frame_rate: String::new(),
                    hdr: HdrFormat::Sdr,
                    color_space: ColorSpace::Unknown,
                    secondary: false,
                    label: String::new(),
                }),
            }
        }).collect();

        let playlist_num = filename.trim_end_matches(".mpls").trim_end_matches(".MPLS");
        let playlist_id = playlist_num.parse::<u16>().unwrap_or(0);

        Some(Title {
            playlist: filename.to_string(),
            playlist_id,
            duration_secs,
            size_bytes: total_size,
            clips,
            streams,
            extents,
        })
    }
}

// ─── Decrypted reader ──────────────────────────────────────────────────────

/// A reader that reads m2ts content, decrypting transparently if needed.
pub struct ContentReader<'a> {
    session: &'a mut DriveSession,
    aacs: Option<&'a AacsState>,
    extents: Vec<Extent>,
    current_extent: usize,
    current_offset: u32, // sectors into current extent
    unit_key_idx: usize,
}

impl Disc {
    /// Open a title for reading. Decryption is automatic — if the disc
    /// is encrypted and keys were found during scan(), content is decrypted
    /// on the fly. Unencrypted discs pass through unchanged.
    ///
    /// ```no_run
    /// # use libfreemkv::{DriveSession, Disc};
    /// # use libfreemkv::disc::ScanOptions;
    /// # use std::path::Path;
    /// # let mut session = DriveSession::open(Path::new("/dev/sr0")).unwrap();
    /// let disc = Disc::scan(&mut session, &ScanOptions::default()).unwrap();
    /// let mut reader = disc.open_title(&mut session, 0).unwrap();
    /// while let Some(unit) = reader.read_unit().unwrap() {
    ///     // unit is 6144 bytes of decrypted content
    /// }
    /// ```
    pub fn open_title<'a>(&'a self, session: &'a mut DriveSession, title_idx: usize) -> Result<ContentReader<'a>> {
        let title = self.titles.get(title_idx).ok_or_else(|| Error::DiscError {
            detail: format!("title index {} out of range (have {})", title_idx, self.titles.len()),
        })?;

        Ok(ContentReader {
            session,
            aacs: self.aacs.as_ref(),
            extents: title.extents.clone(),
            current_extent: 0,
            current_offset: 0,
            unit_key_idx: 0,
        })
    }
}

impl<'a> ContentReader<'a> {
    /// Read the next aligned unit (6144 bytes).
    /// Automatically decrypted if AACS keys are available.
    /// Returns None when all extents are exhausted.
    pub fn read_unit(&mut self) -> Result<Option<Vec<u8>>> {
        if self.current_extent >= self.extents.len() {
            return Ok(None);
        }

        let extent = &self.extents[self.current_extent];
        let lba = extent.start_lba + self.current_offset;

        // Read 3 sectors (one aligned unit)
        let mut unit = vec![0u8; crate::aacs::ALIGNED_UNIT_LEN];
        for i in 0..3u32 {
            let offset = (i as usize) * 2048;
            let mut sector = [0u8; 2048];
            session_read_sector(self.session, lba + i, &mut sector)?;
            unit[offset..offset + 2048].copy_from_slice(&sector);
        }

        // Decrypt if needed
        if let Some(aacs) = &self.aacs {
            if crate::aacs::is_unit_encrypted(&unit) {
                let uk = aacs.unit_keys.get(self.unit_key_idx)
                    .map(|(_, k)| *k)
                    .unwrap_or([0u8; 16]);

                crate::aacs::decrypt_unit_full(
                    &mut unit,
                    &uk,
                    aacs.read_data_key.as_ref(),
                );
            }
        }

        // Advance position
        self.current_offset += 3;
        if self.current_offset >= extent.sector_count {
            self.current_extent += 1;
            self.current_offset = 0;
        }

        Ok(Some(unit))
    }
}

fn session_read_sector(session: &mut DriveSession, lba: u32, buf: &mut [u8; 2048]) -> Result<()> {
    let cdb = [
        crate::scsi::SCSI_READ_10, 0x00,
        (lba >> 24) as u8, (lba >> 16) as u8, (lba >> 8) as u8, lba as u8,
        0x00, 0x00, 0x01, 0x00,
    ];
    session.scsi_execute(&cdb, crate::scsi::DataDirection::FromDevice, buf, 10_000)?;
    Ok(())
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
        _ if audio_format > 0 => format!("{}ch", audio_format),
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
