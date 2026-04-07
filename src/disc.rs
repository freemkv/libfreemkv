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
    /// Disc capacity in sectors
    pub capacity_sectors: u32,
    /// Titles sorted by duration (longest first), then playlist name
    pub titles: Vec<Title>,
    /// AACS state — None if disc is unencrypted or keys unavailable
    pub aacs: Option<AacsState>,
    /// Whether this disc requires AACS decryption
    pub encrypted: bool,
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
    /// Number of clips
    pub clip_count: usize,
    /// All streams (video, audio, subtitle, etc.)
    pub streams: Vec<Stream>,
    /// Sector extents for ripping (clip LBA ranges)
    pub extents: Vec<Extent>,
}

/// A stream within a title.
#[derive(Debug, Clone)]
pub struct Stream {
    /// Stream type
    pub kind: StreamKind,
    /// MPEG-TS packet ID
    pub pid: u16,
    /// Codec
    pub codec: Codec,
    /// ISO 639-2 language code (e.g. "eng", "fra")
    pub language: String,
    /// Video resolution (e.g. "2160p", "1080p")
    pub resolution: String,
    /// Frame rate (e.g. "23.976")
    pub frame_rate: String,
    /// Channel layout (e.g. "5.1", "7.1", "stereo")
    pub channels: String,
    /// Sample rate (e.g. "48kHz")
    pub sample_rate: String,
    /// HDR format
    pub hdr: HdrFormat,
    /// Color space
    pub color_space: ColorSpace,
    /// Whether this is a secondary/enhancement stream
    pub secondary: bool,
    /// Extra label (e.g. "Dolby Vision EL")
    pub label: String,
}

/// Stream type.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum StreamKind {
    Video,
    Audio,
    Subtitle,
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

impl Stream {
    /// Human-readable one-line description.
    pub fn display(&self) -> String {
        match self.kind {
            StreamKind::Video => {
                let mut parts = vec![self.codec.name().to_string()];
                if !self.resolution.is_empty() { parts.push(self.resolution.clone()); }
                if !self.frame_rate.is_empty() { parts.push(format!("{}fps", self.frame_rate)); }
                if self.hdr != HdrFormat::Sdr { parts.push(self.hdr.name().to_string()); }
                if self.color_space != ColorSpace::Unknown && self.color_space != ColorSpace::Bt709 {
                    parts.push(self.color_space.name().to_string());
                }
                if self.secondary { parts.push(format!("[{}]", self.label)); }
                parts.join(" ")
            }
            StreamKind::Audio => {
                let mut parts = vec![self.codec.name().to_string()];
                if !self.channels.is_empty() { parts.push(self.channels.clone()); }
                if !self.sample_rate.is_empty() { parts.push(self.sample_rate.clone()); }
                if !self.language.is_empty() { parts.push(format!("({})", self.language)); }
                if self.secondary { parts.push("[secondary]".to_string()); }
                parts.join(" ")
            }
            StreamKind::Subtitle => {
                let mut parts = vec![self.codec.name().to_string()];
                if !self.language.is_empty() { parts.push(format!("({})", self.language)); }
                parts.join(" ")
            }
        }
    }

    /// Kind as a display string
    pub fn kind_name(&self) -> &'static str {
        match self.kind {
            StreamKind::Video => "Video",
            StreamKind::Audio => "Audio",
            StreamKind::Subtitle => "Subtitle",
        }
    }
}

// ─── AACS state ─────────────────────────────────────────────────────────────

/// AACS decryption state for a disc.
#[derive(Debug)]
pub struct AacsState {
    /// Volume Unique Key
    pub vuk: [u8; 16],
    /// Decrypted unit keys indexed by CPS unit number
    pub unit_keys: Vec<(u32, [u8; 16])>,
    /// Read data key (AACS 2.0 bus decryption) — None for AACS 1.0
    pub read_data_key: Option<[u8; 16]>,
    /// Whether bus encryption is enabled
    pub bus_encryption: bool,
}

// ─── Disc scanning ──────────────────────────────────────────────────────────

/// Options for disc scanning.
pub struct ScanOptions {
    /// Path to KEYDB.cfg for AACS key lookup.
    /// If None, tries ~/.config/aacs/KEYDB.cfg and /etc/aacs/KEYDB.cfg.
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

    /// Resolve KEYDB path: explicit, then standard locations.
    fn resolve_keydb(&self) -> Option<std::path::PathBuf> {
        if let Some(p) = &self.keydb_path {
            if p.exists() { return Some(p.clone()); }
        }
        // Standard locations
        if let Some(home) = std::env::var_os("HOME") {
            let p = std::path::PathBuf::from(home).join(".config/aacs/KEYDB.cfg");
            if p.exists() { return Some(p); }
        }
        let p = std::path::PathBuf::from("/etc/aacs/KEYDB.cfg");
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

        // Step 4: Detect AACS encryption
        let encrypted = udf_fs.find_dir("/AACS").is_some()
            || udf_fs.find_dir("/BDMV/AACS").is_some();

        // Step 5: If encrypted and KEYDB available, authenticate and derive keys
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

        Ok(Disc {
            capacity_sectors: capacity,
            titles,
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
        use crate::aacs_handshake;

        // Load KEYDB
        let keydb = KeyDb::load(keydb_path).map_err(|e| Error::AacsError {
            detail: format!("failed to load KEYDB: {}", e),
        })?;

        let host_cert = keydb.host_cert.as_ref().ok_or_else(|| Error::AacsError {
            detail: "no host certificate in KEYDB".into(),
        })?;

        // Step 1: SCSI handshake → bus key + Volume ID
        let mut auth = aacs_handshake::aacs_authenticate(
            session,
            &host_cert.private_key,
            &host_cert.certificate,
        )?;

        let vid = aacs_handshake::read_volume_id(session, &mut auth)?;

        // Try to read data keys (AACS 2.0 bus encryption)
        let read_data_key = match aacs_handshake::read_data_keys(session, &mut auth) {
            Ok((rdk, _wdk)) => Some(rdk),
            Err(_) => None,
        };

        // Step 2: Read Unit_Key_RO.inf from disc via UDF
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

        // Step 4: Resolve all keys via the full chain
        //   Path 1: disc hash → KEYDB → VUK (fast, 99% of discs)
        //   Path 2: KEYDB media key + VID → VUK
        //   Path 3: MKB + processing keys → media key → VUK (fallback)
        let resolved = aacs::resolve_keys(
            &uk_ro_data,
            cc_data.as_deref(),
            &vid,
            &keydb,
            None, // MKB: TODO read via REPORT DISC STRUCTURE 0x83
        ).ok_or_else(|| Error::AacsError {
            detail: "failed to resolve AACS keys".into(),
        })?;

        Ok(AacsState {
            vuk: resolved.vuk,
            unit_keys: resolved.unit_keys,
            read_data_key,
            bus_encryption: resolved.bus_encryption,
        })
    }

    // ── Internal helpers ────────────────────────────────────────────────────

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

        // Parse each clip for EP map → sector extents
        let mut extents = Vec::new();
        let mut total_size: u64 = 0;
        let clip_count = parsed.play_items.len();

        for play_item in &parsed.play_items {
            let clpi_path = format!("/BDMV/CLIPINF/{}.clpi", play_item.clip_id);
            if let Ok(clpi_data) = udf_fs.read_file(session, &clpi_path) {
                if let Ok(clip_info) = clpi::parse(&clpi_data) {
                    // Use EP map to get sector extents for this clip's time range
                    let clip_extents = clip_info.get_extents(play_item.in_time, play_item.out_time);
                    for ext in &clip_extents {
                        total_size += ext.sector_count as u64 * 2048;
                    }
                    extents.extend(clip_extents);
                }
            }
        }

        // Streams: for now, we know the count but not details
        // (STN table parsing will be added to mpls module)
        let streams = Vec::new();

        let playlist_num = filename.trim_end_matches(".mpls").trim_end_matches(".MPLS");
        let playlist_id = playlist_num.parse::<u16>().unwrap_or(0);

        Some(Title {
            playlist: filename.to_string(),
            playlist_id,
            duration_secs,
            size_bytes: total_size,
            clip_count,
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
        0x28, 0x00,
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
