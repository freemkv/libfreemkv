//! One way to open any input (pipeline design §2.2, slice 4): [`open_source`] takes a URL
//! for every input scheme, `disc://` included, and probes it once. The [`Source`] carries
//! that probe's [`Disc`] layout (for `disc://`, `iso://` and `dir://`), so listing titles,
//! acquiring keys and muxing read the same scan. A caller that already holds a scanned
//! layout (a drive scan reused over a staged image, J14) or an opened session builds the
//! same `Source` with [`Source::from_image`], [`Source::from_reader`] or
//! [`Source::from_session`]; [`crate::mux_with_keys`] and [`crate::input`] take it.

use std::path::{Path, PathBuf};

use crate::ctx::Ctx;
use crate::disc::{Disc, ScanOptions};
use crate::error::{Error, Result};
use crate::sector::SectorSource;
use crate::session::{DeviceTarget, DiscSession, KeySpec};

use super::resolve::{StreamUrl, parse_url};
use super::videomap::{Medium, SourceInfo};

/// One title of a disc the caller already scanned, for [`Source::from_image`] and
/// [`Source::from_reader`]: the title, its container format, and the disc's name.
#[derive(Debug, Clone)]
pub struct ScannedTitle {
    pub title: crate::disc::DiscTitle,
    pub format: crate::disc::ContentFormat,
    /// The disc's name (meta title, else volume id), which names the output title; `None`
    /// keeps the title's own playlist name.
    pub disc_name: Option<String>,
    pub volume_id: String,
}

impl ScannedTitle {
    /// Title `idx` of `disc`, named by the disc. `None` when out of range.
    pub fn of(disc: &Disc, idx: usize) -> Option<Self> {
        Some(ScannedTitle {
            title: disc.titles.get(idx)?.clone(),
            format: disc.content_format,
            disc_name: Some(disc_name(disc)),
            volume_id: disc.volume_id.clone(),
        })
    }

    /// `title` alone: no disc name or volume id.
    pub fn new(title: crate::disc::DiscTitle, format: crate::disc::ContentFormat) -> Self {
        ScannedTitle {
            title,
            format,
            disc_name: None,
            volume_id: String::new(),
        }
    }
}

// A disc's name: its meta title, else its volume id.
fn disc_name(d: &Disc) -> String {
    d.meta_title.clone().unwrap_or_else(|| d.volume_id.clone())
}

/// An opened input: where PES frames come from, and the layout probed from it.
pub struct Source<'a> {
    pub(crate) origin: Origin<'a>,
}

pub(crate) enum Origin<'a> {
    // A live drive session the caller keeps (tray lock, eject, finish stay with it).
    Session(&'a mut DiscSession),
    // `disc://`: a session opened and scanned here.
    Drive(Box<DiscSession>),
    // An image (`iso://`, `dir://`) and the probe's reader.
    Image {
        path: PathBuf,
        folder: bool,
        reader: Box<dyn SectorSource>,
        disc: Disc,
    },
    // An image file read with a title the caller scanned elsewhere (J14): opened at read
    // time, never rescanned.
    Prescanned {
        path: PathBuf,
        title: ScannedTitle,
    },
    // A raw live reader the caller already holds, with the title scanned off it.
    Live {
        reader: Box<dyn SectorSource>,
        title: ScannedTitle,
    },
    // A container or stream input, opened when it is read.
    Stream {
        url: String,
    },
}

/// Open the input `url` and probe it: a drive is brought up and scanned (`disc://`, with
/// `probe`'s credentials and key sources for the handshake), an image or folder is scanned
/// (`iso://`, `dir://`; a folder's AACS verdict is judged from its content). Container and
/// stream inputs (`m2ts://`, `mkv://`, `mp4://`, `mpg://`, `network://`, `stdio://`) open
/// when read. `ctx.halt` stops the bring-up and the scan.
pub fn open_source(url: &str, mut probe: ScanOptions, ctx: &Ctx) -> Result<Source<'static>> {
    if probe.halt.is_none() {
        probe.halt = Some(ctx.halt.clone());
    }
    let origin = match parse_url(url) {
        StreamUrl::Disc { device } => {
            let target = device.map_or(DeviceTarget::Autodetect, DeviceTarget::Path);
            let spec = KeySpec {
                credentials: probe.credentials.take(),
                key_sources: std::mem::take(&mut probe.key_sources),
                ..KeySpec::default()
            };
            let mut session = DiscSession::open_with(target, spec, &ctx.halt)?;
            session.scan_with(probe)?;
            Origin::Drive(Box::new(session))
        }
        StreamUrl::Iso { path } => {
            super::resolve::validate_path(&path, "iso")?;
            let (disc, reader) = probe_image(&path, false, &probe)?;
            Origin::Image {
                path,
                folder: false,
                reader,
                disc,
            }
        }
        StreamUrl::Dir { path } => {
            super::resolve::validate_path(&path, "dir")?;
            let (disc, reader) = probe_image(&path, true, &probe)?;
            Origin::Image {
                path,
                folder: true,
                reader,
                disc,
            }
        }
        StreamUrl::M2ts { .. }
        | StreamUrl::Mkv { .. }
        | StreamUrl::Mp4 { .. }
        | StreamUrl::Mpg { .. }
        | StreamUrl::Network { .. }
        | StreamUrl::Stdio => Origin::Stream {
            url: url.to_string(),
        },
        StreamUrl::Null
        | StreamUrl::Demux { .. }
        | StreamUrl::Video { .. }
        | StreamUrl::Audio { .. }
        | StreamUrl::Sub { .. }
        | StreamUrl::Fvi { .. }
        | StreamUrl::Chapters { .. }
        | StreamUrl::Json { .. } => return Err(Error::StreamWriteOnly),
        StreamUrl::Unknown { raw } => return Err(Error::StreamUrlInvalid { url: raw }),
    };
    Ok(Source { origin })
}

/// The one image probe (`iso://` file or `dir://` folder): scan the image, and judge a
/// folder's AACS verdict from its content (tree shape alone can be wrong for a decrypted
/// folder that kept `AACS/`). Returns the layout and the probe's reader.
pub(crate) fn probe_image(
    path: &Path,
    folder: bool,
    probe: &ScanOptions,
) -> Result<(Disc, Box<dyn SectorSource>)> {
    let mut reader: Box<dyn SectorSource> = if folder {
        Box::new(crate::dirimage::DirImage::open(path)?)
    } else {
        Box::new(crate::io::file_sector_source::FileSectorSource::open(path)?)
    };
    let cap = reader.capacity_sectors();
    let mut disc = Disc::scan_image(&mut reader, cap, probe)?;
    if folder {
        crate::session::apply_folder_encryption_verdict(&mut reader, &mut disc)?;
    }
    Ok((disc, reader))
}

impl<'a> Source<'a> {
    /// A live drive the caller opened and scanned; the session stays the caller's, so it
    /// still locks, ejects and finishes the tray. Its drive is staged as the reader here.
    pub fn from_session(session: &'a mut DiscSession) -> Source<'a> {
        Source {
            origin: Origin::Session(session),
        }
    }

    /// The image file at `path` read with a title the caller already scanned (the drive's,
    /// J14): it is never rescanned, so a sweep's unread filesystem sectors do not matter.
    /// The mux reads this one title whatever `MuxOptions::title_index` says.
    pub fn from_image(path: &Path, title: ScannedTitle) -> Source<'static> {
        Source {
            origin: Origin::Prescanned {
                path: path.to_path_buf(),
                title,
            },
        }
    }

    /// A raw live reader the caller holds, with the title scanned off it (read like
    /// [`Self::from_image`]'s, whatever `MuxOptions::title_index` says).
    pub fn from_reader(reader: Box<dyn SectorSource>, title: ScannedTitle) -> Source<'static> {
        Source {
            origin: Origin::Live { reader, title },
        }
    }

    /// The probed layout: the scanned disc for `disc://`, `iso://`, `dir://` and the
    /// `from_*` forms; `None` for a container or stream input.
    pub fn layout(&self) -> Option<&Disc> {
        match &self.origin {
            Origin::Session(s) => s.disc(),
            Origin::Drive(s) => s.disc(),
            Origin::Image { disc, .. } => Some(disc),
            Origin::Prescanned { .. } | Origin::Live { .. } | Origin::Stream { .. } => None,
        }
    }

    /// The opened drive session, for `disc://` and [`Self::from_session`].
    pub fn session_mut(&mut self) -> Option<&mut DiscSession> {
        match &mut self.origin {
            Origin::Session(s) => Some(s),
            Origin::Drive(s) => Some(s),
            _ => None,
        }
    }

    /// The drive session `open_source("disc://…")` opened, so the caller can finish it
    /// (unlock, eject). `None` for every other input.
    pub fn into_session(self) -> Option<DiscSession> {
        match self.origin {
            Origin::Drive(s) => Some(*s),
            _ => None,
        }
    }

    /// The probe's layout and raw reader, for a caller that keys or copies the image itself
    /// (`iso://`, `dir://`). `None` for every other input.
    pub fn into_image(self) -> Option<(Disc, Box<dyn SectorSource>)> {
        match self.origin {
            Origin::Image { reader, disc, .. } => Some((disc, reader)),
            _ => None,
        }
    }

    /// The raw reader the layout was probed through, for key acquisition before a mux.
    pub fn reader_mut(&mut self) -> Option<&mut dyn SectorSource> {
        match &mut self.origin {
            Origin::Session(s) => s.source_mut(),
            Origin::Drive(s) => s.source_mut(),
            Origin::Image { reader: r, .. } | Origin::Live { reader: r, .. } => Some(r.as_mut()),
            _ => None,
        }
    }

    // The provenance of title `title` read from this source (one place, SO15).
    pub(crate) fn provenance(&self, title: usize) -> SourceInfo {
        let (medium, path) = match &self.origin {
            Origin::Session(s) => (Medium::Disc, format!("disc://{}", s.device_path())),
            Origin::Drive(s) => (Medium::Disc, format!("disc://{}", s.device_path())),
            Origin::Image { path, folder, .. } => (
                Medium::Iso,
                format!(
                    "{}://{}",
                    if *folder { "dir" } else { "iso" },
                    path.display()
                ),
            ),
            Origin::Prescanned { path, .. } => (Medium::Iso, format!("iso://{}", path.display())),
            Origin::Live { .. } => (Medium::Disc, String::new()),
            Origin::Stream { url } => (super::driver::url_medium(&parse_url(url)), url.clone()),
        };
        let (playlist, volume_id) = match (&self.origin, self.layout()) {
            (Origin::Prescanned { title: t, .. } | Origin::Live { title: t, .. }, _) => {
                (t.title.playlist.clone(), t.volume_id.clone())
            }
            (_, Some(d)) => (
                d.titles
                    .get(title)
                    .map(|t| t.playlist.clone())
                    .unwrap_or_default(),
                d.volume_id.clone(),
            ),
            (_, None) => (String::new(), String::new()),
        };
        // A pre-scanned title carries no index; it is reported as 0.
        let title = match &self.origin {
            Origin::Prescanned { .. } | Origin::Live { .. } => 0,
            _ => title,
        };
        SourceInfo {
            medium,
            path,
            title,
            playlist,
            volume_id,
        }
    }

    // The output title's name (SO14): every disc layout names it by the disc (its meta
    // title, else its volume id); a container keeps its own.
    pub(crate) fn title_name(&self) -> Option<String> {
        match &self.origin {
            Origin::Prescanned { title, .. } | Origin::Live { title, .. } => {
                title.disc_name.clone()
            }
            _ => self.layout().map(disc_name),
        }
    }
}
