//! The PES outputs: one closed enum per destination that answers what it carries ([`fit`]),
//! what it takes ([`caps`]) and opens its sink ([`open`]), so no caller matches on schemes.
//!
//! [`fit`]: PesOutput::fit
//! [`caps`]: PesOutput::caps
//! [`open`]: PesOutput::open

use super::demux_sink::{DemuxOptions, DemuxSink, TrackKind};
use super::fit::{FitReport, keep_fit};
use super::network::NetworkStream;
use super::null::NullStream;
use super::resolve::{
    IO_BUF_SIZE, OutputFlush, SinkCaps, StreamUrl, validate_file_path, validate_network_addr,
};
use super::stdio::StdioStream;
use super::videomap::SourceInfo;
use super::{M2tsStream, MkvStream};
use crate::disc::DiscTitle;
use crate::error::Error;
use crate::pes::PesSink;
use std::io;
use std::path::Path;

/// An opened sink and the identity of the file it writes (`None` for a non-file sink, or
/// when the handle's fstat failed).
pub(crate) type Opened = (Box<dyn PesSink>, Option<(u64, u64)>);

// Every caps flag set: what a sink with no restriction (or a non-PES URL) reports.
pub(crate) const FULL: SinkCaps = SinkCaps {
    needs_frames: true,
    carries_mp2_extensions: true,
};

/// One PES output destination, parsed from its URL.
pub(crate) enum PesOutput<'u> {
    Mkv(&'u Path),
    Mp4(&'u Path),
    Mpg(&'u Path),
    M2ts(&'u Path),
    Network(&'u str),
    Stdio,
    Null,
    /// `demux://`: every track to its own file, with the chapters sidecar.
    Demux(&'u Path),
    /// `video://`, `audio://`, `sub://`: the tracks of one class, no sidecar.
    Tracks {
        dir: &'u Path,
        kind: TrackKind,
        scheme: &'static str,
    },
    Fvi(&'u Path),
    Chapters(&'u Path),
    Json(&'u Path),
}

impl<'u> PesOutput<'u> {
    /// The PES output `url` names: [`Error::StreamReadOnly`] for one that is not a PES sink
    /// (`disc://`, `iso://`, `dir://`), [`Error::StreamUrlInvalid`] for an unknown one.
    pub(crate) fn of(url: &'u StreamUrl) -> Result<Self, Error> {
        let tracks = |dir: &'u Path, kind| PesOutput::Tracks {
            dir,
            kind,
            scheme: url.info().name,
        };
        Ok(match url {
            StreamUrl::Mkv { path } => PesOutput::Mkv(path),
            StreamUrl::Mp4 { path } => PesOutput::Mp4(path),
            StreamUrl::Mpg { path } => PesOutput::Mpg(path),
            StreamUrl::M2ts { path } => PesOutput::M2ts(path),
            StreamUrl::Network { addr } => PesOutput::Network(addr),
            StreamUrl::Stdio => PesOutput::Stdio,
            StreamUrl::Null => PesOutput::Null,
            StreamUrl::Demux { dir } => PesOutput::Demux(dir),
            StreamUrl::Video { dir } => tracks(dir, TrackKind::Video),
            StreamUrl::Audio { dir } => tracks(dir, TrackKind::Audio),
            StreamUrl::Sub { dir } => tracks(dir, TrackKind::Subtitle),
            StreamUrl::Fvi { path } => PesOutput::Fvi(path),
            StreamUrl::Chapters { path } => PesOutput::Chapters(path),
            StreamUrl::Json { path } => PesOutput::Json(path),
            StreamUrl::Unknown { raw } => return Err(Error::StreamUrlInvalid { url: raw.clone() }),
            // `iso://`/`dir://` are written whole-disc (`crate::io::open_block_sink`,
            // `crate::io::open_tree_sink`); `disc://` is input only.
            StreamUrl::Disc { .. } | StreamUrl::Iso { .. } | StreamUrl::Dir { .. } => {
                return Err(Error::StreamReadOnly);
            }
        })
    }

    /// The caps of the output `url`; a non-PES URL reports every flag set.
    pub(crate) fn caps_of(url: &StreamUrl) -> SinkCaps {
        PesOutput::of(url).map_or(FULL, |out| out.caps())
    }

    /// What this sink takes (pipeline design §2.2 sink caps).
    pub(crate) fn caps(&self) -> SinkCaps {
        match self {
            PesOutput::Chapters(_) | PesOutput::Json(_) => SinkCaps {
                needs_frames: false,
                ..FULL
            },
            PesOutput::Mkv(_)
            | PesOutput::Mp4(_)
            | PesOutput::M2ts(_)
            | PesOutput::Demux(_)
            | PesOutput::Tracks {
                kind: TrackKind::Audio,
                ..
            } => SinkCaps {
                carries_mp2_extensions: false,
                ..FULL
            },
            PesOutput::Mpg(_)
            | PesOutput::Network(_)
            | PesOutput::Stdio
            | PesOutput::Null
            | PesOutput::Tracks { .. }
            | PesOutput::Fvi(_) => FULL,
        }
    }

    /// The pre-mux plan of `title` for this sink (see [`super::fit::fit_report`]).
    pub(crate) fn fit(&self, title: &DiscTitle) -> FitReport {
        match self {
            PesOutput::Mpg(_) => super::mpg::plan(title).report,
            PesOutput::Mp4(_) => super::mp4::fit_report(title),
            PesOutput::M2ts(_) => keep_fit(title, None, |i| super::m2ts::fit_refusal(title, i)),
            PesOutput::Mkv(_) => keep_fit(title, None, |i| {
                title.streams.get(i).and_then(super::mkv::fit_refusal)
            }),
            PesOutput::Tracks { kind, .. } => keep_fit(title, Some(*kind), |_| None),
            PesOutput::Network(_)
            | PesOutput::Stdio
            | PesOutput::Null
            | PesOutput::Demux(_)
            | PesOutput::Fvi(_)
            | PesOutput::Chapters(_)
            | PesOutput::Json(_) => keep_fit(title, None, |_| None),
        }
    }

    /// Open the sink for `title`; `source` is the input's provenance (only `fvi://` reads
    /// it) and `flush` the counters a file output shares with the mux.
    pub(crate) fn open(
        self,
        title: &DiscTitle,
        source: Option<&SourceInfo>,
        flush: Option<&OutputFlush>,
    ) -> io::Result<Opened> {
        let unfiled = |sink: io::Result<Box<dyn PesSink>>| sink.map(|sink| (sink, None));
        match self {
            PesOutput::Mkv(path) => {
                validate_file_path(path, "mkv")?;
                // Wrap in `WritebackFile` (bounded-cache) so a UHD mux to slow/
                // network staging avoids dirty-page bursts. BufWriter coalesces
                // small EBML writes; Linux fallocate(KEEP_SIZE) cuts fragmentation.
                created(path, || {
                    let (file, id) = writeback_file(path, title.size_bytes, flush)?;
                    let writer: Box<dyn super::WriteSeek + Send> =
                        Box::new(std::io::BufWriter::with_capacity(IO_BUF_SIZE, file));
                    Ok((Box::new(MkvStream::create(writer, title, Some(path))?), id))
                })
            }
            PesOutput::Mp4(path) => {
                validate_file_path(path, "mp4")?;
                // Bounded-cache writeback (like mkv://); the mdat backpatch is an ordinary
                // seek WritebackFile handles. BufWriter coalesces moov writes.
                created(path, || {
                    let (file, id) = writeback_file(path, title.size_bytes, flush)?;
                    let writer = std::io::BufWriter::with_capacity(IO_BUF_SIZE, file);
                    Ok((Box::new(super::mp4::Mp4Sink::create(writer, title)?), id))
                })
            }
            PesOutput::Mpg(path) => {
                validate_file_path(path, "mpg")?;
                // Streaming pack writer, no seeks: the bounded-cache writeback as for mkv://.
                created(path, || {
                    let (file, id) = writeback_file(path, title.size_bytes, flush)?;
                    let writer = std::io::BufWriter::with_capacity(IO_BUF_SIZE, file);
                    Ok((Box::new(super::mpg::MpgSink::create(writer, title)?), id))
                })
            }
            PesOutput::M2ts(path) => {
                validate_file_path(path, "m2ts")?;
                created(path, || {
                    let (file, id) = writeback_file(path, title.size_bytes, flush)?;
                    let writer = std::io::BufWriter::with_capacity(IO_BUF_SIZE, file);
                    Ok((Box::new(M2tsStream::create(writer, title)?), id))
                })
            }
            PesOutput::Network(addr) => {
                // `NetworkStream::connect` refuses only unspecified/multicast/
                // broadcast targets; LAN and loopback hosts are allowed.
                validate_network_addr(addr)?;
                Ok((Box::new(NetworkStream::connect(addr)?.meta(title)), None))
            }
            PesOutput::Stdio => Ok((Box::new(StdioStream::output(title)), None)),
            PesOutput::Null => Ok((Box::new(NullStream::new(title)), None)),
            // The CLI's full `--demux/--naming/--delay/--container/--chapters` flags are not
            // here: this is the default option set, its base seeded from the playlist name.
            PesOutput::Demux(dir) => {
                validate_file_path(dir, "demux")?;
                unfiled(demux_sink_output(dir, title, DemuxOptions::default()))
            }
            PesOutput::Tracks { dir, kind, scheme } => {
                validate_file_path(dir, scheme)?;
                let opts = DemuxOptions {
                    kind_filter: Some(kind),
                    export_chapters: false,
                    ..Default::default()
                };
                unfiled(demux_sink_output(dir, title, opts))
            }
            // The header's `source` describes the INPUT, from caller-supplied `source`,
            // never `path`, which previously broke reproducibility.
            PesOutput::Fvi(path) => {
                validate_file_path(path, "fvi")?;
                created(path, || {
                    let (file, id) = open_file(path)?;
                    let source = source.cloned().unwrap_or_default();
                    let sink = super::fvi_sink::FviSink::create(file, title, source);
                    Ok((Box::new(sink), id))
                })
            }
            // `chapters://`/`json://` write title metadata at construction and
            // ignore the PES stream (see `meta_sink`).
            PesOutput::Chapters(path) => {
                validate_file_path(path, "chapters")?;
                created(path, || {
                    let (file, id) = open_file(path)?;
                    let sink = super::meta_sink::ChaptersSink::create(file, path, title)?;
                    Ok((Box::new(sink), id))
                })
            }
            PesOutput::Json(path) => {
                validate_file_path(path, "json")?;
                created(path, || {
                    let (file, id) = open_file(path)?;
                    Ok((
                        Box::new(super::meta_sink::JsonSink::create(file, title)?),
                        id,
                    ))
                })
            }
        }
    }
}

// Opens a file output's sink with `open`. A file absent before (claimed by create_new) that
// the open then fails to fill is removed, while the path still names it; a file that was
// already there is never removed.
fn created(path: &Path, open: impl FnOnce() -> io::Result<Opened>) -> io::Result<Opened> {
    use crate::io::artifact_lock::file_identity;
    let ours = match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
    {
        Ok(_) => file_identity(path).ok(),
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => None,
        Err(e) => return Err(e),
    };
    let sink = open();
    if sink.is_err()
        && let Some(id) = ours
        && file_identity(path).ok() == Some(id)
        && let Err(e) = std::fs::remove_file(path)
    {
        tracing::warn!(target: "mux", error = %e, "a failed output's new file could not be removed");
    }
    sink
}

// A file output's bounded-cache writer, sharing `flush` when given, and its identity.
fn writeback_file(
    path: &Path,
    size_hint: u64,
    flush: Option<&OutputFlush>,
) -> io::Result<(crate::io::WritebackFile, Option<(u64, u64)>)> {
    let mut w = crate::io::WritebackFile::create_with_size_hint(path, size_hint)?;
    if let Some(f) = flush {
        w.set_flush_progress(f.progress.clone());
        w.set_halt(f.halt.clone());
    }
    let id = w.identity().ok();
    Ok((w, id))
}

// A file output created (truncated) at `path`, and its identity from the handle.
fn open_file(path: &Path) -> io::Result<(std::fs::File, Option<(u64, u64)>)> {
    let file = std::fs::File::create(path)?;
    let id = crate::io::artifact_lock::file_identity_of(&file).ok();
    Ok((file, id))
}

// A `DemuxSink` over `dir`, its base name seeded from the playlist name.
fn demux_sink_output(
    dir: &Path,
    title: &DiscTitle,
    mut opts: DemuxOptions,
) -> io::Result<Box<dyn PesSink>> {
    if !title.playlist.is_empty() {
        opts.base = title.playlist.clone();
    }
    Ok(Box::new(DemuxSink::create(dir, title, &opts)?))
}

#[cfg(test)]
#[path = "pes_output_tests.rs"]
mod tests;
