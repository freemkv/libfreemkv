//! Progressive MP4 (ISO-BMFF) demuxer — the read side of `mp4://`.
//!
//! The inverse of the writer in [`super`]: parse `moov`/`trak`/`stbl`, rebuild a
//! [`DiscTitle`] and a per-sample index (offset/size/timing/sync from
//! `stsc`+`stco`/`co64`+`stsz`, `stts`+`ctts`, `stss`), then stream each sample
//! out as a [`PesFrame`] in decode order. Video NALs are length-prefixed in MP4,
//! so `mp4://` → any sink needs no reframing.
//!
//! Scope: progressive MP4 (`moov` + `mdat`); fragmented MP4 (`moof`, samples in
//! `traf`/`trun`) is out of scope for now.

use crate::disc::{
    AudioChannels, AudioStream, Codec, ColorSpace, DiscTitle, FrameRate, HdrFormat, MeasuredCicp,
    Resolution, SampleRate, Stream as DiscStream, VideoStream,
};
use crate::labels::LabelPurpose;
use crate::pes::{PesFrame, PesSource};
use std::io::{self, Read, Seek, SeekFrom};

const NS: i128 = 1_000_000_000;

// Upper bound on the number of tracks — the per-track PID (`0x1011 + track_idx`) overflows u16
// past ~61k.
const MAX_TRACKS: usize = 512;

// Upper bound on a track's decoded sample count; caps a crafted box's allocation.
const MAX_SAMPLE_COUNT: usize = 1 << 24;

// Smallest FILE bytes one indexed sample is assumed to occupy (divisor for `from_reader`'s
// sample budget).
const MIN_FILE_BYTES_PER_SAMPLE: u64 = 16;

// Ceiling on a single allocation sized from an untrusted MP4 field, since a sparse file can
// inflate `file_len` cheaply.
const MAX_ALLOC_BYTES: u64 = 256 << 20; // 256 MiB

/// One sample's location + timing in the emission plan.
struct SampleRef {
    track: usize,
    offset: u64,
    size: u32,
    /// Composition (presentation) time in nanoseconds.
    pts_ns: i64,
    /// Decode time in nanoseconds — the key the global emission order sorts on.
    dts_ns: i64,
    keyframe: bool,
}

/// MP4 reader: a `Stream` source that emits a file's samples as PES frames.
/// Generic over the backing reader so it works over a `File` (the `mp4://`
/// source) or an in-memory `Cursor` (round-trip tests).
pub struct Mp4Reader<R: Read + Seek> {
    file: R,
    /// Total length of the backing file, captured at open — used to reject a
    /// crafted `stsz` sample size that would over-allocate the per-sample buffer.
    file_len: u64,
    title: DiscTitle,
    samples: Vec<SampleRef>,
    cursor: usize,
}

impl<R: Read + Seek> Mp4Reader<R> {
    /// Index an already-opened seekable MP4 reader.
    pub fn from_reader(mut file: R, name: String) -> io::Result<Self> {
        let file_len = file.seek(SeekFrom::End(0))?;
        file.seek(SeekFrom::Start(0))?;
        let moov = read_moov(&mut file)?;
        let mut title = DiscTitle::empty();
        title.playlist = name;

        // Movie timescale (ISO/IEC 14496-12 §8.2.2). An edit list's
        // `segment_duration` is expressed in it, while its `media_time` is in the
        // track's own media timescale, so both are needed to place an edit.
        let movie_timescale = find_box(&moov, b"mvhd")
            .and_then(mvhd_timescale)
            .filter(|&t| t != 0);

        let mut samples: Vec<SampleRef> = Vec::new();
        let mut codec_privates: Vec<Option<Vec<u8>>> = Vec::new();
        let mut track_idx = 0usize;
        // Global cap on decoded samples across ALL tracks, since many small `trak` boxes
        // could each stay under the per-track cap yet sum past it. Divided by
        // MIN_FILE_BYTES_PER_SAMPLE so a crafted `stsz` can't force a ~1 GiB eager alloc.
        let mut sample_budget = MAX_SAMPLE_COUNT
            .min((file_len / MIN_FILE_BYTES_PER_SAMPLE).min(usize::MAX as u64) as usize);

        // Bound the scan at MAX_TRACKS *matches* so a crafted moov packed with tiny
        // (8-byte) trak headers can't force the scan to materialize a Vec far
        // larger than the moov payload before the per-track cap below ever runs.
        for trak in find_boxes_capped(&moov, b"trak", MAX_TRACKS) {
            // `find_boxes_capped` yields at most MAX_TRACKS matches, so `track_idx`
            // never reaches MAX_TRACKS here (old runtime `break` was dead). Assert the
            // invariant instead so the per-track PID stays within u16 without dead flow.
            debug_assert!(track_idx < MAX_TRACKS, "trak scan exceeded MAX_TRACKS");
            let Some(mdia) = find_box(trak, b"mdia") else {
                tracing::warn!(track = track_idx, "mp4: trak has no mdia, dropping track");
                continue;
            };
            let timescale = find_box(mdia, b"mdhd")
                .and_then(mdhd_timescale)
                .filter(|&t| t != 0) // a crafted mdhd timescale of 0 would divide-by-zero below
                .unwrap_or(90_000);
            let language = find_box(mdia, b"mdhd").and_then(mdhd_language);
            let handler = find_box(mdia, b"hdlr").and_then(hdlr_type);
            let Some(minf) = find_box(mdia, b"minf") else {
                tracing::warn!(track = track_idx, "mp4: mdia has no minf, dropping track");
                continue;
            };
            let Some(stbl) = find_box(minf, b"stbl") else {
                tracing::warn!(track = track_idx, "mp4: minf has no stbl, dropping track");
                continue;
            };
            let Some(stsd) = find_box(stbl, b"stsd") else {
                tracing::warn!(track = track_idx, "mp4: stbl has no stsd, dropping track");
                continue;
            };

            let Some(StsdInfo {
                codec,
                height,
                config,
                cicp,
                dolby_vision,
                channels,
                sample_rate: entry_sample_rate,
                dts_max_rate,
                dts_hd,
            }) = parse_stsd(stsd)
            else {
                tracing::warn!(
                    track = track_idx,
                    "mp4: unrecognised stsd sample entry, dropping track"
                );
                continue;
            };

            // Build the stream model for this track.
            let mut stream = match handler {
                Some(h) if &h == b"vide" => DiscStream::Video(VideoStream {
                    pid: 0x1011 + track_idx as u16,
                    codec,
                    resolution: Resolution::from_height(height as u32),
                    frame_rate: FrameRate::Unknown,
                    hdr: hdr_from_cicp(cicp, dolby_vision),
                    color_space: cicp.map_or(crate::disc::ColorSpace::Unknown, |c| {
                        color_space_from_primaries(c.primaries)
                    }),
                    display_aspect: None,
                    secondary: false,
                    label: String::new(),
                    measured_cicp: cicp,
                }),
                Some(h) if &h == b"soun" => DiscStream::Audio(AudioStream {
                    pid: 0x1100 + track_idx as u16,
                    codec,
                    // `channels` is an untrusted u16; saturate rather than wrap with
                    // `as u8` (a crafted 256 would alias to 0/Mono).
                    channels: AudioChannels::from_count(channels.min(u8::MAX as u16) as u8),
                    language: language.clone().unwrap_or_else(|| "und".into()),
                    sample_rate: SampleRate::from_hz(
                        dts_max_rate
                            .unwrap_or_else(|| audio_rate(entry_sample_rate, timescale, dts_hd)),
                    ),
                    secondary: false,
                    purpose: LabelPurpose::Normal,
                    label: String::new(),
                }),
                _ => {
                    tracing::debug!(
                        track = track_idx,
                        "mp4: non-audio/video handler, skipping track"
                    );
                    continue;
                }
            };

            // Per-sample tables. `stsz` is bounded by the remaining global budget;
            // `stts`/`ctts` need at most one entry per sample, so they are bounded by
            // this track's sample count (indices past it are never read).
            let sizes = find_box(stbl, b"stsz")
                .map(|b| parse_stsz(b, sample_budget))
                .or_else(|| find_box(stbl, b"stz2").map(|b| parse_stz2(b, sample_budget)))
                .unwrap_or_default();
            let n = sizes.len();
            if n == 0 {
                track_idx += 1;
                title.streams.push(stream);
                codec_privates.push(config);
                continue;
            }
            let chunk_offsets = find_box(stbl, b"stco")
                .map(|b| parse_stco(b, false))
                .or_else(|| find_box(stbl, b"co64").map(|b| parse_stco(b, true)))
                .unwrap_or_default();
            if chunk_offsets.is_empty() {
                tracing::warn!(
                    track = track_idx,
                    samples = n,
                    "mp4: stbl has samples but no stco/co64 chunk-offset table, dropping track"
                );
                // No chunk-offset table means every sample offset would resolve to file
                // byte 0 (muxing header bytes as frame data); drop the track instead.
                continue;
            }
            let stsc = find_box(stbl, b"stsc").map(parse_stsc).unwrap_or_default();
            if stsc.is_empty() {
                tracing::warn!(
                    track = track_idx,
                    samples = n,
                    "mp4: stbl has samples but no stsc sample-to-chunk map, dropping track"
                );
                // No sample-to-chunk map: samples can't be placed against the chunk
                // offsets (they would pack from byte 0). Drop the track rather than
                // emit header bytes as frame data — a valid stbl always has stsc.
                continue;
            }
            let offsets = sample_offsets(&sizes, &chunk_offsets, &stsc);
            if offsets.len() < sizes.len() {
                // The stsc passed the non-empty guard but does not place every
                // sample. The unplaced tail has no real offset, so carrying the
                // track would read frames from arbitrary file bytes.
                tracing::warn!(
                    track = track_idx,
                    placed = offsets.len(),
                    samples = n,
                    "mp4: stsc places fewer samples than stsz declares, dropping track"
                );
                continue;
            }
            // ISO 11172-3 layer field: `Mp3` from the esds OTI covers Layers I-III.
            if let DiscStream::Audio(a) = &mut stream
                && a.codec == Codec::Mp3
            {
                a.codec = mpeg_audio_layer(&mut file, file_len, offsets[0]).unwrap_or(a.codec);
            }
            let durations = find_box(stbl, b"stts")
                .map(|b| parse_stts(b, n))
                .unwrap_or_default();
            if durations.len() < n {
                // `stts` is mandatory and must cover every sample (ISO/IEC 14496-12
                // §8.6.1); absent or short gives unmapped tail samples dur=0,
                // collapsing them onto one instant, so refuse both cases.
                tracing::warn!(
                    track = track_idx,
                    durations = durations.len(),
                    samples = n,
                    "mp4: stts does not cover every sample, dropping track"
                );
                // Mirrors the stco/stsc guards above: drop rather than emit
                // degenerate all-zero timing for the unmapped tail.
                continue;
            }
            // Charge the shared budget only for tracks that survive every drop guard.
            sample_budget = sample_budget.saturating_sub(n);
            if let DiscStream::Video(v) = &mut stream {
                v.frame_rate = frame_rate_from_stts(timescale, &durations);
            }
            let track_secs =
                durations.iter().map(|&d| d as u64).sum::<u64>() as f64 / timescale as f64;
            title.duration_secs = title.duration_secs.max(track_secs);
            let ctts = find_box(stbl, b"ctts")
                .map(|b| parse_ctts(b, n))
                .unwrap_or_default();
            let sync = find_box(stbl, b"stss").map(parse_stss);

            // ticks → ns, saturating: a crafted tiny timescale + huge stts deltas can
            // push the i128 quotient past i64::MAX; wrapping it would silently corrupt
            // the sort/timestamps, so clamp instead.
            let to_ns = |ticks: i64| -> i64 {
                (ticks as i128 * NS / timescale as i128).clamp(i64::MIN as i128, i64::MAX as i128)
                    as i64
            };
            // Edit list (ISO/IEC 14496-12 §8.6.5 `edts` / §8.6.6 `elst`): presentation
            // timeline != media timeline. Ignoring it starts every track at media time
            // 0, silently shifting tracks with an encoder-delay/A-V-offset edit.
            let edit_offset_ticks = find_box(trak, b"edts")
                .and_then(|edts| find_box(edts, b"elst"))
                .map(|elst| {
                    let entries = parse_elst(elst);
                    elst_offset_ticks(&entries, movie_timescale, timescale, track_idx)
                })
                .unwrap_or(0);

            let mut decode_ticks: i64 = 0;
            for (i, &size) in sizes.iter().enumerate() {
                let dur = durations.get(i).copied().unwrap_or(0);
                let comp = ctts.get(i).copied().unwrap_or(0);
                let dts_ns = to_ns(decode_ticks.saturating_add(edit_offset_ticks));
                let pts_ticks = decode_ticks
                    .saturating_add(comp as i64)
                    .saturating_add(edit_offset_ticks);
                let pts_ns = to_ns(pts_ticks);
                decode_ticks = decode_ticks.saturating_add(dur as i64);
                let keyframe = match &sync {
                    Some(set) => set.contains(&(i as u32 + 1)),
                    None => true, // no stss → every sample is a sync sample
                };
                samples.push(SampleRef {
                    track: track_idx,
                    offset: offsets.get(i).copied().unwrap_or(0),
                    size,
                    pts_ns,
                    dts_ns,
                    keyframe,
                });
            }

            title.streams.push(stream);
            codec_privates.push(config);
            track_idx += 1;
        }

        if title.streams.is_empty() {
            return Err(crate::error::Error::Mp4Invalid.into());
        }
        title.codec_privates = codec_privates;

        // Emit in global decode order so the consumer sees interleaved,
        // monotonic-DTS frames (a stable sort keeps per-track order on ties).
        samples.sort_by_key(|s| s.dts_ns);

        Ok(Self {
            file,
            file_len,
            title,
            samples,
            cursor: 0,
        })
    }
}

impl<R: Read + Seek + Send> PesSource for Mp4Reader<R> {
    fn read(&mut self) -> io::Result<Option<PesFrame>> {
        let Some(s) = self.samples.get(self.cursor) else {
            return Ok(None);
        };
        self.cursor += 1;
        // `s.size`/`s.offset` come from the untrusted stsz/stco tables; reject a
        // sample that claims to extend past EOF before allocating its buffer, so a
        // crafted size can't force a multi-GB allocation the read would then fail.
        let end = s.offset.checked_add(s.size as u64);
        if s.size as u64 > MAX_ALLOC_BYTES || end.is_none_or(|e| e > self.file_len) {
            return Err(crate::error::Error::Mp4Invalid.into());
        }
        self.file.seek(SeekFrom::Start(s.offset))?;
        let mut data = vec![0u8; s.size as usize];
        self.file.read_exact(&mut data)?;
        Ok(Some(PesFrame {
            discard_padding_ns: 0,
            track: s.track,
            pts: s.pts_ns,
            keyframe: s.keyframe,
            data,
            duration_ns: None,
            source: None,
            coding: None,
        }))
    }

    fn info(&self) -> &DiscTitle {
        &self.title
    }

    fn codec_private(&self, track: usize) -> Option<Vec<u8>> {
        self.title.codec_privates.get(track).and_then(|c| c.clone())
    }
}

// ── box tree navigation ──────────────────────────────────────────────────────

/// Read top-level boxes until `moov`, returning its payload (after the header).
/// Skips over `ftyp`/`mdat`/etc. via seek; samples are read later by offset.
fn read_moov<R: Read + Seek>(file: &mut R) -> io::Result<Vec<u8>> {
    let file_end = file.seek(SeekFrom::End(0))?;
    file.seek(SeekFrom::Start(0))?;
    loop {
        let pos = file.stream_position()?;
        let mut hdr = [0u8; 8];
        if file.read_exact(&mut hdr).is_err() {
            return Err(crate::error::Error::Mp4Invalid.into());
        }
        let size32 = u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]);
        let btype = [hdr[4], hdr[5], hdr[6], hdr[7]];
        // Total box size INCLUDING the header. `size==1` → 64-bit largesize in the
        // next 8 bytes (16-byte header); `size==0` → the box runs to end of file.
        let box_size: u64 = match size32 {
            1 => {
                let mut ext = [0u8; 8];
                if file.read_exact(&mut ext).is_err() {
                    return Err(crate::error::Error::Mp4Invalid.into());
                }
                u64::from_be_bytes(ext)
            }
            0 => file_end.saturating_sub(pos),
            n => n as u64,
        };
        let header_len: u64 = if size32 == 1 { 16 } else { 8 };
        // A box must contain at least its header and not run past EOF; this also
        // guarantees forward progress so a crafted size < 8 can't spin the loop.
        // checked_add stops a 64-bit largesize near u64::MAX from wrapping past the guard.
        if box_size < header_len || pos.checked_add(box_size).is_none_or(|end| end > file_end) {
            return Err(crate::error::Error::Mp4Invalid.into());
        }
        if &btype == b"moov" {
            let payload_len = box_size - header_len;
            // Absolute cap independent of the (sparse-file-inflatable) length.
            if payload_len > MAX_ALLOC_BYTES {
                return Err(crate::error::Error::Mp4Invalid.into());
            }
            let mut buf = vec![0u8; payload_len as usize];
            file.read_exact(&mut buf)?;
            return Ok(buf);
        }
        file.seek(SeekFrom::Start(pos + box_size))?;
    }
}

/// The first child box of `payload` with the given type — returns its payload
/// (bytes after the 8-byte header). One level.
fn find_box<'a>(payload: &'a [u8], want: &[u8; 4]) -> Option<&'a [u8]> {
    // cap=1: a single lookup only needs the first match, so a crafted payload
    // packed with millions of tiny boxes can't force a huge transient match Vec
    // before `.next()` throws all but one entry away.
    find_boxes_capped(payload, want, 1).into_iter().next()
}

// All child boxes of `payload` with the given type, stopping after `cap`
// matches so a crafted payload of minimum-size boxes can't force an
// oversized Vec. Pass `usize::MAX` for "all matches".
fn find_boxes_capped<'a>(payload: &'a [u8], want: &[u8; 4], cap: usize) -> Vec<&'a [u8]> {
    let mut out = Vec::new();
    let mut pos = 0;
    while pos + 8 <= payload.len() && out.len() < cap {
        let size32 = u32::from_be_bytes([
            payload[pos],
            payload[pos + 1],
            payload[pos + 2],
            payload[pos + 3],
        ]) as usize;
        let bt = [
            payload[pos + 4],
            payload[pos + 5],
            payload[pos + 6],
            payload[pos + 7],
        ];
        // ISO/IEC 14496-12 §4.2: size==1 → 64-bit largesize after the type
        // (16-byte header); size==0 → box runs to the payload end. The child
        // scan must honour both or a largesize sibling ends the walk early.
        let (box_size, header_len) = match size32 {
            1 => {
                if pos + 16 > payload.len() {
                    break;
                }
                let large = u64::from_be_bytes([
                    payload[pos + 8],
                    payload[pos + 9],
                    payload[pos + 10],
                    payload[pos + 11],
                    payload[pos + 12],
                    payload[pos + 13],
                    payload[pos + 14],
                    payload[pos + 15],
                ]) as usize;
                (large, 16usize)
            }
            0 => (payload.len() - pos, 8usize),
            n => (n, 8usize),
        };
        // checked_add: `box_size` is an attacker-controlled 64-bit largesize cast to
        // usize; near usize::MAX at nonzero `pos`, plain `pos + box_size` wraps past the
        // guard (release panic on slice / infinite advance). Compute `end` once, reuse it.
        let end = match pos.checked_add(box_size) {
            Some(end) if box_size >= header_len && end <= payload.len() => end,
            _ => break,
        };
        if &bt == want {
            out.push(&payload[pos + header_len..end]);
        }
        pos = end;
    }
    out
}

fn be32(b: &[u8], o: usize) -> u32 {
    u32::from_be_bytes([b[o], b[o + 1], b[o + 2], b[o + 3]])
}
fn be16(b: &[u8], o: usize) -> u16 {
    u16::from_be_bytes([b[o], b[o + 1]])
}

/// mvhd (version 0/1) → movie timescale (ISO/IEC 14496-12 §8.2.2).
fn mvhd_timescale(b: &[u8]) -> Option<u32> {
    let version = b.first().copied()?;
    if version == 1 {
        // version(1)+flags(3) creation(8) modification(8) timescale(4) ...
        (b.len() >= 24).then(|| be32(b, 20))
    } else {
        // version(1)+flags(3) creation(4) modification(4) timescale(4) ...
        (b.len() >= 16).then(|| be32(b, 12))
    }
}

// Upper bound on parsed `elst` entries: only the leading empty edits and the
// FIRST non-empty edit matter, so a cap keeps a crafted `moov` from turning
// a box into a larger Vec than the box itself.
const MAX_ELST_ENTRIES: usize = 1024;

// One `elst` entry: `(segment_duration, media_time, media_rate_integer)`.
type EditListEntry = (u64, i64, i16);

// Parse an `elst` payload (ISO/IEC 14496-12 §8.6.6), clamped by the box's
// own bytes and by `MAX_ELST_ENTRIES`. Version 1 entries are 20 bytes
// (u64+i64+i16+i16); version 0 uses 32-bit duration/time (12 bytes).
fn parse_elst(b: &[u8]) -> Vec<EditListEntry> {
    // `<`↔`<=` here is equivalent, not a coverage gap: at `b.len() == 8` falling
    // through computes `available = (b.len() - 8) / entry_size = 0`, floors `n`
    // to 0, and returns `Vec::new()` either way (confirmed via mutation testing).
    if b.len() < 8 {
        return Vec::new();
    }
    let version = b[0];
    let entry_size = if version == 1 { 20 } else { 12 };
    let declared = be32(b, 4) as usize;
    let available = (b.len() - 8) / entry_size;
    let n = declared.min(available).min(MAX_ELST_ENTRIES);
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let o = 8 + i * entry_size;
        let (seg, media_time, rate_off) = if version == 1 {
            let seg = u64::from_be_bytes([
                b[o],
                b[o + 1],
                b[o + 2],
                b[o + 3],
                b[o + 4],
                b[o + 5],
                b[o + 6],
                b[o + 7],
            ]);
            let mt = i64::from_be_bytes([
                b[o + 8],
                b[o + 9],
                b[o + 10],
                b[o + 11],
                b[o + 12],
                b[o + 13],
                b[o + 14],
                b[o + 15],
            ]);
            (seg, mt, 16)
        } else {
            (be32(b, o) as u64, be32(b, o + 4) as i32 as i64, 8)
        };
        let rate = be16(b, o + rate_off) as i16;
        out.push((seg, media_time, rate));
    }
    out
}

// Presentation-time offset an edit list imposes on a track's samples, in MEDIA timescale ticks:
// (sum of leading empty segment_durations) minus the first non-empty edit's media_time.
fn elst_offset_ticks(
    entries: &[EditListEntry],
    movie_timescale: Option<u32>,
    media_timescale: u32,
    track_idx: usize,
) -> i64 {
    let mut empty_movie_ticks: u64 = 0;
    let mut trim_media_ticks: i64 = 0;
    let mut media_edits = 0usize;
    let mut odd_rate = false;

    for &(segment_duration, media_time, rate) in entries {
        if media_time < 0 {
            // Empty edit: blank presentation time. Only the ones BEFORE the first
            // media edit shift this track's start.
            if media_edits == 0 {
                empty_movie_ticks = empty_movie_ticks.saturating_add(segment_duration);
            }
            continue;
        }
        media_edits += 1;
        if media_edits == 1 {
            trim_media_ticks = media_time;
            odd_rate = rate != 1;
        }
    }

    // `media_edits`/`odd_rate` are read ONLY by this `if` to decide whether to log;
    // they don't feed the return value, so mutating this condition (expected to
    // survive mutation testing) only changes whether the warning fires — same shape below.
    if media_edits > 1 || odd_rate {
        tracing::warn!(
            track = track_idx,
            media_edits,
            odd_rate,
            "mp4: edit list describes a timeline richer than a constant shift \
             (several media edits, or a rate other than 1); only the leading edit \
             is applied and the remainder of the presentation timeline is not"
        );
    }

    // An empty edit's duration is in MOVIE ticks; convert to media ticks before
    // subtracting the media-timescale trim. i128 so neither product overflows.
    let delay_media_ticks = match movie_timescale {
        Some(mts) if empty_movie_ticks > 0 => {
            ((empty_movie_ticks as i128 * media_timescale as i128) / mts as i128)
                .clamp(0, i64::MAX as i128) as i64
        }
        Some(_) => 0,
        None => {
            // Same shape as the `media_edits > 1 || odd_rate` guard above: this arm
            // returns 0 no matter what, so mutating this comparison only changes
            // whether the warning below fires, never the return value.
            if empty_movie_ticks > 0 {
                tracing::warn!(
                    track = track_idx,
                    "mp4: edit list has an empty edit but the movie timescale is \
                     absent or zero, so its delay cannot be converted to media \
                     ticks; the delay is not applied"
                );
            }
            0
        }
    };

    delay_media_ticks.saturating_sub(trim_media_ticks)
}

/// mdhd (version 0/1) → media timescale.
fn mdhd_timescale(b: &[u8]) -> Option<u32> {
    let version = b.first().copied()?;
    if version == 1 {
        // version(1)+flags(3) creation(8) modification(8) timescale(4) ...
        (b.len() >= 24).then(|| be32(b, 20))
    } else {
        // creation(4) modification(4) timescale(4) ...
        (b.len() >= 16).then(|| be32(b, 12))
    }
}

/// mdhd language (5-bit packed ISO 639-2) → lowercase 3-letter code.
fn mdhd_language(b: &[u8]) -> Option<String> {
    let version = b.first().copied()?;
    // v0: vflags(4)+creation(4)+modification(4)+timescale(4)+duration(4) = 20.
    // v1: creation/modification/duration are 64-bit → vflags(4)+8+8+4+8 = 32.
    let off = if version == 1 { 32 } else { 20 };
    if b.len() < off + 2 {
        return None;
    }
    let packed = be16(b, off);
    let c0 = ((packed >> 10) & 0x1F) as u8 + 0x60;
    let c1 = ((packed >> 5) & 0x1F) as u8 + 0x60;
    let c2 = (packed & 0x1F) as u8 + 0x60;
    let s: String = [c0, c1, c2].iter().map(|&c| c as char).collect();
    if s.chars().all(|c| c.is_ascii_lowercase()) {
        Some(s)
    } else {
        None
    }
}

/// hdlr → handler_type fourcc ('vide' / 'soun').
fn hdlr_type(b: &[u8]) -> Option<[u8; 4]> {
    // version+flags(4) pre_defined(4) handler_type(4) ...
    (b.len() >= 12).then(|| [b[8], b[9], b[10], b[11]])
}

/// Decoded first sample entry of an `stsd` box.
struct StsdInfo {
    codec: Codec,
    height: u16,
    config: Option<Vec<u8>>,
    /// Video `colr`/`nclx` colour tags, when present.
    cicp: Option<MeasuredCicp>,
    /// Video sample entry carries a Dolby Vision `dvcC`/`dvvC` config.
    dolby_vision: bool,
    channels: u16,
    /// Integer sample rate (Hz) from the AudioSampleEntry 16.16 samplerate
    /// field (high 16 bits). `0` when absent/video — the caller then falls
    /// back to the mdhd media timescale.
    sample_rate: u32,
    /// DTS `ddts` DTSSamplingFrequency when present and non-zero.
    dts_max_rate: Option<u32>,
    /// `Some(dtsh|dtsl)` for a DTS entry; `None` otherwise.
    dts_hd: Option<bool>,
}

/// stsd → codec + dimensions + codec_private + channel count (first entry).
fn parse_stsd(b: &[u8]) -> Option<StsdInfo> {
    // version+flags(4) entry_count(4) then the first sample entry box.
    // `< 8` vs `<= 8` is equivalent here: at `b.len() == 8` falling through makes
    // `entry` empty, and the next `entry.len() < 8` guard catches that too.
    if b.len() < 8 {
        return None;
    }
    let entry = &b[8..];
    if entry.len() < 8 {
        return None;
    }
    let size = be32(entry, 0) as usize;
    let fourcc = [entry[4], entry[5], entry[6], entry[7]];
    // `size` is untrusted: clamp to [8, entry.len()] so a declared size < 8 (or a
    // truncated entry) yields an empty body instead of panicking on `entry[8..<8]`.
    let body = &entry[8..size.clamp(8, entry.len())];

    let codec = match &fourcc {
        b"hvc1" | b"hev1" => Codec::Hevc,
        b"avc1" | b"avc3" => Codec::H264,
        b"ac-3" => Codec::Ac3,
        b"ec-3" => Codec::Ac3Plus,
        b"mp4a" => Codec::Aac,
        b"dtsc" | b"dtse" | b"dtsh" | b"dtsl" => Codec::Dts,
        _ => return None,
    };

    if matches!(codec, Codec::Hevc | Codec::H264) {
        // VisualSampleEntry: 6 reserved + 2 dri + 16 pre/reserved + width(2)
        // height(2) + 14 + 32 compressorname + 2 depth + 2 pre = 78 bytes, then
        // child boxes (hvcC/avcC, colr, …).
        if body.len() < 78 {
            return None;
        }
        let height = be16(body, 26);
        let children = &body[78..];
        let config = find_box(children, b"hvcC")
            .or_else(|| find_box(children, b"avcC"))
            .map(|c| c.to_vec());
        Some(StsdInfo {
            codec,
            height,
            config,
            cicp: find_box(children, b"colr").and_then(parse_colr_nclx),
            dolby_vision: find_box(children, b"dvcC").is_some()
                || find_box(children, b"dvvC").is_some(),
            channels: 0,
            sample_rate: 0,
            dts_max_rate: None,
            dts_hd: None,
        })
    } else {
        // AudioSampleEntry (ISO/IEC 14496-12 §12.2.3): 28 bytes then children. Under
        // a v0 stsd, entry v1 is QuickTime (+16 bytes) and v2 has a float64 rate at
        // 32, u32 channels at 40, children at 64.
        let version = if b[0] == 0 && body.len() >= 28 {
            be16(body, 8)
        } else {
            0
        };
        let (channels, sample_rate, children) = match version {
            2 if body.len() >= 64 => {
                let rate = f64::from_bits(u64::from_be_bytes(body[32..40].try_into().ok()?));
                let rate = if rate.is_finite() && rate >= 1.0 && rate <= u32::MAX as f64 {
                    rate.round() as u32
                } else {
                    0
                };
                let ch = be32(body, 40).min(u16::MAX as u32) as u16;
                (ch, rate, 64)
            }
            // The 16.16 field's integer part; `audio_rate` falls back to mdhd.
            1 if body.len() >= 44 => (be16(body, 16), be16(body, 24) as u32, 44),
            _ if body.len() >= 28 => (be16(body, 16), be16(body, 24) as u32, 28),
            _ => (2, 0, body.len()),
        };
        let esds = (codec == Codec::Aac)
            .then(|| find_box(&body[children..], b"esds").and_then(parse_esds))
            .flatten();
        // mp4a also carries MPEG-1/2 audio (objectTypeIndication 0x6B / 0x69).
        let (codec, config) = match esds {
            Some((0x69 | 0x6B, _)) => (Codec::Mp3, None),
            Some((_, asc)) => (codec, asc),
            None => (codec, None),
        };
        // ETSI TS 102 366 F.3/F.5: the entry's ChannelCount is ignored for AC-3/E-AC-3;
        // dac3/dec3 carry the real layout.
        let channels = match codec {
            Codec::Ac3 => find_box(&body[children..], b"dac3").and_then(ac3_channels),
            Codec::Ac3Plus => find_box(&body[children..], b"dec3").and_then(ec3_channels),
            _ => None,
        }
        .unwrap_or(channels);
        // ISO AudioSampleEntryV1 carries rates above 65535 in `srat`.
        let sample_rate = find_box(&body[children..], b"srat")
            .filter(|b| b.len() >= 8)
            .map_or(sample_rate, |b| be32(b, 4));
        // ETSI TS 102 114 E.2.2.3: DTSSamplingFrequency is the stream's maximum rate.
        // An unnameable rate stays Unknown; the entry holds only its family base.
        let dts_max_rate = match codec {
            Codec::Dts => find_box(&body[children..], b"ddts")
                .filter(|b| b.len() >= 4)
                .map(|b| be32(b, 0))
                .filter(|&hz| hz != 0),
            _ => None,
        };
        Some(StsdInfo {
            codec,
            height: 0,
            config,
            cicp: None,
            dolby_vision: false,
            channels,
            sample_rate,
            dts_max_rate,
            dts_hd: (codec == Codec::Dts).then_some(matches!(&fourcc, b"dtsh" | b"dtsl")),
        })
    }
}

// Sample rate from the entry rate and the mdhd timescale. The entry wins when
// it is known; a DTS-HD entry (`dts_hd`) holds the family base of a 2x/4x rate.
fn audio_rate(entry: u32, timescale: u32, dts_hd: Option<bool>) -> u32 {
    let known = |hz| SampleRate::from_hz(hz) != SampleRate::Unknown;
    if matches!(entry, 0 | 1 | 0xFFFF) {
        return timescale;
    }
    let is = |k: u32| entry.checked_mul(k) == Some(timescale) && known(timescale);
    let wider = match dts_hd {
        Some(hd) => hd && (is(2) || is(4)),
        None => {
            !known(entry)
                && timescale > 0xFFFF
                && known(timescale)
                && (timescale.is_multiple_of(entry) || entry == timescale & 0xFFFF)
        }
    };
    if wider { timescale } else { entry }
}

/// Read an MPEG-4 expandable descriptor length (ISO/IEC 14496-1), advancing `pos`.
/// Each byte contributes 7 bits, continued while the high bit is set (max 4 bytes).
fn read_descriptor_len(b: &[u8], pos: &mut usize) -> usize {
    let mut len = 0usize;
    for _ in 0..4 {
        let Some(&byte) = b.get(*pos) else { break };
        *pos += 1;
        // `|`↔`^` is equivalent here (same shape as `audio.rs`'s `BitReader::read`):
        // `len << 7` has zeros in its low 7 bits and `byte & 0x7F` is masked to
        // exactly those bits, so the operands never share a set bit.
        len = (len << 7) | (byte & 0x7F) as usize;
        if byte & 0x80 == 0 {
            break;
        }
    }
    len
}

/// esds → AAC AudioSpecificConfig (the A_AAC CodecPrivate), or `None`.
#[cfg(test)]
fn parse_esds_asc(b: &[u8]) -> Option<Vec<u8>> {
    parse_esds(b).and_then(|(_, asc)| asc)
}

/// esds → (objectTypeIndication, AudioSpecificConfig if present). Walks
/// ES_Descriptor(0x03) → DecoderConfigDescriptor(0x04) → DecoderSpecificInfo(0x05).
/// Fully bounds-checked: a malformed/truncated esds returns None, never panics.
fn parse_esds(b: &[u8]) -> Option<(u8, Option<Vec<u8>>)> {
    // esds is a FullBox: version+flags(4), then the ES_Descriptor.
    let mut pos = 4;
    if *b.get(pos)? != 0x03 {
        return None;
    }
    pos += 1;
    read_descriptor_len(b, &mut pos); // ES_Descriptor length (unused)
    pos += 2; // ES_ID
    let flags = *b.get(pos)?;
    pos += 1;
    if flags & 0x80 != 0 {
        pos += 2; // streamDependenceFlag → dependsOn_ES_ID
    }
    if flags & 0x40 != 0 {
        // URL_flag → URLlength(1) + URLstring
        pos += 1 + *b.get(pos)? as usize;
    }
    if flags & 0x20 != 0 {
        pos += 2; // OCRstreamFlag → OCR_ES_Id
    }
    if *b.get(pos)? != 0x04 {
        return None; // DecoderConfigDescriptor
    }
    pos += 1;
    read_descriptor_len(b, &mut pos);
    let oti = *b.get(pos)?;
    // objectTypeIndication(1) + streamType/bufferSizeDB(4) + maxBitrate(4) + avgBitrate(4)
    pos += 13;
    if b.get(pos) != Some(&0x05) {
        return Some((oti, None)); // no DecoderSpecificInfo
    }
    pos += 1;
    let asc_len = read_descriptor_len(b, &mut pos);
    let asc = pos
        .checked_add(asc_len)
        .filter(|&end| asc_len != 0 && end <= b.len())
        .map(|end| b[pos..end].to_vec());
    Some((oti, asc))
}

/// dac3 → total channel count (full-range + LFE).
fn ac3_channels(b: &[u8]) -> Option<u16> {
    // fscod(2) bsid(5) bsmod(3) acmod(3) lfeon(1) bit_rate_code(5) reserved(2)
    let b1 = *b.get(1)?;
    Some(ac3_acmod_channels((b1 >> 3) & 7) + ((b1 >> 2) & 1) as u16)
}

/// dec3 → channel count of the first independent substream plus any dependent
/// substreams' `chan_loc` (each pair bit adds 2, each single 1, LFE2 1).
fn ec3_channels(b: &[u8]) -> Option<u16> {
    // data_rate(13) num_ind_sub(3), then fscod(2) bsid(5) res(1) asvc(1) bsmod(3)
    // acmod(3) lfeon(1) res(3) num_dep_sub(4) chan_loc-or-res(9 | 1).
    let b3 = *b.get(3)?;
    let mut n = ac3_acmod_channels((b3 >> 1) & 7) + (b3 & 1) as u16;
    let b4 = *b.get(4)?;
    if (b4 >> 1) & 0x0F != 0 {
        let loc = ((b4 as u16 & 1) << 8) | *b.get(5)? as u16;
        // MSB-first: Lc/Rc, Lrs/Rrs, Cs, Ts, Lsd/Rsd, Lw/Rw, Lvh/Rvh, Cvh, LFE2.
        const PAIRS: u16 = 0b1_1001_1100;
        n += (loc & PAIRS).count_ones() as u16 * 2 + (loc & !PAIRS & 0x1FF).count_ones() as u16;
    }
    Some(n)
}

fn ac3_acmod_channels(acmod: u8) -> u16 {
    [2, 1, 2, 3, 3, 4, 4, 5][(acmod & 7) as usize]
}

/// colr → CICP triple when the colour type is `nclx`.
fn parse_colr_nclx(b: &[u8]) -> Option<MeasuredCicp> {
    // 'nclx'(4) primaries(2) transfer(2) matrix(2) full_range(1 bit)
    if b.len() < 11 || &b[..4] != b"nclx" {
        return None;
    }
    let code = |o| be16(b, o).min(u8::MAX as u16) as u8;
    Some(MeasuredCicp {
        primaries: code(4),
        transfer: code(6),
        matrix: code(8),
        range: if b[10] & 0x80 != 0 { 2 } else { 1 },
    })
}

fn hdr_from_cicp(cicp: Option<MeasuredCicp>, dolby_vision: bool) -> HdrFormat {
    match cicp.map(|c| c.transfer) {
        _ if dolby_vision => HdrFormat::DolbyVision,
        Some(16) => HdrFormat::Hdr10,
        Some(18) => HdrFormat::Hlg,
        _ => HdrFormat::Sdr,
    }
}

fn color_space_from_primaries(primaries: u8) -> ColorSpace {
    match primaries {
        1 => ColorSpace::Bt709,
        5 => ColorSpace::Bt470bg,
        6 => ColorSpace::Smpte170m,
        9 => ColorSpace::Bt2020,
        _ => ColorSpace::Unknown,
    }
}

/// Codec from the layer bits of the MPEG audio frame header at `offset`: Layer II is
/// `Mp2`, Layer III `Mp3`; unreadable or Layer I gives `None` (caller keeps its label).
fn mpeg_audio_layer<R: Read + Seek>(file: &mut R, file_len: u64, offset: u64) -> Option<Codec> {
    if offset.checked_add(2)? > file_len {
        return None;
    }
    file.seek(SeekFrom::Start(offset)).ok()?;
    let mut h = [0u8; 2];
    file.read_exact(&mut h).ok()?;
    if h[0] != 0xFF || h[1] & 0xE0 != 0xE0 {
        return None;
    }
    match (h[1] >> 1) & 3 {
        2 => Some(Codec::Mp2),
        1 => Some(Codec::Mp3),
        _ => None,
    }
}

/// Nearest standard frame rate (the muxer's table) to the mean `stts` delta, else `Unknown`.
// Mean over deltas within 1 tick of the median: a 1 kHz timescale rounds 41.708 ms frames
// to a 41/42 ms pattern, while one long final sample must not skew the rate.
fn frame_rate_from_stts(timescale: u32, durations: &[u32]) -> FrameRate {
    let mut hist: std::collections::BTreeMap<u64, u64> = std::collections::BTreeMap::new();
    for &d in durations.iter().filter(|&&d| d > 0) {
        *hist.entry(d as u64).or_default() += 1;
    }
    let total: u64 = hist.values().sum();
    if total == 0 {
        return FrameRate::Unknown;
    }
    let mut seen = 0;
    let mut median = 0;
    for (&d, &c) in &hist {
        seen += c;
        if seen * 2 > total {
            median = d;
            break;
        }
    }
    let (n, sum) = hist
        .iter()
        .filter(|&(&d, _)| d.abs_diff(median) <= 1)
        .fold((0u64, 0u64), |(n, s), (&d, &c)| (n + c, s + d * c));
    let fps = timescale as f64 * n as f64 / sum as f64;
    let rate_ok = |r: f64| (fps - r).abs() <= r * 0.001;
    match super::nearest_std_rate(fps).filter(|&(ts, dur)| rate_ok(ts as f64 / dur as f64)) {
        Some((24000, 1001)) => FrameRate::F23_976,
        Some((24, 1)) => FrameRate::F24,
        Some((25, 1)) => FrameRate::F25,
        Some((30000, 1001)) => FrameRate::F29_97,
        Some((30, 1)) => FrameRate::F30,
        Some((50, 1)) => FrameRate::F50,
        Some((60000, 1001)) => FrameRate::F59_94,
        Some((60, 1)) => FrameRate::F60,
        _ => FrameRate::Unknown,
    }
}

/// stsz → per-sample sizes.
fn parse_stsz(b: &[u8], max: usize) -> Vec<u32> {
    if b.len() < 12 {
        return Vec::new();
    }
    let sample_size = be32(b, 4);
    // `count` is untrusted; clamp to the caller's remaining sample budget so neither
    // a single 0xFFFFFFFF nor many crafted tracks can over-allocate (see from_reader).
    let count = (be32(b, 8) as usize).min(max);
    if sample_size != 0 {
        return vec![sample_size; count];
    }
    // Each entry is 4 bytes; `count` also can't exceed what the box actually holds.
    let mut out = Vec::with_capacity(count.min((b.len() - 12) / 4));
    for i in 0..count {
        let o = 12 + i * 4;
        if o + 4 > b.len() {
            break;
        }
        out.push(be32(b, o));
    }
    out
}

/// stz2 (compact sample sizes, field size 4 / 8 / 16 bits) → per-sample sizes.
fn parse_stz2(b: &[u8], max: usize) -> Vec<u32> {
    if b.len() < 12 {
        return Vec::new();
    }
    let field_size = b[7] as usize;
    if !matches!(field_size, 4 | 8 | 16) {
        return Vec::new();
    }
    let count = (be32(b, 8) as usize).min(max);
    let body = &b[12..];
    // `count` entries of `field_size` bits can't exceed the box body.
    let mut out = Vec::with_capacity(count.min(body.len() * 8 / field_size));
    for i in 0..count {
        let size = match field_size {
            4 => body
                .get(i / 2)
                .map(|&v| u32::from(if i % 2 == 0 { v >> 4 } else { v & 0xF })),
            8 => body.get(i).map(|&v| u32::from(v)),
            _ => body
                .get(i * 2..i * 2 + 2)
                .map(|v| u32::from(u16::from_be_bytes([v[0], v[1]]))),
        };
        let Some(size) = size else { break };
        out.push(size);
    }
    out
}

/// stco (32-bit) / co64 (64-bit) → chunk offsets.
fn parse_stco(b: &[u8], is64: bool) -> Vec<u64> {
    // `< 8` vs `<= 8` is equivalent (same shape as `parse_stsd`, and stsc/stts/ctts/stss
    // below): at `b.len() == 8` falling through still fails the per-entry guard on the
    // first entry. `< 8` vs `== 8` is NOT equivalent: it panics on `be32(b, 4)`.
    if b.len() < 8 {
        return Vec::new();
    }
    let count = be32(b, 4) as usize;
    let stride = if is64 { 8 } else { 4 };
    // `count` entries of `stride` bytes can't exceed the box body.
    let mut out = Vec::with_capacity(count.min((b.len() - 8) / stride));
    for i in 0..count {
        let o = 8 + i * stride;
        if o + stride > b.len() {
            break;
        }
        if is64 {
            out.push(u64::from_be_bytes([
                b[o],
                b[o + 1],
                b[o + 2],
                b[o + 3],
                b[o + 4],
                b[o + 5],
                b[o + 6],
                b[o + 7],
            ]));
        } else {
            out.push(be32(b, o) as u64);
        }
    }
    out
}

/// stsc → (first_chunk, samples_per_chunk) entries (1-based first_chunk).
fn parse_stsc(b: &[u8]) -> Vec<(u32, u32)> {
    // `< 8` vs `<= 8`: equivalent, see the proof at `parse_stco`.
    if b.len() < 8 {
        return Vec::new();
    }
    let count = be32(b, 4) as usize;
    // Each entry is 12 bytes; `count` can't exceed what the box actually holds.
    let mut out = Vec::with_capacity(count.min((b.len() - 8) / 12));
    for i in 0..count {
        let o = 8 + i * 12;
        if o + 12 > b.len() {
            break;
        }
        out.push((be32(b, o), be32(b, o + 4)));
    }
    out
}

/// Reconstruct per-sample file offsets from sizes + chunk offsets + stsc.
fn sample_offsets(sizes: &[u32], chunk_offsets: &[u64], stsc: &[(u32, u32)]) -> Vec<u64> {
    let n_chunks = chunk_offsets.len();
    // Expand stsc → samples_per_chunk for every chunk.
    let mut spc = vec![0u32; n_chunks];
    for (idx, &(first, per)) in stsc.iter().enumerate() {
        let start = (first.saturating_sub(1)) as usize;
        let end = stsc
            .get(idx + 1)
            .map(|&(nf, _)| (nf.saturating_sub(1)) as usize)
            .unwrap_or(n_chunks);
        let end = end.min(n_chunks);
        // `<` vs `<=` is equivalent: at `start == end`, `spc[start..end]` is a valid
        // empty slice and `.fill()` on it does nothing, same as skipping the call.
        if start < end {
            spc[start..end].fill(per);
        }
    }
    let mut offsets = Vec::with_capacity(sizes.len());
    let mut sidx = 0usize;
    for (ci, &choff) in chunk_offsets.iter().enumerate() {
        let mut off = choff;
        for _ in 0..spc[ci] {
            if sidx >= sizes.len() {
                break;
            }
            offsets.push(off);
            // `choff`/`sizes` are untrusted; saturate so a crafted co64 offset near
            // u64::MAX can't overflow-panic (the read() EOF guard rejects it later).
            off = off.saturating_add(sizes[sidx] as u64);
            sidx += 1;
        }
    }
    // Samples the stsc did not place have NO known location; fabricating one by
    // packing after the last offset would read a frame from arbitrary file bytes.
    // Report the shortfall instead and let the caller drop the track.
    offsets
}

/// stts → per-sample decode durations (expanded from run-length entries). `max`
/// caps the expansion — the caller passes the track's real sample count, past which
/// entries are never read (and an untrusted run-length must not grow the Vec).
fn parse_stts(b: &[u8], max: usize) -> Vec<u32> {
    // `< 8` vs `<= 8`: equivalent, see the proof at `parse_stco`.
    if b.len() < 8 {
        return Vec::new();
    }
    let count = be32(b, 4) as usize;
    let mut out = Vec::new();
    for i in 0..count {
        let o = 8 + i * 8;
        if o + 8 > b.len() {
            break;
        }
        let n = be32(b, o);
        let delta = be32(b, o + 4);
        for _ in 0..n {
            if out.len() >= max {
                return out;
            }
            out.push(delta);
        }
    }
    out
}

/// ctts → per-sample composition offsets (version 0 unsigned / version 1 signed).
/// `max` caps the expansion, as in [`parse_stts`].
fn parse_ctts(b: &[u8], max: usize) -> Vec<i32> {
    // `< 8` vs `<= 8`: equivalent, see the proof at `parse_stco`.
    if b.len() < 8 {
        return Vec::new();
    }
    // version 0 = unsigned, version 1 = signed offsets; the u32→i32 bit-cast
    // reads both correctly (real composition offsets fit in i32 either way).
    let count = be32(b, 4) as usize;
    let mut out = Vec::new();
    for i in 0..count {
        let o = 8 + i * 8;
        if o + 8 > b.len() {
            break;
        }
        let n = be32(b, o);
        let offset = be32(b, o + 4) as i32;
        for _ in 0..n {
            if out.len() >= max {
                return out;
            }
            out.push(offset);
        }
    }
    out
}

/// stss → set of 1-based sync sample numbers.
fn parse_stss(b: &[u8]) -> std::collections::HashSet<u32> {
    let mut set = std::collections::HashSet::new();
    // `< 8` vs `<= 8`: equivalent, see the proof at `parse_stco`.
    if b.len() < 8 {
        return set;
    }
    let count = be32(b, 4) as usize;
    for i in 0..count {
        let o = 8 + i * 4;
        if o + 4 > b.len() {
            break;
        }
        set.insert(be32(b, o));
    }
    set
}

#[cfg(test)]
#[path = "read_tests.rs"]
mod tests;
