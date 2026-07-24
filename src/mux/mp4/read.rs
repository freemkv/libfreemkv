//! Progressive MP4 (ISO-BMFF) demuxer — the read side of `mp4://`.
//!
//! The inverse of the writer in [`super`]: parse `moov`/`trak`/`stbl`, rebuild a
//! [`DiscTitle`] and a per-sample index (offset/size/timing/sync from
//! `stsc`+`stco`/`co64`+`stsz`, `stts`+`ctts`, `stss`), then stream each sample
//! out as a [`PesFrame`] in decode order. Video NALs are length-prefixed in MP4 —
//! exactly the framing the MKV muxer consumes — so `mp4://` → `mkv://` (and
//! `mp4://` → any sink) needs no reframing.
//!
//! Scope: progressive MP4 (`moov` + `mdat`). Fragmented MP4 (`moof`) is out of
//! scope for now — its samples live in `traf`/`trun`, not `stbl`.

use crate::disc::{
    AudioChannels, AudioStream, Codec, DiscTitle, Resolution, SampleRate, Stream as DiscStream,
    VideoStream,
};
use crate::labels::LabelPurpose;
use crate::pes::{PesFrame, Stream};
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::Path;

const NS: i128 = 1_000_000_000;

/// Upper bound on the number of tracks. Track count is otherwise unbounded (a
/// crafted moov can pack tens of thousands of `trak` boxes), and the per-track
/// PID is `0x1011 + track_idx` — which overflows u16 past ~61k tracks. Real
/// titles have well under a hundred tracks.
const MAX_TRACKS: usize = 512;

/// Upper bound on a track's decoded sample count. MP4 sample-table fields
/// (`stsz` sample_count, `stts`/`ctts` run-lengths) are untrusted 32-bit values;
/// a crafted box can declare billions of entries in a few bytes. Real titles stay
/// far under this (a 10 h/60 fps track is ~2M samples), so clamping to it caps a
/// hostile file's allocation without truncating any legitimate track.
const MAX_SAMPLE_COUNT: usize = 1 << 24;

/// Absolute ceiling on a single allocation sized from an untrusted MP4 field (a
/// per-sample buffer or the `moov` payload). The EOF check alone is not enough:
/// `file_len` is cheaply inflatable with a sparse file (`truncate -s 8G`), so a
/// crafted stsz size or moov box size just under an 8 GiB apparent length would
/// otherwise force a multi-GiB allocation. No real sample or moov approaches this.
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

impl Mp4Reader<File> {
    /// Open and index an MP4 file by path.
    pub fn open(path: &Path) -> io::Result<Self> {
        let name = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("mp4")
            .to_string();
        Self::from_reader(File::open(path)?, name)
    }
}

impl<R: Read + Seek> Mp4Reader<R> {
    /// Index an already-opened seekable MP4 reader.
    pub fn from_reader(mut file: R, name: String) -> io::Result<Self> {
        let file_len = file.seek(SeekFrom::End(0))?;
        file.seek(SeekFrom::Start(0))?;
        let moov = read_moov(&mut file)?;
        let mut title = DiscTitle::empty();
        title.playlist = name;

        let mut samples: Vec<SampleRef> = Vec::new();
        let mut codec_privates: Vec<Option<Vec<u8>>> = Vec::new();
        let mut track_idx = 0usize;
        // Global cap on total decoded samples across ALL tracks — a crafted file
        // with many `trak` boxes must not sum past this even though each track is
        // individually bounded. Real titles stay far under it. Also bound by
        // `file_len`: a sample occupies at least one byte of the file, so a tiny
        // crafted file with a fixed-size `stsz` claiming count=0xFFFFFFFF can't
        // inflate the `sizes`/`Vec<SampleRef>` allocations past the file's own size
        // (a genuine large title has file_len ≫ sample count, so it is unaffected).
        let mut sample_budget = MAX_SAMPLE_COUNT.min(file_len.min(usize::MAX as u64) as usize);

        // Bound the scan at MAX_TRACKS *matches* so a crafted moov packed with tiny
        // (8-byte) trak headers can't force the scan to materialize a Vec far
        // larger than the moov payload before the per-track cap below ever runs.
        for trak in find_boxes_capped(&moov, b"trak", MAX_TRACKS) {
            if track_idx >= MAX_TRACKS {
                break; // bound track count so the per-track PID can't overflow u16
            }
            let Some(mdia) = find_box(trak, b"mdia") else {
                continue;
            };
            let timescale = find_box(mdia, b"mdhd")
                .and_then(mdhd_timescale)
                .filter(|&t| t != 0) // a crafted mdhd timescale of 0 would divide-by-zero below
                .unwrap_or(90_000);
            let language = find_box(mdia, b"mdhd").and_then(mdhd_language);
            let handler = find_box(mdia, b"hdlr").and_then(hdlr_type);
            let Some(minf) = find_box(mdia, b"minf") else {
                continue;
            };
            let Some(stbl) = find_box(minf, b"stbl") else {
                continue;
            };
            let Some(stsd) = find_box(stbl, b"stsd") else {
                continue;
            };

            let Some(StsdInfo {
                codec,
                height,
                config,
                channels,
            }) = parse_stsd(stsd)
            else {
                continue; // unrecognised sample entry — skip the track
            };

            // Build the stream model for this track.
            let stream = match handler {
                Some(h) if &h == b"vide" => DiscStream::Video(VideoStream {
                    pid: 0x1011 + track_idx as u16,
                    codec,
                    resolution: Resolution::from_height(height as u32),
                    frame_rate: crate::disc::FrameRate::Unknown,
                    hdr: crate::disc::HdrFormat::Sdr,
                    color_space: crate::disc::ColorSpace::Unknown,
                    display_aspect: None,
                    secondary: false,
                    label: String::new(),
                    measured_cicp: None,
                }),
                Some(h) if &h == b"soun" => DiscStream::Audio(AudioStream {
                    pid: 0x1100 + track_idx as u16,
                    codec,
                    // `channels` is an untrusted u16; saturate rather than wrap with
                    // `as u8` (a crafted 256 would alias to 0/Mono).
                    channels: AudioChannels::from_count(channels.min(u8::MAX as u16) as u8),
                    language: language.clone().unwrap_or_else(|| "und".into()),
                    sample_rate: SampleRate::from_hz(timescale),
                    secondary: false,
                    purpose: LabelPurpose::Normal,
                    label: String::new(),
                }),
                _ => continue, // non-A/V handler
            };

            // Per-sample tables. `stsz` is bounded by the remaining global budget;
            // `stts`/`ctts` need at most one entry per sample, so they are bounded by
            // this track's sample count (indices past it are never read).
            let sizes = find_box(stbl, b"stsz")
                .map(|b| parse_stsz(b, sample_budget))
                .unwrap_or_default();
            let n = sizes.len();
            if n == 0 {
                track_idx += 1;
                title.streams.push(stream);
                codec_privates.push(config);
                continue;
            }
            sample_budget -= n;
            let chunk_offsets = find_box(stbl, b"stco")
                .map(|b| parse_stco(b, false))
                .or_else(|| find_box(stbl, b"co64").map(|b| parse_stco(b, true)))
                .unwrap_or_default();
            if chunk_offsets.is_empty() {
                // Samples exist but there is no chunk-offset table: the stbl is
                // malformed and every sample offset would resolve to file byte 0
                // (muxing header bytes as frame data). Drop the track rather than
                // emit garbage; an all-tracks-dropped file fails Mp4Invalid below.
                continue;
            }
            let stsc = find_box(stbl, b"stsc").map(parse_stsc).unwrap_or_default();
            if stsc.is_empty() {
                // No sample-to-chunk map: samples can't be placed against the chunk
                // offsets (they would pack from byte 0). Drop the track rather than
                // emit header bytes as frame data — a valid stbl always has stsc.
                continue;
            }
            let offsets = sample_offsets(&sizes, &chunk_offsets, &stsc);
            let durations = find_box(stbl, b"stts")
                .map(|b| parse_stts(b, n))
                .unwrap_or_default();
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
            let mut decode_ticks: i64 = 0;
            for (i, &size) in sizes.iter().enumerate() {
                let dur = durations.get(i).copied().unwrap_or(0);
                let comp = ctts.get(i).copied().unwrap_or(0);
                let dts_ns = to_ns(decode_ticks);
                let pts_ticks = decode_ticks.saturating_add(comp as i64);
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

impl<R: Read + Seek + Send> Stream for Mp4Reader<R> {
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
            track: s.track,
            pts: s.pts_ns,
            keyframe: s.keyframe,
            data,
            duration_ns: None,
            source: None,
            coding: None,
        }))
    }

    fn write(&mut self, _frame: &PesFrame) -> io::Result<()> {
        Err(crate::error::Error::StreamReadOnly.into())
    }

    fn finish(&mut self) -> io::Result<()> {
        Ok(())
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
                file.read_exact(&mut ext)?;
                u64::from_be_bytes(ext)
            }
            0 => file_end.saturating_sub(pos),
            n => n as u64,
        };
        let header_len: u64 = if size32 == 1 { 16 } else { 8 };
        // A box must contain at least its own header and cannot run past EOF. This
        // also guarantees forward progress (box_size >= header_len > 0), so a
        // crafted size < 8 can't spin the loop in place, and bounds every payload
        // allocation to the real file length (no gigabyte over-allocation). Use
        // checked_add so a 64-bit largesize near u64::MAX can't wrap past the guard.
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

/// All child boxes of `payload` with the given type (each as its payload slice),
/// stopping after `cap` matches so a caller that only processes the first `cap`
/// never forces an oversized Vec of slice fat-pointers from a crafted payload
/// packed with minimum-size boxes. Pass `usize::MAX` for "all matches".
fn find_boxes_capped<'a>(payload: &'a [u8], want: &[u8; 4], cap: usize) -> Vec<&'a [u8]> {
    let mut out = Vec::new();
    let mut pos = 0;
    while pos + 8 <= payload.len() && out.len() < cap {
        let size = u32::from_be_bytes([
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
        if size < 8 || pos + size > payload.len() {
            break;
        }
        if &bt == want {
            out.push(&payload[pos + 8..pos + size]);
        }
        pos += size;
    }
    out
}

fn be32(b: &[u8], o: usize) -> u32 {
    u32::from_be_bytes([b[o], b[o + 1], b[o + 2], b[o + 3]])
}
fn be16(b: &[u8], o: usize) -> u16 {
    u16::from_be_bytes([b[o], b[o + 1]])
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
    channels: u16,
}

/// stsd → codec + dimensions + codec_private + channel count (first entry).
fn parse_stsd(b: &[u8]) -> Option<StsdInfo> {
    // version+flags(4) entry_count(4) then the first sample entry box.
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
            channels: 0,
        })
    } else {
        // AudioSampleEntry: 6 reserved + 2 dri + 8 reserved + channelcount(2)
        // samplesize(2) + 2 pre + 2 reserved + samplerate(4) = 28 bytes, then child
        // boxes. AAC (mp4a) carries its AudioSpecificConfig in an `esds` box — the
        // MKV CodecPrivate for A_AAC. AC-3/DTS are self-describing in-band (None).
        let channels = if body.len() >= 28 { be16(body, 16) } else { 2 };
        let config = if matches!(codec, Codec::Aac) && body.len() >= 28 {
            find_box(&body[28..], b"esds").and_then(parse_esds_asc)
        } else {
            None
        };
        Some(StsdInfo {
            codec,
            height: 0,
            config,
            channels,
        })
    }
}

/// Read an MPEG-4 expandable descriptor length (ISO/IEC 14496-1), advancing `pos`.
/// Each byte contributes 7 bits, continued while the high bit is set (max 4 bytes).
fn read_descriptor_len(b: &[u8], pos: &mut usize) -> usize {
    let mut len = 0usize;
    for _ in 0..4 {
        let Some(&byte) = b.get(*pos) else { break };
        *pos += 1;
        len = (len << 7) | (byte & 0x7F) as usize;
        if byte & 0x80 == 0 {
            break;
        }
    }
    len
}

/// esds → AAC AudioSpecificConfig (the A_AAC CodecPrivate), or `None`. Walks
/// ES_Descriptor(0x03) → DecoderConfigDescriptor(0x04) → DecoderSpecificInfo(0x05).
/// Fully bounds-checked: a malformed/truncated esds returns None, never panics.
fn parse_esds_asc(b: &[u8]) -> Option<Vec<u8>> {
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
    // objectTypeIndication(1) + streamType/bufferSizeDB(4) + maxBitrate(4) + avgBitrate(4)
    pos += 13;
    if *b.get(pos)? != 0x05 {
        return None; // DecoderSpecificInfo
    }
    pos += 1;
    let asc_len = read_descriptor_len(b, &mut pos);
    let end = pos.checked_add(asc_len)?;
    if asc_len == 0 || end > b.len() {
        return None;
    }
    Some(b[pos..end].to_vec())
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

/// stco (32-bit) / co64 (64-bit) → chunk offsets.
fn parse_stco(b: &[u8], is64: bool) -> Vec<u64> {
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
    // Any trailing samples with no chunk mapping: pack after the last offset.
    while offsets.len() < sizes.len() {
        let last = offsets.last().copied().unwrap_or(0);
        let last_sz = sizes
            .get(offsets.len().saturating_sub(1))
            .copied()
            .unwrap_or(0);
        offsets.push(last.saturating_add(last_sz as u64));
    }
    offsets
}

/// stts → per-sample decode durations (expanded from run-length entries). `max`
/// caps the expansion — the caller passes the track's real sample count, past which
/// entries are never read (and an untrusted run-length must not grow the Vec).
fn parse_stts(b: &[u8], max: usize) -> Vec<u32> {
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
mod tests {
    use super::*;

    #[test]
    fn stsc_offsets_one_sample_per_chunk() {
        // Our writer's layout: 1 sample/chunk, co64 lists every offset.
        let sizes = vec![100u32, 200, 300];
        let chunks = vec![1000u64, 1100, 1300];
        let stsc = vec![(1u32, 1u32)];
        assert_eq!(
            sample_offsets(&sizes, &chunks, &stsc),
            vec![1000, 1100, 1300]
        );
    }

    #[test]
    fn stsc_offsets_multi_sample_chunks() {
        // 2 samples in chunk 1, 1 in chunk 2.
        let sizes = vec![10u32, 20, 30];
        let chunks = vec![500u64, 900];
        let stsc = vec![(1u32, 2u32), (2u32, 1u32)];
        // chunk1@500: s0@500, s1@510; chunk2@900: s2@900.
        assert_eq!(sample_offsets(&sizes, &chunks, &stsc), vec![500, 510, 900]);
    }

    #[test]
    fn sample_offsets_saturates_on_huge_chunk_offset() {
        // A co64 chunk offset near u64::MAX plus a sample size must saturate, not
        // overflow-panic (debug) / wrap (release) — the read() EOF guard rejects
        // the resulting out-of-range offset later.
        let sizes = vec![10u32, 20];
        let chunks = vec![u64::MAX - 5];
        let stsc = vec![(1u32, 2u32)]; // 2 samples in the single chunk
        let offs = sample_offsets(&sizes, &chunks, &stsc);
        assert_eq!(offs[0], u64::MAX - 5);
        assert_eq!(offs[1], u64::MAX, "(MAX-5)+10 saturates to MAX");
    }

    #[test]
    fn read_rejects_sample_offset_past_eof() {
        use crate::disc::{
            Codec, DiscTitle, FrameRate, HdrFormat, Resolution, Stream as DiscStreamE, VideoStream,
        };
        use crate::mux::mp4::Mp4Sink;
        use crate::pes::{PesFrame, Stream as _};
        use std::io::Cursor;

        // The MP4 writer requires a primary video track, so build an HEVC one.
        let mut t = DiscTitle::empty();
        t.streams = vec![DiscStreamE::Video(VideoStream {
            pid: 0x1011,
            codec: Codec::Hevc,
            resolution: Resolution::R1080p,
            frame_rate: FrameRate::F23_976,
            hdr: HdrFormat::Sdr,
            color_space: crate::disc::ColorSpace::Unknown,
            display_aspect: None,
            secondary: false,
            label: String::new(),
            measured_cicp: None,
        })];
        t.codec_privates = vec![Some(vec![0xAA, 0xBB, 0xCC, 0xDD, 0xEE])];
        let mut buf = Vec::new();
        {
            let mut sink = Mp4Sink::create(Cursor::new(&mut buf), &t).unwrap();
            sink.write(&PesFrame {
                track: 0,
                pts: 0,
                keyframe: true,
                data: vec![0x11u8; 700],
                duration_ns: None,
                source: None,
                coding: None,
            })
            .unwrap();
            sink.finish().unwrap();
        }
        // Faststart writes `moov` near the front and `mdat` after a multi-MiB
        // reserve, so a 64 KiB truncation keeps the parseable moov but drops mdat.
        // from_reader still succeeds; read() must reject the now-out-of-range
        // sample rather than allocate for it or read past EOF.
        buf.truncate(64 * 1024);
        let mut rd = Mp4Reader::from_reader(Cursor::new(buf), "trunc".into()).unwrap();
        let err = rd.read().unwrap_err();
        assert_eq!(
            err.kind(),
            std::io::ErrorKind::InvalidData,
            "a sample offset past EOF must be rejected"
        );
    }

    #[test]
    fn write_then_read_round_trip() {
        // Mux a small A/V title to an in-memory MP4, then demux it back and
        // check the streams, codec_private, and sample payloads survive.
        use crate::disc::{
            AudioChannels, AudioStream, Codec, DiscTitle, FrameRate, HdrFormat, LabelPurpose,
            SampleRate, Stream as DiscStreamE, VideoStream,
        };
        use crate::mux::mp4::Mp4Sink;
        use crate::pes::{PesFrame, Stream as _};
        use std::io::Cursor;

        let mut t = DiscTitle::empty();
        t.streams = vec![
            DiscStreamE::Video(VideoStream {
                pid: 0x1011,
                codec: Codec::Hevc,
                resolution: Resolution::R1080p,
                frame_rate: FrameRate::F23_976,
                hdr: HdrFormat::Sdr,
                color_space: crate::disc::ColorSpace::Unknown,
                display_aspect: None,
                secondary: false,
                label: String::new(),
                measured_cicp: None,
            }),
            DiscStreamE::Audio(AudioStream {
                pid: 0x1100,
                codec: Codec::Ac3,
                channels: AudioChannels::Surround51,
                language: "eng".into(),
                sample_rate: SampleRate::S48,
                secondary: false,
                purpose: LabelPurpose::Normal,
                label: String::new(),
            }),
        ];
        let hvcc = vec![0xAA, 0xBB, 0xCC, 0xDD, 0xEE];
        t.codec_privates = vec![Some(hvcc.clone()), None];

        let ac3 = vec![
            0x0B,
            0x77,
            0x00,
            0x00,
            0b00_010110,
            0b01000_000,
            0b111_00_00_1,
            0x00,
            0xFF,
            0xFF,
        ];
        let d = 41_708_333i64;
        let vid0 = vec![0x11u8; 700];
        let vid1 = vec![0x22u8; 350];

        let mut buf = Vec::new();
        {
            let mk = |track, pts, key, data: Vec<u8>| PesFrame {
                track,
                pts,
                keyframe: key,
                data,
                duration_ns: None,
                source: None,
                coding: None,
            };
            let mut sink = Mp4Sink::create(Cursor::new(&mut buf), &t).unwrap();
            sink.write(&mk(0, 0, true, vid0.clone())).unwrap();
            sink.write(&mk(1, 0, true, ac3.clone())).unwrap();
            sink.write(&mk(0, d, false, vid1.clone())).unwrap();
            sink.write(&mk(1, 32_000_000, true, ac3.clone())).unwrap();
            sink.finish().unwrap();
        }

        let mut rd = Mp4Reader::from_reader(Cursor::new(buf), "rt".into()).unwrap();
        // Two streams: HEVC video + AC-3 audio, and the hvcC round-trips.
        assert_eq!(rd.info().streams.len(), 2);
        assert!(
            matches!(rd.info().streams[0], DiscStreamE::Video(ref v) if v.codec == Codec::Hevc)
        );
        assert!(matches!(rd.info().streams[1], DiscStreamE::Audio(ref a) if a.codec == Codec::Ac3));
        assert_eq!(rd.codec_private(0), Some(hvcc));

        // Read all frames back; match them to what we wrote by (track, size).
        let mut got = Vec::new();
        while let Some(f) = rd.read().unwrap() {
            got.push((f.track, f.data.len(), f.keyframe));
        }
        assert_eq!(got.len(), 4, "4 samples round-trip");
        let vids: Vec<_> = got.iter().filter(|(t, _, _)| *t == 0).collect();
        let auds: Vec<_> = got.iter().filter(|(t, _, _)| *t == 1).collect();
        assert_eq!(vids.len(), 2);
        assert_eq!(auds.len(), 2);
        assert_eq!(vids[0].1, 700, "first video sample size");
        assert_eq!(vids[1].1, 350, "second video sample size");
        assert!(vids[0].2, "first video frame is a keyframe");
        assert_eq!(auds[0].1, ac3.len());
    }

    #[test]
    fn stts_and_ctts_expand() {
        // stts: 3 samples × 1001 ticks.
        let mut stts = Vec::new();
        stts.extend_from_slice(&[0, 0, 0, 0]); // version+flags
        stts.extend_from_slice(&1u32.to_be_bytes()); // entry_count
        stts.extend_from_slice(&3u32.to_be_bytes());
        stts.extend_from_slice(&1001u32.to_be_bytes());
        assert_eq!(parse_stts(&stts, MAX_SAMPLE_COUNT), vec![1001, 1001, 1001]);
    }

    // ── Untrusted-input hardening: a crafted MP4 must never panic or over-allocate.

    #[test]
    fn parse_stsz_fixed_size_caps_hostile_count() {
        // sample_size != 0, count = u32::MAX: a 12-byte box must not allocate ~16 GiB.
        let mut b = Vec::new();
        b.extend_from_slice(&[0, 0, 0, 0]); // version+flags
        b.extend_from_slice(&1u32.to_be_bytes()); // sample_size
        b.extend_from_slice(&u32::MAX.to_be_bytes()); // count
        let out = parse_stsz(&b, MAX_SAMPLE_COUNT);
        assert_eq!(out.len(), MAX_SAMPLE_COUNT);
        // And a smaller budget (the cumulative multi-track cap) bounds it tighter.
        assert_eq!(parse_stsz(&b, 100).len(), 100);
    }

    #[test]
    fn parse_stsz_table_count_bounded_by_box() {
        // sample_size == 0, count huge, but only two real entries in the buffer.
        let mut b = Vec::new();
        b.extend_from_slice(&[0, 0, 0, 0]);
        b.extend_from_slice(&0u32.to_be_bytes()); // sample_size == 0 → table
        b.extend_from_slice(&u32::MAX.to_be_bytes()); // count (lie)
        b.extend_from_slice(&10u32.to_be_bytes());
        b.extend_from_slice(&20u32.to_be_bytes());
        assert_eq!(parse_stsz(&b, MAX_SAMPLE_COUNT), vec![10, 20]);
    }

    #[test]
    fn parse_stco_stsc_count_bounded_by_box() {
        // stco: count lie, one real 32-bit offset.
        let mut stco = Vec::new();
        stco.extend_from_slice(&[0, 0, 0, 0]);
        stco.extend_from_slice(&u32::MAX.to_be_bytes());
        stco.extend_from_slice(&4096u32.to_be_bytes());
        assert_eq!(parse_stco(&stco, false), vec![4096]);
        // stsc: count lie, one real (first_chunk, per) tuple.
        let mut stsc = Vec::new();
        stsc.extend_from_slice(&[0, 0, 0, 0]);
        stsc.extend_from_slice(&u32::MAX.to_be_bytes());
        stsc.extend_from_slice(&1u32.to_be_bytes());
        stsc.extend_from_slice(&7u32.to_be_bytes());
        stsc.extend_from_slice(&0u32.to_be_bytes()); // sample_desc_idx (unused)
        assert_eq!(parse_stsc(&stsc), vec![(1, 7)]);
    }

    #[test]
    fn parse_stts_caps_hostile_runlength() {
        // One entry with a u32::MAX run-length must cap, not push billions.
        let mut stts = Vec::new();
        stts.extend_from_slice(&[0, 0, 0, 0]);
        stts.extend_from_slice(&1u32.to_be_bytes()); // entry_count
        stts.extend_from_slice(&u32::MAX.to_be_bytes()); // n
        stts.extend_from_slice(&33u32.to_be_bytes()); // delta
        assert_eq!(parse_stts(&stts, MAX_SAMPLE_COUNT).len(), MAX_SAMPLE_COUNT);
        // A tight per-track cap (the real sample count) bounds the run-length too.
        assert_eq!(parse_stts(&stts, 5).len(), 5);
    }

    #[test]
    fn parse_stsd_short_size_does_not_panic() {
        // stsd sample entry declaring size = 0 must not panic on `entry[8..<8]`.
        let mut b = Vec::new();
        b.extend_from_slice(&[0, 0, 0, 0]); // version+flags
        b.extend_from_slice(&1u32.to_be_bytes()); // entry_count
        b.extend_from_slice(&0u32.to_be_bytes()); // sample entry size = 0
        b.extend_from_slice(b"avc1"); // fourcc
        // Recognised codec but empty body → None (no dimensions), no panic.
        assert!(parse_stsd(&b).is_none());
    }

    #[test]
    fn read_moov_oversize_is_rejected() {
        use std::io::Cursor;
        // A tiny file whose first box claims to be a `moov` far larger than the file.
        let mut b = Vec::new();
        b.extend_from_slice(&0xFFFF_FFF0u32.to_be_bytes()); // size32 (~4 GiB)
        b.extend_from_slice(b"moov");
        b.extend_from_slice(&[0u8; 16]); // a few real bytes, nowhere near the claim
        assert!(read_moov(&mut Cursor::new(b)).is_err());
    }

    #[test]
    fn parse_esds_extracts_aac_asc() {
        // esds: version/flags + ES_Descriptor(0x03) + DecoderConfigDescriptor(0x04)
        // + DecoderSpecificInfo(0x05) carrying a 2-byte AudioSpecificConfig.
        let esds = vec![
            0, 0, 0, 0, // version+flags
            0x03, 0x19, 0x00, 0x00, 0x00, // ES_Descriptor: tag,len, ES_ID(2), flags(0)
            0x04, 0x11, // DecoderConfigDescriptor: tag,len
            0x40, // objectTypeIndication (AAC)
            0x15, 0, 0, 0, // streamType/bufferSizeDB
            0, 0, 0, 0, // maxBitrate
            0, 0, 0, 0, // avgBitrate
            0x05, 0x02, // DecoderSpecificInfo: tag,len
            0x12, 0x10, // AudioSpecificConfig (AAC-LC 44.1k stereo)
        ];
        assert_eq!(parse_esds_asc(&esds), Some(vec![0x12, 0x10]));
        // A truncated esds must return None, never panic.
        assert_eq!(parse_esds_asc(&esds[..12]), None);
    }

    #[test]
    fn read_moov_over_cap_rejected_despite_inflated_file_len() {
        use std::io::{Read, Seek, SeekFrom};
        // A reader that reports an 8 GiB length on `seek(End)` (trivially forged by
        // a sparse file, e.g. `truncate -s 8G`) but is backed by a tiny crafted
        // header followed by an endless run of zeros. A `moov` whose declared size
        // (512 MiB) is UNDER that inflated length passes the plain EOF check AND
        // (crucially) the payload `read_exact` would SUCCEED against the zero
        // stream — so only the absolute MAX_ALLOC_BYTES (256 MiB) cap can reject
        // it. This makes the test flip to Ok (allocation attempted) if a regression
        // drops the cap and keeps only the (sparse-file-defeatable) EOF check.
        struct InflatedReader {
            data: Vec<u8>,
            pos: u64,
            fake_len: u64,
        }
        impl Read for InflatedReader {
            fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
                let remaining = self.fake_len.saturating_sub(self.pos);
                let n = (out.len() as u64).min(remaining) as usize;
                for (i, byte) in out[..n].iter_mut().enumerate() {
                    let idx = self.pos + i as u64;
                    *byte = if idx < self.data.len() as u64 {
                        self.data[idx as usize]
                    } else {
                        0 // endless zero fill past the crafted header
                    };
                }
                self.pos += n as u64;
                Ok(n)
            }
        }
        impl Seek for InflatedReader {
            fn seek(&mut self, from: SeekFrom) -> io::Result<u64> {
                self.pos = match from {
                    SeekFrom::Start(p) => p,
                    SeekFrom::End(off) => (self.fake_len as i64 + off) as u64,
                    SeekFrom::Current(off) => (self.pos as i64 + off) as u64,
                };
                Ok(self.pos)
            }
        }

        // Only the 8-byte box header is served; the 512 MiB payload is never read.
        let box_size: u32 = (512 << 20) + 8; // 512 MiB > 256 MiB cap, < 8 GiB len
        let mut data = Vec::new();
        data.extend_from_slice(&box_size.to_be_bytes());
        data.extend_from_slice(b"moov");
        let mut rd = InflatedReader {
            data,
            pos: 0,
            fake_len: 8 << 30, // 8 GiB
        };
        // Sanity: the EOF check would pass (claim < inflated len), so a rejection
        // can only come from the MAX_ALLOC_BYTES cap.
        assert!((box_size as u64) < rd.fake_len);
        assert!(read_moov(&mut rd).is_err());
    }

    #[test]
    fn read_moov_size_zero_spans_to_eof() {
        use std::io::Cursor;
        // size32 == 0 means "box extends to end of file"; the moov body is the rest.
        let mut b = Vec::new();
        b.extend_from_slice(&0u32.to_be_bytes()); // size = 0 → to EOF
        b.extend_from_slice(b"moov");
        b.extend_from_slice(&[0xAA, 0xBB, 0xCC]); // body
        assert_eq!(
            read_moov(&mut Cursor::new(b)).unwrap(),
            vec![0xAA, 0xBB, 0xCC]
        );
    }

    #[test]
    fn read_moov_largesize_overflow_is_rejected() {
        use std::io::Cursor;
        // A 64-bit largesize near u64::MAX must not wrap the `pos + box_size` EOF
        // guard (it would otherwise pass and drive an exabyte allocation).
        let mut b = Vec::new();
        b.extend_from_slice(&16u32.to_be_bytes()); // ftyp box, size 16
        b.extend_from_slice(b"ftyp");
        b.extend_from_slice(&[0u8; 8]);
        b.extend_from_slice(&1u32.to_be_bytes()); // moov, size==1 → largesize
        b.extend_from_slice(b"moov");
        b.extend_from_slice(&0xFFFF_FFFF_FFFF_FFF8u64.to_be_bytes());
        assert!(read_moov(&mut Cursor::new(b)).is_err());
    }

    #[test]
    fn read_moov_undersized_box_does_not_hang() {
        use std::io::Cursor;
        // A box whose declared size is < 8 (here 3) must be rejected, not spin the
        // loop in place forever (size.saturating_sub(8) == 0 → no forward progress).
        let mut b = Vec::new();
        b.extend_from_slice(&3u32.to_be_bytes());
        b.extend_from_slice(b"free");
        assert!(read_moov(&mut Cursor::new(b)).is_err());
    }

    /// Wrap `payload` in an ISO-BMFF box with the given 4-byte type.
    fn mp4_box(typ: &[u8; 4], payload: &[u8]) -> Vec<u8> {
        let size = (payload.len() + 8) as u32;
        let mut v = Vec::with_capacity(payload.len() + 8);
        v.extend_from_slice(&size.to_be_bytes());
        v.extend_from_slice(typ);
        v.extend_from_slice(payload);
        v
    }

    /// Build a minimal-but-complete audio `trak` (one AC-3 sample) with the given
    /// media timescale — enough boxes that `from_reader` reaches the per-sample
    /// `to_ns` timestamp conversion (mdia → mdhd/hdlr/minf → stbl → stsd/stsz/
    /// stco/stsc, one sample).
    fn audio_trak(timescale: u32) -> Vec<u8> {
        let mdhd = {
            // v0: version+flags(4) creation(4) modification(4) timescale(4) duration(4).
            let mut p = vec![0u8; 24];
            p[12..16].copy_from_slice(&timescale.to_be_bytes());
            mp4_box(b"mdhd", &p)
        };
        let hdlr = {
            // version+flags(4) pre_defined(4) handler_type(4).
            let mut p = vec![0u8; 12];
            p[8..12].copy_from_slice(b"soun");
            mp4_box(b"hdlr", &p)
        };
        let stsd = {
            let mut p = Vec::new();
            p.extend_from_slice(&[0, 0, 0, 0]); // version+flags
            p.extend_from_slice(&1u32.to_be_bytes()); // entry_count
            p.extend_from_slice(&8u32.to_be_bytes()); // sample entry size (header only)
            p.extend_from_slice(b"ac-3"); // fourcc → Codec::Ac3
            mp4_box(b"stsd", &p)
        };
        let stsz = {
            let mut p = Vec::new();
            p.extend_from_slice(&[0, 0, 0, 0]); // version+flags
            p.extend_from_slice(&10u32.to_be_bytes()); // sample_size (fixed) = 10
            p.extend_from_slice(&1u32.to_be_bytes()); // count = 1
            mp4_box(b"stsz", &p)
        };
        let stco = {
            let mut p = Vec::new();
            p.extend_from_slice(&[0, 0, 0, 0]);
            p.extend_from_slice(&1u32.to_be_bytes()); // count
            p.extend_from_slice(&0u32.to_be_bytes()); // chunk offset 0
            mp4_box(b"stco", &p)
        };
        let stsc = {
            let mut p = Vec::new();
            p.extend_from_slice(&[0, 0, 0, 0]);
            p.extend_from_slice(&1u32.to_be_bytes()); // count
            p.extend_from_slice(&1u32.to_be_bytes()); // first_chunk
            p.extend_from_slice(&1u32.to_be_bytes()); // samples_per_chunk
            p.extend_from_slice(&0u32.to_be_bytes()); // sample_desc_idx
            mp4_box(b"stsc", &p)
        };
        let mut stbl = Vec::new();
        stbl.extend_from_slice(&stsd);
        stbl.extend_from_slice(&stsz);
        stbl.extend_from_slice(&stco);
        stbl.extend_from_slice(&stsc);
        let minf = mp4_box(b"minf", &mp4_box(b"stbl", &stbl));
        let mut mdia = Vec::new();
        mdia.extend_from_slice(&mdhd);
        mdia.extend_from_slice(&hdlr);
        mdia.extend_from_slice(&minf);
        mp4_box(b"trak", &mp4_box(b"mdia", &mdia))
    }

    #[test]
    fn mdhd_timescale_zero_does_not_divide_by_zero() {
        use std::io::Cursor;
        // Without the `.filter(|&t| t != 0)` guard the per-sample `to_ns` closure
        // divides by the zero timescale and panics; with it the track falls back
        // to the 90 kHz default and is indexed normally.
        let moov = mp4_box(b"moov", &audio_trak(0));
        let rd = Mp4Reader::from_reader(Cursor::new(moov), "ts0".into());
        assert!(
            rd.is_ok(),
            "timescale 0 must be handled via fallback, no divide-by-zero panic"
        );
        assert_eq!(
            rd.unwrap().info().streams.len(),
            1,
            "the timescale-0 track is still indexed"
        );
    }

    #[test]
    fn trak_loop_stops_at_max_tracks() {
        use std::io::Cursor;
        // A crafted moov packing more than MAX_TRACKS trak boxes must not index
        // past the cap — the per-track PID `0x1011 + idx` overflows u16 past ~61k
        // tracks. Without the cap this indexes all MAX_TRACKS + 50 tracks.
        let mut traks = Vec::new();
        for _ in 0..(MAX_TRACKS + 50) {
            traks.extend_from_slice(&audio_trak(48_000));
        }
        let moov = mp4_box(b"moov", &traks);
        let rd = Mp4Reader::from_reader(Cursor::new(moov), "many".into()).unwrap();
        assert_eq!(
            rd.info().streams.len(),
            MAX_TRACKS,
            "trak loop must stop at MAX_TRACKS"
        );
    }

    /// A crafted file with a *fixed-size* `stsz` (sample_size != 0) claiming
    /// count = 0xFFFFFFFF must not inflate the sample index past the file's own
    /// byte length. `from_reader` sets `sample_budget = MAX_SAMPLE_COUNT.min(file_len)`,
    /// so a few-hundred-byte file bounds the `Vec<SampleRef>` to a few hundred —
    /// NOT the 16M `MAX_SAMPLE_COUNT` ceiling. Mutation check: revert the budget
    /// to a bare `MAX_SAMPLE_COUNT` and this file yields ~16M samples, failing the
    /// `<= file_len` (and `< MAX_SAMPLE_COUNT`) assertions below.
    #[test]
    fn stsz_sample_count_bounded_by_file_len() {
        use std::io::Cursor;
        // A minimal audio trak, but with a fixed-size stsz lying about its count.
        let mdhd = {
            let mut p = vec![0u8; 24];
            p[12..16].copy_from_slice(&48_000u32.to_be_bytes()); // timescale
            mp4_box(b"mdhd", &p)
        };
        let hdlr = {
            let mut p = vec![0u8; 12];
            p[8..12].copy_from_slice(b"soun");
            mp4_box(b"hdlr", &p)
        };
        let stsd = {
            let mut p = Vec::new();
            p.extend_from_slice(&[0, 0, 0, 0]); // version+flags
            p.extend_from_slice(&1u32.to_be_bytes()); // entry_count
            p.extend_from_slice(&8u32.to_be_bytes()); // sample entry size (header only)
            p.extend_from_slice(b"ac-3"); // fourcc → Codec::Ac3
            mp4_box(b"stsd", &p)
        };
        let stsz = {
            let mut p = Vec::new();
            p.extend_from_slice(&[0, 0, 0, 0]); // version+flags
            p.extend_from_slice(&10u32.to_be_bytes()); // sample_size != 0 (fixed)
            p.extend_from_slice(&0xFFFF_FFFFu32.to_be_bytes()); // count = u32::MAX (lie)
            mp4_box(b"stsz", &p)
        };
        let stco = {
            let mut p = Vec::new();
            p.extend_from_slice(&[0, 0, 0, 0]);
            p.extend_from_slice(&1u32.to_be_bytes()); // count
            p.extend_from_slice(&0u32.to_be_bytes()); // chunk offset 0
            mp4_box(b"stco", &p)
        };
        let stsc = {
            let mut p = Vec::new();
            p.extend_from_slice(&[0, 0, 0, 0]);
            p.extend_from_slice(&1u32.to_be_bytes()); // count
            p.extend_from_slice(&1u32.to_be_bytes()); // first_chunk
            p.extend_from_slice(&1u32.to_be_bytes()); // samples_per_chunk
            p.extend_from_slice(&0u32.to_be_bytes()); // sample_desc_idx
            mp4_box(b"stsc", &p)
        };
        let mut stbl = Vec::new();
        stbl.extend_from_slice(&stsd);
        stbl.extend_from_slice(&stsz);
        stbl.extend_from_slice(&stco);
        stbl.extend_from_slice(&stsc);
        let minf = mp4_box(b"minf", &mp4_box(b"stbl", &stbl));
        let mut mdia = Vec::new();
        mdia.extend_from_slice(&mdhd);
        mdia.extend_from_slice(&hdlr);
        mdia.extend_from_slice(&minf);
        let trak = mp4_box(b"trak", &mp4_box(b"mdia", &mdia));
        let moov = mp4_box(b"moov", &trak);

        let file_len = moov.len() as u64;
        assert!(
            file_len < 1024,
            "fixture stays a few hundred bytes ({file_len})"
        );
        let rd = Mp4Reader::from_reader(Cursor::new(moov), "hostile".into()).unwrap();
        // The index must be bounded by the file's byte length, NOT the 16M ceiling.
        assert!(
            (rd.samples.len() as u64) <= file_len,
            "sample count {} must be bounded by file_len {file_len}, not the count lie",
            rd.samples.len()
        );
        assert!(
            rd.samples.len() < MAX_SAMPLE_COUNT,
            "a tiny file must not allocate the 16M MAX_SAMPLE_COUNT ceiling"
        );
    }

    /// Build an audio `trak` identical to `audio_trak(48_000)` but with the
    /// named stbl child box omitted. Used to reach the untrusted-input guards
    /// that drop a track whose `stsz` says samples exist yet whose `stco`/`co64`
    /// (chunk offsets) or `stsc` (sample-to-chunk map) is missing — without such
    /// a table every sample offset would resolve near file byte 0.
    fn audio_trak_missing(omit: &[u8; 4]) -> Vec<u8> {
        let mdhd = {
            let mut p = vec![0u8; 24];
            p[12..16].copy_from_slice(&48_000u32.to_be_bytes()); // timescale
            mp4_box(b"mdhd", &p)
        };
        let hdlr = {
            let mut p = vec![0u8; 12];
            p[8..12].copy_from_slice(b"soun");
            mp4_box(b"hdlr", &p)
        };
        let stsd = {
            let mut p = Vec::new();
            p.extend_from_slice(&[0, 0, 0, 0]); // version+flags
            p.extend_from_slice(&1u32.to_be_bytes()); // entry_count
            p.extend_from_slice(&8u32.to_be_bytes()); // sample entry size (header only)
            p.extend_from_slice(b"ac-3"); // fourcc → Codec::Ac3
            mp4_box(b"stsd", &p)
        };
        let stsz = {
            let mut p = Vec::new();
            p.extend_from_slice(&[0, 0, 0, 0]); // version+flags
            p.extend_from_slice(&10u32.to_be_bytes()); // sample_size (fixed) = 10
            p.extend_from_slice(&3u32.to_be_bytes()); // count = 3 (samples exist)
            mp4_box(b"stsz", &p)
        };
        let stco = {
            let mut p = Vec::new();
            p.extend_from_slice(&[0, 0, 0, 0]);
            p.extend_from_slice(&1u32.to_be_bytes()); // count
            p.extend_from_slice(&0u32.to_be_bytes()); // chunk offset 0
            mp4_box(b"stco", &p)
        };
        let stsc = {
            let mut p = Vec::new();
            p.extend_from_slice(&[0, 0, 0, 0]);
            p.extend_from_slice(&1u32.to_be_bytes()); // count
            p.extend_from_slice(&1u32.to_be_bytes()); // first_chunk
            p.extend_from_slice(&1u32.to_be_bytes()); // samples_per_chunk
            p.extend_from_slice(&0u32.to_be_bytes()); // sample_desc_idx
            mp4_box(b"stsc", &p)
        };
        let mut stbl = Vec::new();
        stbl.extend_from_slice(&stsd);
        stbl.extend_from_slice(&stsz);
        if omit != b"stco" {
            stbl.extend_from_slice(&stco);
        }
        if omit != b"stsc" {
            stbl.extend_from_slice(&stsc);
        }
        let minf = mp4_box(b"minf", &mp4_box(b"stbl", &stbl));
        let mut mdia = Vec::new();
        mdia.extend_from_slice(&mdhd);
        mdia.extend_from_slice(&hdlr);
        mdia.extend_from_slice(&minf);
        mp4_box(b"trak", &mp4_box(b"mdia", &mdia))
    }

    /// A track with samples (`stsz`) but no chunk-offset table (`stco`/`co64`)
    /// must be DROPPED, not indexed with offsets that resolve near file byte 0.
    /// With it the only track, the whole file fails `Mp4Invalid`.
    /// Mutation check: delete the `if chunk_offsets.is_empty() { continue; }`
    /// guard and `from_reader` returns `Ok` (garbage samples), flipping this to FAIL.
    #[test]
    fn missing_stco_drops_track_all_dropped_is_invalid() {
        use std::io::Cursor;
        let moov = mp4_box(b"moov", &audio_trak_missing(b"stco"));
        let rd = Mp4Reader::from_reader(Cursor::new(moov), "no-stco".into());
        assert!(
            rd.is_err(),
            "a track with stsz but no stco/co64 must be dropped; all-dropped → Mp4Invalid"
        );
    }

    /// A track with samples (`stsz`) and chunk offsets (`stco`) but no
    /// sample-to-chunk map (`stsc`) must be DROPPED — without `stsc` the samples
    /// cannot be placed against the chunk offsets and would pack from byte 0.
    /// Mutation check: delete the `if stsc.is_empty() { continue; }` guard and
    /// `from_reader` returns `Ok`, flipping this to FAIL.
    #[test]
    fn missing_stsc_drops_track_all_dropped_is_invalid() {
        use std::io::Cursor;
        let moov = mp4_box(b"moov", &audio_trak_missing(b"stsc"));
        let rd = Mp4Reader::from_reader(Cursor::new(moov), "no-stsc".into());
        assert!(
            rd.is_err(),
            "a track with stsz + stco but no stsc must be dropped; all-dropped → Mp4Invalid"
        );
    }

    /// Sanity companion: the SAME builder WITH both tables present yields a valid,
    /// indexed single-track file — proving the two Err results above come from the
    /// missing table, not from some unrelated defect in the fixture builder.
    #[test]
    fn audio_trak_missing_none_is_valid() {
        use std::io::Cursor;
        // omit a box that isn't in the stbl → nothing omitted, fixture is complete.
        let moov = mp4_box(b"moov", &audio_trak_missing(b"____"));
        let rd = Mp4Reader::from_reader(Cursor::new(moov), "complete".into())
            .expect("complete stbl (stsz+stco+stsc) must index");
        assert_eq!(rd.info().streams.len(), 1, "the complete track is indexed");
    }

    #[test]
    fn mdhd_language_offsets_per_version() {
        // "eng" packed = 0x15C7. v0 carries it at byte 20, v1 (64-bit times) at 32.
        let packed = [0x15u8, 0xC7];
        let mut v0 = vec![0u8; 22];
        v0[0] = 0; // version 0
        v0[20..22].copy_from_slice(&packed);
        assert_eq!(mdhd_language(&v0).as_deref(), Some("eng"));
        let mut v1 = vec![0u8; 34];
        v1[0] = 1; // version 1
        v1[32..34].copy_from_slice(&packed);
        assert_eq!(mdhd_language(&v1).as_deref(), Some("eng"));
    }
}
