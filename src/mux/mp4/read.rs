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
        let moov = read_moov(&mut file)?;
        let mut title = DiscTitle::empty();
        title.playlist = name;

        let mut samples: Vec<SampleRef> = Vec::new();
        let mut codec_privates: Vec<Option<Vec<u8>>> = Vec::new();
        let mut track_idx = 0usize;

        for trak in find_boxes(&moov, b"trak") {
            let Some(mdia) = find_box(trak, b"mdia") else {
                continue;
            };
            let timescale = find_box(mdia, b"mdhd")
                .and_then(mdhd_timescale)
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
                    channels: AudioChannels::from_count(channels as u8),
                    language: language.clone().unwrap_or_else(|| "und".into()),
                    sample_rate: SampleRate::from_hz(timescale),
                    secondary: false,
                    purpose: LabelPurpose::Normal,
                    label: String::new(),
                }),
                _ => continue, // non-A/V handler
            };

            // Per-sample tables.
            let sizes = find_box(stbl, b"stsz").map(parse_stsz).unwrap_or_default();
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
            let stsc = find_box(stbl, b"stsc").map(parse_stsc).unwrap_or_default();
            let offsets = sample_offsets(&sizes, &chunk_offsets, &stsc);
            let durations = find_box(stbl, b"stts").map(parse_stts).unwrap_or_default();
            let ctts = find_box(stbl, b"ctts").map(parse_ctts).unwrap_or_default();
            let sync = find_box(stbl, b"stss").map(parse_stss);

            let mut decode_ticks: i64 = 0;
            for (i, &size) in sizes.iter().enumerate() {
                let dur = durations.get(i).copied().unwrap_or(0);
                let comp = ctts.get(i).copied().unwrap_or(0);
                let dts_ns = (decode_ticks as i128 * NS / timescale as i128) as i64;
                let pts_ticks = decode_ticks + comp as i64;
                let pts_ns = (pts_ticks as i128 * NS / timescale as i128) as i64;
                decode_ticks += dur as i64;
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
            return Err(crate::error::Error::MkvInvalid.into());
        }
        title.codec_privates = codec_privates;

        // Emit in global decode order so the consumer sees interleaved,
        // monotonic-DTS frames (a stable sort keeps per-track order on ties).
        samples.sort_by_key(|s| s.dts_ns);

        Ok(Self {
            file,
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
    loop {
        let mut hdr = [0u8; 8];
        if file.read_exact(&mut hdr).is_err() {
            return Err(crate::error::Error::MkvInvalid.into());
        }
        let size32 = u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]);
        let btype = [hdr[4], hdr[5], hdr[6], hdr[7]];
        // 64-bit largesize (size==1): the real size is the next 8 bytes; a
        // 16-byte header precedes the payload. size==0 means "to EOF".
        let payload_len: u64 = if size32 == 1 {
            let mut ext = [0u8; 8];
            file.read_exact(&mut ext)?;
            u64::from_be_bytes(ext).saturating_sub(16)
        } else {
            (size32 as u64).saturating_sub(8)
        };
        if &btype == b"moov" {
            let mut buf = vec![0u8; payload_len as usize];
            file.read_exact(&mut buf)?;
            return Ok(buf);
        }
        file.seek(SeekFrom::Current(payload_len as i64))?;
    }
}

/// The first child box of `payload` with the given type — returns its payload
/// (bytes after the 8-byte header). One level.
fn find_box<'a>(payload: &'a [u8], want: &[u8; 4]) -> Option<&'a [u8]> {
    find_boxes(payload, want).into_iter().next()
}

/// All child boxes of `payload` with the given type — each as its payload slice.
fn find_boxes<'a>(payload: &'a [u8], want: &[u8; 4]) -> Vec<&'a [u8]> {
    let mut out = Vec::new();
    let mut pos = 0;
    while pos + 8 <= payload.len() {
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
    let off = if version == 1 { 28 } else { 20 };
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
    let body = &entry[8..size.min(entry.len())];

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
        // samplesize(2) + 2 pre + 2 reserved + samplerate(4) = 28 bytes.
        let channels = if body.len() >= 28 { be16(body, 16) } else { 2 };
        Some(StsdInfo {
            codec,
            height: 0,
            config: None,
            channels,
        })
    }
}

/// stsz → per-sample sizes.
fn parse_stsz(b: &[u8]) -> Vec<u32> {
    if b.len() < 12 {
        return Vec::new();
    }
    let sample_size = be32(b, 4);
    let count = be32(b, 8) as usize;
    if sample_size != 0 {
        return vec![sample_size; count];
    }
    let mut out = Vec::with_capacity(count);
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
    let mut out = Vec::with_capacity(count);
    let stride = if is64 { 8 } else { 4 };
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
    let mut out = Vec::with_capacity(count);
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
            off += sizes[sidx] as u64;
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
        offsets.push(last + last_sz as u64);
    }
    offsets
}

/// stts → per-sample decode durations (expanded from run-length entries).
fn parse_stts(b: &[u8]) -> Vec<u32> {
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
            out.push(delta);
        }
    }
    out
}

/// ctts → per-sample composition offsets (version 0 unsigned / version 1 signed).
fn parse_ctts(b: &[u8]) -> Vec<i32> {
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
        assert_eq!(parse_stts(&stts), vec![1001, 1001, 1001]);
    }
}
