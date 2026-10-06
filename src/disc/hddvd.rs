//! HD-DVD title scanning — `HVDVD_TS/` Enhanced-VOB (`.evo`) enumeration, a **tree-level peer**
//! of DVD/Blu-ray with its own scanner (this file), a peer to [`Disc::scan_bluray_titles`].
//! Title composition is authoritative, from the `ADV_OBJ/VPLST000.XPL` Advanced-Content
//! playlist (parsed with `roxmltree` into one [`DiscTitle`] per `<Title>`, clips resolved via
//! `.MAP` sidecars, container [`ContentFormat::MpegPs`]); falls back to the `HVA*.VTI`
//! clip-name heuristic when no playlist parses.

use super::*;
use crate::mux::ps::{PsDemuxer, dvd_audio_pid};
use crate::sector::SectorSource;
use crate::udf;
use std::collections::BTreeMap;

/// Clip stream-file extension in the HD-DVD `HVDVD_TS/` tree. HD-DVD is a
/// separate tree from BD, so this is a separate constant — deliberately NOT an
/// entry in [`super::bluray`]'s BD-tree `CLIP_STREAM_EXTS`.
const HDDVD_CLIP_EXT: &str = ".evo";

/// Sectors of an `.evo` clip head to demux when probing its elementary streams
/// (~16 MiB). Enough to see the opening video access unit (SPS) plus every
/// interleaved audio sub-stream, without imaging the whole multi-GiB clip.
const EVO_PROBE_SECTORS: u32 = 8192;

/// Cap on the elementary-stream sample retained per stream while probing — a
/// video SPS / audio syncword lands well inside the first few KiB, so 128 KiB
/// is generous while bounding probe memory.
const EVO_ES_SAMPLE_CAP: usize = 128 * 1024;

/// HD-DVD Advanced VTS information file magic (`HVDVD_TS/HVA00001.VTI`). The VTI
/// is the Advanced Content's DVD-IFO analogue: it holds a fixed-stride clip table
/// naming every `.evo` in authored order.
const HDDVD_VTI_MAGIC: &[u8] = b"ADVANCED-VTS";

/// Byte stride between clip-table entries in the VTI. Each entry holds a
/// NUL-terminated `<name>.EVO` at a constant sub-offset, so every clip name
/// shares one residue modulo this stride — the signal used to isolate the table.
const VTI_CLIP_ENTRY_STRIDE: usize = 0x140;

/// Cap on clip-name hits collected from a VTI. A real clip table holds a few
/// dozen entries; this bounds the scan so a crafted VTI packed with millions of
/// `.EVO` tokens (up to the 64 MiB UDF read cap) can't burn CPU/memory.
const MAX_VTI_HITS: usize = 8192;

// Windows sampled per EVO, sectors per window, and EVOs sampled when judging whether an
// HD DVD's content is scrambled.
const SCRAMBLE_SAMPLE_WINDOWS: u64 = 2;
const SCRAMBLE_SAMPLE_SECTORS: u32 = 32;
const SCRAMBLE_SAMPLE_EVOS: usize = 3;

/// Whether an HD DVD's EVOs are AACS-scrambled, judged per pack (`[HD]` §4.3.2:
/// `PES_scrambling_control` is `01` on an encrypted pack). A rip keeps a renamed AACS
/// directory (`ANY!`, `AAC!`) over clear EVOs, so the directory alone proves nothing.
/// Samples windows of the largest EVOs; `None` when no window could be read.
pub(crate) fn hddvd_content_scrambled(
    reader: &mut dyn SectorSource,
    udf_fs: &udf::UdfFs,
) -> Option<bool> {
    let dir = udf_fs.find_dir("/HVDVD_TS")?;
    let mut evos: Vec<(&str, u64)> = dir
        .entries
        .iter()
        .filter(|e| !e.is_dir && e.name.to_ascii_lowercase().ends_with(HDDVD_CLIP_EXT))
        .map(|e| (e.name.as_str(), e.size))
        .collect();
    evos.sort_by_key(|e| std::cmp::Reverse(e.1));
    let mut read_any = false;
    for (name, _) in evos.into_iter().take(SCRAMBLE_SAMPLE_EVOS) {
        let Ok(exts) = udf_fs.file_extents(reader, &format!("/HVDVD_TS/{name}")) else {
            continue;
        };
        let total: u64 = exts.iter().map(|e| u64::from(e.1)).sum();
        for w in 1..=SCRAMBLE_SAMPLE_WINDOWS {
            let mut off = total * w / (SCRAMBLE_SAMPLE_WINDOWS + 1);
            let Some(&(lba, left)) = exts.iter().find(|e| {
                let hit = off < u64::from(e.1);
                if !hit {
                    off -= u64::from(e.1);
                }
                hit
            }) else {
                continue;
            };
            let n = (u64::from(left) - off).min(u64::from(SCRAMBLE_SAMPLE_SECTORS)) as u32;
            let mut buf = vec![0u8; n as usize * crate::consts::SECTOR_BYTES];
            let Ok(got) = reader.read_sectors(lba + off as u32, n as u16, &mut buf, false) else {
                continue;
            };
            read_any = true;
            let pack_scrambled = |p: &[u8; crate::consts::SECTOR_BYTES]| {
                crate::aacs::hddvd::classify(p) == crate::aacs::hddvd::PackKind::Scrambled
            };
            if buf[..got.min(buf.len())]
                .as_chunks::<{ crate::consts::SECTOR_BYTES }>()
                .0
                .iter()
                .any(pack_scrambled)
            {
                return Some(true);
            }
        }
    }
    read_any.then_some(false)
}

// Parses the ADVANCED-VTS VTI clip-name table (authored order): collects every NUL-terminated
// `*.EVO` name and keeps the largest group sharing one residue mod the stride (the clip table).
fn parse_vti_clip_order(vti: &[u8]) -> Vec<String> {
    if !vti.starts_with(HDDVD_VTI_MAGIC) {
        return Vec::new();
    }
    let is_name_byte = |b: u8| b.is_ascii_graphic();
    // Bucket hits by residue-mod-stride in a SINGLE pass — the clip table shares
    // one residue, so the largest bucket is it (avoids an O(stride*hits) rescan).
    let mut buckets: std::collections::HashMap<usize, Vec<(usize, String)>> =
        std::collections::HashMap::new();
    let mut count = 0usize;
    let mut i = 0usize;
    while i < vti.len() && count < MAX_VTI_HITS {
        if !is_name_byte(vti[i]) {
            i += 1;
            continue;
        }
        let start = i;
        while i < vti.len() && is_name_byte(vti[i]) {
            i += 1;
        }
        let name = &vti[start..i];
        let nul_terminated = i < vti.len() && vti[i] == 0;
        if nul_terminated && name.len() >= 5 && name[name.len() - 4..].eq_ignore_ascii_case(b".EVO")
        {
            buckets
                .entry(start % VTI_CLIP_ENTRY_STRIDE)
                .or_default()
                .push((start, String::from_utf8_lossy(name).into_owned()));
            count += 1;
        }
    }
    // Pick the largest residue bucket (the clip table). On a size tie, break by the
    // bucket's smallest offset, since `HashMap` iteration order is randomized and
    // `max_by_key` alone could pick a different bucket run-to-run on identical bytes.
    let Some(mut best) = buckets
        .into_values()
        .max_by_key(|g| (g.len(), std::cmp::Reverse(g.iter().map(|(o, _)| *o).min())))
    else {
        return Vec::new();
    };
    best.sort_by_key(|(o, _)| *o);
    best.into_iter().map(|(_, n)| n).collect()
}

// Whether a clip is a part of the split feature (case-insensitive): a name beginning `feature`
// (`FEATURE_1`/`_2`, `feature`/`feature_Divide`) or a primary-EVOB part `PEVOB_<n>`.
fn is_feature_clip(name: &str) -> bool {
    let base = name.rsplit_once('.').map(|(b, _)| b).unwrap_or(name);
    let lower = base.to_ascii_lowercase();
    lower.starts_with("feature")
        || lower
            .strip_prefix("pevob_")
            .is_some_and(|n| !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()))
}

// Sniffs a video codec from an MPEG-PS video ES sample by start code: MPEG-2 (`B3`), VC-1
// (`0F`), or H.264 (inferred from an SPS NAL, type 7).
fn sniff_video_codec(es: &[u8]) -> Option<Codec> {
    let mut saw_h264_sps = false;
    let mut i = 0usize;
    while i + 4 <= es.len() {
        if es[i] == 0x00 && es[i + 1] == 0x00 && es[i + 2] == 0x01 {
            let code = es[i + 3];
            match code {
                0xB3 => return Some(Codec::Mpeg2),
                0x0F => return Some(Codec::Vc1),
                // H.264 SPS: mask off nal_ref_idc (bits 6-5); keep the
                // forbidden_zero_bit (must be 0) + nal_unit_type (low 5 bits).
                // 0x07/0x27/0x47/0x67 all decode to a type-7 SPS.
                _ if (code & 0x9F) == 0x07 => saw_h264_sps = true,
                _ => {}
            }
            // Skip the whole consumed `00 00 01 <code>` marker (4 bytes) so the
            // code byte isn't re-read as the start of an overlapping start code.
            i += 4;
        } else {
            i += 1;
        }
    }
    saw_h264_sps.then_some(Codec::H264)
}

// Sniffs a `private_stream_1` sub-stream sample for the DD+ (E-AC-3) `0x0B77`
// syncword — the only audio codec recognized today (real HD-DVD titles use
// sub-ids `0xC0..=0xC7`). `None` on no match, so the caller drops the stream.
fn sniff_audio_codec(es: &[u8]) -> Option<Codec> {
    let has_sync = es.windows(2).any(|w| w[0] == 0x0B && w[1] == 0x77);
    has_sync.then_some(Codec::Ac3Plus)
}

// Demuxes an `.evo` clip head into one Stream per elementary stream (video + DD+ audio), codec
// sniffed from the ES bytes. Empty vec on unreadable/no stream; `Err` only on a cancelled
// probe.
fn probe_evo_streams(
    reader: &mut dyn SectorSource,
    extents: &[Extent],
    halt: Option<&crate::halt::Halt>,
) -> Result<Vec<Stream>> {
    let mut demux = PsDemuxer::new();
    let mut video: Vec<u8> = Vec::new();
    // Routing PID of the video track, from the first video PES seen: `DVD_VIDEO_PID`
    // for a plain 0xE0-0xEF stream, or `0xFD00 | stream_id_extension` for HD-DVD
    // extended-stream-id video. Kept in lockstep with `PsPacket::dvd_pid`.
    let mut video_pid: Option<u16> = None;
    // sub_id -> ES sample, ordered so audio tracks surface in sub-id order.
    let mut audio: BTreeMap<u8, Vec<u8>> = BTreeMap::new();

    let mut remaining = EVO_PROBE_SECTORS;
    let mut buf = vec![0u8; 512 * crate::consts::SECTOR_BYTES];
    'outer: for ext in extents {
        let mut lba = ext.start_lba;
        let mut left = ext.sector_count;
        while left > 0 && remaining > 0 {
            // 1 MiB chunks (512 sectors). Poll every chunk, not just per-clip:
            // a 16 MiB probe can sit in the drive's retry path on marginal
            // media, and per-clip granularity alone would leave a Stop waiting.
            if halt.is_some_and(|h| h.is_cancelled()) {
                return Err(crate::error::Error::Halted);
            }
            let n = left.min(remaining).min(512) as u16;
            let chunk = &mut buf[..n as usize * crate::consts::SECTOR_BYTES];
            match reader.read_sectors(lba, n, chunk, false) {
                Ok(_) => {}
                // A live-drive Stop surfaces HERE, via `Drive::checked_exec`
                // failing with `Halted`. Swallowing it like other read errors
                // would report an un-probed clip as probed.
                Err(crate::error::Error::Halted) => {
                    return Err(crate::error::Error::Halted);
                }
                Err(e) => {
                    tracing::warn!(
                        target: "freemkv::disc",
                        lba,
                        code = e.code(),
                        "evo probe read failed; title keeps the streams found so far"
                    );
                    break 'outer;
                }
            }
            for pkt in demux.feed(chunk) {
                collect_es(&pkt, &mut video, &mut video_pid, &mut audio);
            }
            lba += n as u32;
            left -= n as u32;
            remaining -= n as u32;
        }
    }
    for pkt in demux.flush() {
        collect_es(&pkt, &mut video, &mut video_pid, &mut audio);
    }

    let mut streams = Vec::new();
    // Only emit video once the codec is actually identified from the sampled
    // head; guessing would tag a VC-1 (or still-encrypted) clip wrong and feed
    // it the wrong parser. A real clear clip always carries its header early.
    if let (Some(pid), Some(codec)) = (video_pid, sniff_video_codec(&video)) {
        streams.push(Stream::Video(VideoStream {
            pid,
            codec,
            // HD-DVD is HD (1080). The muxer reads the true coded dimensions
            // from the H.264/VC-1 bitstream; this is a coarse default only.
            resolution: Resolution::R1080p,
            frame_rate: FrameRate::F23_976,
            hdr: HdrFormat::Sdr,
            color_space: ColorSpace::Bt709,
            display_aspect: None,
            secondary: false,
            label: String::new(),
            measured_cicp: None,
        }));
    }
    for (sub, sample) in &audio {
        let Some(codec) = sniff_audio_codec(sample) else {
            continue;
        };
        let Some(pid) = dvd_audio_pid(*sub) else {
            continue;
        };
        streams.push(Stream::Audio(AudioStream {
            pid,
            codec,
            // DD+ main tracks are 5.1; E-AC-3 channel counts are not decoded at
            // scan time, so this is a default (a 2.0 track is over-stated as
            // 5.1 in the header — the compressed audio itself is unaffected).
            channels: AudioChannels::Surround51,
            language: String::new(),
            sample_rate: SampleRate::S48,
            secondary: false,
            purpose: crate::disc::LabelPurpose::Normal,
            label: String::new(),
        }));
    }
    Ok(streams)
}

/// Accumulate a demuxed PES packet's elementary-stream bytes into the video /
/// per-audio-sub-id sample buffers (bounded by [`EVO_ES_SAMPLE_CAP`]).
fn collect_es(
    pkt: &crate::mux::ps::PsPacket,
    video: &mut Vec<u8>,
    video_pid: &mut Option<u16>,
    audio: &mut BTreeMap<u8, Vec<u8>>,
) {
    use crate::consts::pes_stream_id::{PRIVATE_STREAM_1, VIDEO, VIDEO_MAX};
    const EXTENDED_STREAM_ID: u8 = 0xFD;
    // Main-video VC-1 rides extended-stream-id `0xFD` ext `0x55`. Ext `0x56` is the VC-1 sub
    // video (picture-in-picture), not routed; TrueHD rides private_stream_1, never `0xFD`.
    const VC1_STREAM_ID_EXT: u8 = 0x55;
    // Whether this packet is the VC-1 video sub-stream of the 0xFD extended id.
    let is_vc1_ext =
        pkt.stream_id == EXTENDED_STREAM_ID && pkt.sub_stream_id == Some(VC1_STREAM_ID_EXT);
    match pkt.stream_id {
        // Plain MPEG video (0xE0-0xEF), or the VC-1 sub-stream of the HD-DVD
        // extended-stream-id (0xFD). Both feed the single video ES sample; the
        // routing PID comes from `PsPacket::dvd_pid` so it matches the demuxer.
        VIDEO..=VIDEO_MAX => {
            if video_pid.is_none() {
                *video_pid = pkt.dvd_pid();
            }
            if video.len() < EVO_ES_SAMPLE_CAP {
                video.extend_from_slice(&pkt.data);
            }
        }
        EXTENDED_STREAM_ID if is_vc1_ext => {
            if video_pid.is_none() {
                *video_pid = pkt.dvd_pid();
            }
            if video.len() < EVO_ES_SAMPLE_CAP {
                video.extend_from_slice(&pkt.data);
            }
        }
        PRIVATE_STREAM_1 => {
            if let Some(sub) = pkt.sub_stream_id
                && (0xC0..=0xC7).contains(&sub)
            {
                let slot = audio.entry(sub).or_default();
                if slot.len() < EVO_ES_SAMPLE_CAP {
                    slot.extend_from_slice(&pkt.data);
                }
            }
        }
        _ => {}
    }
}

// ─── Advanced-Content playlist (XPL) ───
// `ADV_OBJ/VPLST000.XPL` is the real player playlist: `<Title>`s naming clips
// in order with in/out points, duration, name, chapters — parsed as real XML.

/// One clip reference inside an XPL `<Title>`: the resolved `.evo` name (lower
/// case) and the clip's placement on the title timeline, in seconds.
struct XplClip {
    evo: String,
    begin_secs: f64,
    end_secs: f64,
}

/// One `<Title>` from the XPL: number, display name, total duration, its clips
/// in playback order, and chapter start times (seconds).
struct XplTitle {
    number: u16,
    name: String,
    duration_secs: f64,
    clips: Vec<XplClip>,
    chapters: Vec<f64>,
}

/// Parse an `HH:MM:SS:FF` (or `MM:SS:FF`) title timecode, `FF` in `time_base`
/// frames/sec (the `<TitleSet timeBase>`), into seconds. `None` on a malformed field.
/// A 60fps timeBase is scaled by 1001/1000: measured, not from a spec (a real disc's
/// second feature clip starts at 2951.604 s where 60 exact puts it at 2948.667 s).
/// Other timeBases (24/30/50) are unverified on real media and taken at face value.
fn parse_timecode(s: &str, time_base: u32) -> Option<f64> {
    let n: Vec<u32> = s
        .split(':')
        .map(|p| p.trim().parse::<u32>())
        .collect::<std::result::Result<Vec<u32>, _>>()
        .ok()?;
    let tb = time_base.max(1) as f64;
    let (h, m, sec, f) = match n.as_slice() {
        [h, m, s, f] => (*h, *m, *s, *f),
        [m, s, f] => (0, *m, *s, *f),
        _ => return None,
    };
    let nominal = h as f64 * 3600.0 + m as f64 * 60.0 + sec as f64 + f as f64 / tb;
    Some(if time_base == 60 {
        nominal * 1.001
    } else {
        nominal
    })
}

/// `timeBase="60fps"` → 60. Defaults to 60 when unparseable.
fn parse_frame_rate(s: &str) -> u32 {
    let digits: String = s.chars().take_while(|c| c.is_ascii_digit()).collect();
    digits.parse().unwrap_or(60)
}

/// `<PrimaryAudioVideoClip src="file:///.../FEATURE_1.MAP">` → `feature_1.evo`:
/// take the basename, drop the extension, normalise to a lower-case `.evo` name
/// (the playlist references the `.MAP` sidecar; the A/V is the same-stem `.EVO`).
fn evo_from_src(src: &str) -> Option<String> {
    let base = src.rsplit(['/', '\\']).next().unwrap_or(src);
    let stem = base.rsplit_once('.').map(|(s, _)| s).unwrap_or(base);
    if stem.is_empty() {
        return None;
    }
    Some(format!("{}.evo", stem.to_ascii_lowercase()))
}

// Max XPL element nesting depth. A real VPLST000.XPL is ~6 deep; 32 gives a
// 5x margin for extra wrapper layers while staying trivially stack-safe.
const MAX_XPL_DEPTH: usize = 32;

// Amplification bounds: cap declared titles/clips/chapters against a crafted playlist turning a
// small file into huge probe/memory cost.
const MAX_XPL_TITLES: usize = 512;

// Max XPL file size. A real playlist is KBs; 1 MiB bounds the DOM roxmltree builds from
// disc-supplied XML.
const MAX_XPL_BYTES: usize = 1024 * 1024;

// Max `.evo` clips resolved per scan (each costs an ICB read and a probe).
const MAX_HDDVD_CLIPS: usize = 512;

const MAX_XPL_CLIPS_PER_TITLE: usize = 256;

const MAX_XPL_CHAPTERS_PER_TITLE: usize = 1024;

// Memoized `probe_evo_streams`, keyed on the resolved extent list so repeat probes of the same
// physical clip (via different names/titles) are one read.
#[derive(Default)]
struct EvoProbeCache {
    seen: std::collections::HashMap<Vec<(u32, u32)>, Vec<Stream>>,
}

impl EvoProbeCache {
    /// Streams for `extents` — probed on first sight, replayed from the memo
    /// afterwards.
    fn streams(
        &mut self,
        reader: &mut dyn SectorSource,
        extents: &[Extent],
        halt: Option<&crate::halt::Halt>,
    ) -> Result<Vec<Stream>> {
        // Key on the head the probe actually reads (first EVO_PROBE_SECTORS), so
        // titles sharing a head clip but differing later share one probe.
        let mut budget = EVO_PROBE_SECTORS;
        let mut key: Vec<(u32, u32)> = Vec::new();
        for e in extents {
            if budget == 0 {
                break;
            }
            let n = e.sector_count.min(budget);
            key.push((e.start_lba, n));
            budget -= n;
        }
        if let Some(hit) = self.seen.get(&key) {
            return Ok(hit.clone());
        }
        // A cancelled probe is not memoised: it never established what the
        // clip holds, so replaying it for the next title that shares these
        // extents would spread one Stop into a disc-wide "no streams" verdict.
        let streams = probe_evo_streams(reader, extents, halt)?;
        self.seen.insert(key, streams.clone());
        Ok(streams)
    }
}

// Rejects XML whose nesting exceeds MAX_XPL_DEPTH before it reaches the recursive-descent
// parser (unbounded nesting can stack-overflow-abort a process, which no Err/catch_unwind can
// contain).
fn xpl_depth_within_limit(text: &str) -> bool {
    let b = text.as_bytes();
    let mut i = 0usize;
    let mut depth = 0usize;
    // Index just past `needle`, or the end of input when unterminated.
    let after = |from: usize, needle: &[u8]| -> usize {
        b[from..]
            .windows(needle.len())
            .position(|w| w == needle)
            .map_or(b.len(), |p| from + p + needle.len())
    };
    while i < b.len() {
        if b[i] != b'<' {
            i += 1;
            continue;
        }
        match b.get(i + 1) {
            Some(b'/') => {
                depth = depth.saturating_sub(1);
                i = after(i, b">");
            }
            Some(b'?') => i = after(i, b"?>"),
            Some(b'!') if b[i..].starts_with(b"<!--") => i = after(i, b"-->"),
            Some(b'!') if b[i..].starts_with(b"<![CDATA[") => i = after(i, b"]]>"),
            // `<!DOCTYPE …>` and friends. An internal subset is not tracked;
            // `allow_dtd` is off by default, so such a document is rejected by
            // the parser anyway.
            Some(b'!') => i = after(i, b">"),
            _ => {
                // Element start tag: walk to its `>`, ignoring `>` inside
                // quoted attribute values, and note whether it self-closes.
                let mut j = i + 1;
                let mut quote = 0u8;
                let mut prev = 0u8;
                while j < b.len() {
                    let c = b[j];
                    if quote != 0 {
                        if c == quote {
                            quote = 0;
                        }
                    } else if c == b'"' || c == b'\'' {
                        quote = c;
                    } else if c == b'>' {
                        break;
                    }
                    prev = c;
                    j += 1;
                }
                if prev != b'/' {
                    depth += 1;
                    if depth > MAX_XPL_DEPTH {
                        return false;
                    }
                }
                i = j + 1;
            }
        }
    }
    true
}

// `[HD]` §4.4.2: the 283-byte header of an AACS-encapsulated Advanced Resource File, and its
// FILE_ID. Bytes 7..11 hold the Resource Data size Nfs (Tables 4-12 to 4-14).
const ARF_HEADER_LEN: usize = 283;
const ARF_FILE_ID: &[u8; 4] = b"AACS";

// The Resource Data of an AACS-encapsulated ARF (a genuine disc's playlist), or the bytes
// themselves when bare (a rip strips the wrapper). Hash (`12h`), MAC (`02h`) and Non-Protected
// (`21h`) formats carry it in the clear after the header; `None` for the encrypted ones.
fn arf_resource(bytes: &[u8]) -> Option<&[u8]> {
    if bytes.len() < ARF_HEADER_LEN || !bytes.starts_with(ARF_FILE_ID) {
        return Some(bytes);
    }
    match bytes[4] {
        0x02 | 0x12 | 0x21 => {
            let nfs = u32::from_be_bytes([bytes[7], bytes[8], bytes[9], bytes[10]]) as usize;
            let end = ARF_HEADER_LEN.saturating_add(nfs).min(bytes.len());
            Some(&bytes[ARF_HEADER_LEN..end])
        }
        _ => None,
    }
}

// Parses the Advanced-Content playlist into its titles, matching elements by
// LOCAL name (default `HDDVDVideo/Playlist` namespace). Empty for a
// non-XML/non-playlist blob or one over MAX_XPL_DEPTH (falls back to clip-name).
fn parse_xpl_titles(xpl: &[u8]) -> Vec<XplTitle> {
    let Some(xpl) = arf_resource(xpl) else {
        tracing::warn!(target: "freemkv::disc", "playlist is an encrypted AACS ARF: not parsed");
        return Vec::new();
    };
    let text = String::from_utf8_lossy(xpl);
    if !xpl_depth_within_limit(&text) {
        return Vec::new();
    }
    let Ok(doc) = roxmltree::Document::parse(&text) else {
        return Vec::new();
    };
    let local = |n: &roxmltree::Node, name: &str| n.tag_name().name() == name;

    // Title timecodes count frames in <TitleSet timeBase>; tickBase is the markup
    // clock and only a fallback when timeBase is absent (default 60fps).
    let title_set = doc.descendants().find(|n| local(n, "TitleSet"));
    let time_base = title_set
        .and_then(|n| n.attribute("timeBase").or_else(|| n.attribute("tickBase")))
        .map(parse_frame_rate)
        .unwrap_or(60);

    let mut titles = Vec::new();
    for tnode in doc
        .descendants()
        .filter(|n| local(n, "Title"))
        .take(MAX_XPL_TITLES)
    {
        let number = tnode
            .attribute("titleNumber")
            .and_then(|s| s.trim().parse::<u16>().ok())
            .unwrap_or(0);
        let name = tnode
            .attribute("displayName")
            .or_else(|| tnode.attribute("id"))
            .unwrap_or("")
            .to_string();
        let duration_secs = tnode
            .attribute("titleDuration")
            .and_then(|s| parse_timecode(s, time_base))
            .unwrap_or(0.0);

        let mut clips = Vec::new();
        for c in tnode
            .descendants()
            .filter(|n| local(n, "PrimaryAudioVideoClip"))
        {
            // Bounded by MAX_XPL_CLIPS_PER_TITLE. `descendants()`, not
            // `children()`, is needed to reach wrapped clips, but that also
            // means nested `<Title>`s multiply hits, so `break` bounds the walk.
            if clips.len() >= MAX_XPL_CLIPS_PER_TITLE {
                break;
            }
            let Some(evo) = c.attribute("src").and_then(evo_from_src) else {
                continue;
            };
            let begin_secs = c
                .attribute("titleTimeBegin")
                .and_then(|s| parse_timecode(s, time_base))
                .unwrap_or(0.0);
            let end_secs = c
                .attribute("titleTimeEnd")
                .and_then(|s| parse_timecode(s, time_base))
                .unwrap_or(begin_secs);
            clips.push(XplClip {
                evo,
                begin_secs,
                end_secs,
            });
        }
        if clips.is_empty() {
            continue;
        }

        // Bounded by MAX_XPL_CHAPTERS_PER_TITLE — the same `descendants()`
        // amplification as the clip loop above, on the same crafted playlist.
        let chapters = tnode
            .descendants()
            .filter(|n| local(n, "Chapter"))
            .filter_map(|ch| {
                ch.attribute("titleTimeBegin")
                    .and_then(|s| parse_timecode(s, time_base))
            })
            .take(MAX_XPL_CHAPTERS_PER_TITLE)
            .collect();

        titles.push(XplTitle {
            number,
            name,
            duration_secs,
            clips,
            chapters,
        });
    }
    titles
}

// Reads `path`; `Ok(None)` when it exceeds MAX_XPL_BYTES (no over-read).
fn read_xpl_capped(
    reader: &mut dyn SectorSource,
    udf_fs: &udf::UdfFs,
    path: &str,
) -> Result<Option<Vec<u8>>> {
    let bytes = udf_fs.read_file_prefix(reader, path, MAX_XPL_BYTES + 1)?;
    Ok((bytes.len() <= MAX_XPL_BYTES).then_some(bytes))
}

/// Read the Advanced-Content playlist `ADV_OBJ/VPLST*.XPL`, if present. `Err` only on
/// `Halted`; an unreadable or oversized playlist is logged and reads as absent.
fn read_adv_obj_xpl(reader: &mut dyn SectorSource, udf_fs: &udf::UdfFs) -> Result<Option<Vec<u8>>> {
    let Some(dir) = udf_fs.find_dir("/ADV_OBJ") else {
        return Ok(None);
    };
    let Some(name) = dir.entries.iter().find_map(|e| {
        let lower = e.name.to_ascii_lowercase();
        (!e.is_dir && lower.starts_with("vplst") && lower.ends_with(".xpl")).then(|| e.name.clone())
    }) else {
        return Ok(None);
    };
    // The name came from the directory listing, so a failure is a real error.
    match read_xpl_capped(reader, udf_fs, &format!("/ADV_OBJ/{name}")) {
        Ok(Some(b)) => Ok(Some(b)),
        Ok(None) => {
            tracing::warn!(
                target: "freemkv::disc",
                xpl = ?name,
                code = crate::error::E_XPL_TOO_LARGE,
                "playlist exceeds the parser cap; falling back to the per-clip heuristic"
            );
            Ok(None)
        }
        Err(crate::error::Error::Halted) => Err(crate::error::Error::Halted),
        Err(e) => {
            tracing::warn!(
                target: "freemkv::disc",
                xpl = ?name,
                code = e.code(),
                "playlist unreadable; falling back to the per-clip heuristic"
            );
            Ok(None)
        }
    }
}

// Filters a file's (lba, sectors) list to readable extents. The flag is set when a non-empty
// extent at lba 0 was dropped, so the plan is short of the declared size.
fn usable_extents(file_exts: &[(u32, u32)]) -> (Vec<Extent>, bool) {
    let mut truncated = false;
    let mut extents = Vec::new();
    for &(lba, sectors) in file_exts {
        if sectors > 0 && lba == 0 {
            truncated = true;
        }
        if sectors > 0 && lba > 0 {
            extents.push(Extent {
                start_lba: lba,
                sector_count: sectors,
            });
        }
    }
    (extents, truncated)
}

// Composes DiscTitles from parsed XPL titles: resolves clips to extents,
// concatenates in playback order, carries title-time in/out points (45 kHz
// ticks) onto each Clip, and attaches duration/name/chapters.
fn compose_xpl_titles(
    reader: &mut dyn SectorSource,
    xpl_titles: &[XplTitle],
    clip_extents: &BTreeMap<String, (String, u64, Vec<Extent>)>,
    unusable: &std::collections::HashSet<String>,
    halt: Option<&crate::halt::Halt>,
) -> Result<Vec<DiscTitle>> {
    let mut titles = Vec::new();
    // One memo for the whole playlist: a playlist legitimately carries several
    // titles over the same clip (angles, a branch, a seamless-join variant), and
    // a crafted one can name a single `.evo` from all MAX_XPL_TITLES titles.
    let mut probes = EvoProbeCache::default();
    for t in xpl_titles {
        // A clip with no truthful read plan poisons every title naming it:
        // composing around it emits a title short by that clip's bytes while
        // its durations and chapter offsets still assume them.
        if t.clips.iter().any(|c| unusable.contains(&c.evo)) {
            continue;
        }
        // A clip absent from the disc (or dropped by MAX_HDDVD_CLIPS) is equally
        // unrenderable: drop the title rather than emit it short.
        if t.clips.iter().any(|c| !clip_extents.contains_key(&c.evo)) {
            continue;
        }
        let mut extents = Vec::new();
        let mut size_bytes = 0u64;
        let mut parts = Vec::new();
        // A crafted clip list can name the same `.evo` any number of times.
        // Mirrors bluray.rs's `seen_clips.insert(...)` gate: push a clip's
        // extents/size only the first time its `.evo` key is seen.
        let mut seen_evos: std::collections::HashSet<&str> = std::collections::HashSet::new();
        for c in &t.clips {
            let Some((orig, size, exts)) = clip_extents.get(&c.evo) else {
                continue;
            };
            if seen_evos.insert(c.evo.as_str()) {
                extents.extend_from_slice(exts);
                size_bytes = size_bytes.saturating_add(*size);
            }
            parts.push(Clip {
                feed_span: None,
                clip_id: orig
                    .rsplit_once('.')
                    .map(|(b, _)| b)
                    .unwrap_or(orig)
                    .to_string(),
                in_time: (c.begin_secs * 45000.0).clamp(0.0, u32::MAX as f64) as u32,
                out_time: (c.end_secs * 45000.0).clamp(0.0, u32::MAX as f64) as u32,
                duration_secs: (c.end_secs - c.begin_secs).max(0.0),
                source_packets: 0,
            });
        }
        if parts.is_empty() {
            continue;
        }
        let streams = probes.streams(reader, &extents, halt)?;
        let chapters = t
            .chapters
            .iter()
            .enumerate()
            .map(|(i, &ts)| Chapter {
                time_secs: ts.max(0.0),
                name: super::chapter_name(i),
            })
            .collect();
        titles.push(DiscTitle {
            // Language-neutral identifier (no user-facing English in the library):
            // matches the UDF `TITLE_*` volume-label style. Apps localize display.
            playlist: if t.name.is_empty() {
                format!("TITLE_{}", t.number)
            } else {
                t.name.clone()
            },
            playlist_id: t.number,
            duration_secs: t.duration_secs,
            size_bytes,
            clips: parts,
            streams,
            chapters,
            extents,
            content_format: ContentFormat::MpegPs,
            codec_privates: Vec::new(),
        });
    }
    Ok(titles)
}

impl Disc {
    // Scans HD-DVD titles from HVDVD_TS/.evo clips, joining feature clips via the VTI (or one
    // title per clip if unparseable). Halted is the only Err this returns (cancellation),
    // everything else best-effort.
    pub(super) fn scan_hddvd_titles(
        reader: &mut dyn SectorSource,
        udf_fs: &udf::UdfFs,
        halt: Option<&crate::halt::Halt>,
    ) -> Result<Vec<DiscTitle>> {
        let Some(ts_dir) = udf_fs.find_dir("/HVDVD_TS") else {
            return Ok(Vec::new());
        };
        // Snapshot clips (name, size) and the VTI navigation file. The `ts_dir`
        // borrow must end before the `udf_fs` reads below re-borrow it.
        let mut clips: Vec<(String, u64)> = Vec::new();
        let mut vti_name: Option<String> = None;
        for e in &ts_dir.entries {
            if e.is_dir {
                continue;
            }
            let lower = e.name.to_ascii_lowercase();
            if lower.ends_with(HDDVD_CLIP_EXT) {
                // Bounded by MAX_HDDVD_CLIPS. Surplus entries are DROPPED, not
                // `break`ed on, so a crafted disc can't hide the `.vti` behind
                // a wall of `.evo` names (each clip costs an ICB read + probe).
                if clips.len() < MAX_HDDVD_CLIPS {
                    clips.push((e.name.clone(), e.size));
                }
            } else if lower.ends_with(".vti") && vti_name.is_none() {
                vti_name = Some(e.name.clone());
            }
        }

        // Authored clip order from the VTI clip table (empty if no VTI). The
        // name came from `ts_dir.entries`, so a failed read is a real I/O
        // error, not an absent file — log it rather than silently falling back.
        let order: Vec<String> = match vti_name {
            None => Vec::new(),
            Some(n) => match udf_fs.read_file(reader, &format!("/HVDVD_TS/{n}")) {
                Ok(bytes) => parse_vti_clip_order(&bytes),
                Err(e) => {
                    tracing::warn!(
                        target: "freemkv::disc",
                        vti = ?n,
                        code = e.code(),
                        "authored clip order unreadable; falling back to the per-clip heuristic"
                    );
                    Vec::new()
                }
            },
        };

        // Resolve each clip's physical extents once, keyed by lower-case name.
        let mut clip_extents: BTreeMap<String, (String, u64, Vec<Extent>)> = BTreeMap::new();
        // Clips that EXIST but have no truthful read plan (unrecorded extent,
        // see `UdfFs::file_extents`). Composing around one silently shorts a
        // title's runtime, so any title naming one is dropped instead.
        let mut unusable: std::collections::HashSet<String> = std::collections::HashSet::new();
        for (name, size) in &clips {
            if halt.is_some_and(|h| h.is_cancelled()) {
                return Err(crate::error::Error::Halted);
            }
            let mut extents = Vec::new();
            let mut truncated = false;
            match udf_fs.file_extents(reader, &format!("/HVDVD_TS/{name}")) {
                Ok(file_exts) => {
                    (extents, truncated) = usable_extents(&file_exts);
                }
                Err(crate::error::Error::UdfUnrecordedExtent { .. }) => {
                    // Marking unusable drops every title naming this clip;
                    // silently composing without it would present half a movie
                    // as a whole one. Named constant, not the old literal 6017.
                    tracing::warn!(
                        target: "freemkv::disc",
                        clip = ?name,
                        code = crate::error::E_UDF_UNRECORDED_EXTENT,
                        "clip carries an unrecorded extent; dropping every title that names it"
                    );
                    unusable.insert(name.to_ascii_lowercase());
                }
                Err(crate::error::Error::Halted) => return Err(crate::error::Error::Halted),
                // EVERY other failure (scratched ICB, dead AD chain, embedded
                // data, ...) means no truthful read plan; used to fall through
                // a bare `Err(_) => {}`, unmarked and unlogged.
                Err(e) => {
                    tracing::warn!(
                        target: "freemkv::disc",
                        clip = ?name,
                        code = e.code(),
                        "clip extents could not be resolved; dropping every title that names it"
                    );
                    unusable.insert(name.to_ascii_lowercase());
                }
            }
            // `Ok` with NO usable extent (empty/filtered AD list) is unusable
            // too: else a zero-byte `FEATURE_2.EVO` beside a healthy part one
            // silently composed a whole-runtime title missing half the movie.
            if extents.is_empty() || truncated {
                if unusable.insert(name.to_ascii_lowercase()) {
                    // Not the neighbouring 6017: an empty AD list is not an
                    // unrecorded extent, and flattening the two would account a
                    // zero-byte file as an authoring hole.
                    tracing::warn!(
                        target: "freemkv::disc",
                        clip = ?name,
                        code = crate::error::E_UDF_NO_USABLE_EXTENT,
                        "clip resolved to no usable extent; dropping every title that names it"
                    );
                }
            } else {
                clip_extents.insert(name.to_ascii_lowercase(), (name.clone(), *size, extents));
            }
        }

        // Authoritative composition from `ADV_OBJ/VPLST*.XPL`, if present and
        // parseable. The clip-name heuristic below is the fallback when it's
        // absent, unparseable, or resolves to no on-disc clips.
        if let Some(xpl) = read_adv_obj_xpl(reader, udf_fs)? {
            let composed = compose_xpl_titles(
                reader,
                &parse_xpl_titles(&xpl),
                &clip_extents,
                &unusable,
                halt,
            )?;
            if !composed.is_empty() {
                return Ok(composed);
            }
        }

        // Feature clips, in authored order, that resolved to extents. If ANY
        // authored feature part has no truthful read plan, emit no composed
        // feature at all rather than one silently short that part's bytes.
        let feature: Vec<String> = if order
            .iter()
            .filter(|n| is_feature_clip(n))
            .any(|n| unusable.contains(&n.to_ascii_lowercase()))
        {
            Vec::new()
        } else {
            order
                .iter()
                .filter(|n| is_feature_clip(n))
                .filter(|n| clip_extents.contains_key(&n.to_ascii_lowercase()))
                .cloned()
                .collect()
        };
        let feature_set: std::collections::HashSet<String> =
            feature.iter().map(|n| n.to_ascii_lowercase()).collect();

        let mut titles = Vec::new();
        let mut next_id = 0u16;
        // One memo across the composed feature title and every per-clip title:
        // nothing de-duplicates a FID's ICB LBA, so any number of directory
        // entries can name ONE File Entry and resolve to identical extents.
        let mut probes = EvoProbeCache::default();

        // The composed feature title: concatenate its parts' extents in authored
        // order. Streams are probed from the head (the first part). One `Clip` per
        // part records the composition.
        if !feature.is_empty() {
            let mut extents = Vec::new();
            let mut size_bytes = 0u64;
            let mut parts = Vec::new();
            for n in &feature {
                if let Some((orig, size, exts)) = clip_extents.get(&n.to_ascii_lowercase()) {
                    extents.extend_from_slice(exts);
                    size_bytes = size_bytes.saturating_add(*size);
                    parts.push(Clip {
                        feed_span: None,
                        clip_id: orig
                            .rsplit_once('.')
                            .map(|(b, _)| b)
                            .unwrap_or(orig)
                            .to_string(),
                        in_time: 0,
                        out_time: 0,
                        duration_secs: 0.0,
                        source_packets: 0,
                    });
                }
            }
            let streams = probes.streams(reader, &extents, halt)?;
            titles.push(DiscTitle {
                playlist: "FEATURE".to_string(),
                playlist_id: next_id,
                duration_secs: 0.0,
                size_bytes,
                clips: parts,
                streams,
                chapters: Vec::new(),
                extents,
                content_format: ContentFormat::MpegPs,
                codec_privates: Vec::new(),
            });
            next_id = next_id.saturating_add(1);
        }

        // Every remaining clip is its own title (unchanged behaviour). Iterated in
        // directory order; when there is no VTI/feature this emits ALL clips.
        for (name, _size) in &clips {
            if halt.is_some_and(|h| h.is_cancelled()) {
                return Err(crate::error::Error::Halted);
            }
            let key = name.to_ascii_lowercase();
            if feature_set.contains(&key) {
                continue;
            }
            let Some((orig, size, extents)) = clip_extents.get(&key) else {
                continue;
            };
            // Probe the clip head for its elementary streams so the mux path
            // builds a non-empty `pid_to_track` and actually routes packets.
            let streams = probes.streams(reader, extents, halt)?;
            let clip_id = orig
                .rsplit_once('.')
                .map(|(base, _)| base.to_string())
                .unwrap_or_else(|| orig.clone());
            titles.push(DiscTitle {
                playlist: orig.clone(),
                playlist_id: next_id,
                duration_secs: 0.0,
                size_bytes: *size,
                clips: vec![Clip {
                    feed_span: None,
                    clip_id,
                    in_time: 0,
                    out_time: 0,
                    duration_secs: 0.0,
                    source_packets: 0,
                }],
                streams,
                chapters: Vec::new(),
                extents: extents.clone(),
                content_format: ContentFormat::MpegPs,
                codec_privates: Vec::new(),
            });
            next_id = next_id.saturating_add(1);
        }
        Ok(titles)
    }
}

#[cfg(test)]
#[path = "hddvd_tests.rs"]
mod tests;
