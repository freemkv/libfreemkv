//! HD-DVD title scanning — `HVDVD_TS/` Enhanced-VOB (`.evo`) enumeration.
//!
//! HD-DVD is a **tree-level peer** of DVD and Blu-ray (not a stream variant like
//! FMTS): its content lives in `HVDVD_TS/` as `.evo` clips — Enhanced VOB, an
//! MPEG **program** stream — each with a small `.map` timemap sidecar, navigated
//! by `.xpl`/`.ifo` playlists in `HVDVD_TS/` and `ADV_OBJ/`. Because it is a
//! different tree with a different playlist format, it gets its OWN scanner
//! (this file), a peer to [`Disc::scan_bluray_titles`] — the two-format design
//! rule: a genuinely different format is a new enumerator, not an extension
//! bolted into the BD path.
//!
//! Title composition — authoritative, from the Advanced-Content playlist. HD-DVD
//! ships a real player playlist at `ADV_OBJ/VPLST000.XPL` (DVD-Forum
//! `HDDVDVideo/Playlist` XML). The scanner parses it (with a real XML parser,
//! `roxmltree`) into one [`DiscTitle`] per `<Title>`: its `<PrimaryAudioVideoClip>`
//! clips in playback order (each an EVO, referenced via its `.MAP` sidecar), the
//! `titleDuration`, the `displayName`, and the `<ChapterList>`. A layer-break
//! split (`FEATURE_1` + `FEATURE_2`, or `feature`/`feature_Divide`) is composed
//! into ONE title with the two parts as clips, carrying each clip's title-time
//! in/out points (45 kHz ticks) so a seamless join can be spliced onto one
//! timeline. Container is [`ContentFormat::MpegPs`], so the existing PS mux path
//! handles it. Per-clip streams are enumerated by demuxing the clip head and
//! building one [`Stream`] per distinct elementary stream (video + DD+ audio),
//! codec sniffed from the ES bytes.
//!
//! When no playlist is present (or it fails to parse), the scanner falls back to
//! the older clip-name heuristic: parse the `HVA*.VTI` clip table, join the
//! `feature*`-named clips into one title, and emit every other clip on its own.
//!
//! Not parsed yet: subtitles (8-bit RLC on `0xBD` sub `0x20..=0x3F`) and per-track
//! audio languages (the XPL carries `<Audio description=...>` but they are not yet
//! wired onto the streams). Extents and size are real (the ripper images the
//! clips).

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

/// HD-DVD Standard Content navigation file magic (`HVDVD_TS/HVA*.VTI`). The VTI
/// is the DVD-IFO analogue: it holds a fixed-stride clip table naming every
/// `.evo` in authored order.
const HDDVD_VTI_MAGIC: &[u8] = b"ADVANCED-VTS";

/// Byte stride between clip-table entries in the VTI. Each entry holds a
/// NUL-terminated `<name>.EVO` at a constant sub-offset, so every clip name
/// shares one residue modulo this stride — the signal used to isolate the table.
const VTI_CLIP_ENTRY_STRIDE: usize = 0x140;

/// Cap on clip-name hits collected from a VTI. A real clip table holds a few
/// dozen entries; this bounds the scan so a crafted VTI packed with millions of
/// `.EVO` tokens (up to the 64 MiB UDF read cap) can't burn CPU/memory.
const MAX_VTI_HITS: usize = 8192;

/// Parse the clip-name table from an `ADVANCED-VTS` VTI, returning clip
/// filenames in authored (table) order.
///
/// The table is a run of `VTI_CLIP_ENTRY_STRIDE`-spaced records, each carrying a
/// NUL-terminated `<name>.EVO`. Rather than trust the (imprecise) header pointer,
/// this collects every NUL-terminated `*.EVO` name and keeps the largest group
/// sharing one residue modulo the stride — the clip table — in offset order.
/// Returns empty for a non-VTI blob or one with no recognizable table.
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
    // Pick the largest residue bucket (the clip table). On a size tie, break
    // deterministically by the bucket's smallest offset — `HashMap` iteration
    // order is randomized, so `max_by_key` alone could pick a different bucket
    // run-to-run on identical bytes.
    let Some(mut best) = buckets
        .into_values()
        .max_by_key(|g| (g.len(), std::cmp::Reverse(g.iter().map(|(o, _)| *o).min())))
    else {
        return Vec::new();
    };
    best.sort_by_key(|(o, _)| *o);
    best.into_iter().map(|(_, n)| n).collect()
}

/// Whether a clip belongs to the main feature. HD-DVD Standard Content authors
/// the feature as one or more clips whose name begins `feature` (case-insensitive)
/// — `FEATURE_1`/`FEATURE_2` (a layer-break split) or `feature`/`feature_Divide`.
/// The feature is imaged as ONE title by concatenating these in authored order.
fn is_feature_clip(name: &str) -> bool {
    let base = name.rsplit_once('.').map(|(b, _)| b).unwrap_or(name);
    base.to_ascii_lowercase().starts_with("feature")
}

/// Sniff a video codec from a program-stream video elementary-stream sample by
/// its MPEG / Annex-B start codes:
///   * `00 00 01 B3` → MPEG-2 (sequence_header)
///   * `00 00 01 0F` → VC-1 (BD/HD-DVD sequence-header BDU)
///   * `00 00 01 [x7]` H.264 SPS NAL (type 7, forbidden_zero_bit clear) → H.264
///
/// Returns `None` when no recognizable start code is present. The scan prefers
/// the unambiguous MPEG-2 / VC-1 sequence headers; H.264 is inferred from an SPS
/// NAL so a stray slice/picture code can't be mistaken for a different codec.
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

/// Sniff an audio codec from a `private_stream_1` sub-stream sample. Today only
/// Dolby Digital Plus (E-AC-3) is recognized — its `0x0B77` syncword — which is
/// what ANCHORMAN / SHAUN carry on sub-ids `0xC0..=0xC7`. Returns `None` for an
/// unrecognized sample so the caller drops the stream rather than mislabeling it.
fn sniff_audio_codec(es: &[u8]) -> Option<Codec> {
    let has_sync = es.windows(2).any(|w| w[0] == 0x0B && w[1] == 0x77);
    has_sync.then_some(Codec::Ac3Plus)
}

/// Demux the head of an `.evo` clip (through the disc's [`SectorSource`]) and
/// build one [`Stream`] per distinct elementary stream found: the video track
/// (mapped to the canonical [`DVD_VIDEO_PID`]) and every DD+ audio sub-stream
/// (mapped via [`dvd_audio_pid`]). Codec is sniffed from the demuxed ES bytes.
///
/// Mirrors the stream construction in `Disc::scan_dvd_titles`; resolution /
/// language / channels use sane HD-DVD defaults (the muxer reads the true pixel
/// dimensions from the H.264 SPS, and E-AC-3 channel counts are not decoded
/// here). Returns an empty vec when the clip cannot be read or carries no
/// recognizable stream (e.g. an AACS-encrypted clip probed as ciphertext).
///
/// Cancellation is checked before every chunk read and is the ONE condition
/// that returns `Err` rather than an empty vec: a probe cut short by a Stop has
/// not established that the clip carries no streams, and reporting it as if it
/// had would enumerate a stream-less title as a scanned fact. A read that fails
/// for any other reason keeps the existing best-effort behaviour.
fn probe_evo_streams(
    reader: &mut dyn SectorSource,
    extents: &[Extent],
    halt: Option<&crate::halt::Halt>,
) -> Result<Vec<Stream>> {
    let mut demux = PsDemuxer::new();
    let mut video: Vec<u8> = Vec::new();
    // Routing PID of the video track, captured from the first video PES seen:
    // `DVD_VIDEO_PID` for a plain 0xE0-0xEF stream (Anchorman's H.264 on 0xE2),
    // or `0xFD00 | stream_id_extension` for an HD-DVD extended-stream-id video
    // (Shaun's VC-1 on 0xFD ext 0x55). Kept in lockstep with `PsPacket::dvd_pid`
    // so the emitted `Stream` PID matches what the demuxer routes at mux time.
    let mut video_pid: Option<u16> = None;
    // sub_id -> ES sample, ordered so audio tracks surface in sub-id order.
    let mut audio: BTreeMap<u8, Vec<u8>> = BTreeMap::new();

    let mut remaining = EVO_PROBE_SECTORS;
    'outer: for ext in extents {
        let mut lba = ext.start_lba;
        let mut left = ext.sector_count;
        while left > 0 && remaining > 0 {
            // 1 MiB read chunks (512 sectors) keep buffers small.
            // Poll before every chunk, as the PGS probe does: a clip's probe
            // budget is 16 MiB of blocking reads, and on marginal media each
            // one can sit in the drive's retry path, so per-clip granularity
            // alone would leave a Stop waiting on a dead disc.
            if halt.is_some_and(|h| h.is_cancelled()) {
                return Err(crate::error::Error::Halted);
            }
            let n = left.min(remaining).min(512) as u16;
            let mut buf = vec![0u8; n as usize * crate::consts::SECTOR_BYTES];
            match reader.read_sectors(lba, n, &mut buf, false) {
                Ok(_) => {}
                // A live-drive Stop surfaces HERE, not through the token:
                // `Drive::checked_exec` fails every command with `Halted` once
                // the flag is set. Swallowing it (as any other read error is
                // swallowed) reports an un-probed clip as a probed one.
                Err(crate::error::Error::Halted) => {
                    return Err(crate::error::Error::Halted);
                }
                Err(_) => break 'outer,
            }
            for pkt in demux.feed(&buf) {
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
    // Emit the video stream only when the codec was actually identified from the
    // sampled head. Guessing (e.g. defaulting to H.264) would tag a VC-1 — or a
    // still-encrypted — clip with the wrong codec, so the mux applies the wrong
    // parser and produces a corrupt track; dropping it is the honest outcome
    // (matches the audio path below), and a real clear clip always carries its
    // sequence header / SPS at the head, so this never fires on a normal disc.
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
    /// VC-1 video rides extended-stream-id `0xFD` with `stream_id_extension`
    /// `0x55`. HD audio (MLP/TrueHD) can also use `0xFD` with other extensions —
    /// routing those to their own audio tracks is deferred (see the HD-DVD
    /// program-chain follow-up); until then only the VC-1 extension is treated as
    /// video, so an audio `0xFD` sub-stream can never mis-stamp the video PID.
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

// ─────────────────────── Advanced-Content playlist (XPL) ──────────────────
//
// HD-DVD Advanced Content ships an authoritative playlist at
// `ADV_OBJ/VPLST000.XPL` (DVD-Forum `HDDVDVideo/Playlist` XML). It is the real
// player playlist: a `<TitleSet>` of `<Title>`s, each naming its
// `<PrimaryAudioVideoClip>` clips (an EVO, via its `.MAP` sidecar) in playback
// order with title-time in/out points, a `titleDuration`, a `displayName`, and a
// `<ChapterList>`. Parsing it gives authoritative title composition — clips,
// duration, name, chapters — instead of the `feature*` clip-name heuristic, plus
// the per-clip title-time offsets needed to splice a layer-break split
// (`FEATURE_1` + `FEATURE_2`) onto one continuous timeline. Parsed with a real
// XML parser (`roxmltree`), not a hand-rolled scanner — the XPL is genuine XML.

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

/// Parse an `HH:MM:SS:FF` (or `MM:SS:FF`) timecode at `tick_base` frames/sec into
/// seconds. `None` on a malformed field.
fn parse_timecode(s: &str, tick_base: u32) -> Option<f64> {
    let n: Vec<u32> = s
        .split(':')
        .map(|p| p.trim().parse::<u32>())
        .collect::<std::result::Result<Vec<u32>, _>>()
        .ok()?;
    let tb = tick_base.max(1) as f64;
    let (h, m, sec, f) = match n.as_slice() {
        [h, m, s, f] => (*h, *m, *s, *f),
        [m, s, f] => (0, *m, *s, *f),
        _ => return None,
    };
    Some(h as f64 * 3600.0 + m as f64 * 60.0 + sec as f64 + f as f64 / tb)
}

/// `tickBase="60fps"` → 60. Defaults to 60 when absent/unparseable.
fn parse_tick_base(s: &str) -> u32 {
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

/// Maximum element nesting depth accepted in an XPL. A real `VPLST000.XPL` is
/// about six levels deep at its deepest
/// (`Playlist`/`TitleSet`/`Title`/`PrimaryAudioVideoClip`/`Video`, or
/// `.../ChapterList/Chapter`); 32 leaves a 5x margin for authoring tools that
/// wrap extra `ApplicationSegment`/`ObjectMappingList` layers, while staying
/// trivially stack-safe.
const MAX_XPL_DEPTH: usize = 32;

/// Most `<Title>` elements one playlist may declare.
///
/// The depth guard above bounds NESTING; this bounds BREADTH, and they are
/// separate attacks. A flat playlist passes the depth check trivially and can
/// still declare a title per 51 bytes, so a 64 MiB `VPLST000.XPL` — exactly
/// `udf::MAX_FILE_BYTES` — reaches ~1.3 million of them.
///
/// That matters here and not on the Blu-ray side because of where the count
/// comes from: `bluray.rs` learns its playlists from the MPLS files actually
/// present in the UDF directory, so the medium bounds it. An XPL DECLARES its
/// titles, so nothing does.
///
/// The cost is not memory but drive time. `compose_xpl_titles` runs
/// `probe_evo_streams` once per title, and each probe reads real sectors — so
/// an uncapped count turns a `Disc::scan()` the operator expects to take
/// seconds into tens of terabytes of optical reads on a disc that is only
/// pretending to be large.
///
/// The bound that matters is `cap x EVO_PROBE_SECTORS`, since each title costs
/// one probe: `compose_xpl_titles` memoizes on the title's RESOLVED extent list,
/// and distinct titles built from different combinations of the same clips have
/// different lists, so each one misses the memo. At 4096 that product was 64 GiB
/// of optical reads — the same worst case, and the same contradiction with
/// "seconds", as the sibling [`MAX_HDDVD_CLIPS`] path.
///
/// 512 keeps it at 8 GiB and remains far above any real disc (retail HD-DVDs
/// carry tens of titles).
const MAX_XPL_TITLES: usize = 512;

/// Cap on `.evo` clips taken from the `HVDVD_TS/` directory listing.
///
/// [`MAX_XPL_TITLES`] bounds what a PLAYLIST may declare, and
/// [`xpl_depth_within_limit`] bounds how deeply it may nest — but both live on
/// the `ADV_OBJ/*.XPL` path, and a crafted disc simply OMITS `/ADV_OBJ`. Control
/// then reaches the clip-name fallback in [`Disc::scan_hddvd_titles`], which
/// emits one title per DIRECTORY ENTRY and probes each. That is the same
/// unbounded-probe cost, reached without the playlist.
///
/// The directory is the only bound the medium supplies, and `udf::MAX_DIR_BYTES`
/// (1 MiB) still leaves room for ~24,000 FIDs. Memoizing the probe
/// ([`EvoProbeCache`]) collapses entries that resolve to the SAME extents, which
/// closes the many-names-one-File-Entry case — but it cannot close this one: a
/// File Entry is a single sector, so an attacker can give every FID its own,
/// each declaring its own extent, for ~48 MiB of image. Every probe then misses
/// the memo and costs a full [`EVO_PROBE_SECTORS`] (16 MiB) read — hundreds of
/// GiB on a `Disc::scan()` the operator expects to take seconds.
///
/// The cap's job is therefore to bound `cap x EVO_PROBE_SECTORS`, and only that
/// product means anything. It was 4096 — chosen to match [`MAX_XPL_TITLES`],
/// which bounds a different path with no per-item read cost — and 4096 x 16 MiB
/// is 64 GiB of optical reads: more than dual-layer HD-DVD media physically
/// holds (reachable anyway, since distinct extent lists may overlap on the
/// medium) and tens of minutes to hours at drive speeds. That is not "a scan
/// the operator expects to take seconds"; the cap stopped literal
/// unboundedness while permitting a worst case the same doc ruled out.
///
/// 512 keeps the product at 8 GiB and still leaves a wide margin over any real
/// disc — retail HD-DVDs carry TENS of `.evo` clips, so this is ~10x the
/// real-world maximum, the same sizing convention
/// [`MAX_XPL_CHAPTERS_PER_TITLE`] uses against the 99-chapter authoring
/// ceiling. No genuine disc loses a title.
///
/// Pinned by `the_clip_cap_and_probe_budget_bound_a_scans_worst_case_read_volume`,
/// which asserts the PRODUCT rather than either constant, so raising one
/// without re-examining the other fails.
const MAX_HDDVD_CLIPS: usize = 512;

/// Most `<PrimaryAudioVideoClip>` elements one `<Title>` may contribute.
///
/// The fourth amplification axis on this function, and the one the other three
/// guards leave open. [`MAX_XPL_DEPTH`] bounds NESTING, [`MAX_XPL_TITLES`]
/// bounds how many titles a playlist DECLARES, and [`MAX_HDDVD_CLIPS`] bounds
/// the directory fallback — none of them bounds how many clips a SINGLE title
/// collects.
///
/// Two things multiply here. A 64 MiB XPL (exactly `udf::MAX_FILE_BYTES`) holds
/// ~1.7 million `<PrimaryAudioVideoClip src="A.EVO"/>` elements at ~38 bytes
/// each. And the collector below is `descendants()`, not `children()`, so a clip
/// nested inside N ancestor `<Title>` elements is collected by EVERY one of
/// them — and the depth cap still permits 32 such ancestors (measured: a bare
/// `<Title>` root nests 32 deep and still parses, because the guard counts the
/// self-closing clip element as non-nesting). The product is ~1.7M x 32 ≈ 56
/// million heap-allocated [`XplClip`]s, each with its own `String`, and then as
/// many [`Clip`]s again in [`compose_xpl_titles`].
///
/// Unlike the title cap, the cost here is MEMORY rather than drive time:
/// `compose_xpl_titles` de-duplicates by `.evo` name before probing
/// (`seen_evos`), so repeats cost no extra sector reads — but the `Vec`s are
/// built and held regardless.
///
/// 256 is far above any real disc. A retail HD-DVD title is a handful of clips —
/// a feature is one, or two across a layer break; even a seamless-branching or
/// multi-angle title is tens. It also keeps the aggregate bounded: at most
/// [`MAX_XPL_TITLES`] x 256 clips, rather than the tens of millions above.
const MAX_XPL_CLIPS_PER_TITLE: usize = 256;

/// Most `<Chapter>` elements one `<Title>` may contribute.
///
/// The same `descendants()` amplification as [`MAX_XPL_CLIPS_PER_TITLE`], on the
/// second unbounded `collect()` in the same loop, and bounded separately so that
/// capping clips does not simply move the attack next door. A chapter is a bare
/// `f64` rather than a `String`-carrying struct, so each one is cheaper — but a
/// `<Chapter titleTimeBegin="…"/>` element is also smaller, so the element count
/// an XPL can reach is comparable.
///
/// 1024 is far above any real disc: the DVD/HD-DVD authoring convention tops out
/// at 99 chapters per title, so this leaves a 10x margin.
const MAX_XPL_CHAPTERS_PER_TITLE: usize = 1024;

/// Memoized [`probe_evo_streams`], keyed on the RESOLVED extent list.
///
/// Probing is the expensive half of title composition: each pass reads up to
/// [`EVO_PROBE_SECTORS`] (16 MiB) off the medium. Both composition paths can
/// reach the same physical clip many times over — an XPL naming one `.evo` from
/// many `<Title>`s, or a directory whose FIDs all point at one File Entry — and
/// a probe is a pure function of the extents it reads, so the second and later
/// passes over an identical extent list are re-reads of bytes already seen.
///
/// The key is the extent list itself, NOT the clip name or title: two names for
/// one File Entry share a key (one probe), while two genuinely different clips
/// have different `start_lba`s and so keep their own probes and their own
/// streams. Cache size is bounded by the caller's title/clip cap
/// ([`MAX_XPL_TITLES`] / [`MAX_HDDVD_CLIPS`]).
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
        let key: Vec<(u32, u32)> = extents
            .iter()
            .map(|e| (e.start_lba, e.sector_count))
            .collect();
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

/// Reject a disc-supplied XML blob whose element nesting exceeds `MAX_XPL_DEPTH`,
/// BEFORE it reaches the parser. `roxmltree` is recursive-descent and its
/// depth-10 limit applies only to ENTITY expansion, so element nesting is
/// unbounded: a few hundred KB of well-formed XML — far under the read cap —
/// overflows the thread stack. That is a process ABORT, not an `Err` and not an
/// unwind, so neither the `Document::parse` fallback nor `catch_unwind` can
/// contain it; the only fix is to not hand the document over at all. A byte-size
/// cap is deliberately NOT added: file size is already bounded by the UDF read
/// path, and a size cap does not close this class (a 180 KB document already
/// aborts) — depth is the bound that matters.
///
/// This is a conservative scan, not a validator: it tracks `<name …>` /
/// `</name>` while skipping comments, CDATA, processing instructions and
/// declarations, and treats `<… />` as non-nesting. A miscount can only cost a
/// pathological document its fast path, and the failure mode is the existing
/// one — fall back to the clip-name heuristic.
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

/// Parse the Advanced-Content playlist into its titles. Elements are matched by
/// LOCAL name (the document is in the `HDDVDVideo/Playlist` default namespace).
/// Returns empty for a non-XML / non-playlist blob, or one nested deeper than
/// [`MAX_XPL_DEPTH`], so the caller falls back to the clip-name heuristic.
fn parse_xpl_titles(xpl: &[u8]) -> Vec<XplTitle> {
    let text = String::from_utf8_lossy(xpl);
    if !xpl_depth_within_limit(&text) {
        return Vec::new();
    }
    let Ok(doc) = roxmltree::Document::parse(&text) else {
        return Vec::new();
    };
    let local = |n: &roxmltree::Node, name: &str| n.tag_name().name() == name;

    // tickBase lives on <TitleSet> (default 60fps).
    let tick_base = doc
        .descendants()
        .find(|n| local(n, "TitleSet"))
        .and_then(|n| n.attribute("tickBase"))
        .map(parse_tick_base)
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
            .and_then(|s| parse_timecode(s, tick_base))
            .unwrap_or(0.0);

        let mut clips = Vec::new();
        for c in tnode
            .descendants()
            .filter(|n| local(n, "PrimaryAudioVideoClip"))
        {
            // Bounded by MAX_XPL_CLIPS_PER_TITLE. `descendants()` is deliberate
            // — a real playlist wraps its clips in `<PrimaryAudioVideoClipList>`
            // or an `ApplicationSegment`, so `children()` would miss them — but
            // it also means a clip nested inside N ancestor `<Title>`s is
            // collected N times over, and the depth guard still allows 32 such
            // ancestors. `break` rather than dropping surplus: unlike the
            // directory scan (which must keep reading to find the `.vti`),
            // nothing later in THIS loop is needed, and stopping bounds the walk
            // as well as the allocation. Chapters are collected by a separate
            // iterator below, so they are unaffected.
            if clips.len() >= MAX_XPL_CLIPS_PER_TITLE {
                break;
            }
            let Some(evo) = c.attribute("src").and_then(evo_from_src) else {
                continue;
            };
            let begin_secs = c
                .attribute("titleTimeBegin")
                .and_then(|s| parse_timecode(s, tick_base))
                .unwrap_or(0.0);
            let end_secs = c
                .attribute("titleTimeEnd")
                .and_then(|s| parse_timecode(s, tick_base))
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
                    .and_then(|s| parse_timecode(s, tick_base))
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

/// Read the Advanced-Content playlist `ADV_OBJ/VPLST*.XPL`, if present.
fn read_adv_obj_xpl(reader: &mut dyn SectorSource, udf_fs: &udf::UdfFs) -> Option<Vec<u8>> {
    let dir = udf_fs.find_dir("/ADV_OBJ")?;
    let name = dir.entries.iter().find_map(|e| {
        let lower = e.name.to_ascii_lowercase();
        (!e.is_dir && lower.starts_with("vplst") && lower.ends_with(".xpl")).then(|| e.name.clone())
    })?;
    udf_fs.read_file(reader, &format!("/ADV_OBJ/{name}")).ok()
}

/// Compose [`DiscTitle`]s from parsed XPL titles: resolve each title's clips to
/// physical extents, concatenate them in playback order, carry the title-time
/// in/out points onto each [`Clip`] (45 kHz ticks — the offset that splices a
/// layer-break split onto one timeline), and attach the duration, name, and
/// chapters. A title whose clips resolve to no on-disc extents is skipped.
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
        // A clip that exists but has no truthful read plan poisons every
        // title that names it: composing around it would emit a title short
        // by that clip's bytes while its durations and chapter offsets still
        // assume them.
        if t.clips.iter().any(|c| unusable.contains(&c.evo)) {
            continue;
        }
        let mut extents = Vec::new();
        let mut size_bytes = 0u64;
        let mut parts = Vec::new();
        // A crafted `<PrimaryAudioVideoClip>` list can name the same `.evo`
        // any number of times (XML has no fixed element count, unlike MPLS's
        // binary PlayItem count). Mirrors bluray.rs's
        // `first_ref = seen_clips.insert(...)` gate (`:113`/`:117`): push a
        // clip's physical extents and size only the first time its key is
        // seen, so a clip named N times contributes its extent list ONCE
        // instead of N times over. The analogous key here is the `.evo`
        // filename — this format has no clip_id, and `.evo` is what
        // `clip_extents` is already keyed by.
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
    /// Scan HD-DVD titles from the `HVDVD_TS/` `.evo` clips.
    ///
    /// The main feature is authored as one or more `.evo` clips (a layer-break
    /// split — `FEATURE_1`/`FEATURE_2` or `feature`/`feature_Divide`). The
    /// `HVA*.VTI` navigation file names every clip in authored order; this parses
    /// it to concatenate the feature clips into ONE title (so the largest-title
    /// pick gets the whole movie, not just part 1), emitting every other clip as
    /// its own title. Falls back to one title per clip when the VTI is absent or
    /// unparseable, so a disc with no readable navigation still enumerates.
    /// `chapters`/duration are left empty pending deeper VTI parsing.
    /// Cancellation: `halt` is polled once per clip and again before every
    /// probe chunk, and a read that fails with [`Error::Halted`] — how a live
    /// drive reports a Stop, since `Drive::checked_exec` fails every command
    /// once its flag is set — is propagated rather than swallowed. `Halted` is
    /// the only error this returns; every other read failure keeps its
    /// best-effort behaviour. It has to be an error and not a short title
    /// list: a cancelled enumeration that returned `Ok` would be
    /// indistinguishable from a disc that genuinely holds fewer titles, and
    /// the caller would cache and act on it.
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
                // Bounded by MAX_HDDVD_CLIPS: each clip taken here costs an ICB
                // read to resolve its extents and (in the fallback path below) a
                // full probe_evo_streams pass. Surplus entries are DROPPED rather
                // than `break`ing the loop, so a crafted disc cannot hide the
                // `.vti` navigation file behind a wall of `.evo` names.
                if clips.len() < MAX_HDDVD_CLIPS {
                    clips.push((e.name.clone(), e.size));
                }
            } else if lower.ends_with(".vti") && vti_name.is_none() {
                vti_name = Some(e.name.clone());
            }
        }

        // Authored clip order from the VTI clip table (empty if no VTI).
        let order: Vec<String> = vti_name
            .and_then(|n| udf_fs.read_file(reader, &format!("/HVDVD_TS/{n}")).ok())
            .map(|b| parse_vti_clip_order(&b))
            .unwrap_or_default();

        // Resolve each clip's physical extents once, keyed by lower-case name.
        let mut clip_extents: BTreeMap<String, (String, u64, Vec<Extent>)> = BTreeMap::new();
        // Clips that EXIST but cannot be given a truthful read plan, because
        // the file carries an unrecorded (never-written) extent — see
        // `UdfFs::file_extents`. Kept apart from "did not resolve": a title
        // built without one of these is short by that clip's runtime while
        // still claiming its duration and size, which is data loss shaped
        // like an ordinary rip. Any title naming one is dropped instead.
        let mut unusable: std::collections::HashSet<String> = std::collections::HashSet::new();
        for (name, size) in &clips {
            if halt.is_some_and(|h| h.is_cancelled()) {
                return Err(crate::error::Error::Halted);
            }
            let mut extents = Vec::new();
            match udf_fs.file_extents(reader, &format!("/HVDVD_TS/{name}")) {
                Ok(file_exts) => {
                    for (lba, sectors) in file_exts {
                        if sectors > 0 && lba > 0 {
                            extents.push(Extent {
                                start_lba: lba,
                                sector_count: sectors,
                            });
                        }
                    }
                }
                Err(crate::error::Error::UdfUnrecordedExtent { .. }) => {
                    // Say so. Marking the clip unusable drops every title that
                    // names it, and in the fallback case the OTHER part of a
                    // split feature is still offered standalone — so half a
                    // movie is presented as a whole one. Silently returning Ok
                    // with the feature missing is the shape of data loss this
                    // refusal exists to prevent, pointed the other way.
                    //
                    // The Blu-ray half of this same fix already warns; this
                    // file had no diagnostics at all. Logging is exempt from
                    // the crate's no-English rule (errors stay numeric: E6017).
                    tracing::warn!(
                        target: "freemkv::disc",
                        clip = ?name,
                        code = 6017,
                        "clip carries an unrecorded extent; dropping every title that names it"
                    );
                    unusable.insert(name.to_ascii_lowercase());
                }
                Err(crate::error::Error::Halted) => return Err(crate::error::Error::Halted),
                // EVERY other failure means the same thing: no truthful read
                // plan for this clip. A scratched sector under its ICB
                // (DiscRead), an allocation-descriptor chain that never
                // terminated (UdfAdChainTooLong), a file whose data is
                // embedded rather than extent-mapped (UdfEmbeddedData) — all
                // of them used to land in a bare `Err(_) => {}`: no log, and
                // the clip NOT marked unusable, so a split feature still
                // composed from FEATURE_1 alone and presented half a movie as
                // a whole one. That is the very outcome the arm above was
                // written to prevent, reachable through every error but one.
                //
                // The code logged is the error's own. Reusing 6017 here would
                // account a scratched disc as an authoring hole.
                //
                // (`UdfNotFound` needs no special case: these names came from
                // `ts_dir.entries`, so the lookup cannot miss.)
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
            if !extents.is_empty() {
                clip_extents.insert(name.to_ascii_lowercase(), (name.clone(), *size, extents));
            }
        }

        // Authoritative composition from the Advanced-Content playlist
        // (`ADV_OBJ/VPLST*.XPL`): real per-title clip lists, durations, names,
        // chapters, and title-time offsets. This is the primary path; the
        // clip-name heuristic below is the fallback when the playlist is absent
        // or unparseable (or resolves to no on-disc clips).
        if let Some(xpl) = read_adv_obj_xpl(reader, udf_fs) {
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

        // Feature clips, in authored order, that actually resolved to extents.
        // If ANY authored feature part has no truthful read plan, the composed
        // feature title would silently omit that part's bytes — emit no
        // composed feature at all rather than a short one. (The per-clip
        // titles below are unaffected: each stands alone and an unusable clip
        // simply yields no title.)
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
mod tests {
    use super::*;
    use crate::udf::fixture::*;

    /// Build a UDF with an `HVDVD_TS/` tree holding the listed `.evo` clips
    /// (name, sector count, data LBA).
    fn make_hddvd_fs(disc: &mut MemDisc, evos: &[(&str, u32, u32)]) -> crate::udf::UdfFs {
        // ICBs are handed out from 100 upward, one per EVO, so the index IS
        // the offset from that base.
        let files: Vec<_> = evos
            .iter()
            .enumerate()
            .map(|(i, (name, sectors, data_lba))| {
                file(
                    name,
                    100 + i as u32,
                    *data_lba,
                    u64::from(*sectors) * 2048,
                    true,
                )
            })
            .collect();
        let root = DirSpec {
            name: String::new(),
            icb_lba: 10,
            dir_data_lba: 11,
            files: Vec::new(),
            subdirs: vec![DirSpec {
                name: "HVDVD_TS".to_string(),
                icb_lba: 20,
                dir_data_lba: 21,
                files,
                subdirs: vec![],
            }],
        };
        build_udf_skeleton(disc, 10);
        lay_dir(disc, &root);
        crate::udf::read_filesystem(disc).expect("fs")
    }

    /// HD-DVD's own enumerator yields one title per `.evo`, MpegPs container,
    /// with real physical extents (mirrors the BD `.m2ts` extent path).
    #[test]
    fn scan_hddvd_titles_enumerates_evo_extents() {
        let mut disc = MemDisc::new();
        let udf = make_hddvd_fs(
            &mut disc,
            &[("FEATURE.EVO", 2000, 5000), ("BLOOP.EVO", 300, 9000)],
        );
        let titles = Disc::scan_hddvd_titles(&mut disc, &udf, None).expect("scan");
        assert_eq!(titles.len(), 2, "one title per .evo clip");
        for t in &titles {
            assert_eq!(
                t.content_format,
                ContentFormat::MpegPs,
                "EVO is a program stream"
            );
            assert_eq!(t.extents.len(), 1);
        }
        let feature = titles.iter().find(|t| t.playlist == "FEATURE.EVO").unwrap();
        assert_eq!(feature.extents[0].start_lba, PART_START + 5000);
        assert_eq!(feature.extents[0].sector_count, 2000);
        assert_eq!(
            feature.clips[0].clip_id, "FEATURE",
            "clip_id drops the extension"
        );
    }

    // ── VTI playlist parsing + feature composition ────────────────────────

    /// Build a synthetic `ADVANCED-VTS` VTI whose clip table lists `clips` in
    /// order — one fixed-stride entry each, NUL-terminated name at `entry+0x42`.
    fn synthetic_vti(clips: &[&str]) -> Vec<u8> {
        let table_start = 0x200usize;
        let mut v = vec![0u8; table_start + clips.len() * VTI_CLIP_ENTRY_STRIDE];
        v[..HDDVD_VTI_MAGIC.len()].copy_from_slice(HDDVD_VTI_MAGIC);
        for (i, name) in clips.iter().enumerate() {
            let off = table_start + i * VTI_CLIP_ENTRY_STRIDE + 0x42;
            v[off..off + name.len()].copy_from_slice(name.as_bytes());
            // The byte after the name stays 0 (NUL terminator).
        }
        v
    }

    #[test]
    fn parse_vti_clip_order_reads_table_in_authored_order() {
        let vti = synthetic_vti(&[
            "DELOGO.EVO",
            "FEATURE_1.EVO",
            "FEATURE_2.EVO",
            "TRAILER.EVO",
        ]);
        let order = parse_vti_clip_order(&vti);
        assert_eq!(
            order,
            vec![
                "DELOGO.EVO".to_string(),
                "FEATURE_1.EVO".to_string(),
                "FEATURE_2.EVO".to_string(),
                "TRAILER.EVO".to_string(),
            ]
        );
        // A non-VTI blob yields nothing.
        assert!(parse_vti_clip_order(b"not a vti").is_empty());
    }

    #[test]
    fn parse_vti_clip_order_caps_hits_on_a_crafted_vti() {
        // A crafted VTI packed with far more than MAX_VTI_HITS `.EVO` tokens must
        // not scan/collect them all (a CPU/memory amplification on a routine
        // scan). The result is capped, and parsing stays fast.
        let mut vti = Vec::with_capacity(1_000_000);
        vti.extend_from_slice(HDDVD_VTI_MAGIC);
        // ~160k tokens of the form "X.EVO\0" — well over the 8192 cap.
        for _ in 0..(MAX_VTI_HITS * 20) {
            vti.extend_from_slice(b"X.EVO\0");
        }
        let out = parse_vti_clip_order(&vti);
        assert!(
            out.len() <= MAX_VTI_HITS,
            "collected hits capped at MAX_VTI_HITS, got {}",
            out.len()
        );
    }

    #[test]
    fn parse_vti_clip_order_is_deterministic_on_a_bucket_size_tie() {
        // Two residue buckets of EQUAL size must resolve to the SAME winner every
        // call — `HashMap` iteration is randomized, so a `max_by_key` without a
        // deterministic tie-break could pick a different bucket run-to-run on
        // identical bytes. Build a VTI whose stray `.EVO` names tie the real
        // table's bucket count, then assert the result is stable across calls.
        let mut vti = vec![0u8; 0x600];
        vti[..HDDVD_VTI_MAGIC.len()].copy_from_slice(HDDVD_VTI_MAGIC);
        let put = |v: &mut Vec<u8>, off: usize, name: &str| {
            v[off..off + name.len()].copy_from_slice(name.as_bytes());
        };
        // Bucket A (residue 0x42): two names at stride 0x140.
        put(&mut vti, 0x142, "A1.EVO");
        put(&mut vti, 0x282, "A2.EVO");
        // Bucket B (residue 0x50): two names — same count, different residue.
        put(&mut vti, 0x150, "B1.EVO");
        put(&mut vti, 0x290, "B2.EVO");

        let first = parse_vti_clip_order(&vti);
        for _ in 0..20 {
            assert_eq!(
                parse_vti_clip_order(&vti),
                first,
                "tie-break must be deterministic across repeated calls"
            );
        }
        assert!(!first.is_empty());
    }

    #[test]
    fn is_feature_clip_matches_the_feature_naming_variants() {
        // Layer-break split (Shaun / Anchorman) and the divide form (Harry Potter).
        assert!(is_feature_clip("FEATURE_1.EVO"));
        assert!(is_feature_clip("FEATURE_2.EVO"));
        assert!(is_feature_clip("feature.EVO"));
        assert!(is_feature_clip("feature_Divide.EVO"));
        // Extras are not the feature.
        assert!(!is_feature_clip("TRAILER.EVO"));
        assert!(!is_feature_clip("DLS_01.EVO"));
        assert!(!is_feature_clip("EPK.EVO"));
    }

    /// The SECOND size accumulator. `compose_xpl_titles` sums an XPL's clip
    /// sizes; this one sums the composed FEATURE title's parts, and it reads
    /// the same disc-declared `u64` Information Length from the same
    /// `clip_extents` map. A split feature whose two parts each declare a size
    /// near `u64::MAX` overflows it exactly as the XPL path did.
    ///
    /// Nothing reconciles the declared size against the extents that back it —
    /// here each part declares `u64::MAX` while occupying ten sectors — because
    /// the allocation descriptor's length field is only 32 bits wide, so the
    /// disc can always claim more than it holds.
    ///
    /// Mutation: restore `size_bytes += *size` in the feature-composition path
    /// and this goes red (debug: attempt to add with overflow).
    #[test]
    fn scan_hddvd_feature_composition_saturates_absurd_disc_declared_sizes() {
        let mut disc = MemDisc::new();
        let vti = synthetic_vti(&["FEATURE_1.EVO", "FEATURE_2.EVO"]);
        let files = vec![
            file("FEATURE_1.EVO", 100, 5000, u64::MAX, true),
            file("FEATURE_2.EVO", 101, 8000, u64::MAX, true),
            file_with("HVA00001.VTI", 103, 15000, vti, true),
        ];
        let root = DirSpec {
            name: String::new(),
            icb_lba: 10,
            dir_data_lba: 11,
            files: Vec::new(),
            subdirs: vec![DirSpec {
                name: "HVDVD_TS".to_string(),
                icb_lba: 20,
                dir_data_lba: 21,
                files,
                subdirs: vec![],
            }],
        };
        build_udf_skeleton(&mut disc, 10);
        lay_dir(&mut disc, &root);
        let udf = crate::udf::read_filesystem(&mut disc).expect("fs");

        let titles = Disc::scan_hddvd_titles(&mut disc, &udf, None)
            .expect("a hostile size field must not fail the scan");
        let feat = titles
            .iter()
            .find(|t| t.playlist == "FEATURE")
            .expect("composed feature title");
        assert_eq!(
            feat.size_bytes,
            u64::MAX,
            "the part sizes must saturate at u64::MAX, never wrap to a small number"
        );
    }

    #[test]
    fn scan_hddvd_composes_split_feature_into_one_title() {
        // A disc whose feature is FEATURE_1 + FEATURE_2 (a layer-break split), plus
        // a TRAILER extra. The VTI names them in authored order; the scan must
        // JOIN the two feature parts into one title (so the largest-title pick is
        // the whole movie) and keep the trailer as its own title.
        let mut disc = MemDisc::new();
        let vti = synthetic_vti(&["FEATURE_1.EVO", "FEATURE_2.EVO", "TRAILER.EVO"]);
        let files = vec![
            file("FEATURE_1.EVO", 100, 5000, 10 * 2048, true),
            file("FEATURE_2.EVO", 101, 8000, 6 * 2048, true),
            file("TRAILER.EVO", 102, 12000, 2 * 2048, true),
            file_with("HVA00001.VTI", 103, 15000, vti, true),
        ];
        let root = DirSpec {
            name: String::new(),
            icb_lba: 10,
            dir_data_lba: 11,
            files: Vec::new(),
            subdirs: vec![DirSpec {
                name: "HVDVD_TS".to_string(),
                icb_lba: 20,
                dir_data_lba: 21,
                files,
                subdirs: vec![],
            }],
        };
        build_udf_skeleton(&mut disc, 10);
        lay_dir(&mut disc, &root);
        let udf = crate::udf::read_filesystem(&mut disc).expect("fs");

        let titles = Disc::scan_hddvd_titles(&mut disc, &udf, None).expect("scan");
        // One composed FEATURE title + the trailer.
        assert_eq!(titles.len(), 2, "feature parts merged, trailer separate");
        let feat = titles
            .iter()
            .find(|t| t.playlist == "FEATURE")
            .expect("composed feature title");
        assert_eq!(feat.clips.len(), 2, "both feature parts recorded");
        assert_eq!(feat.size_bytes, (10 + 6) * 2048, "part sizes summed");
        // Extents concatenated in authored order: FEATURE_1 (lba 5000) then
        // FEATURE_2 (lba 8000) — the movie plays through in order.
        assert_eq!(feat.extents.len(), 2);
        assert_eq!(feat.extents[0].start_lba, PART_START + 5000);
        assert_eq!(feat.extents[0].sector_count, 10);
        assert_eq!(feat.extents[1].start_lba, PART_START + 8000);
        assert_eq!(feat.extents[1].sector_count, 6);
        // The largest title is the whole feature, not just part 1.
        let largest = titles.iter().max_by_key(|t| t.size_bytes).unwrap();
        assert_eq!(largest.playlist, "FEATURE");
        assert!(titles.iter().any(|t| t.playlist == "TRAILER.EVO"));
    }

    /// A split feature (FEATURE_1 + FEATURE_2) where ONE part carries an
    /// unrecorded extent must not be composed into a FEATURE title at all.
    ///
    /// Composing around the unusable part is the dangerous outcome: the
    /// title would play through as if it were the whole movie while silently
    /// missing that part's runtime, and its size still counts it. The other
    /// clips remain available as their own titles — refusing the composition
    /// is not the same as refusing the disc.
    #[test]
    fn scan_hddvd_does_not_compose_a_feature_over_an_unrecorded_part() {
        let mut disc = MemDisc::new();
        let vti = synthetic_vti(&["FEATURE_1.EVO", "FEATURE_2.EVO", "TRAILER.EVO"]);
        let files = vec![
            file("FEATURE_1.EVO", 100, 5000, 10 * 2048, true),
            file("FEATURE_2.EVO", 101, 8000, 6 * 2048, true),
            file("TRAILER.EVO", 102, 12000, 2 * 2048, true),
            file_with("HVA00001.VTI", 103, 15000, vti, true),
        ];
        let root = DirSpec {
            name: String::new(),
            icb_lba: 10,
            dir_data_lba: 11,
            files: Vec::new(),
            subdirs: vec![DirSpec {
                name: "HVDVD_TS".to_string(),
                icb_lba: 20,
                dir_data_lba: 21,
                files,
                subdirs: vec![],
            }],
        };
        build_udf_skeleton(&mut disc, 10);
        lay_dir(&mut disc, &root);
        // FEATURE_2's ICB (laid at PART_START + 101): an unrecorded 2048-byte
        // extent at LBA 7999, then its real content at 8000.
        let mut icb = build_file_icb(6 * 2048, 8000, false);
        icb[212..216].copy_from_slice(&16u32.to_le_bytes());
        icb[216..220].copy_from_slice(&0x4000_0800u32.to_le_bytes()); // type 1
        icb[220..224].copy_from_slice(&7999u32.to_le_bytes());
        icb[224..228].copy_from_slice(&(6u32 * 2048).to_le_bytes()); // type 0
        icb[228..232].copy_from_slice(&8000u32.to_le_bytes());
        disc.put_bytes(PART_START + 101, &icb);
        let udf = crate::udf::read_filesystem(&mut disc).expect("fs");

        let titles = Disc::scan_hddvd_titles(&mut disc, &udf, None).expect("scan");
        assert!(
            !titles.iter().any(|t| t.playlist == "FEATURE"),
            "a feature part with no truthful read plan must not be composed \
             into a title that silently omits it; got {:?}",
            titles
                .iter()
                .map(|t| (&t.playlist, t.extents.len()))
                .collect::<Vec<_>>()
        );
        // The clips that DID resolve are still offered on their own.
        assert!(
            titles.iter().any(|t| t.playlist == "TRAILER.EVO"),
            "refusing the composition must not refuse the healthy clips"
        );
        assert!(
            titles
                .iter()
                .all(|t| t.extents.iter().all(|e| e.start_lba != PART_START + 7999)),
            "no title may read the unrecorded extent"
        );
    }

    // ── codec sniffing ────────────────────────────────────────────────────

    #[test]
    fn sniff_video_codec_recognizes_h264_vc1_mpeg2() {
        // H.264 SPS NAL (type 7). 0x67/0x27/0x47 all decode to type 7.
        assert_eq!(
            sniff_video_codec(&[0x00, 0x00, 0x01, 0x67, 0x42, 0x00, 0x1E]),
            Some(Codec::H264)
        );
        assert_eq!(
            sniff_video_codec(&[0x11, 0x00, 0x00, 0x01, 0x27, 0x64]),
            Some(Codec::H264)
        );
        // VC-1 sequence-header BDU (0x0F).
        assert_eq!(
            sniff_video_codec(&[0x00, 0x00, 0x01, 0x0F, 0xC0]),
            Some(Codec::Vc1)
        );
        // MPEG-2 sequence_header (0xB3).
        assert_eq!(
            sniff_video_codec(&[0x00, 0x00, 0x01, 0xB3, 0x2D]),
            Some(Codec::Mpeg2)
        );
        // A slice/picture-only sample (no SPS/sequence) is indeterminate.
        assert_eq!(sniff_video_codec(&[0x00, 0x00, 0x01, 0x61, 0x9A]), None);
        assert_eq!(sniff_video_codec(&[0xDE, 0xAD, 0xBE, 0xEF]), None);

        // Overlap regression: a picture_start_code (0x00) whose payload begins
        // with 00 00 must advance a full 4 bytes so the code byte isn't re-read
        // as the start of a new marker. Here the picture is followed by a real
        // MPEG-2 sequence header — the scan must reach it cleanly and return
        // Mpeg2 (and, critically, not be confused by the 1-byte overlap).
        assert_eq!(
            sniff_video_codec(&[0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0xB3, 0x2D]),
            Some(Codec::Mpeg2)
        );
        // A lone picture_start_code with a 00-heavy payload and no following real
        // start code stays indeterminate (the overlap must not fabricate one).
        assert_eq!(
            sniff_video_codec(&[0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00]),
            None
        );

        // Every byte of the `00 00 01` marker is load-bearing: a near-miss
        // that gets ANY one of the three bytes wrong must not be recognized.
        assert_eq!(
            sniff_video_codec(&[0x05, 0x00, 0x01, 0xB3]),
            None,
            "leading byte must be 0x00, not just any byte"
        );
        assert_eq!(
            sniff_video_codec(&[0x00, 0x05, 0x01, 0xB3]),
            None,
            "second byte must be 0x00, not just any byte"
        );
        assert_eq!(
            sniff_video_codec(&[0x00, 0xFF, 0x01, 0xB3]),
            None,
            "the middle byte of the marker must actually be checked, not skipped"
        );
    }

    #[test]
    fn sniff_audio_codec_recognizes_eac3_syncword() {
        assert_eq!(
            sniff_audio_codec(&[0x00, 0x0B, 0x77, 0x12, 0x34]),
            Some(Codec::Ac3Plus)
        );
        assert_eq!(sniff_audio_codec(&[0x00, 0x01, 0x02, 0x03]), None);
        // Both syncword bytes are required together: a lone 0x0B with no 0x77
        // partner anywhere must not be recognized.
        assert_eq!(
            sniff_audio_codec(&[0x0B, 0x00, 0x0B, 0x01]),
            None,
            "0x0B alone (no 0x77 partner) is not the E-AC-3 syncword"
        );
    }

    // ── EVO head probe → streams ──────────────────────────────────────────

    /// A minimal bounded PES: `00 00 01 [id] [len:2] 80 00 00 [payload]`.
    fn pes(stream_id: u8, payload: &[u8]) -> Vec<u8> {
        let mut v = vec![0x00, 0x00, 0x01, stream_id];
        let len = (3 + payload.len()) as u16; // flags1+flags2+hdl + payload
        v.extend_from_slice(&len.to_be_bytes());
        v.extend_from_slice(&[0x80, 0x00, 0x00]);
        v.extend_from_slice(payload);
        v
    }

    /// Synthetic EVO program-stream: pack header, a video PES (H.264 SPS+IDR on
    /// stream_id 0xE2, exactly as ANCHORMAN carries it), two DD+ audio PES
    /// (sub-ids 0xC0/0xC1, each with the 4-byte sub-header + E-AC-3 syncword),
    /// then program-end.
    fn synthetic_evo() -> Vec<u8> {
        let mut d = Vec::new();
        // MPEG-2 pack header (14 bytes, stuffing 0).
        d.extend_from_slice(&[
            0x00, 0x00, 0x01, 0xBA, 0x44, 0x00, 0x04, 0x00, 0x04, 0x01, 0x01, 0x89, 0xC3, 0xF8,
        ]);
        // Video PES on stream_id 0xE2 (Anchorman's H.264 sub-id in the 0xE0-0xEF
        // range): SPS (type 7) + IDR (type 5) Annex-B.
        let video_es = [
            0x00, 0x00, 0x01, 0x67, 0x42, 0x00, 0x1E, 0xAB, 0xCD, // SPS
            0x00, 0x00, 0x01, 0x65, 0x88, 0x00, // IDR slice
        ];
        d.extend_from_slice(&pes(0xE2, &video_es));
        // DD+ audio PES: sub-id + 4-byte sub-header (num_frames + ptr) folded in
        // — the demuxer strips 4 bytes, leaving the E-AC-3 syncword.
        for sub in [0xC0u8, 0xC1] {
            let audio_payload = [
                sub, 0x01, 0x00, 0x00, // sub-id + num_frames(1) + ptr(2)
                0x0B, 0x77, 0xDE, 0xAD, // E-AC-3 syncword + body
            ];
            d.extend_from_slice(&pes(0xBD, &audio_payload));
        }
        d.extend_from_slice(&[0x00, 0x00, 0x01, 0xB9]); // program end
        d
    }

    /// Build a UDF whose `HVDVD_TS/FEATURE.EVO` holds the given raw bytes.
    fn make_hddvd_fs_with_evo(disc: &mut MemDisc, evo: &[u8]) -> crate::udf::UdfFs {
        let root = DirSpec {
            name: String::new(),
            icb_lba: 10,
            dir_data_lba: 11,
            files: Vec::new(),
            subdirs: vec![DirSpec {
                name: "HVDVD_TS".to_string(),
                icb_lba: 20,
                dir_data_lba: 21,
                files: vec![file_with("FEATURE.EVO", 100, 5000, evo.to_vec(), true)],
                subdirs: vec![],
            }],
        };
        build_udf_skeleton(disc, 10);
        lay_dir(disc, &root);
        crate::udf::read_filesystem(disc).expect("fs")
    }

    /// An `.evo` whose ICB declares an UNRECORDED (ECMA-167 4/14.14.1.1
    /// type-1) extent must yield NO title.
    ///
    /// Those sectors were never written, so the clip's real content there is
    /// zeros while the media holds whatever was left behind. Feeding them to
    /// the mux as stream bytes splices undefined data into the middle of the
    /// ripped `.evo`; leaving them out slides the rest of the file. Neither is
    /// a rip, so the title is not offered at all — data loss must never look
    /// like success.
    #[test]
    fn scan_hddvd_titles_refuses_a_clip_with_an_unrecorded_extent() {
        let mut disc = MemDisc::new();
        let udf = make_hddvd_fs_with_evo(&mut disc, &synthetic_evo());

        // Control: unpatched, this fixture really does produce a title — so a
        // later empty result means the hole was rejected, not that the
        // fixture was inert.
        assert_eq!(
            Disc::scan_hddvd_titles(&mut disc, &udf, None)
                .expect("scan")
                .len(),
            1,
            "fixture must yield a title before the hole is introduced"
        );

        // Rewrite FEATURE.EVO's ICB (laid at PART_START + 100) with a
        // two-descriptor short-AD list: an unrecorded 2048-byte extent at LBA
        // 4999, then the real 4096-byte content at 5000.
        let mut icb = build_file_icb(4096, 5000, false);
        icb[212..216].copy_from_slice(&16u32.to_le_bytes()); // l_ad: two short ADs
        icb[216..220].copy_from_slice(&0x4000_0800u32.to_le_bytes()); // type 1, 2048 bytes
        icb[220..224].copy_from_slice(&4999u32.to_le_bytes());
        icb[224..228].copy_from_slice(&4096u32.to_le_bytes()); // type 0, 4096 bytes
        icb[228..232].copy_from_slice(&5000u32.to_le_bytes());
        disc.put_bytes(PART_START + 100, &icb);

        let titles = Disc::scan_hddvd_titles(&mut disc, &udf, None).expect("scan");
        assert!(
            titles.is_empty(),
            "a clip with an unrecorded extent has no truthful read plan; got \
             extents {:?}",
            titles.iter().map(|t| &t.extents).collect::<Vec<_>>()
        );
    }

    /// End-to-end: scanning an `.evo` whose head carries an H.264 video PES and
    /// two DD+ audio PES yields a title with the video track (canonical
    /// DVD_VIDEO_PID) and both DD+ tracks (0xBDC0 / 0xBDC1) — the non-empty
    /// `streams` the mux path needs to route packets (the historical blocker).
    #[test]
    fn scan_hddvd_titles_probes_streams_from_evo_head() {
        let mut disc = MemDisc::new();
        let udf = make_hddvd_fs_with_evo(&mut disc, &synthetic_evo());
        let titles = Disc::scan_hddvd_titles(&mut disc, &udf, None).expect("scan");
        assert_eq!(titles.len(), 1);
        let t = &titles[0];

        let video: Vec<_> = t
            .streams
            .iter()
            .filter_map(|s| match s {
                Stream::Video(v) => Some(v),
                _ => None,
            })
            .collect();
        assert_eq!(video.len(), 1, "one video track probed");
        assert_eq!(video[0].codec, Codec::H264, "SPS sniffed as H.264");
        assert_eq!(
            video[0].pid,
            crate::mux::ps::DVD_VIDEO_PID,
            "video routes to canonical PID"
        );

        let audio: Vec<_> = t
            .streams
            .iter()
            .filter_map(|s| match s {
                Stream::Audio(a) => Some(a),
                _ => None,
            })
            .collect();
        assert_eq!(audio.len(), 2, "both DD+ sub-streams probed");
        assert!(audio.iter().all(|a| a.codec == Codec::Ac3Plus));
        let pids: Vec<u16> = audio.iter().map(|a| a.pid).collect();
        assert_eq!(pids, vec![0xBDC0, 0xBDC1], "DD+ PIDs 0xBDC0/0xBDC1");
    }

    /// A clip whose head carries no recognizable stream (unreadable /
    /// ciphertext) leaves `streams` empty rather than fabricating one — the
    /// title still enumerates (extents are real).
    #[test]
    fn scan_hddvd_titles_empty_streams_when_head_unrecognized() {
        let mut disc = MemDisc::new();
        // 4 KiB of junk with no PS start codes.
        let junk = vec![0x55u8; 4096];
        let udf = make_hddvd_fs_with_evo(&mut disc, &junk);
        let titles = Disc::scan_hddvd_titles(&mut disc, &udf, None).expect("scan");
        assert_eq!(titles.len(), 1);
        assert!(
            titles[0].streams.is_empty(),
            "no recognizable stream → empty, not fabricated"
        );
    }

    /// A PES on the HD-DVD extended-stream-id (0xFD) carrying the given
    /// `stream_id_extension` in a minimal PES extension: flags1=0x80, flags2=0x01
    /// (PES_extension only), header_data_length=3, optional bytes
    /// `[ext_flags=0x01][field_len=0x81][ext]` — exactly the shape SHAUN's VC-1
    /// video PES uses (ext 0x55).
    fn pes_extended(stream_id_extension: u8, payload: &[u8]) -> Vec<u8> {
        let mut v = vec![0x00, 0x00, 0x01, 0xFD];
        let opt = [0x01u8, 0x81, stream_id_extension];
        let len = (3 + opt.len() + payload.len()) as u16; // flags1+flags2+hdl + opt + payload
        v.extend_from_slice(&len.to_be_bytes());
        v.extend_from_slice(&[0x80, 0x01, opt.len() as u8]);
        v.extend_from_slice(&opt);
        v.extend_from_slice(payload);
        v
    }

    /// Synthetic EVO carrying VC-1 video on the extended-stream-id 0xFD (ext
    /// 0x55), as SHAUN OF THE DEAD does, plus one DD+ audio PES.
    fn synthetic_evo_vc1() -> Vec<u8> {
        let mut d = Vec::new();
        d.extend_from_slice(&[
            0x00, 0x00, 0x01, 0xBA, 0x44, 0x00, 0x04, 0x00, 0x04, 0x01, 0x01, 0x89, 0xC3, 0xF8,
        ]);
        // VC-1 sequence header (00 00 01 0F) + a frame BDU (00 00 01 0D).
        let video_es = [
            0x00, 0x00, 0x01, 0x0F, 0xC5, 0x00, 0x00, // sequence header BDU
            0x00, 0x00, 0x01, 0x0D, 0x12, 0x34, // frame BDU
        ];
        d.extend_from_slice(&pes_extended(0x55, &video_es));
        let audio_payload = [0xC0u8, 0x01, 0x00, 0x00, 0x0B, 0x77, 0xDE, 0xAD];
        d.extend_from_slice(&pes(0xBD, &audio_payload));
        d.extend_from_slice(&[0x00, 0x00, 0x01, 0xB9]);
        d
    }

    /// Build a bare PsPacket for the collect_es routing test.
    fn ps_pkt(stream_id: u8, sub: Option<u8>, data: Vec<u8>) -> crate::mux::ps::PsPacket {
        crate::mux::ps::PsPacket {
            stream_id,
            sub_stream_id: sub,
            pts: None,
            dts: None,
            data,
            source: None,
        }
    }

    #[test]
    fn collect_es_routes_only_vc1_0xfd_to_video() {
        use crate::mux::ps::hddvd_extended_pid;
        // The 0xFD guard: only the VC-1 extension (0x55) is video. An HD-audio
        // 0xFD sub-stream (e.g. 0x72) that arrives FIRST must NOT stamp video_pid
        // with its PID or pollute the video sample — else the real video track is
        // lost. (Routing 0xFD audio to its own track is deferred.)
        let mut video = Vec::new();
        let mut video_pid: Option<u16> = None;
        let mut audio = BTreeMap::new();
        // Audio-on-0xFD (ext 0x72) first — must be ignored by the video path.
        collect_es(
            &ps_pkt(0xFD, Some(0x72), vec![0xAA; 32]),
            &mut video,
            &mut video_pid,
            &mut audio,
        );
        assert!(
            video.is_empty(),
            "0xFD audio sub-stream not routed to video"
        );
        assert_eq!(video_pid, None, "0xFD audio did not stamp the video PID");
        // Then the real VC-1 video (ext 0x55).
        collect_es(
            &ps_pkt(0xFD, Some(0x55), vec![0xBB; 32]),
            &mut video,
            &mut video_pid,
            &mut audio,
        );
        assert_eq!(
            video_pid,
            Some(hddvd_extended_pid(0x55)),
            "video PID stamped from the VC-1 0xFD sub-stream (0xFD55)"
        );
        assert_eq!(video.len(), 32, "VC-1 0xFD payload accumulated as video");
    }

    /// End-to-end: an `.evo` whose video rides the extended-stream-id 0xFD yields
    /// a VC-1 video track routed to `0xFD00 | ext` (0xFD55) — the PID the demuxer
    /// derives from the same stream_id_extension, so mux-time routing lines up.
    #[test]
    fn scan_hddvd_titles_probes_vc1_on_extended_stream_id() {
        let mut disc = MemDisc::new();
        let udf = make_hddvd_fs_with_evo(&mut disc, &synthetic_evo_vc1());
        let titles = Disc::scan_hddvd_titles(&mut disc, &udf, None).expect("scan");
        assert_eq!(titles.len(), 1);

        let video: Vec<_> = titles[0]
            .streams
            .iter()
            .filter_map(|s| match s {
                Stream::Video(v) => Some(v),
                _ => None,
            })
            .collect();
        assert_eq!(video.len(), 1, "one video track probed");
        assert_eq!(video[0].codec, Codec::Vc1, "VC-1 sequence header sniffed");
        assert_eq!(
            video[0].pid,
            crate::mux::ps::hddvd_extended_pid(0x55),
            "VC-1 routes to the extended-stream-id PID 0xFD55"
        );
    }

    // ─────────────────────── Advanced-Content playlist (XPL) ──────────────

    /// A minimal but faithful VPLST000.XPL: the DVD-Forum default namespace, an
    /// XML comment, a `<TitleSet tickBase="60fps">`, a MainMovie title whose main
    /// feature is a two-clip layer-break split (FEATURE_1 + FEATURE_2, seamless)
    /// with two chapters, and a separate deleted-scene title. Attribute order
    /// varies (as it does across real discs).
    const SYNTH_XPL: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<Playlist majorVersion="1" minorVersion="0" xmlns="http://www.dvdforum.org/2005/HDDVDVideo/Playlist">
  <!-- Authored with TOSHIBA AdvMain -->
  <TitleSet timeBase="60fps" tickBase="60fps" defaultLanguage="en">
    <Title titleNumber="2" titleDuration="01:37:20:00" id="MainMovie" displayName="Main Movie">
      <PrimaryAudioVideoClip titleTimeBegin="00:00:00:00" titleTimeEnd="00:48:29:50" src="file:///dvddisc/HVDVD_TS/FEATURE_1.MAP" dataSource="Disc">
        <Video track="1" mediaAttr="2"/>
        <Audio track="1" streamNumber="1" description="English DD+"/>
      </PrimaryAudioVideoClip>
      <PrimaryAudioVideoClip titleTimeBegin="00:48:29:50" clipTimeBegin="00:00:00:00" titleTimeEnd="01:37:20:00" src="file:///dvddisc/HVDVD_TS/FEATURE_2.MAP" seamless="true">
        <Video track="1" mediaAttr="2"/>
      </PrimaryAudioVideoClip>
      <ChapterList>
        <Chapter displayName="Chapter  1" titleTimeBegin="00:00:00:00" />
        <Chapter displayName="Chapter  2" titleTimeBegin="00:03:40:30" />
      </ChapterList>
    </Title>
    <Title titleNumber="7" titleDuration="00:00:44:29" id="Deleted5" displayName="Deleted Scenes - Veronica Past">
      <PrimaryAudioVideoClip titleTimeBegin="00:00:00:00" titleTimeEnd="00:00:44:29" src="file:///dvddisc/HVDVD_TS/DEL5_VERONICAPAST.MAP" />
    </Title>
  </TitleSet>
</Playlist>"#;

    #[test]
    fn parse_timecode_hhmmssff_at_60fps() {
        // 01:37:20:00 = 5840 s exactly.
        assert!((parse_timecode("01:37:20:00", 60).unwrap() - 5840.0).abs() < 1e-6);
        // 00:48:29:50 = 48m29s + 50/60 frames.
        let want = 48.0 * 60.0 + 29.0 + 50.0 / 60.0;
        assert!((parse_timecode("00:48:29:50", 60).unwrap() - want).abs() < 1e-6);
        // MM:SS:FF short form (hours omitted).
        assert!((parse_timecode("02:05:15", 60).unwrap() - (125.0 + 15.0 / 60.0)).abs() < 1e-6);
        assert_eq!(parse_timecode("garbage", 60), None);
        assert_eq!(parse_timecode("", 60), None);
    }

    #[test]
    fn evo_from_src_maps_map_sidecar_to_evo() {
        assert_eq!(
            evo_from_src("file:///dvddisc/HVDVD_TS/FEATURE_1.MAP").as_deref(),
            Some("feature_1.evo")
        );
        // Already an EVO, or lowercase feature — normalise to lower `.evo`.
        assert_eq!(evo_from_src("feature.EVO").as_deref(), Some("feature.evo"));
        assert_eq!(
            evo_from_src("file:///x/feature_Divide.MAP").as_deref(),
            Some("feature_divide.evo")
        );
        // Empty stem → None (defensive against a malformed src).
        assert_eq!(evo_from_src("file:///x/").as_deref(), None);
    }

    #[test]
    fn parse_xpl_titles_reads_titles_clips_chapters_durations() {
        let titles = parse_xpl_titles(SYNTH_XPL.as_bytes());
        assert_eq!(titles.len(), 2, "MainMovie + one deleted-scene title");

        let mm = &titles[0];
        assert_eq!(mm.number, 2);
        assert_eq!(mm.name, "Main Movie");
        assert!(
            (mm.duration_secs - 5840.0).abs() < 1e-6,
            "97:20 from titleDuration"
        );
        // The layer-break split is ONE title with TWO clips, contiguous timeline.
        assert_eq!(mm.clips.len(), 2);
        assert_eq!(mm.clips[0].evo, "feature_1.evo");
        assert_eq!(mm.clips[1].evo, "feature_2.evo");
        assert!(
            (mm.clips[0].end_secs - mm.clips[1].begin_secs).abs() < 1e-6,
            "FEATURE_2 begins exactly where FEATURE_1 ends (seamless join)"
        );
        assert!(
            mm.clips[1].begin_secs > 0.0,
            "second clip carries a title-time offset"
        );
        assert_eq!(mm.chapters.len(), 2);
        assert!((mm.chapters[1] - (3.0 * 60.0 + 40.0 + 30.0 / 60.0)).abs() < 1e-6);

        let del = &titles[1];
        assert_eq!(del.name, "Deleted Scenes - Veronica Past");
        assert_eq!(del.clips.len(), 1);
        assert_eq!(del.clips[0].evo, "del5_veronicapast.evo");
    }

    #[test]
    fn parse_xpl_titles_returns_empty_on_non_xml() {
        assert!(parse_xpl_titles(b"not xml at all").is_empty());
        assert!(parse_xpl_titles(&[0xFF, 0x00, 0x01, 0x02]).is_empty());
        assert!(parse_xpl_titles(b"<Playlist></Playlist>").is_empty());
    }

    /// A disc-supplied playlist nested far deeper than any real one must be
    /// REFUSED BEFORE the XML parser sees it. `roxmltree` is recursive-descent
    /// and its depth-10 limit covers only ENTITY expansion, so unbounded element
    /// nesting blows the thread stack — an ABORT, which no `catch_unwind` and no
    /// `Err` fallback can contain. ~50k levels is a few hundred KB of well-formed
    /// XML, far under the read cap. The correct outcome is the same as any other
    /// unusable playlist: an empty `Vec`, so the caller falls back to the
    /// clip-name heuristic.
    /// BREADTH, not depth: a flat playlist passes the nesting guard trivially
    /// and can still declare a title per ~51 bytes, so a 64 MiB XPL (exactly
    /// `udf::MAX_FILE_BYTES`) reaches ~1.3 million of them.
    ///
    /// The cost is drive time, not memory: `compose_xpl_titles` runs
    /// `probe_evo_streams` once per title and each probe reads real sectors,
    /// so an uncapped count turns a scan the operator expects to take seconds
    /// into tens of terabytes of optical reads. `bluray.rs` is not exposed to
    /// this because its playlist count comes from the MPLS files present in
    /// the UDF directory; an XPL DECLARES its titles, so nothing bounds them.
    ///
    /// Mutation: delete the `.take(MAX_XPL_TITLES)` and this goes red at the
    /// declared count.
    #[test]
    fn parse_xpl_titles_caps_a_playlist_declaring_absurdly_many_titles() {
        const DECLARED: usize = MAX_XPL_TITLES + 500;
        let mut xpl = String::with_capacity(DECLARED * 56 + 128);
        xpl.push_str(r#"<?xml version="1.0" encoding="utf-8"?><Playlist><TitleSet>"#);
        for _ in 0..DECLARED {
            xpl.push_str(r#"<Title><PrimaryAudioVideoClip src="A.EVO"/></Title>"#);
        }
        xpl.push_str("</TitleSet></Playlist>");

        let titles = parse_xpl_titles(xpl.as_bytes());
        assert_eq!(
            titles.len(),
            MAX_XPL_TITLES,
            "a playlist declaring {DECLARED} titles must be capped at \
             {MAX_XPL_TITLES}; each surviving title costs a real probe_evo_streams \
             pass over the medium"
        );
    }

    /// The control: a realistic playlist keeps every title it declares.
    #[test]
    fn parse_xpl_titles_keeps_every_title_of_a_realistic_playlist() {
        let titles = parse_xpl_titles(SYNTH_XPL.as_bytes());
        assert!(
            !titles.is_empty() && titles.len() < MAX_XPL_TITLES,
            "the synthetic real-world playlist must survive the cap untouched, \
             got {} titles",
            titles.len()
        );
    }

    /// Build a playlist that nests `nesting` `<Title>` elements inside one
    /// another and puts `clips` self-closing `<PrimaryAudioVideoClip>` elements
    /// at the innermost level, plus `chapters` `<Chapter>` elements.
    ///
    /// `parse_xpl_titles` collects a title's clips with `descendants()`, not
    /// `children()`, so EVERY one of the `nesting` ancestors collects the SAME
    /// innermost clip list — the amplification this exercises.
    fn nested_title_xpl(nesting: usize, clips: usize, chapters: usize) -> String {
        let mut xpl = String::with_capacity(nesting * 16 + clips * 40 + chapters * 40 + 128);
        xpl.push_str(r#"<?xml version="1.0" encoding="utf-8"?><Playlist><TitleSet>"#);
        for _ in 0..nesting {
            xpl.push_str("<Title>");
        }
        for _ in 0..clips {
            xpl.push_str(r#"<PrimaryAudioVideoClip src="A.EVO"/>"#);
        }
        for _ in 0..chapters {
            xpl.push_str(r#"<Chapter titleTimeBegin="00:00:01:00"/>"#);
        }
        for _ in 0..nesting {
            xpl.push_str("</Title>");
        }
        xpl.push_str("</TitleSet></Playlist>");
        xpl
    }

    /// CLIPS PER TITLE — the axis the title cap, the depth cap and the
    /// directory cap all leave open.
    ///
    /// `MAX_XPL_TITLES` bounds how many titles a playlist may declare and
    /// `MAX_XPL_DEPTH` bounds how deeply it may nest, but NEITHER bounds how
    /// many `<PrimaryAudioVideoClip>` elements a single title collects. Worse,
    /// the collector is `descendants()`, so one clip nested inside N ancestor
    /// `<Title>`s is collected N times over — the clip count MULTIPLIES by the
    /// nesting the depth guard still permits.
    ///
    /// A 64 MiB XPL (`udf::MAX_FILE_BYTES`) holds ~1.7M clip elements at ~38
    /// bytes each; with the 32 `<Title>` ancestors the depth cap still permits,
    /// that is ~56 million heap-allocated `XplClip`s, and then as many `Clip`s
    /// again in `compose_xpl_titles`. This uses small numbers that exercise the
    /// same multiplication without a 64 MiB fixture: pre-fix it collected 756
    /// clips per title, 18,900 across the 25 nested titles, from a document
    /// declaring only 756 clip elements — an exact 25x, the nesting count.
    ///
    /// Mutation: delete the `clips.len() >= MAX_XPL_CLIPS_PER_TITLE` break and
    /// this goes red at the unamplified per-title count.
    #[test]
    fn parse_xpl_titles_caps_clips_per_title_against_descendant_amplification() {
        const NESTING: usize = 25;
        const CLIPS: usize = MAX_XPL_CLIPS_PER_TITLE + 500;

        let xpl = nested_title_xpl(NESTING, CLIPS, 0);
        let titles = parse_xpl_titles(xpl.as_bytes());

        // The nesting itself must survive the depth guard, or this test would
        // pass for the wrong reason (an empty fallback).
        assert_eq!(
            titles.len(),
            NESTING,
            "all {NESTING} nested <Title> elements must parse, so the \
             amplification is really being exercised"
        );

        let total: usize = titles.iter().map(|t| t.clips.len()).sum();
        for t in &titles {
            assert!(
                t.clips.len() <= MAX_XPL_CLIPS_PER_TITLE,
                "a title collected {} clips, above the {MAX_XPL_CLIPS_PER_TITLE} \
                 cap; total across titles {total} (a 64 MiB XPL scales this to \
                 tens of millions)",
                t.clips.len()
            );
        }
        assert!(
            total <= NESTING * MAX_XPL_CLIPS_PER_TITLE,
            "aggregate clip count {total} must be bounded by titles x cap"
        );
    }

    /// CHAPTERS PER TITLE — the same `descendants()` amplification on the
    /// second unbounded `collect()` in the same loop. Bounded separately so
    /// closing the clip axis does not simply move the attack next door.
    ///
    /// Mutation: delete the `.take(MAX_XPL_CHAPTERS_PER_TITLE)` and this goes
    /// red at the declared count.
    #[test]
    fn parse_xpl_titles_caps_chapters_per_title() {
        const NESTING: usize = 25;
        const CHAPTERS: usize = MAX_XPL_CHAPTERS_PER_TITLE + 500;

        // One clip, so the title is kept (`clips.is_empty()` skips otherwise).
        let xpl = nested_title_xpl(NESTING, 1, CHAPTERS);
        let titles = parse_xpl_titles(xpl.as_bytes());
        assert_eq!(titles.len(), NESTING);

        for t in &titles {
            assert!(
                t.chapters.len() <= MAX_XPL_CHAPTERS_PER_TITLE,
                "a title collected {} chapters, above the \
                 {MAX_XPL_CHAPTERS_PER_TITLE} cap",
                t.chapters.len()
            );
        }
    }

    /// The control: a realistic multi-clip title still resolves EVERY one of its
    /// clips and chapters. `SYNTH_XPL`'s main movie is a layer-break split — two
    /// clips on one timeline — plus two chapters; losing either to the cap would
    /// cost a genuine disc half its feature.
    ///
    /// Mutation: set `MAX_XPL_CLIPS_PER_TITLE` to 1 and this goes red.
    #[test]
    fn parse_xpl_titles_keeps_every_clip_of_a_realistic_title() {
        let titles = parse_xpl_titles(SYNTH_XPL.as_bytes());
        assert_eq!(titles.len(), 2);
        assert_eq!(
            titles[0].clips.len(),
            2,
            "the layer-break split must keep BOTH clips through the cap"
        );
        assert_eq!(titles[0].clips[0].evo, "feature_1.evo");
        assert_eq!(titles[0].clips[1].evo, "feature_2.evo");
        assert_eq!(
            titles[0].chapters.len(),
            2,
            "both chapters must survive the chapter cap"
        );
        assert_eq!(titles[1].clips.len(), 1);
    }

    #[test]
    fn parse_xpl_titles_refuses_deeply_nested_playlist() {
        const DEPTH: usize = 50_000;
        let mut xpl = String::with_capacity(DEPTH * 8 + 64);
        xpl.push_str(r#"<?xml version="1.0" encoding="utf-8"?>"#);
        for _ in 0..DEPTH {
            xpl.push_str("<n>");
        }
        for _ in 0..DEPTH {
            xpl.push_str("</n>");
        }
        assert!(
            parse_xpl_titles(xpl.as_bytes()).is_empty(),
            "a hostile nesting depth must fall back, not abort the process"
        );
    }

    /// The depth guard must not reject real discs. A genuine `VPLST000.XPL` is a
    /// handful of levels deep; this is the same document the parsing test uses,
    /// plus self-closing tags, a comment and a processing instruction that the
    /// pre-parse scanner has to account for without inflating its depth count.
    #[test]
    fn parse_xpl_titles_accepts_real_world_nesting_depth() {
        let titles = parse_xpl_titles(SYNTH_XPL.as_bytes());
        assert_eq!(titles.len(), 2, "a real playlist still parses");
        assert_eq!(titles[0].clips.len(), 2);

        // Comments and processing instructions carry `<` and `/>` that a naive
        // scanner would miscount as element nesting.
        let decl = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>";
        let tricky = SYNTH_XPL.replacen(
            decl,
            &format!("{decl}\n<!-- <a><a><a><a><a> -->\n<?authoring <b><b><b> ?>"),
            1,
        );
        assert_eq!(parse_xpl_titles(tricky.as_bytes()).len(), 2);
    }

    /// Build a UDF with `HVDVD_TS/` `.evo` clips plus an `ADV_OBJ/VPLST000.XPL`
    /// carrying `xpl`, so `scan_hddvd_titles` takes the playlist path.
    fn make_hddvd_fs_xpl(
        disc: &mut MemDisc,
        evos: &[(&str, u32, u32)],
        xpl: &[u8],
    ) -> crate::udf::UdfFs {
        // ICBs are handed out from 100 upward, one per EVO, so the index IS
        // the offset from that base.
        let hv_files: Vec<_> = evos
            .iter()
            .enumerate()
            .map(|(i, (name, sectors, data_lba))| {
                file(
                    name,
                    100 + i as u32,
                    *data_lba,
                    u64::from(*sectors) * 2048,
                    true,
                )
            })
            .collect();
        let root = DirSpec {
            name: String::new(),
            icb_lba: 10,
            dir_data_lba: 11,
            files: Vec::new(),
            subdirs: vec![
                DirSpec {
                    name: "HVDVD_TS".to_string(),
                    icb_lba: 20,
                    dir_data_lba: 21,
                    files: hv_files,
                    subdirs: vec![],
                },
                DirSpec {
                    name: "ADV_OBJ".to_string(),
                    icb_lba: 30,
                    dir_data_lba: 31,
                    files: vec![file_with("VPLST000.XPL", 40, 4000, xpl.to_vec(), true)],
                    subdirs: vec![],
                },
            ],
        };
        build_udf_skeleton(disc, 10);
        lay_dir(disc, &root);
        crate::udf::read_filesystem(disc).expect("fs")
    }

    /// The authoritative path: when a VPLST000.XPL is present, titles come from
    /// the playlist — the layer-break split (FEATURE_1 + FEATURE_2) is composed
    /// into ONE title with the real duration, name, chapters, and per-clip
    /// title-time offsets — not the clip-name heuristic.
    #[test]
    fn scan_hddvd_composes_titles_from_xpl_playlist() {
        let mut disc = MemDisc::new();
        let udf = make_hddvd_fs_xpl(
            &mut disc,
            &[
                ("FEATURE_1.EVO", 2000, 5000),
                ("FEATURE_2.EVO", 1800, 9000),
                ("DEL5_VERONICAPAST.EVO", 100, 12000),
            ],
            SYNTH_XPL.as_bytes(),
        );
        let titles = Disc::scan_hddvd_titles(&mut disc, &udf, None).expect("scan");
        assert_eq!(
            titles.len(),
            2,
            "the two playlist titles that resolve to clips"
        );

        let mm = titles
            .iter()
            .find(|t| t.playlist == "Main Movie")
            .expect("MainMovie composed from the playlist");
        assert_eq!(mm.playlist_id, 2);
        assert!(
            (mm.duration_secs - 5840.0).abs() < 1.0,
            "97:20 duration from titleDuration, not 0/unknown"
        );
        assert_eq!(
            mm.clips.len(),
            2,
            "layer-break split kept as ONE title, two clips"
        );
        // FEATURE_2's clip carries the 48:29 offset (45 kHz ticks) — the datum
        // that splices it onto FEATURE_1's timeline instead of restarting at 0.
        assert!(
            mm.clips[1].in_time > 100_000_000,
            "FEATURE_2 offset onto the title timeline (48:29 * 45000), got {}",
            mm.clips[1].in_time
        );
        assert_eq!(mm.chapters.len(), 2);
        assert_eq!(mm.chapters[0].name, "1", "bare ordinal chapter name");
        // Both feature halves are in ONE title's extents.
        assert!(!mm.extents.is_empty());
    }

    // ── parse_vti_clip_order: bound / cap / termination edge cases ────────

    /// The outer scan cap (`MAX_VTI_HITS`) must be exact, not off-by-one. Every
    /// entry here is stride-aligned so they ALL land in one residue bucket —
    /// unlike the scattered-token cap test above, the cap is directly visible
    /// in the output length (the total scanned-hit count IS the bucket size).
    #[test]
    fn parse_vti_clip_order_caps_hits_at_exact_boundary_same_residue() {
        let table_start = 0x200usize;
        let n = MAX_VTI_HITS + 50;
        let mut v = vec![0u8; table_start + n * VTI_CLIP_ENTRY_STRIDE];
        v[..HDDVD_VTI_MAGIC.len()].copy_from_slice(HDDVD_VTI_MAGIC);
        for i in 0..n {
            let off = table_start + i * VTI_CLIP_ENTRY_STRIDE + 0x42;
            v[off..off + b"X.EVO".len()].copy_from_slice(b"X.EVO");
        }
        let out = parse_vti_clip_order(&v);
        assert_eq!(
            out.len(),
            MAX_VTI_HITS,
            "the scan must stop at exactly MAX_VTI_HITS, not one past it"
        );
    }

    /// A name-byte run that reaches the exact end of the buffer with NO NUL
    /// terminator must not be read out of bounds — the inner scan (and the
    /// nul-terminated check) must stop at the buffer boundary rather than
    /// indexing one past it.
    #[test]
    fn parse_vti_clip_order_handles_unterminated_name_run_at_buffer_end() {
        let mut v = HDDVD_VTI_MAGIC.to_vec();
        v.extend_from_slice(b"TRAILING_JUNK_NO_TERMINATOR"); // all ascii-graphic, no NUL, ends at EOF
        let out = parse_vti_clip_order(&v);
        assert!(
            out.is_empty(),
            "unterminated trailing run yields no entries (and must not panic)"
        );
    }

    /// A `.EVO`-suffixed name run followed by an in-bounds byte that is NOT a
    /// NUL must not be treated as terminated — being merely in-bounds is not
    /// the same as actually finding a NUL.
    #[test]
    fn parse_vti_clip_order_requires_actual_nul_terminator_not_just_in_bounds() {
        let mut v = HDDVD_VTI_MAGIC.to_vec();
        v.extend_from_slice(b"FEATURE.EVO");
        v.push(0x01); // in-bounds terminator byte, but NOT a NUL
        v.extend_from_slice(&[0u8; 16]);
        let out = parse_vti_clip_order(&v);
        assert!(
            out.is_empty(),
            "a non-NUL byte after .EVO must not count as terminated"
        );
    }

    /// A NUL-terminated ascii run that does NOT end in ".EVO" must never be
    /// collected, no matter how many repeat at a shared residue: nul-
    /// termination and the `.EVO`-suffix check are independent gates, one must
    /// not short-circuit the other away.
    #[test]
    fn parse_vti_clip_order_rejects_nul_terminated_names_without_evo_suffix() {
        let mut v = HDDVD_VTI_MAGIC.to_vec();
        for _ in 0..20 {
            v.extend_from_slice(b"HELLO\0"); // nul-terminated, 5 bytes, not .EVO
        }
        let out = parse_vti_clip_order(&v);
        assert!(
            out.is_empty(),
            "non-.EVO nul-terminated names must not be collected"
        );
    }

    /// A short (<4-byte) nul-terminated name run must be rejected by the
    /// length guard BEFORE the `.EVO`-suffix slice runs — slicing a name
    /// shorter than 4 bytes at `name.len() - 4` would otherwise underflow.
    /// Must not panic, and must not be collected.
    #[test]
    fn parse_vti_clip_order_short_circuits_length_check_before_slicing_short_names() {
        let mut v = HDDVD_VTI_MAGIC.to_vec();
        v.push(b' '); // non-name-byte separator: isolates "AB" from the magic run
        v.extend_from_slice(b"AB\0"); // 2-byte name, under the 4-byte slice width
        let out = parse_vti_clip_order(&v);
        assert!(
            out.is_empty(),
            "short name is rejected without slicing/panicking"
        );
    }

    // ── EVO_ES_SAMPLE_CAP / collect_es capping ─────────────────────────────

    /// The documented sample cap is 128 KiB, i.e. `128 * 1024`.
    #[test]
    fn evo_es_sample_cap_is_128_kib() {
        assert_eq!(EVO_ES_SAMPLE_CAP, 128 * 1024);
    }

    /// Plain-video-range (`0xE0..=0xEF`) samples stop growing once the buffer
    /// has reached the cap — a subsequent packet must not push it past.
    #[test]
    fn collect_es_caps_video_sample_at_the_length_cap() {
        use crate::consts::pes_stream_id::VIDEO;
        let mut video = Vec::new();
        let mut video_pid: Option<u16> = None;
        let mut audio = BTreeMap::new();
        collect_es(
            &ps_pkt(VIDEO, None, vec![0xAA; EVO_ES_SAMPLE_CAP]),
            &mut video,
            &mut video_pid,
            &mut audio,
        );
        assert_eq!(video.len(), EVO_ES_SAMPLE_CAP);
        collect_es(
            &ps_pkt(VIDEO, None, vec![0xBB; 16]),
            &mut video,
            &mut video_pid,
            &mut audio,
        );
        assert_eq!(
            video.len(),
            EVO_ES_SAMPLE_CAP,
            "no further growth once at the cap"
        );
    }

    /// The VC-1 extended-stream-id (0xFD, ext 0x55) video branch has its own
    /// cap check; it must behave identically to the plain-video branch.
    #[test]
    fn collect_es_caps_vc1_video_sample_at_the_length_cap() {
        let mut video = Vec::new();
        let mut video_pid: Option<u16> = None;
        let mut audio = BTreeMap::new();
        collect_es(
            &ps_pkt(0xFD, Some(0x55), vec![0xAA; EVO_ES_SAMPLE_CAP]),
            &mut video,
            &mut video_pid,
            &mut audio,
        );
        assert_eq!(video.len(), EVO_ES_SAMPLE_CAP);
        collect_es(
            &ps_pkt(0xFD, Some(0x55), vec![0xBB; 16]),
            &mut video,
            &mut video_pid,
            &mut audio,
        );
        assert_eq!(
            video.len(),
            EVO_ES_SAMPLE_CAP,
            "no further growth once at the cap (VC-1 0xFD branch)"
        );
    }

    /// The per-sub-id audio branch has its own cap check; same requirement.
    #[test]
    fn collect_es_caps_audio_sample_at_the_length_cap() {
        use crate::consts::pes_stream_id::PRIVATE_STREAM_1;
        let mut video = Vec::new();
        let mut video_pid: Option<u16> = None;
        let mut audio = BTreeMap::new();
        collect_es(
            &ps_pkt(PRIVATE_STREAM_1, Some(0xC0), vec![0xAA; EVO_ES_SAMPLE_CAP]),
            &mut video,
            &mut video_pid,
            &mut audio,
        );
        assert_eq!(audio[&0xC0].len(), EVO_ES_SAMPLE_CAP);
        collect_es(
            &ps_pkt(PRIVATE_STREAM_1, Some(0xC0), vec![0xBB; 16]),
            &mut video,
            &mut video_pid,
            &mut audio,
        );
        assert_eq!(
            audio[&0xC0].len(),
            EVO_ES_SAMPLE_CAP,
            "no further growth once at the cap (audio branch)"
        );
    }

    // ── read_adv_obj_xpl: prefix AND suffix are both required ──────────────

    /// A file matching the `vplst` prefix but NOT the `.xpl` suffix must not
    /// be adopted as the playlist — both conditions are independently
    /// required, one must not be short-circuited away by the other.
    #[test]
    fn read_adv_obj_xpl_requires_both_vplst_prefix_and_xpl_suffix() {
        let mut disc = MemDisc::new();
        let root = DirSpec {
            name: String::new(),
            icb_lba: 10,
            dir_data_lba: 11,
            files: Vec::new(),
            subdirs: vec![DirSpec {
                name: "ADV_OBJ".to_string(),
                icb_lba: 30,
                dir_data_lba: 31,
                files: vec![file_with(
                    "VPLST_NOTES.TXT",
                    40,
                    4000,
                    b"not a playlist".to_vec(),
                    true,
                )],
                subdirs: vec![],
            }],
        };
        build_udf_skeleton(&mut disc, 10);
        lay_dir(&mut disc, &root);
        let udf = crate::udf::read_filesystem(&mut disc).expect("fs");
        assert!(
            read_adv_obj_xpl(&mut disc, &udf).is_none(),
            "prefix match alone (not ending .xpl) must not select a file"
        );
    }

    /// An Advanced-Content playlist title that names a clip with no truthful
    /// read plan must be dropped whole, not composed from its other clips.
    ///
    /// The unusable clip is absent from `clip_extents` (it never resolved),
    /// which on its own just makes the loop skip it — emitting a title that
    /// plays short by that clip while its chapter offsets and durations still
    /// assume it. The `unusable` set is what distinguishes "this clip does not
    /// exist" from "this clip exists and cannot be ripped truthfully".
    #[test]
    fn compose_xpl_titles_drops_a_title_naming_an_unusable_clip() {
        let clip_extents: BTreeMap<String, (String, u64, Vec<Extent>)> = [(
            "a.evo".to_string(),
            (
                "A.EVO".to_string(),
                1000u64,
                vec![Extent {
                    start_lba: 1,
                    sector_count: 1,
                }],
            ),
        )]
        .into_iter()
        .collect();
        // b.evo exists on the disc but carries an unrecorded extent.
        let unusable: std::collections::HashSet<String> =
            ["b.evo".to_string()].into_iter().collect();
        let xpl_titles = vec![XplTitle {
            number: 1,
            name: "T".to_string(),
            duration_secs: 20.0,
            clips: vec![
                XplClip {
                    evo: "a.evo".to_string(),
                    begin_secs: 0.0,
                    end_secs: 10.0,
                },
                XplClip {
                    evo: "b.evo".to_string(),
                    begin_secs: 10.0,
                    end_secs: 20.0,
                },
            ],
            chapters: vec![],
        }];
        let mut disc = MemDisc::new();
        let titles = compose_xpl_titles(&mut disc, &xpl_titles, &clip_extents, &unusable, None)
            .expect("compose");
        assert!(
            titles.is_empty(),
            "half of this title's runtime cannot be read, so composing it \
             would ship a short title as a complete one; got {:?}",
            titles
                .iter()
                .map(|t| (t.clips.len(), t.size_bytes))
                .collect::<Vec<_>>()
        );
    }

    // ── compose_xpl_titles: size/offset arithmetic ─────────────────────────

    /// Direct unit test of the arithmetic composing a title from its XPL
    /// clips: clip sizes are SUMMED (not multiplied), title-time in/out ticks
    /// are `seconds * 45000` (not divided), and `duration_secs` is
    /// `end - begin` (not `end + begin` or a division).
    #[test]
    fn compose_xpl_titles_sums_sizes_and_computes_in_out_times() {
        let clip_extents: BTreeMap<String, (String, u64, Vec<Extent>)> = [
            (
                "a.evo".to_string(),
                (
                    "A.EVO".to_string(),
                    1000u64,
                    vec![Extent {
                        start_lba: 1,
                        sector_count: 1,
                    }],
                ),
            ),
            (
                "b.evo".to_string(),
                (
                    "B.EVO".to_string(),
                    2000u64,
                    vec![Extent {
                        start_lba: 2,
                        sector_count: 1,
                    }],
                ),
            ),
        ]
        .into_iter()
        .collect();
        let xpl_titles = vec![XplTitle {
            number: 1,
            name: "T".to_string(),
            duration_secs: 10.0,
            clips: vec![
                XplClip {
                    evo: "a.evo".to_string(),
                    begin_secs: 2.0,
                    end_secs: 5.0,
                },
                XplClip {
                    evo: "b.evo".to_string(),
                    begin_secs: 5.0,
                    end_secs: 9.0,
                },
            ],
            chapters: vec![],
        }];
        let mut disc = MemDisc::new();
        let titles = compose_xpl_titles(
            &mut disc,
            &xpl_titles,
            &clip_extents,
            &std::collections::HashSet::new(),
            None,
        )
        .expect("compose");
        assert_eq!(titles.len(), 1);
        let t = &titles[0];
        assert_eq!(t.size_bytes, 3000, "clip sizes summed, not multiplied");
        assert_eq!(
            t.clips[0].in_time,
            (2.0f64 * 45000.0) as u32,
            "in_time is begin_secs * 45000, not divided"
        );
        assert_eq!(
            t.clips[0].out_time,
            (5.0f64 * 45000.0) as u32,
            "out_time is end_secs * 45000, not divided"
        );
        assert!(
            (t.clips[0].duration_secs - 3.0).abs() < 1e-9,
            "duration_secs is end_secs - begin_secs, not +/÷: got {}",
            t.clips[0].duration_secs
        );
        assert!(
            (t.clips[1].duration_secs - 4.0).abs() < 1e-9,
            "second clip's duration is also end - begin: got {}",
            t.clips[1].duration_secs
        );
    }

    /// A title's `size_bytes` is a running sum of DISC-DECLARED clip sizes: the
    /// `u64` UDF File Entry Information Length, which nothing cross-checks
    /// against the extent list that file actually occupies. Two clips can
    /// therefore each declare a size near `u64::MAX`.
    ///
    /// Summed with a plain `+=`, that overflows: a panic in a debug build —
    /// a crash on untrusted disc content, which this library must never do —
    /// and a wrap to a small garbage total in release, which misreports the
    /// title's size while looking entirely normal.
    ///
    /// Mutation: restore `size_bytes += *size` in `compose_xpl_titles` and this
    /// goes red (debug: attempt to add with overflow).
    #[test]
    fn compose_xpl_titles_saturates_absurd_disc_declared_clip_sizes() {
        let clip_extents: BTreeMap<String, (String, u64, Vec<Extent>)> = [
            (
                "a.evo".to_string(),
                (
                    "A.EVO".to_string(),
                    u64::MAX,
                    vec![Extent {
                        start_lba: 1,
                        sector_count: 1,
                    }],
                ),
            ),
            (
                "b.evo".to_string(),
                (
                    "B.EVO".to_string(),
                    u64::MAX,
                    vec![Extent {
                        start_lba: 2,
                        sector_count: 1,
                    }],
                ),
            ),
        ]
        .into_iter()
        .collect();
        let xpl_titles = vec![XplTitle {
            number: 1,
            name: "T".to_string(),
            duration_secs: 10.0,
            clips: vec![
                XplClip {
                    evo: "a.evo".to_string(),
                    begin_secs: 0.0,
                    end_secs: 5.0,
                },
                XplClip {
                    evo: "b.evo".to_string(),
                    begin_secs: 5.0,
                    end_secs: 10.0,
                },
            ],
            chapters: vec![],
        }];
        let mut disc = MemDisc::new();
        let titles = compose_xpl_titles(
            &mut disc,
            &xpl_titles,
            &clip_extents,
            &std::collections::HashSet::new(),
            None,
        )
        .expect("a hostile size field must not fail the scan either");
        assert_eq!(titles.len(), 1);
        assert_eq!(
            titles[0].size_bytes,
            u64::MAX,
            "the sum must saturate at u64::MAX, never wrap to a small number"
        );
    }

    /// A crafted Advanced-Content playlist can name the SAME `.evo` in
    /// `<PrimaryAudioVideoClip>` many times over (the XML has no fixed element
    /// count, unlike MPLS's binary PlayItem count). Each repeat must NOT push
    /// another copy of that clip's extent list onto the title — mirroring
    /// bluray.rs's `first_ref = seen_clips.insert(...)` gate (`:113`/`:117`),
    /// which pushes a clip's extents only the first time its id is seen. Here
    /// the analogous key is the `.evo` filename (this file has no clip_id;
    /// `.evo` is what `clip_extents` is keyed by). Without the gate, extents
    /// grows by the referenced clip's full extent list on EVERY repetition —
    /// unbounded by the number of on-disc files, bounded only by playlist
    /// size.
    #[test]
    fn compose_xpl_titles_dedups_repeated_clip_references_by_evo() {
        let clip_extents: BTreeMap<String, (String, u64, Vec<Extent>)> = [(
            "a.evo".to_string(),
            (
                "A.EVO".to_string(),
                1000u64,
                vec![
                    Extent {
                        start_lba: 1,
                        sector_count: 1,
                    },
                    Extent {
                        start_lba: 2,
                        sector_count: 1,
                    },
                    Extent {
                        start_lba: 3,
                        sector_count: 1,
                    },
                ],
            ),
        )]
        .into_iter()
        .collect();

        const REPEATS: usize = 5000;
        let clips: Vec<XplClip> = (0..REPEATS)
            .map(|i| XplClip {
                evo: "a.evo".to_string(),
                begin_secs: i as f64,
                end_secs: i as f64 + 1.0,
            })
            .collect();
        let xpl_titles = vec![XplTitle {
            number: 1,
            name: "T".to_string(),
            duration_secs: REPEATS as f64,
            clips,
            chapters: vec![],
        }];
        let mut disc = MemDisc::new();
        let titles = compose_xpl_titles(
            &mut disc,
            &xpl_titles,
            &clip_extents,
            &std::collections::HashSet::new(),
            None,
        )
        .expect("compose");
        assert_eq!(titles.len(), 1);
        assert_eq!(
            titles[0].extents.len(),
            3,
            "the same .evo named {REPEATS} times must contribute its 3 extents \
             ONCE, not {REPEATS} times over — got {} extents",
            titles[0].extents.len()
        );
    }

    /// Control for the de-dup above: a playlist naming several DISTINCT
    /// clips (not repeats of one) must still resolve every one of them —
    /// the de-dup key must not accidentally collapse different clips
    /// together. If this test is made to fail by weakening the de-dup key
    /// to something constant, that proves the key is doing real work rather
    /// than vacuously deduping everything.
    #[test]
    fn compose_xpl_titles_resolves_all_distinct_clips_despite_dedup() {
        let clip_extents: BTreeMap<String, (String, u64, Vec<Extent>)> = [
            (
                "a.evo".to_string(),
                (
                    "A.EVO".to_string(),
                    1000u64,
                    vec![Extent {
                        start_lba: 1,
                        sector_count: 1,
                    }],
                ),
            ),
            (
                "b.evo".to_string(),
                (
                    "B.EVO".to_string(),
                    2000u64,
                    vec![Extent {
                        start_lba: 2,
                        sector_count: 1,
                    }],
                ),
            ),
            (
                "c.evo".to_string(),
                (
                    "C.EVO".to_string(),
                    3000u64,
                    vec![Extent {
                        start_lba: 3,
                        sector_count: 1,
                    }],
                ),
            ),
        ]
        .into_iter()
        .collect();
        let xpl_titles = vec![XplTitle {
            number: 1,
            name: "T".to_string(),
            duration_secs: 30.0,
            clips: vec![
                XplClip {
                    evo: "a.evo".to_string(),
                    begin_secs: 0.0,
                    end_secs: 5.0,
                },
                XplClip {
                    evo: "b.evo".to_string(),
                    begin_secs: 5.0,
                    end_secs: 15.0,
                },
                XplClip {
                    evo: "c.evo".to_string(),
                    begin_secs: 15.0,
                    end_secs: 30.0,
                },
            ],
            chapters: vec![],
        }];
        let mut disc = MemDisc::new();
        let titles = compose_xpl_titles(
            &mut disc,
            &xpl_titles,
            &clip_extents,
            &std::collections::HashSet::new(),
            None,
        )
        .expect("compose");
        assert_eq!(titles.len(), 1);
        assert_eq!(
            titles[0].extents.len(),
            3,
            "three DISTINCT clips must all resolve — one extent each, not \
             collapsed by an over-eager de-dup key"
        );
        assert_eq!(
            titles[0].size_bytes, 6000,
            "all three distinct sizes summed"
        );
    }

    // ── Disc::scan_hddvd_titles: VTI-selection / extent-filter guards ──────

    /// A file carrying the real `ADVANCED-VTS` magic but the WRONG extension
    /// (not `.vti`) must never be adopted as the navigation file. Absent a
    /// real `.vti` file, the scan must fall back to one title per clip (no
    /// VTI-driven feature composition) rather than trusting a same-content
    /// impostor by name-agnostic magic alone.
    #[test]
    fn scan_hddvd_titles_ignores_a_vti_look_alike_with_the_wrong_extension() {
        let mut disc = MemDisc::new();
        let vti_bytes = synthetic_vti(&["FEATURE_1.EVO", "FEATURE_2.EVO"]);
        let files = vec![
            file_with("IMPOSTER.DAT", 90, 20000, vti_bytes, true),
            file("FEATURE_1.EVO", 100, 5000, 10 * 2048, true),
            file("FEATURE_2.EVO", 101, 8000, 6 * 2048, true),
        ];
        let root = DirSpec {
            name: String::new(),
            icb_lba: 10,
            dir_data_lba: 11,
            files: Vec::new(),
            subdirs: vec![DirSpec {
                name: "HVDVD_TS".to_string(),
                icb_lba: 20,
                dir_data_lba: 21,
                files,
                subdirs: vec![],
            }],
        };
        build_udf_skeleton(&mut disc, 10);
        lay_dir(&mut disc, &root);
        let udf = crate::udf::read_filesystem(&mut disc).expect("fs");

        let titles = Disc::scan_hddvd_titles(&mut disc, &udf, None).expect("scan");
        assert_eq!(
            titles.len(),
            2,
            "no real .vti present -> no VTI-driven composition, one title per clip"
        );
    }

    /// A clip whose extents cannot be resolved must drop the SPLIT FEATURE.
    ///
    /// Catches reverting the `Err(e) => { warn; unusable.insert(..) }` arm in
    /// `Disc::scan_hddvd_titles` back to the bare `Err(_) => {}` it replaced.
    ///
    /// RED BEFORE GREEN, and TWO earlier attempts at this test did NOT go red.
    /// The first asserted "no title names the broken clip" — that passes either
    /// way, because a clip that resolves to no extents is never inserted into
    /// `clip_extents` and so yields no per-clip title regardless. The second
    /// fixed that but shipped no `.vti`: `order` is built solely from
    /// `parse_vti_clip_order` of the navigation file, so with no `.vti` it is
    /// EMPTY, `feature` is empty, and no composed title is ever built — the
    /// assertion held vacuously with the fix reverted. Hence the synthetic
    /// `HVA00001.VTI` below: it is what makes the composer run at all.
    ///
    /// The defect lives one level up from the per-clip titles, in the composed
    /// feature: `unusable` is what tells the composer that a part is MISSING
    /// rather than merely absent, and the old bare `Err(_) => {}` populated it
    /// for nothing but an unrecorded extent. So a scratched sector under
    /// FEATURE_2's ICB (`Error::DiscRead`) left FEATURE_1 composing a title
    /// named "FEATURE" by itself — half a movie offered as the whole one.
    #[test]
    fn scan_hddvd_titles_drops_a_split_feature_whose_part_cannot_be_read() {
        let mut disc = MemDisc::new();
        // The VTI clip table is the ONLY source of authored order, and the
        // composed feature title exists only for clips it names. Without it
        // this test cannot distinguish the fix from its absence.
        let vti_bytes = synthetic_vti(&["FEATURE_1.EVO", "FEATURE_2.EVO"]);
        let files = vec![
            file_with("HVA00001.VTI", 90, 20000, vti_bytes, true),
            file("FEATURE_1.EVO", 100, 5000, 4 * 2048, true),
            file("FEATURE_2.EVO", 101, 9000, 4 * 2048, true),
        ];
        let root = DirSpec {
            name: String::new(),
            icb_lba: 10,
            dir_data_lba: 11,
            files: Vec::new(),
            subdirs: vec![DirSpec {
                name: "HVDVD_TS".to_string(),
                icb_lba: 20,
                dir_data_lba: 21,
                files,
                subdirs: vec![],
            }],
        };
        build_udf_skeleton(&mut disc, 10);
        lay_dir(&mut disc, &root);
        // Blank FEATURE_2's ICB (laid at PART_START + 101). Its descriptor tag
        // is then 0, neither 261 nor 266 — what the parser sees when the sector
        // holding an ICB cannot be read back intact. Deliberately NOT an
        // unrecorded extent: that class was already handled.
        disc.put_bytes(PART_START + 101, &[0u8; 2048]);
        let udf = crate::udf::read_filesystem(&mut disc).expect("fs");
        assert!(
            matches!(
                udf.file_extents(&mut disc, "/HVDVD_TS/FEATURE_2.EVO"),
                Err(crate::error::Error::DiscRead { .. })
            ),
            "fixture must fail with DiscRead, not UdfUnrecordedExtent"
        );
        // Guard the guard: if the VTI ever stopped parsing, `order` would be
        // empty and the assertion below would hold for the wrong reason.
        assert_eq!(
            parse_vti_clip_order(&synthetic_vti(&["FEATURE_1.EVO", "FEATURE_2.EVO"])),
            vec!["FEATURE_1.EVO".to_string(), "FEATURE_2.EVO".to_string()],
            "fixture VTI must yield both feature parts in authored order"
        );

        let titles = Disc::scan_hddvd_titles(&mut disc, &udf, None).expect("scan");
        // FEATURE_1 alone must not be offered as the feature. It may still
        // appear as its own standalone clip title — that stands alone and is
        // honest — but nothing may present it as the composed whole.
        let composed: Vec<_> = titles
            .iter()
            .filter(|t| t.clips.len() > 1 || t.playlist.eq_ignore_ascii_case("FEATURE"))
            .map(|t| &t.playlist)
            .collect();
        assert!(
            composed.is_empty(),
            "a split feature missing one part must not compose; got {composed:?}"
        );
    }

    /// A clip whose file has a zero-byte size (a degenerate/empty allocation:
    /// its ICB's allocation descriptor has `data_len == 0`, the UDF AD-list
    /// terminator, so `file_extents` yields no extent at all) must not
    /// produce a title. NOTE: this exercises the *upstream* `data_len == 0`
    /// terminator path in [`crate::udf::UdfFs::file_extents`], not the
    /// `sectors > 0 && lba > 0` guard in `scan_hddvd_titles` itself — with
    /// this fixture (`file_extents` never returns a `(lba, 0)` tuple, and
    /// `PART_START` unconditionally makes every resolved `lba` positive)
    /// that guard is unreachable in a divergent way; kept here as a
    /// regression check on the zero-byte-file behavior in its own right.
    #[test]
    fn scan_hddvd_titles_excludes_a_clip_with_zero_sectors() {
        let mut disc = MemDisc::new();
        let files = vec![
            file("REAL.EVO", 100, 5000, 4 * 2048, true), // ordinary, valid clip
            file("BOGUS.EVO", 101, 9000, 0, true),       // size 0 -> zero-sector extent
        ];
        let root = DirSpec {
            name: String::new(),
            icb_lba: 10,
            dir_data_lba: 11,
            files: Vec::new(),
            subdirs: vec![DirSpec {
                name: "HVDVD_TS".to_string(),
                icb_lba: 20,
                dir_data_lba: 21,
                files,
                subdirs: vec![],
            }],
        };
        build_udf_skeleton(&mut disc, 10);
        lay_dir(&mut disc, &root);
        let udf = crate::udf::read_filesystem(&mut disc).expect("fs");

        let titles = Disc::scan_hddvd_titles(&mut disc, &udf, None).expect("scan");
        assert_eq!(
            titles.len(),
            1,
            "the zero-sector clip must not produce a title"
        );
        assert_eq!(titles[0].playlist, "REAL.EVO");
    }

    // ── probe_evo_streams: sector-cursor bookkeeping ───────────────────────

    /// A zero-sector extent must be skipped outright, never entering the read
    /// loop — `left` (an unsigned sector count) starting at 0 must gate the
    /// loop closed. (A `left >= 0` tautology here would spin forever: `n`
    /// would be pinned at 0, so neither `lba`, `left`, nor `remaining` would
    /// ever change — a non-terminating loop reachable from a crafted extent
    /// list.)
    #[test]
    fn probe_evo_streams_skips_a_zero_sector_extent_without_reading() {
        let mut disc = MemDisc::new();
        let extent = Extent {
            start_lba: 500_000,
            sector_count: 0,
        };
        let streams =
            probe_evo_streams(&mut disc, std::slice::from_ref(&extent), None).expect("probe");
        assert!(streams.is_empty(), "a zero-sector extent yields no streams");
    }

    /// The read cursor must advance FORWARD by each chunk's sector count, not
    /// backward — real content living only in the second 512-sector (1 MiB)
    /// chunk must be reached.
    #[test]
    fn probe_evo_streams_advances_lba_forward_across_chunk_reads() {
        let mut disc = MemDisc::new();
        let start_lba = 100_000u32;
        // First chunk (512 sectors): inert filler, no start codes.
        disc.put_bytes(start_lba, &vec![0x55u8; 512 * 2048]);
        // Second chunk: the real EVO content (H.264 video PES).
        let evo = synthetic_evo();
        disc.put_bytes(start_lba + 512, &evo);
        let extent = Extent {
            start_lba,
            sector_count: 512 + (evo.len() as u32).div_ceil(2048),
        };

        let streams =
            probe_evo_streams(&mut disc, std::slice::from_ref(&extent), None).expect("probe");
        let has_h264 = streams
            .iter()
            .any(|s| matches!(s, Stream::Video(v) if v.codec == Codec::H264));
        assert!(
            has_h264,
            "the second 1 MiB chunk must be read from the correct (forward) LBA"
        );
    }

    /// The read loop must stop at the extent's DECLARED `sector_count` — data
    /// living just past it must never be read (a buffer over-read past the
    /// caller-supplied extent bound, on untrusted disc-layout input).
    #[test]
    fn probe_evo_streams_stops_reading_at_the_extents_declared_sector_count() {
        let mut disc = MemDisc::new();
        let start_lba = 200_000u32;
        let declared_sectors = 4u32;
        disc.put_bytes(start_lba, &vec![0x55u8; declared_sectors as usize * 2048]);
        // Real H.264 PES data placed just PAST the declared extent — must
        // never be read.
        let evo = synthetic_evo();
        disc.put_bytes(start_lba + declared_sectors, &evo);
        let extent = Extent {
            start_lba,
            sector_count: declared_sectors,
        };

        let streams =
            probe_evo_streams(&mut disc, std::slice::from_ref(&extent), None).expect("probe");
        let has_h264 = streams
            .iter()
            .any(|s| matches!(s, Stream::Video(v) if v.codec == Codec::H264));
        assert!(
            !has_h264,
            "must not read past the extent's declared sector_count"
        );
    }

    /// The total read budget (`EVO_PROBE_SECTORS`) must be enforced ACROSS
    /// extents, not just within one — once it is exhausted by an earlier
    /// extent, a later extent in the same probe must not be read at all.
    #[test]
    fn probe_evo_streams_caps_total_reads_across_extents_at_evo_probe_sectors() {
        let mut disc = MemDisc::new();
        let first_lba = 300_000u32;
        disc.put_bytes(first_lba, &vec![0x55u8; EVO_PROBE_SECTORS as usize * 2048]);
        // A second extent, following the first in the extents list: once the
        // whole EVO_PROBE_SECTORS budget is spent on the first, this must
        // never be reached.
        let second_lba = first_lba + EVO_PROBE_SECTORS;
        let evo = synthetic_evo();
        disc.put_bytes(second_lba, &evo);

        let extents = vec![
            Extent {
                start_lba: first_lba,
                sector_count: EVO_PROBE_SECTORS,
            },
            Extent {
                start_lba: second_lba,
                sector_count: 10,
            },
        ];
        let streams = probe_evo_streams(&mut disc, &extents, None).expect("probe");
        let has_h264 = streams
            .iter()
            .any(|s| matches!(s, Stream::Video(v) if v.codec == Codec::H264));
        assert!(
            !has_h264,
            "must not read past the total EVO_PROBE_SECTORS budget across extents"
        );
    }

    // ── clip-name fallback: probe amplification ────────────────────────────

    /// A [`SectorSource`] wrapper that counts how many reads START at each
    /// watched LBA, plus the total sectors pulled off the medium. A probe of a
    /// clip always issues its first read at the clip's first extent LBA, so the
    /// hit count for that LBA IS the number of `probe_evo_streams` passes over
    /// that clip.
    struct ProbeCounter {
        inner: MemDisc,
        watch: Vec<u32>,
        hits: Vec<u32>,
        sectors_read: u64,
    }

    impl ProbeCounter {
        fn new(inner: MemDisc, watch: Vec<u32>) -> Self {
            let hits = vec![0; watch.len()];
            Self {
                inner,
                watch,
                hits,
                sectors_read: 0,
            }
        }
    }

    impl SectorSource for ProbeCounter {
        fn read_sectors(
            &mut self,
            lba: u32,
            count: u16,
            buf: &mut [u8],
            recovery: bool,
        ) -> crate::error::Result<usize> {
            if let Some(i) = self.watch.iter().position(|w| *w == lba) {
                self.hits[i] += 1;
            }
            self.sectors_read += count as u64;
            self.inner.read_sectors(lba, count, buf, recovery)
        }
    }

    /// Lay an `HVDVD_TS/` holding exactly the given `(name, icb_lba, data_lba,
    /// size)` clips — unlike `make_hddvd_fs`, the caller chooses each entry's
    /// ICB, so many names can be pointed at ONE File Entry.
    fn lay_hddvd_clips(disc: &mut MemDisc, specs: Vec<crate::udf::fixture::FileSpec>) {
        let root = DirSpec {
            name: String::new(),
            icb_lba: 10,
            dir_data_lba: 11,
            files: Vec::new(),
            subdirs: vec![DirSpec {
                name: "HVDVD_TS".to_string(),
                icb_lba: 20,
                dir_data_lba: 21,
                files: specs,
                subdirs: vec![],
            }],
        };
        build_udf_skeleton(disc, 10);
        lay_dir(disc, &root);
    }

    /// THE THIRD AMPLIFICATION AXIS on HD-DVD title composition, past both
    /// existing guards. `xpl_depth_within_limit` bounds XML NESTING;
    /// `MAX_XPL_TITLES` + the `.evo` de-dup bound what an XPL can DECLARE. Both
    /// live on the playlist path — and an attacker simply omits `/ADV_OBJ`, so
    /// `read_adv_obj_xpl` returns `None` and neither guard is ever consulted.
    /// Control then reaches the clip-name FALLBACK, which emits one title per
    /// `.evo` DIRECTORY ENTRY and probes each.
    ///
    /// Nothing de-duplicates a FID's ICB LBA, so every name in a 1 MiB
    /// directory (`udf::MAX_DIR_BYTES`, ~24,000 FIDs) can point at ONE File
    /// Entry. Each resolves to the SAME extents and each costs a full
    /// `EVO_PROBE_SECTORS` (16 MiB) probe: ~375 GiB of optical reads from a
    /// directory that describes a single file.
    ///
    /// Memoizing the probe on the resolved extent list collapses that to ONE
    /// probe, because all of those names resolve to one extent list.
    ///
    /// Mutation: drop the memo lookup in `EvoProbeCache::streams` (always
    /// probe) and this goes red at `ENTRIES` probes.
    #[test]
    fn scan_hddvd_titles_probes_once_for_entries_resolving_to_identical_extents() {
        const ENTRIES: usize = 512;
        const SHARED_ICB: u32 = 100;
        const SHARED_DATA: u32 = 5000;

        let mut disc = MemDisc::new();
        // Every FID names a different file and points at the SAME File Entry.
        let specs: Vec<_> = (0..ENTRIES)
            .map(|i| {
                file(
                    &format!("C{i:04}.EVO"),
                    SHARED_ICB,
                    SHARED_DATA,
                    4 * 2048,
                    true,
                )
            })
            .collect();
        lay_hddvd_clips(&mut disc, specs);
        let udf = crate::udf::read_filesystem(&mut disc).expect("fs");

        let probe_lba = PART_START + SHARED_DATA;
        let mut counter = ProbeCounter::new(disc, vec![probe_lba]);
        let titles = Disc::scan_hddvd_titles(&mut counter, &udf, None).expect("scan");

        assert_eq!(
            counter.hits[0], 1,
            "{ENTRIES} directory entries all resolving to the SAME extents must \
             cost ONE probe_evo_streams pass, not one per entry; got {} probes",
            counter.hits[0]
        );
        // And the whole scan's read volume stays bounded — the real currency
        // here is drive time, not title count.
        assert!(
            counter.sectors_read < 4 * ENTRIES as u64,
            "total sectors read must not scale with the entry count, got {}",
            counter.sectors_read
        );
        assert!(!titles.is_empty(), "the clips still enumerate");
    }

    /// The clip cap and the per-probe read budget are ONE bound, not two, and
    /// only their product is meaningful: `MAX_HDDVD_CLIPS` probes each cost up
    /// to `EVO_PROBE_SECTORS`, so the pair fixes the worst-case drive time of a
    /// `Disc::scan()`.
    ///
    /// `EvoProbeCache` does not help against a crafted disc here: it keys on the
    /// RESOLVED extent list, and distinct-but-overlapping extent lists all
    /// reading one physical region miss the memo every time while every read
    /// succeeds. So the product is genuinely reachable, and it is the number
    /// that has to stay within what a scan can absorb.
    ///
    /// 8 GiB is the ceiling asserted here. It is far above any genuine disc —
    /// retail HD-DVDs carry tens of `.evo` clips, i.e. well under 1 GiB of
    /// probing — and far below the 64 GiB the pair used to permit, which
    /// exceeded the capacity of the dual-layer media this format tops out at
    /// and took tens of minutes of optical reads.
    ///
    /// This is a relationship, not a magic number: raising EITHER constant
    /// without re-examining the other trips it.
    #[test]
    fn the_clip_cap_and_probe_budget_bound_a_scans_worst_case_read_volume() {
        const CEILING_BYTES: u64 = 8 * 1024 * 1024 * 1024;
        let budget = |cap: usize| {
            cap as u64 * u64::from(EVO_PROBE_SECTORS) * crate::consts::SECTOR_BYTES as u64
        };
        // BOTH title-composition paths pay one probe per item, and a crafted
        // disc reaches either one: the directory fallback when `/ADV_OBJ` is
        // omitted, the playlist path when it is present. Capping only one moves
        // the amplification next door.
        for (name, cap) in [
            ("MAX_HDDVD_CLIPS", MAX_HDDVD_CLIPS),
            ("MAX_XPL_TITLES", MAX_XPL_TITLES),
        ] {
            let worst_case = budget(cap);
            assert!(
                worst_case <= CEILING_BYTES,
                "{name} ({cap}) x EVO_PROBE_SECTORS ({EVO_PROBE_SECTORS}) = \
                 {worst_case} bytes of probe reads, over the {CEILING_BYTES}-byte \
                 ceiling a scan can absorb"
            );
        }
    }

    /// CONTROL: memoization must not silently collapse DISTINCT clips. A
    /// legitimate disc with several different `.evo` files still probes each
    /// one, and each title keeps the streams of ITS OWN clip.
    ///
    /// Mutation: make `EvoProbeCache`'s key a constant (ignore the extents) and
    /// this goes red — the junk clip inherits the first clip's H.264 streams.
    #[test]
    fn scan_hddvd_titles_still_probes_each_distinct_clip() {
        let mut disc = MemDisc::new();
        let evo = synthetic_evo();
        let junk = vec![0x55u8; 4 * 2048];
        let (a_data, b_data, c_data) = (5000u32, 6000u32, 7000u32);
        lay_hddvd_clips(
            &mut disc,
            vec![
                file_with("A_REAL.EVO", 100, a_data, evo.clone(), true),
                file_with("B_JUNK.EVO", 101, b_data, junk, true),
                file_with("C_REAL.EVO", 102, c_data, evo, true),
            ],
        );
        let udf = crate::udf::read_filesystem(&mut disc).expect("fs");

        let watch = vec![
            PART_START + a_data,
            PART_START + b_data,
            PART_START + c_data,
        ];
        let mut counter = ProbeCounter::new(disc, watch);
        let titles = Disc::scan_hddvd_titles(&mut counter, &udf, None).expect("scan");

        assert_eq!(
            titles.len(),
            3,
            "every distinct clip still resolves a title"
        );

        // The substantive assertion first: each title carries the streams of ITS
        // OWN clip. This is what a wrong memo key actually costs.
        let by_name = |n: &str| titles.iter().find(|t| t.playlist == n).expect(n);
        assert!(
            by_name("A_REAL.EVO")
                .streams
                .iter()
                .any(|s| matches!(s, Stream::Video(v) if v.codec == Codec::H264)),
            "the real clip keeps its own probed streams"
        );
        assert!(
            by_name("B_JUNK.EVO").streams.is_empty(),
            "the unrecognizable clip must NOT inherit another clip's streams"
        );
        assert!(
            by_name("C_REAL.EVO")
                .streams
                .iter()
                .any(|s| matches!(s, Stream::Video(v) if v.codec == Codec::H264)),
            "a third distinct clip is probed on its own extents"
        );

        assert_eq!(
            counter.hits,
            vec![1, 1, 1],
            "each DISTINCT clip is still probed exactly once"
        );
    }

    /// Memoization alone is NOT the whole fix. Collapsing identical extent
    /// lists bounds the one-File-Entry attack, but distinct extent lists are
    /// only as bounded as the File Entries an attacker cares to lay down — and
    /// a File Entry is ONE sector. A 1 MiB directory's ~24,000 FIDs can each
    /// point at their own 1-sector File Entry (a ~48 MiB image) declaring its
    /// own huge extent, so every probe misses the memo and the amplification is
    /// back. [`MAX_HDDVD_CLIPS`] is the bound that closes that.
    ///
    /// Mutation: drop the `clips.len() < MAX_HDDVD_CLIPS` gate in the directory
    /// scan and this goes red at `ENTRIES`.
    #[test]
    fn scan_hddvd_titles_caps_a_directory_declaring_absurdly_many_clips() {
        const ENTRIES: usize = MAX_HDDVD_CLIPS + 300;

        let mut disc = MemDisc::new();
        // Each entry gets its OWN File Entry and its OWN data extent, so no two
        // resolve to the same extent list and the memo never hits.
        let specs: Vec<_> = (0..ENTRIES)
            .map(|i| {
                // ICBs live far past the directory's own FID data (~230 KB
                // from LBA 21) so laying the FIDs cannot clobber them.
                file(
                    &format!("C{i:05}.EVO"),
                    100_000 + i as u32,
                    1_000_000 + i as u32 * 8,
                    2048,
                    true,
                )
            })
            .collect();
        lay_hddvd_clips(&mut disc, specs);
        let udf = crate::udf::read_filesystem(&mut disc).expect("fs");

        let titles = Disc::scan_hddvd_titles(&mut disc, &udf, None).expect("scan");
        assert_eq!(
            titles.len(),
            MAX_HDDVD_CLIPS,
            "a directory declaring {ENTRIES} clips must be capped at \
             {MAX_HDDVD_CLIPS}; each surviving clip costs a real \
             probe_evo_streams pass over the medium"
        );
    }

    /// The control for the cap: a realistic disc keeps every clip it carries.
    #[test]
    fn scan_hddvd_titles_keeps_every_clip_of_a_realistic_disc() {
        let mut disc = MemDisc::new();
        let udf = make_hddvd_fs(
            &mut disc,
            &[
                ("FEATURE_1.EVO", 2000, 5000),
                ("FEATURE_2.EVO", 1800, 9000),
                ("TRAILER.EVO", 300, 12000),
                ("DELOGO.EVO", 40, 13000),
            ],
        );
        let titles = Disc::scan_hddvd_titles(&mut disc, &udf, None).expect("scan");
        assert_eq!(
            titles.len(),
            4,
            "a real disc's clips must survive the cap untouched"
        );
    }
}
