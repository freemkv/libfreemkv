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
//! Scope today: enumerate the `.evo` clips and yield one [`DiscTitle`] per clip
//! (container [`ContentFormat::MpegPs`], so the existing PS mux path handles it).
//! Per-clip streams ARE enumerated: the clip head is demuxed through the PS
//! demuxer and one [`Stream`] is built per distinct elementary stream (video +
//! DD+ audio sub-streams), with the codec sniffed from the ES bytes — this is
//! what the mux path needs to route packets.
//!
//! What is NOT parsed yet — and is honestly stubbed, not faked:
//!   * `.xpl` playlist ordering (title composition / chapters, FEATURE_1+2 join),
//!   * `.map` timemap → real durations,
//!   * subtitles (8-bit RLC on `0xBD` sub `0x20..=0x3F`).
//!
//! Extents and size ARE real (the ripper needs those to image a clip); durations
//! and chapters are left empty rather than guessed.

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
            i += 3;
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
fn probe_evo_streams(reader: &mut dyn SectorSource, extents: &[Extent]) -> Vec<Stream> {
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
            let n = left.min(remaining).min(512) as u16;
            let mut buf = vec![0u8; n as usize * crate::consts::SECTOR_BYTES];
            if reader.read_sectors(lba, n, &mut buf, false).is_err() {
                break 'outer;
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
    if let Some(pid) = video_pid {
        // Default to H.264 when a video PES was seen but the codec could not be
        // sniffed from the sampled head — the demux found video, just no
        // recognizable start code yet; dropping it would leave the title with no
        // video track and fail the mux.
        let codec = sniff_video_codec(&video).unwrap_or(Codec::H264);
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
    streams
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
    match pkt.stream_id {
        // Plain MPEG video (0xE0-0xEF), or the HD-DVD extended-stream-id (0xFD)
        // that carries VC-1 video. Both feed the single video ES sample; the
        // routing PID comes from `PsPacket::dvd_pid` so it matches the demuxer.
        VIDEO..=VIDEO_MAX | EXTENDED_STREAM_ID => {
            if video_pid.is_none() {
                *video_pid = pkt.dvd_pid();
            }
            if video.len() < EVO_ES_SAMPLE_CAP {
                video.extend_from_slice(&pkt.data);
            }
        }
        PRIVATE_STREAM_1 => {
            if let Some(sub) = pkt.sub_stream_id {
                if (0xC0..=0xC7).contains(&sub) {
                    let slot = audio.entry(sub).or_default();
                    if slot.len() < EVO_ES_SAMPLE_CAP {
                        slot.extend_from_slice(&pkt.data);
                    }
                }
            }
        }
        _ => {}
    }
}

impl Disc {
    /// Scan HD-DVD titles from the `HVDVD_TS/` `.evo` clips.
    ///
    /// One [`DiscTitle`] per `.evo` with real physical extents (from the UDF
    /// allocation descriptors) and declared size; `streams`/`chapters`/duration
    /// are left empty pending `.xpl`/EVO parsing (see module docs). Returns an
    /// empty vec when `HVDVD_TS/` is absent or holds no readable `.evo`.
    pub(super) fn scan_hddvd_titles(
        reader: &mut dyn SectorSource,
        udf_fs: &udf::UdfFs,
    ) -> Vec<DiscTitle> {
        let mut titles = Vec::new();
        let Some(ts_dir) = udf_fs.find_dir("/HVDVD_TS") else {
            return titles;
        };
        // Snapshot clip (name, size) first: the `ts_dir` borrow must end before
        // the `udf_fs.file_extents` calls below re-borrow `udf_fs`.
        let clips: Vec<(String, u64)> = ts_dir
            .entries
            .iter()
            .filter(|e| !e.is_dir && e.name.to_ascii_lowercase().ends_with(HDDVD_CLIP_EXT))
            .map(|e| (e.name.clone(), e.size))
            .collect();

        for (idx, (name, size)) in clips.iter().enumerate() {
            let path = format!("/HVDVD_TS/{name}");
            let mut extents = Vec::new();
            if let Ok(file_exts) = udf_fs.file_extents(reader, &path) {
                for (lba, sectors) in file_exts {
                    if sectors > 0 && lba > 0 {
                        extents.push(Extent {
                            start_lba: lba,
                            sector_count: sectors,
                        });
                    }
                }
            }
            if extents.is_empty() {
                continue;
            }
            // Probe the clip head for its elementary streams so the mux path
            // builds a non-empty `pid_to_track` and actually routes packets.
            // Without this the PS demuxer drops every packet and the mux emits
            // nothing (the historical HD-DVD blocker).
            let streams = probe_evo_streams(reader, &extents);
            let clip_id = name
                .rsplit_once('.')
                .map(|(base, _)| base.to_string())
                .unwrap_or_else(|| name.clone());
            titles.push(DiscTitle {
                playlist: name.clone(),
                playlist_id: idx as u16,
                duration_secs: 0.0,
                size_bytes: *size,
                clips: vec![Clip {
                    clip_id,
                    in_time: 0,
                    out_time: 0,
                    duration_secs: 0.0,
                    source_packets: 0,
                }],
                streams,
                chapters: Vec::new(),
                extents,
                content_format: ContentFormat::MpegPs,
                codec_privates: Vec::new(),
            });
        }
        titles
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::udf::fixture::*;

    /// Build a UDF with an `HVDVD_TS/` tree holding the listed `.evo` clips
    /// (name, sector count, data LBA).
    fn make_hddvd_fs(disc: &mut MemDisc, evos: &[(&str, u32, u32)]) -> crate::udf::UdfFs {
        let mut files = Vec::new();
        let mut icb = 100u32;
        for (name, sectors, data_lba) in evos {
            files.push(file(name, icb, *data_lba, sectors * 2048, true));
            icb += 1;
        }
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
        let titles = Disc::scan_hddvd_titles(&mut disc, &udf);
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
    }

    #[test]
    fn sniff_audio_codec_recognizes_eac3_syncword() {
        assert_eq!(
            sniff_audio_codec(&[0x00, 0x0B, 0x77, 0x12, 0x34]),
            Some(Codec::Ac3Plus)
        );
        assert_eq!(sniff_audio_codec(&[0x00, 0x01, 0x02, 0x03]), None);
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

    /// End-to-end: scanning an `.evo` whose head carries an H.264 video PES and
    /// two DD+ audio PES yields a title with the video track (canonical
    /// DVD_VIDEO_PID) and both DD+ tracks (0xBDC0 / 0xBDC1) — the non-empty
    /// `streams` the mux path needs to route packets (the historical blocker).
    #[test]
    fn scan_hddvd_titles_probes_streams_from_evo_head() {
        let mut disc = MemDisc::new();
        let udf = make_hddvd_fs_with_evo(&mut disc, &synthetic_evo());
        let titles = Disc::scan_hddvd_titles(&mut disc, &udf);
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
        let titles = Disc::scan_hddvd_titles(&mut disc, &udf);
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

    /// End-to-end: an `.evo` whose video rides the extended-stream-id 0xFD yields
    /// a VC-1 video track routed to `0xFD00 | ext` (0xFD55) — the PID the demuxer
    /// derives from the same stream_id_extension, so mux-time routing lines up.
    #[test]
    fn scan_hddvd_titles_probes_vc1_on_extended_stream_id() {
        let mut disc = MemDisc::new();
        let udf = make_hddvd_fs_with_evo(&mut disc, &synthetic_evo_vc1());
        let titles = Disc::scan_hddvd_titles(&mut disc, &udf);
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
}
