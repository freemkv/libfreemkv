use super::*;
use crate::udf::fixture::*;
// MPLS builder (BD-ROM PlayList spec). Mirrors the layout `mpls::parse`
// reads (header@0, PlayList@playlist_start, PlayListMark@mark_start).

struct PiSpec {
    clip_id: [u8; 5],
    in_time: u32,
    out_time: u32,
}

struct MarkSpec {
    mark_type: u8,
    play_item_ref: u16,
    timestamp: u32,
}

/// One STN stream entry: stream_entry (len(1)=3, type(1)=0x01, pid(2))
/// plus stream_attributes (len(1) + coding_type(1) + payload). Matches
/// the mpls.rs test builders.
fn se_video(pid: u16, coding_type: u8) -> Vec<u8> {
    let mut out = vec![3u8, 0x01];
    out.extend_from_slice(&pid.to_be_bytes());
    let attrs = vec![coding_type, 0x10]; // format/rate nibbles
    out.push(attrs.len() as u8);
    out.extend_from_slice(&attrs);
    out
}
fn se_audio(pid: u16, coding_type: u8, lang: &[u8; 3]) -> Vec<u8> {
    let mut out = vec![3u8, 0x01];
    out.extend_from_slice(&pid.to_be_bytes());
    // PGS in an audio slot uses PG layout (coding_type + lang(3)); the
    // builder only needs the non-PGS audio layout here.
    let attrs = vec![coding_type, 0x21, lang[0], lang[1], lang[2]];
    out.push(attrs.len() as u8);
    out.extend_from_slice(&attrs);
    out
}
fn se_pg(pid: u16, coding_type: u8, lang: &[u8; 3]) -> Vec<u8> {
    let mut out = vec![3u8, 0x01];
    out.extend_from_slice(&pid.to_be_bytes());
    let attrs = vec![coding_type, lang[0], lang[1], lang[2]];
    out.push(attrs.len() as u8);
    out.extend_from_slice(&attrs);
    out
}
/// HEVC video stream entry carrying the third (HDR) attribute byte:
/// high nibble = dynamic_range, low nibble = color_space (mpls.rs only
/// parses this byte for coding_type == HEVC and sa.len() > 2).
fn se_video_hevc(pid: u16, dynamic_range: u8, color_space: u8) -> Vec<u8> {
    se_video_hevc_flags(pid, dynamic_range, color_space, None)
}

// HEVC entry with an optional fourth attribute byte (cr_flag / hdr_plus_flag).
fn se_video_hevc_flags(pid: u16, dynamic_range: u8, color_space: u8, flags: Option<u8>) -> Vec<u8> {
    let mut out = vec![3u8, 0x01];
    out.extend_from_slice(&pid.to_be_bytes());
    let hdr_byte = (dynamic_range << 4) | color_space;
    let mut attrs = vec![0x24u8, 0x10, hdr_byte]; // coding_type = HEVC
    attrs.extend(flags);
    out.push(attrs.len() as u8);
    out.extend_from_slice(&attrs);
    out
}

/// Build an MPLS playlist. `stn_counts` = (video, audio, pg, ig,
/// sec_audio, sec_video, pip_pg, dv); `stream_entries` are appended on
/// the FIRST play item in that order.
fn build_mpls(
    items: &[PiSpec],
    stn_counts: (u8, u8, u8, u8, u8, u8, u8, u8),
    stream_entries: &[Vec<u8>],
    marks: &[MarkSpec],
) -> Vec<u8> {
    let playlist_start: u32 = 40;
    let mut buf = Vec::new();
    buf.extend_from_slice(b"MPLS0200"); // type+version
    buf.extend_from_slice(&playlist_start.to_be_bytes()); // [8..12]
    buf.extend_from_slice(&[0u8; 28]); // mark_start placeholder + pad to 40

    // PlayList section: length(4) + reserved(2) + num_play_items(2)
    // + num_sub_paths(2) header.
    let pl_start = buf.len();
    buf.extend_from_slice(&[0u8; 4]); // length placeholder
    buf.extend_from_slice(&[0u8; 2]); // reserved
    buf.extend_from_slice(&(items.len() as u16).to_be_bytes());
    buf.extend_from_slice(&[0u8; 2]); // num_sub_paths

    for (idx, pi) in items.iter().enumerate() {
        let mut item = Vec::new();
        item.extend_from_slice(&pi.clip_id); // [0..5]
        item.extend_from_slice(b"M2TS"); // [5..9] codec_id
        item.push(0); // [9] reserved
        item.push(0); // [10] is_multi_angle (bit 4) + connection_condition (low nibble)
        item.push(0); // [11] stc_id / reserved
        item.extend_from_slice(&pi.in_time.to_be_bytes()); // [12..16]
        item.extend_from_slice(&pi.out_time.to_be_bytes()); // [16..20]
        item.extend_from_slice(&[0u8; 8]); // [20..28] UO_mask
        item.push(0); // [28] misc
        item.push(0); // [29] still_mode
        item.extend_from_slice(&[0u8; 2]); // [30..32] still_time
        if idx == 0 {
            // STN table: length(2)+reserved(2)+counts(8)+reserved(4).
            let stn_start = item.len();
            item.extend_from_slice(&[0u8; 2]); // length placeholder
            item.extend_from_slice(&[0u8; 2]); // reserved
            item.push(stn_counts.0);
            item.push(stn_counts.1);
            item.push(stn_counts.2);
            item.push(stn_counts.3);
            item.push(stn_counts.4);
            item.push(stn_counts.5);
            item.push(stn_counts.6);
            item.push(stn_counts.7);
            item.extend_from_slice(&[0u8; 4]); // reserved
            for se in stream_entries {
                item.extend_from_slice(se);
            }
            let stn_len = (item.len() - stn_start - 2) as u16;
            item[stn_start..stn_start + 2].copy_from_slice(&stn_len.to_be_bytes());
        }
        buf.extend_from_slice(&(item.len() as u16).to_be_bytes());
        buf.extend_from_slice(&item);
    }

    let pl_len = (buf.len() - pl_start - 4) as u32;
    buf[pl_start..pl_start + 4].copy_from_slice(&pl_len.to_be_bytes());

    // PlayListMark section.
    let mark_start = buf.len() as u32;
    buf[12..16].copy_from_slice(&mark_start.to_be_bytes());
    let mark_section_len = 2 + marks.len() * 14;
    buf.extend_from_slice(&(mark_section_len as u32).to_be_bytes());
    buf.extend_from_slice(&(marks.len() as u16).to_be_bytes());
    for m in marks {
        buf.push(0); // [0] reserved
        buf.push(m.mark_type); // [1] mark_type
        buf.extend_from_slice(&m.play_item_ref.to_be_bytes()); // [2..4]
        buf.extend_from_slice(&m.timestamp.to_be_bytes()); // [4..8]
        buf.extend_from_slice(&[0u8; 6]); // [8..14] PID + duration
    }
    buf
}

// CLPI builder. `clpi::parse` reads "HDMV" magic, prog_info_start@12,
// cpi_start@16, source_packet_count@56. Zeroing prog_info/cpi disables them.

fn build_clpi(source_packet_count: u32) -> Vec<u8> {
    let mut d = vec![0u8; 60];
    d[0..4].copy_from_slice(b"HDMV");
    d[4..8].copy_from_slice(b"0200");
    // seq_info_start/prog_info_start/cpi_start all 0 → skipped.
    d[56..60].copy_from_slice(&source_packet_count.to_be_bytes());
    d
}

// ---------------------------------------------------------------
// Tests: parse_playlist
// ---------------------------------------------------------------

/// A playlist whose summed PlayItem duration is < 30 s is a menu /
/// clip-info stub and must be dropped (bluray.rs: `duration_secs <
/// 30.0 → None`). 45000 ticks/s timebase: 29 s = 1_305_000 ticks.
#[test]
fn parse_playlist_drops_under_30_seconds() {
    let mut disc = MemDisc::new();
    let mpls = build_mpls(
        &[PiSpec {
            clip_id: *b"00001",
            in_time: 0,
            out_time: 29 * 45000, // 29 s < 30 s threshold
        }],
        (0, 0, 0, 0, 0, 0, 0, 0),
        &[],
        &[],
    );
    let udf = make_min_fs(&mut disc);
    assert!(
        Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls)
            .expect("scan")
            .is_none(),
        "playlists shorter than 30s must be skipped"
    );
}

// At exactly 30s the playlist is kept (`< 30.0` is strict); uses a fully wired BDMV so the
// boundary, not an unresolvable clip, is what's pinned.
#[test]
fn parse_playlist_keeps_exactly_30_seconds() {
    let mut disc = MemDisc::new();
    let mpls = build_mpls(
        &[PiSpec {
            clip_id: *b"00001",
            in_time: 0,
            out_time: 30 * 45000,
        }],
        (0, 0, 0, 0, 0, 0, 0, 0),
        &[],
        &[],
    );
    let udf = make_bdmv_fs(&mut disc, &[("00001", 100, 400, 5000)]);
    let t = Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls)
        .expect("scan")
        .expect("30s playlist must be kept");
    assert!((t.duration_secs - 30.0).abs() < 1e-6);
}

/// Garbage that isn't an MPLS must yield None (parse error path), not
/// panic. mpls::parse rejects on missing "MPLS" magic.
#[test]
fn parse_playlist_rejects_non_mpls() {
    let mut disc = MemDisc::new();
    let udf = make_min_fs(&mut disc);
    let junk = vec![0u8; 100];
    let (t, events) = crate::testlog::capture(|| {
        Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &junk).expect("scan")
    });
    assert!(t.is_none());
    let line = events
        .iter()
        .find(|e| e.target == "freemkv::disc")
        .unwrap_or_else(|| panic!("a dropped playlist must be logged; got {events:?}"));
    assert_eq!(line.level, tracing::Level::WARN);
    assert_eq!(line.message(), format!("E{}", crate::error::E_MPLS_PARSE));
}

/// Build the minimal fixture: an empty `BDMV/` directory (no STREAM,
/// no CLPINF), returning the navigable UdfFs plus a populated disc.
fn make_min_fs(disc: &mut MemDisc) -> udf::UdfFs {
    // Empty BDMV/PLAYLIST so directory navigation in parse_playlist's
    // clip lookups still works even when no clip files exist.
    let root = DirSpec {
        name: String::new(),
        icb_lba: 10,
        dir_data_lba: 11,
        files: Vec::new(),
        subdirs: vec![DirSpec {
            name: "BDMV".to_string(),
            icb_lba: 12,
            dir_data_lba: 13,
            files: Vec::new(),
            subdirs: vec![],
        }],
    };
    build_udf_skeleton(disc, 10);
    lay_dir(disc, &root);
    udf::read_filesystem(disc).expect("fs")
}

/// Full BDMV with STREAM + CLPINF for the listed clip ids. Each clip's
/// .m2ts gets a Long-AD ICB with `sectors` sectors at a distinct LBA;
/// each .clpi declares `packets` source packets. Returns the UdfFs.
fn make_bdmv_fs(
    disc: &mut MemDisc,
    clips: &[(
        &str,
        u32, /*sectors*/
        u32, /*packets*/
        u32, /*data_lba*/
    )],
) -> udf::UdfFs {
    make_bdmv_fs_ext(disc, clips, "m2ts")
}

/// As [`make_bdmv_fs`] but the STREAM file carries `stream_ext` instead of
/// `.m2ts` (e.g. "fmts" for an AACS 2.1 feature clip, "ssif" for 3D) — drives
/// the [`CLIP_STREAM_EXTS`] fallback in `parse_playlist`.
fn make_bdmv_fs_ext(
    disc: &mut MemDisc,
    clips: &[(
        &str,
        u32, /*sectors*/
        u32, /*packets*/
        u32, /*data_lba*/
    )],
    stream_ext: &str,
) -> udf::UdfFs {
    // Layout LBAs: pick widely separated values to avoid collisions.
    let mut stream_files = Vec::new();
    let mut clipinf_files = Vec::new();
    let mut icb = 100u32;
    for (name, sectors, packets, data_lba) in clips {
        let m2ts = format!("{name}.{stream_ext}");
        // Size in bytes — file_extents derives sectors via div_ceil(2048).
        let size = sectors * 2048;
        stream_files.push(file(&m2ts, icb, *data_lba, size as u64, true));
        icb += 1;
        let clpi = format!("{name}.clpi");
        clipinf_files.push(file_with(
            &clpi,
            icb,
            *data_lba + 1000,
            build_clpi(*packets),
            false,
        ));
        icb += 1;
    }
    let bdmv = DirSpec {
        name: "BDMV".to_string(),
        icb_lba: 20,
        dir_data_lba: 21,
        files: Vec::new(),
        subdirs: vec![
            DirSpec {
                name: "STREAM".to_string(),
                icb_lba: 22,
                dir_data_lba: 23,
                files: stream_files,
                subdirs: vec![],
            },
            DirSpec {
                name: "CLIPINF".to_string(),
                icb_lba: 24,
                dir_data_lba: 25,
                files: clipinf_files,
                subdirs: vec![],
            },
        ],
    };
    let root = DirSpec {
        name: String::new(),
        icb_lba: 10,
        dir_data_lba: 11,
        files: Vec::new(),
        subdirs: vec![bdmv],
    };
    build_udf_skeleton(disc, 10);
    lay_dir(disc, &root);
    udf::read_filesystem(disc).expect("fs")
}

// BDMV with a real 3D layout: `.ssif` under BDMV/STREAM/SSIF/<clip>.ssif (unlike
// make_bdmv_fs_ext) plus a matching.clpi — resolving it latches `is_3d = true`.
fn make_bdmv_fs_ssif(
    disc: &mut MemDisc,
    clips: &[(
        &str,
        u32, /*sectors*/
        u32, /*packets*/
        u32, /*data_lba*/
    )],
) -> udf::UdfFs {
    let mut ssif_files = Vec::new();
    let mut clipinf_files = Vec::new();
    let mut icb = 200u32;
    for (name, sectors, packets, data_lba) in clips {
        let ssif = format!("{name}.ssif");
        let size = sectors * 2048;
        ssif_files.push(file(&ssif, icb, *data_lba, size as u64, true));
        icb += 1;
        let clpi = format!("{name}.clpi");
        clipinf_files.push(file_with(
            &clpi,
            icb,
            *data_lba + 1000,
            build_clpi(*packets),
            false,
        ));
        icb += 1;
    }
    let bdmv = DirSpec {
        name: "BDMV".to_string(),
        icb_lba: 40,
        dir_data_lba: 41,
        files: Vec::new(),
        subdirs: vec![
            DirSpec {
                name: "STREAM".to_string(),
                icb_lba: 42,
                dir_data_lba: 43,
                files: Vec::new(),
                subdirs: vec![DirSpec {
                    name: "SSIF".to_string(),
                    icb_lba: 44,
                    dir_data_lba: 45,
                    files: ssif_files,
                    subdirs: vec![],
                }],
            },
            DirSpec {
                name: "CLIPINF".to_string(),
                icb_lba: 46,
                dir_data_lba: 47,
                files: clipinf_files,
                subdirs: vec![],
            },
        ],
    };
    let root = DirSpec {
        name: String::new(),
        icb_lba: 10,
        dir_data_lba: 11,
        files: Vec::new(),
        subdirs: vec![bdmv],
    };
    build_udf_skeleton(disc, 10);
    lay_dir(disc, &root);
    udf::read_filesystem(disc).expect("fs")
}

/// Single-clip playlist: size_bytes = source_packets * 192 and the
/// physical extent is pulled from the m2ts Long-AD ICB. Per bluray.rs:
/// `total_size += pkt_count * 192`; extents from file_extents.
#[test]
fn parse_playlist_single_clip_size_and_extent() {
    let mut disc = MemDisc::new();
    // 1000 sectors of m2ts at LBA 5000 (data_lba arg); 4000 packets.
    let udf = make_bdmv_fs(&mut disc, &[("00001", 1000, 4000, 5000)]);
    let mpls = build_mpls(
        &[PiSpec {
            clip_id: *b"00001",
            in_time: 0,
            out_time: 60 * 45000, // 60 s
        }],
        (0, 0, 0, 0, 0, 0, 0, 0),
        &[],
        &[],
    );
    let t = Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls)
        .expect("scan")
        .expect("title");
    // BD source packet = 192 bytes (188 TS + 4-byte timestamp header).
    assert_eq!(t.size_bytes, 4000 * 192);
    assert_eq!(t.extents.len(), 1, "one m2ts → one extent");
    // file_extents absolute LBA = partition_start + data_lba.
    assert_eq!(t.extents[0].start_lba, PART_START + 5000);
    assert_eq!(t.extents[0].sector_count, 1000);
    assert_eq!(t.clips.len(), 1);
    assert_eq!(t.clips[0].source_packets, 4000);
}

// duration_secs = (out_time - in_time) / 45000 (BD 45kHz clock). Uses a
// 75s duration whose ticks aren't a multiple of a small constant, so a
// `*`/`%` swap for `/` would not coincidentally match.
#[test]
fn parse_playlist_clip_duration_secs_computed_from_ticks() {
    let mut disc = MemDisc::new();
    let udf = make_bdmv_fs(&mut disc, &[("00001", 100, 400, 5000)]);
    let mpls = build_mpls(
        &[PiSpec {
            clip_id: *b"00001",
            in_time: 45000,
            out_time: 45000 + 75 * 45000, // 75s clip
        }],
        (0, 0, 0, 0, 0, 0, 0, 0),
        &[],
        &[],
    );
    let t = Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls)
        .expect("scan")
        .expect("title");
    assert_eq!(t.clips.len(), 1);
    assert!(
        (t.clips[0].duration_secs - 75.0).abs() < 1e-6,
        "clip duration_secs must be ticks/45000 seconds, got {}",
        t.clips[0].duration_secs
    );
}

// AACS 2.1: feature clip is `00001.fmts`, not `.m2ts`; CLIP_STREAM_EXTS fallback must still
// resolve the extent (used to error → empty rip).
#[test]
fn parse_playlist_fmts_clip_resolves_extent() {
    let mut disc = MemDisc::new();
    // Only a .fmts stream exists for clip 00001 (no .m2ts on disc).
    let udf = make_bdmv_fs_ext(&mut disc, &[("00001", 1000, 4000, 5000)], "fmts");
    let mpls = build_mpls(
        &[PiSpec {
            clip_id: *b"00001",
            in_time: 0,
            out_time: 60 * 45000,
        }],
        (0, 0, 0, 0, 0, 0, 0, 0),
        &[],
        &[],
    );
    let t = Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls)
        .expect("scan")
        .expect("title");
    assert_eq!(t.size_bytes, 4000 * 192, "size from .clpi source packets");
    assert_eq!(
        t.extents.len(),
        1,
        "the .fmts extent must be resolved via fallback"
    );
    assert_eq!(t.extents[0].start_lba, PART_START + 5000);
    assert_eq!(t.extents[0].sector_count, 1000);
}

// 0.31.0 dedup path: a playlist referencing the same clip_id from multiple PlayItems must
// count extents/bytes exactly once, though each PlayItem still gets a Clip entry.
#[test]
fn parse_playlist_dedups_repeated_clip_extents_and_size() {
    let mut disc = MemDisc::new();
    let udf = make_bdmv_fs(&mut disc, &[("00001", 1000, 4000, 5000)]);
    let mpls = build_mpls(
        &[
            PiSpec {
                clip_id: *b"00001",
                in_time: 0,
                out_time: 60 * 45000,
            },
            PiSpec {
                clip_id: *b"00001", // SAME clip — second reference
                in_time: 60 * 45000,
                out_time: 120 * 45000,
            },
        ],
        (0, 0, 0, 0, 0, 0, 0, 0),
        &[],
        &[],
    );
    let t = Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls)
        .expect("scan")
        .expect("title");
    // Extent and size counted ONCE despite two PlayItems.
    assert_eq!(
        t.extents.len(),
        1,
        "repeated clip must not duplicate extent"
    );
    assert_eq!(
        t.size_bytes,
        4000 * 192,
        "size counted once per unique clip"
    );
    // But BOTH PlayItems are recorded as Clip entries (differing times).
    assert_eq!(t.clips.len(), 2, "each PlayItem still gets a Clip entry");
    assert_eq!(t.clips[0].clip_id, "00001");
    assert_eq!(t.clips[1].clip_id, "00001");
}

// A CLPI with no packet count (cut short) still sizes the clip, from its extents.
#[test]
fn parse_playlist_unknown_clpi_count_sizes_from_extents() {
    let mut disc = MemDisc::new();
    let udf = make_bdmv_fs(&mut disc, &[("00001", 1000, 0, 5000)]);
    let mpls = build_mpls(
        &[PiSpec {
            clip_id: *b"00001",
            in_time: 0,
            out_time: 60 * 45000,
        }],
        (0, 0, 0, 0, 0, 0, 0, 0),
        &[],
        &[],
    );
    let t = Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls)
        .expect("scan")
        .expect("title");
    assert_eq!(t.size_bytes, 1000 * 2048);
}

/// Distinct clips each contribute their own extent and bytes, in
/// PlayItem order (mux relies on extent order).
#[test]
fn parse_playlist_distinct_clips_accumulate_in_order() {
    let mut disc = MemDisc::new();
    let udf = make_bdmv_fs(
        &mut disc,
        &[("00001", 1000, 4000, 5000), ("00002", 500, 2000, 9000)],
    );
    let mpls = build_mpls(
        &[
            PiSpec {
                clip_id: *b"00001",
                in_time: 0,
                out_time: 60 * 45000,
            },
            PiSpec {
                clip_id: *b"00002",
                in_time: 0,
                out_time: 30 * 45000,
            },
        ],
        (0, 0, 0, 0, 0, 0, 0, 0),
        &[],
        &[],
    );
    let t = Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls)
        .expect("scan")
        .expect("title");
    assert_eq!(t.extents.len(), 2);
    assert_eq!(t.extents[0].start_lba, PART_START + 5000);
    assert_eq!(t.extents[1].start_lba, PART_START + 9000);
    assert_eq!(t.size_bytes, (4000 + 2000) * 192);
}

// A clip whose.clpi is missing must yield NO title, not one advertising the full runtime
// with none of its bytes, and must log the read's OWN error code.
#[test]
fn parse_playlist_missing_clpi_yields_no_title() {
    let mut disc = MemDisc::new();
    // STREAM has the m2ts but CLIPINF is empty for this clip.
    let udf = make_bdmv_fs(&mut disc, &[]); // no clips wired
    // Re-lay a STREAM-only tree: put an m2ts but no clpi.
    let udf = {
        let _ = udf;
        let bdmv = DirSpec {
            name: "BDMV".to_string(),
            icb_lba: 20,
            dir_data_lba: 21,
            files: Vec::new(),
            subdirs: vec![
                DirSpec {
                    name: "STREAM".to_string(),
                    icb_lba: 22,
                    dir_data_lba: 23,
                    files: vec![file("00009.m2ts", 100, 5000, 1000 * 2048, true)],
                    subdirs: vec![],
                },
                DirSpec {
                    name: "CLIPINF".to_string(),
                    icb_lba: 24,
                    dir_data_lba: 25,
                    files: Vec::new(), // no .clpi
                    subdirs: vec![],
                },
            ],
        };
        let root = DirSpec {
            name: String::new(),
            icb_lba: 10,
            dir_data_lba: 11,
            files: Vec::new(),
            subdirs: vec![bdmv],
        };
        let mut d2 = MemDisc::new();
        build_udf_skeleton(&mut d2, 10);
        lay_dir(&mut d2, &root);
        disc = d2;
        udf::read_filesystem(&mut disc).expect("fs")
    };
    let mpls = build_mpls(
        &[PiSpec {
            clip_id: *b"00009",
            in_time: 0,
            out_time: 60 * 45000,
        }],
        (0, 0, 0, 0, 0, 0, 0, 0),
        &[],
        &[],
    );
    let (t, events) = crate::testlog::capture(|| {
        Disc::parse_playlist(&mut disc, &udf, "00009.mpls", &mpls).expect("scan")
    });
    assert!(
        t.is_none(),
        "a clip with no .clpi cannot be sized or resolved, so offering the \
             title would advertise the full play-item runtime with none of the \
             clip's bytes behind it; got {:?}",
        t.map(|t| (t.size_bytes, t.extents))
    );
    let line = events
        .iter()
        .find(|e| e.target == "freemkv::disc")
        .unwrap_or_else(|| panic!("a dropped title must be accounted; got {events:?}"));
    assert_eq!(line.field("clip"), Some("\"00009\""));
    assert_eq!(
        line.message(),
        format!("E{}", crate::error::E_UDF_NOT_FOUND),
        "the read's OWN code — an absent .clpi is not a scratched or \
             malformed one, and triaging them together hides the population \
             that actually exists: {line:?}"
    );
}

// A clip stream whose ICB declares an UNRECORDED (ECMA-167 4/14.14.1.1 type-1) extent must
// not yield a title: neither reading nor dropping it is truthful.
#[test]
fn parse_playlist_unrecorded_extent_yields_no_title() {
    let mut disc = MemDisc::new();
    // The m2ts ICB is rewritten below to carry TWO short ADs: a
    // zero-length one (0 sectors) followed by a real 4096-byte one.
    let udf = {
        let bdmv = DirSpec {
            name: "BDMV".to_string(),
            icb_lba: 20,
            dir_data_lba: 21,
            files: Vec::new(),
            subdirs: vec![
                DirSpec {
                    name: "STREAM".to_string(),
                    icb_lba: 22,
                    dir_data_lba: 23,
                    files: vec![file("00001.m2ts", 100, 5000, 4096, false)],
                    subdirs: vec![],
                },
                DirSpec {
                    name: "CLIPINF".to_string(),
                    icb_lba: 24,
                    dir_data_lba: 25,
                    files: vec![file_with("00001.clpi", 102, 8000, build_clpi(4000), false)],
                    subdirs: vec![],
                },
            ],
        };
        let root = DirSpec {
            name: String::new(),
            icb_lba: 10,
            dir_data_lba: 11,
            files: Vec::new(),
            subdirs: vec![bdmv],
        };
        build_udf_skeleton(&mut disc, 10);
        lay_dir(&mut disc, &root);
        // Rewrite the .m2ts ICB with a two-descriptor short-AD list: AD0 is
        // a type-1 (allocated, not recorded) zero-length descriptor at LBA
        // 4999 that survives `read_icb_extents`; AD1 is the real content.
        let mut icb = build_file_icb(4096, 5000, false);
        icb[212..216].copy_from_slice(&16u32.to_le_bytes()); // l_ad: two short ADs
        icb[216..220].copy_from_slice(&0x4000_0800u32.to_le_bytes()); // type 1, 2048 bytes
        icb[220..224].copy_from_slice(&4999u32.to_le_bytes());
        icb[224..228].copy_from_slice(&4096u32.to_le_bytes()); // type 0, 4096 bytes
        icb[228..232].copy_from_slice(&5000u32.to_le_bytes());
        disc.put_bytes(PART_START + 100, &icb);
        udf::read_filesystem(&mut disc).expect("fs")
    };
    // The fixture must really carry the unrecorded descriptor, or the
    // behaviour under test is never reached: the hole's byte-space position,
    // then the real content.
    assert_eq!(
        udf.file_extents_addressing(&mut disc, "/BDMV/STREAM/00001.m2ts")
            .expect("extents"),
        vec![(PART_START + 4999, 1), (PART_START + 5000, 2)],
        "fixture must present one unrecorded extent that OCCUPIES byte \
             space, and one real one"
    );
    assert!(
        matches!(
            udf.file_extents(&mut disc, "/BDMV/STREAM/00001.m2ts"),
            Err(Error::UdfUnrecordedExtent { .. })
        ),
        "a read plan over an unrecorded extent must be refused"
    );
    let mpls = build_mpls(
        &[PiSpec {
            clip_id: *b"00001",
            in_time: 0,
            out_time: 60 * 45000,
        }],
        (0, 0, 0, 0, 0, 0, 0, 0),
        &[],
        &[],
    );
    let t = Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls).expect("scan");
    assert!(
        t.is_none(),
        "the only clip has no truthful read plan, so offering the title \
             would mean ripping undefined sectors as content; got {:?}",
        t.map(|t| t.extents)
    );
}

// An unrecorded extent was never the only way a clip fails to resolve: a scratched-sector
// DiscRead on the.m2ts ICB must drop the title too, not fall through to "absent".
#[test]
fn parse_playlist_unreadable_clip_icb_yields_no_title() {
    let mut disc = MemDisc::new();
    let udf = {
        let bdmv = DirSpec {
            name: "BDMV".to_string(),
            icb_lba: 20,
            dir_data_lba: 21,
            files: Vec::new(),
            subdirs: vec![
                DirSpec {
                    name: "STREAM".to_string(),
                    icb_lba: 22,
                    dir_data_lba: 23,
                    files: vec![file("00001.m2ts", 100, 5000, 4096, false)],
                    subdirs: vec![],
                },
                DirSpec {
                    name: "CLIPINF".to_string(),
                    icb_lba: 24,
                    dir_data_lba: 25,
                    files: vec![file_with("00001.clpi", 102, 8000, build_clpi(4000), false)],
                    subdirs: vec![],
                },
            ],
        };
        let root = DirSpec {
            name: String::new(),
            icb_lba: 10,
            dir_data_lba: 11,
            files: Vec::new(),
            subdirs: vec![bdmv],
        };
        build_udf_skeleton(&mut disc, 10);
        lay_dir(&mut disc, &root);
        // Corrupt ONLY the descriptor tag, leaving a structurally valid ICB
        // behind it: an unreadable/garbled sector, deliberately NOT an
        // unrecorded extent — the point is the error class the old code ignored.
        let mut icb = build_file_icb(4096, 5000, false);
        icb[0..2].copy_from_slice(&999u16.to_le_bytes());
        disc.put_bytes(PART_START + 100, &icb);
        udf::read_filesystem(&mut disc).expect("fs")
    };
    // The fixture must really produce a non-unrecorded error, or the
    // behaviour under test is never reached.
    assert!(
        matches!(
            udf.file_extents(&mut disc, "/BDMV/STREAM/00001.m2ts"),
            Err(Error::DiscRead { .. })
        ),
        "fixture must fail with DiscRead, not UdfUnrecordedExtent"
    );
    let mpls = build_mpls(
        &[PiSpec {
            clip_id: *b"00001",
            in_time: 0,
            out_time: 60 * 45000,
        }],
        (0, 0, 0, 0, 0, 0, 0, 0),
        &[],
        &[],
    );
    let (t, events) = crate::testlog::capture(|| {
        Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls).expect("scan")
    });
    assert!(
        t.is_none(),
        "a clip whose extents could not be resolved must drop the title, \
             not yield one that counts the clip's runtime and ships none of \
             its bytes; got {:?}",
        t.map(|t| (t.size_bytes, t.extents))
    );
    // ...and the refusal is accounted with the SCRATCH's own code.
    // Mutation: a fixed `"E6017"` here files a scratched sector as an
    // authoring hole and fails this assertion.
    let line = events
        .iter()
        .find(|e| e.target == "freemkv::disc")
        .unwrap_or_else(|| panic!("a dropped title must be accounted; got {events:?}"));
    assert_eq!(
        line.message(),
        format!("E{}", crate::error::E_DISC_READ),
        "the error's OWN code: {line:?}"
    );
}

// A non-absence SSIF failure that the.m2ts fallback papers over must be LOGGED with its own
// code, while the (degraded 2D) title still ships.
#[test]
fn parse_playlist_logs_a_non_absence_ssif_failure_the_m2ts_fallback_hid() {
    let mut disc = MemDisc::new();
    let bdmv = DirSpec {
        name: "BDMV".to_string(),
        icb_lba: 20,
        dir_data_lba: 21,
        files: Vec::new(),
        subdirs: vec![
            DirSpec {
                name: "STREAM".to_string(),
                icb_lba: 22,
                dir_data_lba: 23,
                // The base view is healthy and resolves normally.
                files: vec![file("00001.m2ts", 100, 5000, 1000 * 2048, true)],
                subdirs: vec![DirSpec {
                    name: "SSIF".to_string(),
                    icb_lba: 26,
                    dir_data_lba: 27,
                    files: vec![file("00001.ssif", 104, 6000, 4096, false)],
                    subdirs: vec![],
                }],
            },
            DirSpec {
                name: "CLIPINF".to_string(),
                icb_lba: 24,
                dir_data_lba: 25,
                files: vec![file_with("00001.clpi", 102, 8000, build_clpi(4000), false)],
                subdirs: vec![],
            },
        ],
    };
    let root = DirSpec {
        name: String::new(),
        icb_lba: 10,
        dir_data_lba: 11,
        files: Vec::new(),
        subdirs: vec![bdmv],
    };
    build_udf_skeleton(&mut disc, 10);
    lay_dir(&mut disc, &root);
    // Corrupt ONLY the SSIF's descriptor tag: structurally valid ICB
    // behind a tag the parser rejects, i.e. what a garbled sector looks
    // like. Not an absence — the directory entry is still there.
    let mut icb = build_file_icb(4096, 6000, false);
    icb[0..2].copy_from_slice(&999u16.to_le_bytes());
    disc.put_bytes(PART_START + 104, &icb);
    let udf = udf::read_filesystem(&mut disc).expect("fs");

    // The fixture must really produce a NON-ABSENCE error on the SSIF and
    // a clean resolve on the .m2ts, or the behaviour under test is never
    // reached and the test would pass for the wrong reason.
    assert!(
        matches!(
            udf.file_extents(&mut disc, "/BDMV/STREAM/SSIF/00001.ssif"),
            Err(Error::DiscRead { .. })
        ),
        "fixture must fail the SSIF with DiscRead, not UdfNotFound"
    );
    assert!(
        udf.file_extents(&mut disc, "/BDMV/STREAM/00001.m2ts")
            .is_ok()
    );

    let mpls = build_mpls(
        &[PiSpec {
            clip_id: *b"00001",
            in_time: 0,
            out_time: 60 * 45000,
        }],
        (0, 0, 0, 0, 0, 0, 0, 0),
        &[],
        &[],
    );
    let (t, events) = crate::testlog::capture(|| {
        Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls).expect("scan")
    });
    let t = t.expect("the base view resolved, so the 2D title still ships");
    assert_eq!(t.extents.len(), 1, "base-view extents present");

    let line = events
        .iter()
        .find(|e| e.target == "freemkv::disc")
        .unwrap_or_else(|| {
            panic!("silently shipping 2D off a 3D disc must be logged; got {events:?}")
        });
    assert_eq!(line.level, tracing::Level::WARN);
    assert_eq!(line.field("clip"), Some("\"00001\""));
    assert_eq!(
        line.message(),
        format!("E{}", crate::error::E_DISC_READ),
        "the SSIF failure's OWN code, not a fixed one: {line:?}"
    );
}

// ---------------------------------------------------------------
// Tests: STN stream mapping
// ---------------------------------------------------------------

/// stream_type 1 video (HEVC 0x24) → Stream::Video with the parsed PID
/// and codec. coding_type 0x24 maps to HEVC (Codec::from_coding_type).
#[test]
fn parse_playlist_maps_video_stream() {
    let mut disc = MemDisc::new();
    let udf = make_bdmv_fs(&mut disc, &[("00001", 100, 400, 5000)]);
    let mpls = build_mpls(
        &[PiSpec {
            clip_id: *b"00001",
            in_time: 0,
            out_time: 60 * 45000,
        }],
        (1, 0, 0, 0, 0, 0, 0, 0),
        &[se_video(0x1011, 0x24)],
        &[],
    );
    let t = Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls)
        .expect("scan")
        .expect("title");
    let videos: Vec<_> = t
        .streams
        .iter()
        .filter_map(|s| match s {
            Stream::Video(v) => Some(v),
            _ => None,
        })
        .collect();
    assert_eq!(videos.len(), 1);
    assert_eq!(videos[0].pid, 0x1011);
    assert_eq!(videos[0].codec, Codec::Hevc);
}

// HEVC HDR byte (sa[2]): high nibble = dynamic_range, low nibble =
// color_space. dynamic_range 1 -> HDR10, color_space 2 -> BT.2020.
#[test]
fn parse_playlist_maps_hdr10_bt2020_from_hevc_nibbles() {
    let mut disc = MemDisc::new();
    let udf = make_bdmv_fs(&mut disc, &[("00001", 100, 400, 5000)]);
    let mpls = build_mpls(
        &[PiSpec {
            clip_id: *b"00001",
            in_time: 0,
            out_time: 60 * 45000,
        }],
        (1, 0, 0, 0, 0, 0, 0, 0),
        &[se_video_hevc(0x1011, 1, 2)],
        &[],
    );
    let t = Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls)
        .expect("scan")
        .expect("title");
    let v = t
        .streams
        .iter()
        .find_map(|s| match s {
            Stream::Video(v) => Some(v),
            _ => None,
        })
        .expect("video stream");
    assert_eq!(v.hdr, HdrFormat::Hdr10);
    assert_eq!(v.color_space, ColorSpace::Bt2020);
}

// audio_format 6 claims no count: DTS-HD / DD+ (up to 7.1) stay Unknown, not 5.1.
#[test]
fn multi_channel_audio_format_is_not_claimed_as_5_1_for_7_1_capable_codecs() {
    for c in [Codec::DtsHdMa, Codec::DtsHdHr, Codec::Ac3Plus] {
        assert_eq!(playlist_channels(6, c), AudioChannels::Unknown, "{c:?}");
        assert_eq!(playlist_channels(3, c), AudioChannels::Stereo, "{c:?}");
    }
    for c in [Codec::Ac3, Codec::Dts, Codec::TrueHd, Codec::Lpcm] {
        assert_eq!(playlist_channels(6, c), AudioChannels::Surround51, "{c:?}");
    }
}

/// HDR10 base with hdr_plus_flag -> HDR10+; the DV enhancement-layer entry of the
/// same playlist (dynamic_range 2) stays Dolby Vision whatever its flag byte holds.
#[test]
fn parse_playlist_maps_hdr_plus_flag_to_hdr10_plus() {
    let mut disc = MemDisc::new();
    let udf = make_bdmv_fs(&mut disc, &[("00001", 100, 400, 5000)]);
    let el = se_video_hevc_flags(0x1015, 2, 2, Some(0x40));
    let mpls = build_mpls(
        &[PiSpec {
            clip_id: *b"00001",
            in_time: 0,
            out_time: 60 * 45000,
        }],
        (1, 0, 0, 0, 0, 0, 0, 1),
        &[se_video_hevc_flags(0x1011, 1, 2, Some(0x40)), el],
        &[],
    );
    let t = Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls)
        .expect("scan")
        .expect("title");
    let hdr: Vec<_> = t
        .streams
        .iter()
        .filter_map(|s| match s {
            Stream::Video(v) => Some(v.hdr),
            _ => None,
        })
        .collect();
    assert_eq!(hdr, vec![HdrFormat::Hdr10Plus, HdrFormat::DolbyVision]);
}

/// dynamic_range 2 -> DolbyVision, color_space 1 -> BT.709: the other
/// pair of named arms in the same two match expressions.
#[test]
fn parse_playlist_maps_dolby_vision_bt709_from_hevc_nibbles() {
    let mut disc = MemDisc::new();
    let udf = make_bdmv_fs(&mut disc, &[("00001", 100, 400, 5000)]);
    let mpls = build_mpls(
        &[PiSpec {
            clip_id: *b"00001",
            in_time: 0,
            out_time: 60 * 45000,
        }],
        (1, 0, 0, 0, 0, 0, 0, 0),
        &[se_video_hevc(0x1011, 2, 1)],
        &[],
    );
    let t = Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls)
        .expect("scan")
        .expect("title");
    let v = t
        .streams
        .iter()
        .find_map(|s| match s {
            Stream::Video(v) => Some(v),
            _ => None,
        })
        .expect("video stream");
    assert_eq!(v.hdr, HdrFormat::DolbyVision);
    assert_eq!(v.color_space, ColorSpace::Bt709);
}

// A PGS coding_type (0x90) in the AUDIO STN slot must route to Subtitle,
// not Audio: guards against audio-slot data silently becoming a fake
// audio track when it is really PGS.
#[test]
fn parse_playlist_pgs_in_audio_slot_becomes_subtitle() {
    let mut disc = MemDisc::new();
    let udf = make_bdmv_fs(&mut disc, &[("00001", 100, 400, 5000)]);
    let mpls = build_mpls(
        &[PiSpec {
            clip_id: *b"00001",
            in_time: 0,
            out_time: 60 * 45000,
        }],
        // 1 audio entry, but its coding_type is PGS (0x90).
        (0, 1, 0, 0, 0, 0, 0, 0),
        &[se_pg(0x1100, 0x90, b"eng")],
        &[],
    );
    let t = Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls)
        .expect("scan")
        .expect("title");
    assert!(
        t.streams.iter().all(|s| !matches!(s, Stream::Audio(_))),
        "PGS in audio slot must NOT become an audio stream"
    );
    assert!(
        t.streams
            .iter()
            .any(|s| matches!(s, Stream::Subtitle(sub) if sub.codec == Codec::Pgs)),
        "PGS in audio slot must become a PGS subtitle"
    );
}

/// A real audio entry (AC-3 0x81) in the audio slot → Stream::Audio.
#[test]
fn parse_playlist_maps_audio_stream() {
    let mut disc = MemDisc::new();
    let udf = make_bdmv_fs(&mut disc, &[("00001", 100, 400, 5000)]);
    let mpls = build_mpls(
        &[PiSpec {
            clip_id: *b"00001",
            in_time: 0,
            out_time: 60 * 45000,
        }],
        (0, 1, 0, 0, 0, 0, 0, 0),
        &[se_audio(0x1100, 0x81, b"eng")],
        &[],
    );
    let t = Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls)
        .expect("scan")
        .expect("title");
    let audios: Vec<_> = t
        .streams
        .iter()
        .filter_map(|s| match s {
            Stream::Audio(a) => Some(a),
            _ => None,
        })
        .collect();
    assert_eq!(audios.len(), 1);
    assert_eq!(audios[0].codec, Codec::Ac3);
    assert_eq!(audios[0].language, "eng");
    assert!(
        !audios[0].secondary,
        "a primary (stream_type 2) audio entry must not be marked secondary"
    );
}

/// A secondary-audio STN entry (stream_type 5, e.g. a director's
/// commentary track) must set `AudioStream::secondary` (bluray.rs
/// `secondary: s.stream_type == 5`).
#[test]
fn parse_playlist_secondary_audio_flag_set_for_stream_type_5() {
    let mut disc = MemDisc::new();
    let udf = make_bdmv_fs(&mut disc, &[("00001", 100, 400, 5000)]);
    let mpls = build_mpls(
        &[PiSpec {
            clip_id: *b"00001",
            in_time: 0,
            out_time: 60 * 45000,
        }],
        (0, 0, 0, 0, 1, 0, 0, 0), // one secondary-audio (stream_type 5) entry
        &[se_audio(0x1a00, 0x83, b"eng")],
        &[],
    );
    let t = Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls)
        .expect("scan")
        .expect("title");
    let audios: Vec<_> = t
        .streams
        .iter()
        .filter_map(|s| match s {
            Stream::Audio(a) => Some(a),
            _ => None,
        })
        .collect();
    assert_eq!(audios.len(), 1);
    assert!(
        audios[0].secondary,
        "stream_type 5 (secondary audio) must set AudioStream::secondary"
    );
}

/// stream_type 3 PG (PGS 0x90) → Stream::Subtitle with language.
#[test]
fn parse_playlist_maps_pg_subtitle() {
    let mut disc = MemDisc::new();
    let udf = make_bdmv_fs(&mut disc, &[("00001", 100, 400, 5000)]);
    let mpls = build_mpls(
        &[PiSpec {
            clip_id: *b"00001",
            in_time: 0,
            out_time: 60 * 45000,
        }],
        (0, 0, 1, 0, 0, 0, 0, 0),
        &[se_pg(0x1200, 0x90, b"fra")],
        &[],
    );
    let t = Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls)
        .expect("scan")
        .expect("title");
    let subs: Vec<_> = t
        .streams
        .iter()
        .filter_map(|s| match s {
            Stream::Subtitle(sub) => Some(sub),
            _ => None,
        })
        .collect();
    assert_eq!(subs.len(), 1);
    assert_eq!(subs[0].codec, Codec::Pgs);
    assert_eq!(subs[0].language, "fra");
}

/// A PiP (secondary) PG entry is picture-in-picture commentary graphics, not a
/// main-feature subtitle track: it is kept out of the subtitle list.
#[test]
fn parse_playlist_skips_pip_pg_subtitles() {
    let mut disc = MemDisc::new();
    let udf = make_bdmv_fs(&mut disc, &[("00001", 100, 400, 5000)]);
    let mpls = build_mpls(
        &[PiSpec {
            clip_id: *b"00001",
            in_time: 0,
            out_time: 60 * 45000,
        }],
        (0, 0, 1, 0, 0, 0, 1, 0), // one PG, one PiP PG
        &[se_pg(0x1200, 0x90, b"fra"), se_pg(0x1a20, 0x90, b"eng")],
        &[],
    );
    let t = Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls)
        .expect("scan")
        .expect("title");
    let subs: Vec<u16> = t
        .streams
        .iter()
        .filter_map(|s| match s {
            Stream::Subtitle(sub) => Some(sub.pid),
            _ => None,
        })
        .collect();
    assert_eq!(subs, vec![0x1200]);
}

// ---------------------------------------------------------------
// Tests: Blu-ray 3D dependent-view stream
// ---------------------------------------------------------------

/// When a clip resolves via `STREAM/SSIF/<clip>.ssif`, `is_3d` latches
/// and a synthetic MVC dependent-view stream is added at base_pid + 1.
/// Verifies its `pid`, `secondary`, and `label` fields.
#[test]
fn parse_playlist_3d_adds_dependent_view_stream() {
    let mut disc = MemDisc::new();
    let udf = make_bdmv_fs_ssif(&mut disc, &[("00001", 1000, 4000, 5000)]);
    let mpls = build_mpls(
        &[PiSpec {
            clip_id: *b"00001",
            in_time: 0,
            out_time: 60 * 45000,
        }],
        (1, 0, 0, 0, 0, 0, 0, 0),
        // Base (left-eye) view only -- STN table omits the dependent view.
        &[se_video(0x1011, 0x1B)],
        &[],
    );
    let t = Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls)
        .expect("scan")
        .expect("title");
    let videos: Vec<_> = t
        .streams
        .iter()
        .filter_map(|s| match s {
            Stream::Video(v) => Some(v),
            _ => None,
        })
        .collect();
    assert_eq!(
        videos.len(),
        2,
        "a 3D title must add one dependent-view video stream"
    );
    let dep = videos
        .iter()
        .find(|v| v.pid == 0x1012)
        .expect("dependent-view stream at base_pid + 1");
    assert!(dep.secondary, "dependent view must be marked secondary");
    assert_eq!(
        dep.label,
        crate::disc::MVC_DEPENDENT_LABEL,
        "dependent view must carry the MVC dependent-view label"
    );
}

// If the STN table already lists a video stream at base_pid + 1 (e.g. an
// authoring tool populated STN_table_SS), the synthetic push must be
// skipped: never duplicate an existing dependent-view entry.
#[test]
fn parse_playlist_3d_does_not_duplicate_existing_dependent_stream() {
    let mut disc = MemDisc::new();
    let udf = make_bdmv_fs_ssif(&mut disc, &[("00001", 1000, 4000, 5000)]);
    let mpls = build_mpls(
        &[PiSpec {
            clip_id: *b"00001",
            in_time: 0,
            out_time: 60 * 45000,
        }],
        (1, 0, 0, 0, 0, 1, 0, 0), // primary video + secondary (PiP) video
        &[se_video(0x1011, 0x1B), se_video(0x1012, 0x1B)],
        &[],
    );
    let t = Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls)
        .expect("scan")
        .expect("title");
    let dep_count = t
        .streams
        .iter()
        .filter(|s| matches!(s, Stream::Video(v) if v.pid == 0x1012))
        .count();
    assert_eq!(
        dep_count, 1,
        "an already-present stream at base_pid + 1 must not be duplicated"
    );
}

/// The 3D `.ssif` interleaves the dependent view, so `size_bytes` follows the
/// extents the read plan covers, not the base-view-only CLPI packet count.
#[test]
fn parse_playlist_3d_size_covers_the_interleaved_extents() {
    let mut disc = MemDisc::new();
    let udf = make_bdmv_fs_ssif(&mut disc, &[("00001", 1000, 400, 5000)]);
    let mpls = build_mpls(
        &[PiSpec {
            clip_id: *b"00001",
            in_time: 0,
            out_time: 60 * 45000,
        }],
        (1, 0, 0, 0, 0, 0, 0, 0),
        &[se_video(0x1011, 0x1B)],
        &[],
    );
    let t = Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls)
        .expect("scan")
        .expect("title");
    assert_eq!(t.size_bytes, 1000 * 2048);
}

fn se_raw(pid: u16, attrs: &[u8]) -> Vec<u8> {
    let mut out = vec![3u8, 0x01];
    out.extend_from_slice(&pid.to_be_bytes());
    out.push(attrs.len() as u8);
    out.extend_from_slice(attrs);
    out
}

/// The MPLS format/rate nibbles reach the right `Stream` fields, and the
/// secondary-video (6) and Dolby Vision EL (7) categories are kept as video.
#[test]
fn parse_playlist_maps_format_rate_nibbles_and_secondary_video() {
    let mut disc = MemDisc::new();
    let udf = make_bdmv_fs(&mut disc, &[("00001", 100, 400, 5000)]);
    let mpls = build_mpls(
        &[PiSpec {
            clip_id: *b"00001",
            in_time: 0,
            out_time: 60 * 45000,
        }],
        (1, 1, 0, 0, 0, 1, 0, 1),
        &[
            se_raw(0x1011, &[0x1B, 0x63]),                   // 1080p, 25 fps
            se_raw(0x1100, &[0x81, 0x64, b'e', b'n', b'g']), // 5.1, 96 kHz
            // Secondary video carries audio-ref and PG-ref counts after the entry.
            [se_raw(0x1B00, &[0x1B, 0x63]), vec![0u8; 4]].concat(),
            se_raw(0x1015, &[0x24, 0x63]),
        ],
        &[],
    );
    let t = Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls)
        .expect("scan")
        .expect("title");
    let videos: Vec<_> = t.video_streams().collect();
    let audio = t.audio_streams().next().expect("audio");
    assert_eq!(videos.len(), 3, "primary, secondary and EL video kept");
    assert_eq!(videos[0].resolution, Resolution::R1080p);
    assert_eq!(videos[0].frame_rate, FrameRate::F25);
    assert_eq!(audio.channels, AudioChannels::Surround51);
    assert_eq!(audio.sample_rate, SampleRate::S96);
}

/// One unreadable playlist is skipped with a warning; the healthy one survives.
#[test]
fn scan_bluray_titles_skips_an_unreadable_playlist() {
    struct FailingReader<'a> {
        inner: &'a mut MemDisc,
        fail_lba: u32,
    }
    impl SectorSource for FailingReader<'_> {
        fn read_sectors(
            &mut self,
            lba: u32,
            count: u16,
            buf: &mut [u8],
            recovery: bool,
        ) -> Result<usize> {
            if lba == self.fail_lba {
                return Err(Error::DiscRead {
                    sector: lba as u64,
                    status: Some(0x02),
                    sense: None,
                });
            }
            self.inner.read_sectors(lba, count, buf, recovery)
        }
    }
    let mut disc = MemDisc::new();
    let udf = two_playlist_bd_fs(&mut disc);
    let mut reader = FailingReader {
        inner: &mut disc,
        fail_lba: PART_START + 30000, // 00800.mpls's data extent
    };
    let titles = Disc::scan_bluray_titles(&mut reader, &udf, None).expect("scan survives");
    assert_eq!(titles.len(), 1);
    assert_eq!(titles[0].playlist, "00801.mpls");
}

/// A Stop that lands during the final playlist's reads is `Halted`, not a
/// silently truncated list.
#[test]
fn scan_bluray_titles_polls_halt_after_the_last_playlist() {
    struct CancellingReader<'a> {
        inner: &'a mut MemDisc,
        halt: crate::halt::Halt,
        cancel_lba: u32,
    }
    impl SectorSource for CancellingReader<'_> {
        fn read_sectors(
            &mut self,
            lba: u32,
            count: u16,
            buf: &mut [u8],
            recovery: bool,
        ) -> Result<usize> {
            if lba == self.cancel_lba {
                self.halt.cancel();
            }
            self.inner.read_sectors(lba, count, buf, recovery)
        }
    }
    let mut disc = MemDisc::new();
    let udf = two_playlist_bd_fs(&mut disc);
    let halt = crate::halt::Halt::new();
    let mut reader = CancellingReader {
        inner: &mut disc,
        halt: halt.clone(),
        cancel_lba: PART_START + 40000, // 00801.mpls, the last playlist
    };
    let res = Disc::scan_bluray_titles(&mut reader, &udf, Some(&halt));
    assert!(matches!(res, Err(Error::Halted)), "got {res:?}");
}

/// Control characters in the disc-authored title never reach a front end,
/// whether raw or written as a character reference.
#[test]
fn read_meta_title_drops_control_characters() {
    let mut disc = MemDisc::new();
    let xml = b"<x><di:name>A&#27;[2JB\x07C</di:name></x>".to_vec();
    let dl = DirSpec {
        name: "DL".to_string(),
        icb_lba: 30,
        dir_data_lba: 31,
        files: vec![file_with("bdmt_eng.xml", 104, 50000, xml, false)],
        subdirs: vec![],
    };
    let meta = DirSpec {
        name: "META".to_string(),
        icb_lba: 28,
        dir_data_lba: 29,
        files: Vec::new(),
        subdirs: vec![dl],
    };
    let bdmv = DirSpec {
        name: "BDMV".to_string(),
        icb_lba: 20,
        dir_data_lba: 21,
        files: Vec::new(),
        subdirs: vec![meta],
    };
    let root = DirSpec {
        name: String::new(),
        icb_lba: 10,
        dir_data_lba: 11,
        files: Vec::new(),
        subdirs: vec![bdmv],
    };
    build_udf_skeleton(&mut disc, 10);
    lay_dir(&mut disc, &root);
    let udf = udf::read_filesystem(&mut disc).expect("fs");
    assert_eq!(
        Disc::read_meta_title(&mut disc, &udf),
        Some("A[2JBC".to_string())
    );
}

// ---------------------------------------------------------------
// Tests: chapters
// ---------------------------------------------------------------

/// Only mark_type 1 (entry-mark) becomes a chapter; type 2 (link
/// point) and type 0 (reserved) are dropped (bluray.rs filter
/// `m.mark_type == 1`).
#[test]
fn parse_playlist_only_entry_marks_become_chapters() {
    let mut disc = MemDisc::new();
    let udf = make_bdmv_fs(&mut disc, &[("00001", 100, 400, 5000)]);
    let mpls = build_mpls(
        &[PiSpec {
            clip_id: *b"00001",
            in_time: 0,
            out_time: 120 * 45000,
        }],
        (0, 0, 0, 0, 0, 0, 0, 0),
        &[],
        &[
            MarkSpec {
                mark_type: 1,
                play_item_ref: 0,
                timestamp: 0,
            },
            MarkSpec {
                mark_type: 2,
                play_item_ref: 0,
                timestamp: 30 * 45000,
            }, // link → drop
            MarkSpec {
                mark_type: 1,
                play_item_ref: 0,
                timestamp: 60 * 45000,
            },
            MarkSpec {
                mark_type: 0,
                play_item_ref: 0,
                timestamp: 90 * 45000,
            }, // reserved → drop
        ],
    );
    let t = Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls)
        .expect("scan")
        .expect("title");
    assert_eq!(
        t.chapters.len(),
        2,
        "only the two type-1 marks are chapters"
    );
}

// A mark referencing PlayItem 1 is placed at (sum of preceding PlayItem
// durations) + (offset within its own PlayItem); PI0 = 60s, mark at
// PI1's in_time → chapter at exactly 60s.
#[test]
fn parse_playlist_chapter_time_accounts_for_preceding_play_items() {
    let mut disc = MemDisc::new();
    let udf = make_bdmv_fs(&mut disc, &[("00001", 100, 400, 5000)]);
    let pi1_in = 10 * 45000u32;
    let mpls = build_mpls(
        &[
            PiSpec {
                clip_id: *b"00001",
                in_time: 0,
                out_time: 60 * 45000, // PI0 lasts 60 s
            },
            PiSpec {
                clip_id: *b"00001",
                in_time: pi1_in,
                out_time: pi1_in + 60 * 45000,
            },
        ],
        (0, 0, 0, 0, 0, 0, 0, 0),
        &[],
        &[MarkSpec {
            mark_type: 1,
            play_item_ref: 1,
            timestamp: pi1_in, // at the very start of PI1
        }],
    );
    let t = Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls)
        .expect("scan")
        .expect("title");
    assert_eq!(t.chapters.len(), 1);
    // preceding (PI0 = 60s) + within (timestamp - pi1.in_time = 0) = 60s.
    assert!(
        (t.chapters[0].time_secs - 60.0).abs() < 1e-6,
        "chapter must sit at 60s, got {}",
        t.chapters[0].time_secs
    );
}

// within = (timestamp - pi.in_time) / 45000. Uses a non-round 5s offset
// added to a non-zero 60s preceding, so a `*`/`%` swap for `/` would not
// coincidentally match the total.
#[test]
fn parse_playlist_chapter_within_offset_divides_ticks_to_seconds() {
    let mut disc = MemDisc::new();
    let udf = make_bdmv_fs(&mut disc, &[("00001", 100, 400, 5000)]);
    let pi1_in = 10 * 45000u32;
    let within_ticks = 5 * 45000u32; // 5s into PI1
    let mpls = build_mpls(
        &[
            PiSpec {
                clip_id: *b"00001",
                in_time: 0,
                out_time: 60 * 45000, // PI0 lasts 60s
            },
            PiSpec {
                clip_id: *b"00001",
                in_time: pi1_in,
                out_time: pi1_in + 60 * 45000,
            },
        ],
        (0, 0, 0, 0, 0, 0, 0, 0),
        &[],
        &[MarkSpec {
            mark_type: 1,
            play_item_ref: 1,
            timestamp: pi1_in + within_ticks,
        }],
    );
    let t = Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls)
        .expect("scan")
        .expect("title");
    assert_eq!(t.chapters.len(), 1);
    assert!(
        (t.chapters[0].time_secs - 65.0).abs() < 1e-6,
        "chapter time must be preceding(60s) + within(5s) = 65s, got {}",
        t.chapters[0].time_secs
    );
}

/// A mark whose timestamp precedes its PlayItem's in_time would yield a
/// negative within-offset; bluray.rs clamps the chapter to 0.0 (`if
/// time_secs < 0.0 { 0.0 }`). Never emits a negative chapter time.
#[test]
fn parse_playlist_negative_chapter_time_clamped_to_zero() {
    let mut disc = MemDisc::new();
    let udf = make_bdmv_fs(&mut disc, &[("00001", 100, 400, 5000)]);
    let mpls = build_mpls(
        &[PiSpec {
            clip_id: *b"00001",
            in_time: 50 * 45000,
            out_time: 110 * 45000,
        }],
        (0, 0, 0, 0, 0, 0, 0, 0),
        &[],
        &[MarkSpec {
            mark_type: 1,
            play_item_ref: 0,
            timestamp: 0, // before in_time → would be negative
        }],
    );
    let t = Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls)
        .expect("scan")
        .expect("title");
    assert_eq!(t.chapters.len(), 1);
    assert_eq!(t.chapters[0].time_secs, 0.0);
}

/// A mark referencing a non-existent PlayItem index is dropped via the
/// `?` on `play_items.get(pi_idx)` — must not panic or index OOB.
#[test]
fn parse_playlist_mark_with_bad_play_item_ref_dropped() {
    let mut disc = MemDisc::new();
    let udf = make_bdmv_fs(&mut disc, &[("00001", 100, 400, 5000)]);
    let mpls = build_mpls(
        &[PiSpec {
            clip_id: *b"00001",
            in_time: 0,
            out_time: 60 * 45000,
        }],
        (0, 0, 0, 0, 0, 0, 0, 0),
        &[],
        &[MarkSpec {
            mark_type: 1,
            play_item_ref: 99, // no such PlayItem
            timestamp: 0,
        }],
    );
    let t = Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls)
        .expect("scan")
        .expect("title");
    assert!(
        t.chapters.is_empty(),
        "out-of-range mark ref must be dropped"
    );
}

// ---------------------------------------------------------------
// Tests: playlist id parsing
// ---------------------------------------------------------------

/// playlist_id is the numeric stem of the filename with the .mpls
/// suffix stripped case-insensitively (bluray.rs `playlist_num`).
#[test]
fn parse_playlist_id_strips_suffix_case_insensitive() {
    let mut disc = MemDisc::new();
    let udf = make_bdmv_fs(&mut disc, &[("00001", 100, 400, 5000)]);
    let mpls = build_mpls(
        &[PiSpec {
            clip_id: *b"00001",
            in_time: 0,
            out_time: 60 * 45000,
        }],
        (0, 0, 0, 0, 0, 0, 0, 0),
        &[],
        &[],
    );
    // Uppercase suffix must still parse the numeric stem.
    let t = Disc::parse_playlist(&mut disc, &udf, "00800.MPLS", &mpls)
        .expect("scan")
        .expect("title");
    assert_eq!(t.playlist_id, 800);
    assert_eq!(
        t.playlist, "00800.MPLS",
        "playlist field keeps original name"
    );
}

/// A non-numeric stem falls back to playlist_id 0 (`parse::<u16>()
/// .unwrap_or(0)`), never panics.
#[test]
fn parse_playlist_id_non_numeric_defaults_zero() {
    let mut disc = MemDisc::new();
    let udf = make_bdmv_fs(&mut disc, &[("00001", 100, 400, 5000)]);
    let mpls = build_mpls(
        &[PiSpec {
            clip_id: *b"00001",
            in_time: 0,
            out_time: 60 * 45000,
        }],
        (0, 0, 0, 0, 0, 0, 0, 0),
        &[],
        &[],
    );
    let t = Disc::parse_playlist(&mut disc, &udf, "MENU.mpls", &mpls)
        .expect("scan")
        .expect("title");
    assert_eq!(t.playlist_id, 0);
}

// A filename >= 5 bytes but NOT ending in ".mpls" must not have its last 5 bytes stripped;
// the whole string fails the numeric parse instead.
#[test]
fn parse_playlist_id_falls_back_to_zero_when_suffix_is_not_mpls() {
    let mut disc = MemDisc::new();
    let udf = make_bdmv_fs(&mut disc, &[("00001", 100, 400, 5000)]);
    let mpls = build_mpls(
        &[PiSpec {
            clip_id: *b"00001",
            in_time: 0,
            out_time: 60 * 45000,
        }],
        (0, 0, 0, 0, 0, 0, 0, 0),
        &[],
        &[],
    );
    // "00800zzzzz": stripping the last 5 bytes would leave "00800" (a
    // valid u16), but the suffix isn't ".mpls" so nothing may be
    // stripped -- the whole (non-numeric) string must fail to parse.
    let t = Disc::parse_playlist(&mut disc, &udf, "00800zzzzz", &mpls)
        .expect("scan")
        .expect("title");
    assert_eq!(
        t.playlist_id, 0,
        "a filename not ending in .mpls must not have its last 5 bytes stripped"
    );
}

// ---------------------------------------------------------------
// Tests: scan_bluray_titles
// ---------------------------------------------------------------

/// scan_bluray_titles enumerates BDMV/PLAYLIST/*.mpls and keeps only
/// playlists that parse to a >= 30s title. A short one is dropped.
#[test]
fn scan_bluray_titles_keeps_long_drops_short() {
    let mut disc = MemDisc::new();
    // Build full tree with PLAYLIST holding two .mpls + STREAM/CLIPINF.
    let long_mpls = build_mpls(
        &[PiSpec {
            clip_id: *b"00001",
            in_time: 0,
            out_time: 7200 * 45000, // 2 h
        }],
        (0, 0, 0, 0, 0, 0, 0, 0),
        &[],
        &[],
    );
    let short_mpls = build_mpls(
        &[PiSpec {
            clip_id: *b"00001",
            in_time: 0,
            out_time: 5 * 45000, // 5 s menu
        }],
        (0, 0, 0, 0, 0, 0, 0, 0),
        &[],
        &[],
    );
    // m2ts (Long-AD) + clpi for clip 00001.
    let stream = DirSpec {
        name: "STREAM".to_string(),
        icb_lba: 22,
        dir_data_lba: 23,
        files: vec![file("00001.m2ts", 100, 5000, 1000 * 2048, true)],
        subdirs: vec![],
    };
    let clipinf = DirSpec {
        name: "CLIPINF".to_string(),
        icb_lba: 24,
        dir_data_lba: 25,
        files: vec![file_with("00001.clpi", 102, 8000, build_clpi(4000), false)],
        subdirs: vec![],
    };
    let playlist = DirSpec {
        name: "PLAYLIST".to_string(),
        icb_lba: 26,
        dir_data_lba: 27,
        files: vec![
            file_with("00800.mpls", 104, 30000, long_mpls, false),
            file_with("00801.mpls", 110, 40000, short_mpls, false),
        ],
        subdirs: vec![],
    };
    let bdmv = DirSpec {
        name: "BDMV".to_string(),
        icb_lba: 20,
        dir_data_lba: 21,
        files: Vec::new(),
        subdirs: vec![stream, clipinf, playlist],
    };
    let root = DirSpec {
        name: String::new(),
        icb_lba: 10,
        dir_data_lba: 11,
        files: Vec::new(),
        subdirs: vec![bdmv],
    };
    build_udf_skeleton(&mut disc, 10);
    lay_dir(&mut disc, &root);
    let udf = udf::read_filesystem(&mut disc).expect("fs");

    let titles = Disc::scan_bluray_titles(&mut disc, &udf, None).expect("scan");
    assert_eq!(titles.len(), 1, "only the 2h playlist should survive");
    assert_eq!(titles[0].playlist_id, 800);
}

// A non-directory PLAYLIST entry not ending in ".mpls" must be skipped even if its content
// parses as a valid MPLS: extension gating, not content sniffing.
#[test]
fn scan_bluray_titles_skips_non_mpls_extension_file() {
    let mut disc = MemDisc::new();
    let mpls = build_mpls(
        &[PiSpec {
            clip_id: *b"00001",
            in_time: 0,
            out_time: 7200 * 45000, // 2h -- easily long enough to be kept
        }],
        (0, 0, 0, 0, 0, 0, 0, 0),
        &[],
        &[],
    );
    let playlist = DirSpec {
        name: "PLAYLIST".to_string(),
        icb_lba: 26,
        dir_data_lba: 27,
        files: vec![file_with("00800.dat", 104, 30000, mpls, false)],
        subdirs: vec![],
    };
    let bdmv = DirSpec {
        name: "BDMV".to_string(),
        icb_lba: 20,
        dir_data_lba: 21,
        files: Vec::new(),
        subdirs: vec![playlist],
    };
    let root = DirSpec {
        name: String::new(),
        icb_lba: 10,
        dir_data_lba: 11,
        files: Vec::new(),
        subdirs: vec![bdmv],
    };
    build_udf_skeleton(&mut disc, 10);
    lay_dir(&mut disc, &root);
    let udf = udf::read_filesystem(&mut disc).expect("fs");

    let titles = Disc::scan_bluray_titles(&mut disc, &udf, None).expect("scan");
    assert!(
        titles.is_empty(),
        "a PLAYLIST entry not ending in .mpls must be skipped regardless of content"
    );
}

/// With no PLAYLIST directory, scan_bluray_titles returns an empty
/// vec (the `find_dir` is None) — never panics.
#[test]
fn scan_bluray_titles_no_playlist_dir_is_empty() {
    let mut disc = MemDisc::new();
    let udf = make_min_fs(&mut disc); // BDMV exists, no PLAYLIST
    assert!(
        Disc::scan_bluray_titles(&mut disc, &udf, None)
            .expect("scan")
            .is_empty()
    );
}

// A SectorSource that fails every read in `halt_range` with Error::Halted — how a live
// drive behaves once Stop is pressed. Reads outside the range succeed.
struct HaltingReader<'a> {
    inner: &'a mut MemDisc,
    halt_range: std::ops::Range<u32>,
}
impl SectorSource for HaltingReader<'_> {
    fn read_sectors(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        recovery: bool,
    ) -> Result<usize> {
        if self.halt_range.contains(&lba) {
            return Err(Error::Halted);
        }
        self.inner.read_sectors(lba, count, buf, recovery)
    }
}

/// Lay a BDMV holding TWO 2-hour playlists (00800.mpls at data LBA 30000,
/// 00801.mpls at 40000) over one fully wired clip. Both playlists are
/// keepable, so a scan that returns fewer than two titles has LOST one.
fn two_playlist_bd_fs(disc: &mut MemDisc) -> udf::UdfFs {
    let long_mpls = || {
        build_mpls(
            &[PiSpec {
                clip_id: *b"00001",
                in_time: 0,
                out_time: 7200 * 45000, // 2 h
            }],
            (0, 0, 0, 0, 0, 0, 0, 0),
            &[],
            &[],
        )
    };
    let stream = DirSpec {
        name: "STREAM".to_string(),
        icb_lba: 22,
        dir_data_lba: 23,
        files: vec![file("00001.m2ts", 100, 5000, 1000 * 2048, true)],
        subdirs: vec![],
    };
    let clipinf = DirSpec {
        name: "CLIPINF".to_string(),
        icb_lba: 24,
        dir_data_lba: 25,
        files: vec![file_with("00001.clpi", 102, 8000, build_clpi(4000), false)],
        subdirs: vec![],
    };
    let playlist = DirSpec {
        name: "PLAYLIST".to_string(),
        icb_lba: 26,
        dir_data_lba: 27,
        files: vec![
            file_with("00800.mpls", 104, 30000, long_mpls(), false),
            file_with("00801.mpls", 110, 40000, long_mpls(), false),
        ],
        subdirs: vec![],
    };
    let bdmv = DirSpec {
        name: "BDMV".to_string(),
        icb_lba: 20,
        dir_data_lba: 21,
        files: Vec::new(),
        subdirs: vec![stream, clipinf, playlist],
    };
    let root = DirSpec {
        name: String::new(),
        icb_lba: 10,
        dir_data_lba: 11,
        files: Vec::new(),
        subdirs: vec![bdmv],
    };
    build_udf_skeleton(disc, 10);
    lay_dir(disc, &root);
    udf::read_filesystem(disc).expect("fs")
}

// A Stop on a live drive never touches ScanOptions::halt; the enumerator must not swallow a
// Halted read into a successful (shorter) scan. Halt lands on the LAST playlist
// deliberately.
#[test]
fn halted_playlist_read_is_not_reported_as_a_shorter_disc() {
    let mut disc = MemDisc::new();
    let udf = two_playlist_bd_fs(&mut disc);
    // Sanity: both playlists enumerate when nothing is cancelled, so a
    // truncated result below can only be the cancel.
    assert_eq!(
        Disc::scan_bluray_titles(&mut disc, &udf, None)
            .expect("scan")
            .len(),
        2,
        "fixture must offer two keepable playlists"
    );
    let mut reader = HaltingReader {
        inner: &mut disc,
        halt_range: PART_START + 40000..u32::MAX, // 00801.mpls's data extent
    };
    let res = Disc::scan_bluray_titles(&mut reader, &udf, None);
    assert!(
        matches!(res, Err(Error::Halted)),
        "a read cancelled by the drive's own halt flag must surface as a \
             cancelled scan, not as a shorter title list; got {:?}",
        res.map(|ts| ts.iter().map(|t| t.playlist.clone()).collect::<Vec<_>>())
    );
}

// The same cancel landing on a.clpi read must not be classified as an unresolvable clip
// either — it must propagate, not be logged as a disc defect.
#[test]
fn halted_clpi_read_is_not_accounted_as_an_unresolvable_clip() {
    let mut disc = MemDisc::new();
    let udf = two_playlist_bd_fs(&mut disc);
    let mpls = build_mpls(
        &[PiSpec {
            clip_id: *b"00001",
            in_time: 0,
            out_time: 7200 * 45000,
        }],
        (0, 0, 0, 0, 0, 0, 0, 0),
        &[],
        &[],
    );
    // Only the CLIPINF data extent is cancelled; the MPLS bytes are
    // already in hand and every other structure reads normally.
    let mut reader = HaltingReader {
        inner: &mut disc,
        halt_range: PART_START + 8000..PART_START + 8001,
    };
    let res = Disc::parse_playlist(&mut reader, &udf, "00800.mpls", &mpls);
    assert!(
        matches!(res, Err(Error::Halted)),
        "a cancelled .clpi read must propagate, not drop the title as a \
             disc defect; got {:?}",
        res.map(|t| t.map(|t| (t.size_bytes, t.extents)))
    );
}

// And the same cancel landing on file_extents (the clip's ICB, not its CLIPINF) must
// propagate too, not yield a title with runtime counted and zero bytes behind it.
#[test]
fn halted_extent_resolve_is_not_a_title_missing_its_clip() {
    let mut disc = MemDisc::new();
    let udf = two_playlist_bd_fs(&mut disc);
    let mpls = build_mpls(
        &[PiSpec {
            clip_id: *b"00001",
            in_time: 0,
            out_time: 7200 * 45000,
        }],
        (0, 0, 0, 0, 0, 0, 0, 0),
        &[],
        &[],
    );
    // ONE sector is cancelled: the .m2ts ICB (LBA 100), exactly where
    // `file_extents` looks. The .clpi still reads, so the earlier CLIPINF
    // arm is not the thing under test here.
    let mut reader = HaltingReader {
        inner: &mut disc,
        halt_range: PART_START + 100..PART_START + 101,
    };
    let res = Disc::parse_playlist(&mut reader, &udf, "00800.mpls", &mpls);
    assert!(
        matches!(res, Err(Error::Halted)),
        "a cancelled extent resolve must propagate, not yield a title \
             claiming its full runtime with none of the clip's bytes; got {:?}",
        res.map(|t| t.map(|t| (t.size_bytes, t.extents)))
    );
}

// ---------------------------------------------------------------
// Tests: read_meta_title
// ---------------------------------------------------------------

/// read_meta_title extracts <di:name> from BDMV/META/DL/*eng*.xml and
/// prefers the English file (bluray.rs `eng.or_else(first)`).
#[test]
fn read_meta_title_extracts_english_di_name() {
    let mut disc = MemDisc::new();
    let xml = b"<x><di:name>My Movie</di:name></x>".to_vec();
    let dl = DirSpec {
        name: "DL".to_string(),
        icb_lba: 30,
        dir_data_lba: 31,
        files: vec![file_with("bdmt_eng.xml", 104, 50000, xml, false)],
        subdirs: vec![],
    };
    let meta = DirSpec {
        name: "META".to_string(),
        icb_lba: 28,
        dir_data_lba: 29,
        files: Vec::new(),
        subdirs: vec![dl],
    };
    let bdmv = DirSpec {
        name: "BDMV".to_string(),
        icb_lba: 20,
        dir_data_lba: 21,
        files: Vec::new(),
        subdirs: vec![meta],
    };
    let root = DirSpec {
        name: String::new(),
        icb_lba: 10,
        dir_data_lba: 11,
        files: Vec::new(),
        subdirs: vec![bdmv],
    };
    build_udf_skeleton(&mut disc, 10);
    lay_dir(&mut disc, &root);
    let udf = udf::read_filesystem(&mut disc).expect("fs");
    assert_eq!(
        Disc::read_meta_title(&mut disc, &udf),
        Some("My Movie".to_string())
    );
}

/// Entities in <di:name> are decoded by read_meta_title itself.
#[test]
fn read_meta_title_decodes_xml_entities() {
    let mut disc = MemDisc::new();
    let xml = b"<x><di:name>Tom &amp; Jerry</di:name></x>".to_vec();
    let dl = DirSpec {
        name: "DL".to_string(),
        icb_lba: 30,
        dir_data_lba: 31,
        files: vec![file_with("bdmt_eng.xml", 104, 50000, xml, false)],
        subdirs: vec![],
    };
    let meta = DirSpec {
        name: "META".to_string(),
        icb_lba: 28,
        dir_data_lba: 29,
        files: Vec::new(),
        subdirs: vec![dl],
    };
    let bdmv = DirSpec {
        name: "BDMV".to_string(),
        icb_lba: 20,
        dir_data_lba: 21,
        files: Vec::new(),
        subdirs: vec![meta],
    };
    let root = DirSpec {
        name: String::new(),
        icb_lba: 10,
        dir_data_lba: 11,
        files: Vec::new(),
        subdirs: vec![bdmv],
    };
    build_udf_skeleton(&mut disc, 10);
    lay_dir(&mut disc, &root);
    let udf = udf::read_filesystem(&mut disc).expect("fs");
    assert_eq!(
        Disc::read_meta_title(&mut disc, &udf),
        Some("Tom & Jerry".to_string())
    );
}

/// The placeholder title "Blu-ray" and empty titles are rejected
/// (bluray.rs `!title.is_empty() && title != "Blu-ray"`).
#[test]
fn read_meta_title_rejects_placeholder_and_empty() {
    for body in ["<di:name>Blu-ray</di:name>", "<di:name>   </di:name>"] {
        let mut disc = MemDisc::new();
        let dl = DirSpec {
            name: "DL".to_string(),
            icb_lba: 30,
            dir_data_lba: 31,
            files: vec![file_with(
                "bdmt_eng.xml",
                104,
                50000,
                body.as_bytes().to_vec(),
                false,
            )],
            subdirs: vec![],
        };
        let meta = DirSpec {
            name: "META".to_string(),
            icb_lba: 28,
            dir_data_lba: 29,
            files: Vec::new(),
            subdirs: vec![dl],
        };
        let bdmv = DirSpec {
            name: "BDMV".to_string(),
            icb_lba: 20,
            dir_data_lba: 21,
            files: Vec::new(),
            subdirs: vec![meta],
        };
        let root = DirSpec {
            name: String::new(),
            icb_lba: 10,
            dir_data_lba: 11,
            files: Vec::new(),
            subdirs: vec![bdmv],
        };
        build_udf_skeleton(&mut disc, 10);
        lay_dir(&mut disc, &root);
        let udf = udf::read_filesystem(&mut disc).expect("fs");
        assert_eq!(
            Disc::read_meta_title(&mut disc, &udf),
            None,
            "placeholder/empty title must be rejected for body {body:?}"
        );
    }
}

// A non-.xml file must be ignored even if its content looks like valid meta XML: extension
// gating, not content sniffing, decides eligibility.
#[test]
fn read_meta_title_ignores_non_xml_file_regardless_of_content() {
    let mut disc = MemDisc::new();
    let bogus = b"<x><di:name>Should Not Be Used</di:name></x>".to_vec();
    let dl = DirSpec {
        name: "DL".to_string(),
        icb_lba: 30,
        dir_data_lba: 31,
        files: vec![file_with("bdmt_eng.txt", 104, 50000, bogus, false)],
        subdirs: vec![],
    };
    let meta = DirSpec {
        name: "META".to_string(),
        icb_lba: 28,
        dir_data_lba: 29,
        files: Vec::new(),
        subdirs: vec![dl],
    };
    let bdmv = DirSpec {
        name: "BDMV".to_string(),
        icb_lba: 20,
        dir_data_lba: 21,
        files: Vec::new(),
        subdirs: vec![meta],
    };
    let root = DirSpec {
        name: String::new(),
        icb_lba: 10,
        dir_data_lba: 11,
        files: Vec::new(),
        subdirs: vec![bdmv],
    };
    build_udf_skeleton(&mut disc, 10);
    lay_dir(&mut disc, &root);
    let udf = udf::read_filesystem(&mut disc).expect("fs");
    assert_eq!(
        Disc::read_meta_title(&mut disc, &udf),
        None,
        "a non-.xml file must be ignored even if its content looks like valid meta XML"
    );
}

/// No META directory → None.
#[test]
fn read_meta_title_no_meta_dir_is_none() {
    let mut disc = MemDisc::new();
    let udf = make_min_fs(&mut disc);
    assert_eq!(Disc::read_meta_title(&mut disc, &udf), None);
}

#[test]
fn xml_text_decode_entities_and_cdata() {
    assert_eq!(xml_text_decode("Tom &amp; Jerry"), "Tom & Jerry");
    assert_eq!(
        xml_text_decode("&lt;A&gt; &#65;&#x42; &bogus; &"),
        "<A> AB &bogus; &"
    );
    assert_eq!(xml_text_decode("<![CDATA[Tom & Jerry]]>"), "Tom & Jerry");
}

// A clip whose stream file is absent under every name must drop the title (like a
// missing .clpi), not keep it counting bytes that have no extents.
#[test]
fn parse_playlist_missing_stream_yields_no_title() {
    // CLIPINF has the .clpi but STREAM has no stream file.
    let mut disc = MemDisc::new();
    let udf = {
        let bdmv = DirSpec {
            name: "BDMV".to_string(),
            icb_lba: 20,
            dir_data_lba: 21,
            files: Vec::new(),
            subdirs: vec![
                DirSpec {
                    name: "STREAM".to_string(),
                    icb_lba: 22,
                    dir_data_lba: 23,
                    files: Vec::new(),
                    subdirs: vec![],
                },
                DirSpec {
                    name: "CLIPINF".to_string(),
                    icb_lba: 24,
                    dir_data_lba: 25,
                    files: vec![file_with("00001.clpi", 102, 8000, build_clpi(4000), false)],
                    subdirs: vec![],
                },
            ],
        };
        let root = DirSpec {
            name: String::new(),
            icb_lba: 10,
            dir_data_lba: 11,
            files: Vec::new(),
            subdirs: vec![bdmv],
        };
        build_udf_skeleton(&mut disc, 10);
        lay_dir(&mut disc, &root);
        udf::read_filesystem(&mut disc).expect("fs")
    };
    let mpls = build_mpls(
        &[PiSpec {
            clip_id: *b"00001",
            in_time: 0,
            out_time: 60 * 45000,
        }],
        (0, 0, 0, 0, 0, 0, 0, 0),
        &[],
        &[],
    );
    let t = Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls).expect("scan");
    assert!(t.is_none(), "missing stream file must drop the title");
}

struct CountingDisc {
    inner: MemDisc,
    reads: usize,
}
impl SectorSource for CountingDisc {
    fn read_sectors(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        recovery: bool,
    ) -> Result<usize> {
        self.reads += 1;
        self.inner.read_sectors(lba, count, buf, recovery)
    }
}

// Repeated PlayItems of one clip must not re-read its .clpi.
#[test]
fn parse_playlist_repeated_clip_reads_clpi_once() {
    let pi = |i: u32| PiSpec {
        clip_id: *b"00001",
        in_time: i * 60 * 45000,
        out_time: (i + 1) * 60 * 45000,
    };
    let mut reads = Vec::new();
    for n in [1u32, 6] {
        let mut disc = MemDisc::new();
        let udf = make_bdmv_fs(&mut disc, &[("00001", 1000, 4000, 5000)]);
        let mut cd = CountingDisc {
            inner: disc,
            reads: 0,
        };
        let items: Vec<PiSpec> = (0..n).map(pi).collect();
        let mpls = build_mpls(&items, (0, 0, 0, 0, 0, 0, 0, 0), &[], &[]);
        Disc::parse_playlist(&mut cd, &udf, "00001.mpls", &mpls)
            .expect("scan")
            .expect("title");
        reads.push(cd.reads);
    }
    assert_eq!(reads[0], reads[1], "extra PlayItems of a seen clip re-read");
}

// Reads that touch one LBA (the .clpi of clip 00001 at 8000).
struct LbaWatch {
    inner: MemDisc,
    lba: u32,
    hits: u32,
}
impl SectorSource for LbaWatch {
    fn capacity_sectors(&self) -> u32 {
        self.inner.capacity_sectors()
    }
    fn read_sectors(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        recovery: bool,
    ) -> Result<usize> {
        if lba <= self.lba && self.lba < lba + count as u32 {
            self.hits += 1;
        }
        self.inner.read_sectors(lba, count, buf, recovery)
    }
}

// N playlists naming one clip must read its .clpi once per scan, not once per playlist.
#[test]
fn scan_bluray_titles_reads_shared_clpi_once_across_playlists() {
    let mut disc = MemDisc::new();
    let udf = two_playlist_bd_fs(&mut disc);
    let mut w = LbaWatch {
        inner: disc,
        lba: PART_START + 8000,
        hits: 0,
    };
    let titles = Disc::scan_bluray_titles(&mut w, &udf, None).expect("scan");
    assert_eq!(titles.len(), 2);
    assert_eq!(w.hits, 1, "shared .clpi re-read per playlist");
}

// The in-loop poll must fire before any playlist is read (the post-loop poll alone would
// also yield Halted, so count reads).
#[test]
fn scan_bluray_titles_polls_halt_before_reading_each_playlist() {
    let mut disc = MemDisc::new();
    let udf = two_playlist_bd_fs(&mut disc);
    let mut cd = CountingDisc {
        inner: disc,
        reads: 0,
    };
    let halt = crate::halt::Halt::new();
    halt.cancel();
    let res = Disc::scan_bluray_titles(&mut cd, &udf, Some(&halt));
    assert!(matches!(res, Err(Error::Halted)), "got {res:?}");
    assert_eq!(cd.reads, 0, "a cancelled scan must not read any playlist");
}

// Each clip's feed_span is its byte range in the concatenated extents; a repeated clip
// reuses its first span and adds no bytes.
#[test]
fn parse_playlist_feed_spans_follow_extent_order_and_repeat() {
    let mut disc = MemDisc::new();
    let udf = make_bdmv_fs(
        &mut disc,
        &[("00001", 1000, 4000, 5000), ("00002", 500, 2000, 9000)],
    );
    let pi = |c: &[u8; 5], i: u32| PiSpec {
        clip_id: *c,
        in_time: i * 60 * 45000,
        out_time: (i + 1) * 60 * 45000,
    };
    let mpls = build_mpls(
        &[pi(b"00001", 0), pi(b"00002", 1), pi(b"00001", 2)],
        (0, 0, 0, 0, 0, 0, 0, 0),
        &[],
        &[],
    );
    let t = Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls)
        .expect("scan")
        .expect("title");
    assert_eq!(t.extents.len(), 2);
    let s1 = 1000u64 * 2048;
    let s2 = s1 + 500 * 2048;
    assert_eq!(t.clips[0].feed_span, Some((0, s1)));
    assert_eq!(t.clips[1].feed_span, Some((s1, s2)));
    assert_eq!(t.clips[2].feed_span, Some((0, s1)), "repeat reuses span");
}

// A clip with no extents gets no feed_span and does not shift later clips' spans. (UDF
// itself yields no entry for a zero-length file, so this is the empty-span guard, not the
// extent filter; the filter is covered by `push_extents_*` below.)
#[test]
fn parse_playlist_clip_without_extents_gets_no_feed_span() {
    let mut disc = MemDisc::new();
    let udf = make_bdmv_fs(
        &mut disc,
        &[("00001", 0, 4000, 5000), ("00002", 500, 2000, 9000)],
    );
    let pi = |c: &[u8; 5], i: u32| PiSpec {
        clip_id: *c,
        in_time: i * 60 * 45000,
        out_time: (i + 1) * 60 * 45000,
    };
    let mpls = build_mpls(
        &[pi(b"00001", 0), pi(b"00002", 1)],
        (0, 0, 0, 0, 0, 0, 0, 0),
        &[],
        &[],
    );
    let t = Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls)
        .expect("scan")
        .expect("title");
    assert_eq!(t.extents.len(), 1);
    assert_eq!(t.clips[0].feed_span, None);
    assert_eq!(t.clips[1].feed_span, Some((0, 500 * 2048)));
}

// The extent filter: zero-sector and LBA-0 extents are dropped and do not advance feed_pos.
// Each half is pinned separately (a lone `sectors > 0` or lone `lba > 0` must fail).
#[test]
fn push_extents_drops_empty_and_lba_zero_extents_without_advancing() {
    let mut extents = Vec::new();
    let mut pos = 0u64;
    push_extents(
        vec![(100, 2), (200, 0), (0, 5), (300, 3)],
        &mut extents,
        &mut pos,
    );
    let got: Vec<(u32, u32)> = extents
        .iter()
        .map(|e| (e.start_lba, e.sector_count))
        .collect();
    assert_eq!(got, vec![(100, 2), (300, 3)]);
    assert_eq!(pos, 5 * 2048);
}

#[test]
fn push_extents_drops_zero_sector_extent_alone() {
    let mut extents = Vec::new();
    let mut pos = 0u64;
    push_extents(vec![(200, 0), (300, 1)], &mut extents, &mut pos);
    assert_eq!(extents.len(), 1);
    assert_eq!(pos, 2048);
}

#[test]
fn push_extents_drops_lba_zero_extent_alone() {
    let mut extents = Vec::new();
    let mut pos = 0u64;
    push_extents(vec![(0, 4), (300, 1)], &mut extents, &mut pos);
    assert_eq!(extents.len(), 1);
    assert_eq!(pos, 2048);
}

// A clip whose file has several allocation descriptors: its extents are all kept in
// order, its feed_span covers their sum, and the next clip starts after it. A zero-length
// AD ends the list (ECMA-167 4/12), so a descriptor after it never appears.
#[test]
fn parse_playlist_multi_extent_clip_feed_span_covers_sum() {
    let mut disc = MemDisc::new();
    let stream = vec![
        file_ads(
            "00001.m2ts",
            100,
            &[
                (5000, 300 * 2048),
                (7000, 200 * 2048),
                (0, 0),
                (8000, 99 * 2048),
            ],
            true,
        ),
        file("00002.m2ts", 101, 9000, 500 * 2048, true),
    ];
    let clipinf = vec![
        file_with("00001.clpi", 102, 6000, build_clpi(4000), false),
        file_with("00002.clpi", 103, 6100, build_clpi(2000), false),
    ];
    let mk = |name: &str, icb, dd, files, subdirs| DirSpec {
        name: name.to_string(),
        icb_lba: icb,
        dir_data_lba: dd,
        files,
        subdirs,
    };
    let root = mk(
        "",
        10,
        11,
        Vec::new(),
        vec![mk(
            "BDMV",
            20,
            21,
            Vec::new(),
            vec![
                mk("STREAM", 22, 23, stream, vec![]),
                mk("CLIPINF", 24, 25, clipinf, vec![]),
            ],
        )],
    );
    build_udf_skeleton(&mut disc, 10);
    lay_dir(&mut disc, &root);
    let udf = udf::read_filesystem(&mut disc).expect("fs");
    let pi = |c: &[u8; 5], i: u32| PiSpec {
        clip_id: *c,
        in_time: i * 60 * 45000,
        out_time: (i + 1) * 60 * 45000,
    };
    let mpls = build_mpls(
        &[pi(b"00001", 0), pi(b"00002", 1)],
        (0, 0, 0, 0, 0, 0, 0, 0),
        &[],
        &[],
    );
    let t = Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls)
        .expect("scan")
        .expect("title");
    let got: Vec<(u32, u32)> = t
        .extents
        .iter()
        .map(|e| (e.start_lba, e.sector_count))
        .collect();
    let ps = udf.partition_start();
    assert_eq!(
        got,
        vec![(ps + 5000, 300), (ps + 7000, 200), (ps + 9000, 500)]
    );
    let s1 = 500u64 * 2048;
    assert_eq!(t.clips[0].feed_span, Some((0, s1)));
    assert_eq!(t.clips[1].feed_span, Some((s1, s1 + 500 * 2048)));
}

// Chapter names number the surviving entry marks 1..n, not their original mark index.
#[test]
fn parse_playlist_chapter_names_are_dense_ordinals_after_filter() {
    let mut disc = MemDisc::new();
    let udf = make_bdmv_fs(&mut disc, &[("00001", 100, 400, 5000)]);
    let mk = |mark_type: u8, secs: u32| MarkSpec {
        mark_type,
        play_item_ref: 0,
        timestamp: secs * 45000,
    };
    let mpls = build_mpls(
        &[PiSpec {
            clip_id: *b"00001",
            in_time: 0,
            out_time: 120 * 45000,
        }],
        (0, 0, 0, 0, 0, 0, 0, 0),
        &[],
        &[mk(2, 0), mk(1, 10), mk(2, 20), mk(1, 30), mk(1, 40)],
    );
    let t = Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls)
        .expect("scan")
        .expect("title");
    let names: Vec<&str> = t.chapters.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, ["1", "2", "3"]);
}

// With both bdmt_eng.xml and another language present, English wins regardless of order;
// with no English file the first XML is used.
#[test]
fn read_meta_title_prefers_english_then_falls_back_to_first() {
    let title_of = |names: &[&str]| {
        let mut disc = MemDisc::new();
        let files = names
            .iter()
            .enumerate()
            .map(|(i, n)| {
                let xml = format!("<x><di:name>T-{n}</di:name></x>").into_bytes();
                file_with(n, 104 + i as u32, 50000 + i as u32 * 10, xml, false)
            })
            .collect();
        let dl = DirSpec {
            name: "DL".to_string(),
            icb_lba: 30,
            dir_data_lba: 31,
            files,
            subdirs: vec![],
        };
        let meta = DirSpec {
            name: "META".to_string(),
            icb_lba: 28,
            dir_data_lba: 29,
            files: Vec::new(),
            subdirs: vec![dl],
        };
        let bdmv = DirSpec {
            name: "BDMV".to_string(),
            icb_lba: 20,
            dir_data_lba: 21,
            files: Vec::new(),
            subdirs: vec![meta],
        };
        let root = DirSpec {
            name: String::new(),
            icb_lba: 10,
            dir_data_lba: 11,
            files: Vec::new(),
            subdirs: vec![bdmv],
        };
        build_udf_skeleton(&mut disc, 10);
        lay_dir(&mut disc, &root);
        let udf = udf::read_filesystem(&mut disc).expect("fs");
        Disc::read_meta_title(&mut disc, &udf)
    };
    assert_eq!(
        title_of(&["bdmt_fra.xml", "bdmt_eng.xml"]).as_deref(),
        Some("T-bdmt_eng.xml")
    );
    assert_eq!(
        title_of(&["bdmt_fra.xml", "bdmt_deu.xml"]).as_deref(),
        Some("T-bdmt_fra.xml")
    );
}
