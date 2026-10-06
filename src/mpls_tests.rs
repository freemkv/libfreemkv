use super::*;

/// A mark entry for test MPLS building.
struct TestMark {
    mark_type: u8,
    play_item_ref: u16,
    timestamp: u32,
}

/// Build a minimal MPLS binary with given play items and STN streams on the first item.
/// STN counts: (n_video, n_audio, n_pg, n_ig, n_sec_audio, n_sec_video, n_pip_pg, n_dv)
fn build_mpls(
    play_items_data: &[(
        /*clip_id*/ &[u8; 5],
        /*conn*/ u8,
        /*in_time, out_time*/ u32,
        u32,
    )],
    stn_counts: (u8, u8, u8, u8, u8, u8, u8, u8),
    stream_entries: &[Vec<u8>], // raw stream entry + attributes bytes for each stream
) -> Vec<u8> {
    build_mpls_with_marks(play_items_data, stn_counts, stream_entries, &[])
}

fn build_mpls_with_marks(
    play_items_data: &[(
        /*clip_id*/ &[u8; 5],
        /*conn*/ u8,
        /*in_time, out_time*/ u32,
        u32,
    )],
    stn_counts: (u8, u8, u8, u8, u8, u8, u8, u8),
    stream_entries: &[Vec<u8>],
    marks: &[TestMark],
) -> Vec<u8> {
    let playlist_start: u32 = 40; // right after the 40-byte header
    let mut buf = Vec::new();

    // File header: "MPLS" + version + playlist_start + mark_start placeholder
    buf.extend_from_slice(b"MPLS0200");
    buf.extend_from_slice(&playlist_start.to_be_bytes());
    // mark_start placeholder (will be patched), extension_start, padding to 40 bytes
    buf.extend_from_slice(&[0u8; 28]);

    // PlayList section starts here (offset 40)
    // PlayList: length(4) + reserved(2) + num_play_items(2) + num_sub_paths(2) = 10 header bytes
    let pl_start = buf.len();
    buf.extend_from_slice(&[0u8; 4]); // length placeholder
    buf.extend_from_slice(&[0u8; 2]); // reserved
    buf.extend_from_slice(&(play_items_data.len() as u16).to_be_bytes());
    buf.extend_from_slice(&[0u8; 2]); // num_sub_paths

    for (idx, (clip_id, conn, in_time, out_time)) in play_items_data.iter().enumerate() {
        // Build play item content
        let mut item = Vec::new();
        // [0..5] clip_id
        item.extend_from_slice(*clip_id);
        // [5..9] codec_id ("M2TS")
        item.extend_from_slice(b"M2TS");
        // [9] fully reserved
        item.push(0);
        // [10] is_multi_angle (bit 4) + connection_condition (low nibble)
        item.push(*conn & 0x0F);
        // [11] stc_id / reserved
        item.push(0);
        // [12..16] in_time
        item.extend_from_slice(&in_time.to_be_bytes());
        // [16..20] out_time
        item.extend_from_slice(&out_time.to_be_bytes());
        // [20..28] UO_mask_table
        item.extend_from_slice(&[0u8; 8]);
        // [28] misc flags
        item.push(0);
        // [29] still_mode
        item.push(0);
        // [30..32] still_time
        item.extend_from_slice(&[0u8; 2]);

        // STN table (only for the first play item)
        if idx == 0 {
            // STN header: length(2) + reserved(2) + counts(8) + reserved(4) = 16 bytes
            let stn_header_start = item.len();
            item.extend_from_slice(&[0u8; 2]); // STN length placeholder
            item.extend_from_slice(&[0u8; 2]); // reserved
            item.push(stn_counts.0); // n_video
            item.push(stn_counts.1); // n_audio
            item.push(stn_counts.2); // n_pg
            item.push(stn_counts.3); // n_ig
            item.push(stn_counts.4); // n_sec_audio
            item.push(stn_counts.5); // n_sec_video
            item.push(stn_counts.6); // n_pip_pg
            item.push(stn_counts.7); // n_dv
            item.extend_from_slice(&[0u8; 4]); // reserved

            // Stream entries
            for se in stream_entries {
                item.extend_from_slice(se);
            }

            // Patch STN length
            let stn_len = (item.len() - stn_header_start - 2) as u16;
            let stn_len_bytes = stn_len.to_be_bytes();
            item[stn_header_start] = stn_len_bytes[0];
            item[stn_header_start + 1] = stn_len_bytes[1];
        }

        // Write item_length(2) + item
        let item_length = item.len() as u16;
        buf.extend_from_slice(&item_length.to_be_bytes());
        buf.extend_from_slice(&item);
    }

    // Patch PlayList length
    let pl_len = (buf.len() - pl_start - 4) as u32;
    let pl_len_bytes = pl_len.to_be_bytes();
    buf[pl_start] = pl_len_bytes[0];
    buf[pl_start + 1] = pl_len_bytes[1];
    buf[pl_start + 2] = pl_len_bytes[2];
    buf[pl_start + 3] = pl_len_bytes[3];

    // Write PlayListMark section
    let mark_start = buf.len() as u32;
    // Patch mark_start offset in header (bytes 12-15)
    let ms_bytes = mark_start.to_be_bytes();
    buf[12] = ms_bytes[0];
    buf[13] = ms_bytes[1];
    buf[14] = ms_bytes[2];
    buf[15] = ms_bytes[3];

    // Mark section: length(4) + num_marks(2) + marks(14 each)
    let mark_section_len = 2 + marks.len() * 14;
    buf.extend_from_slice(&(mark_section_len as u32).to_be_bytes());
    buf.extend_from_slice(&(marks.len() as u16).to_be_bytes());
    for m in marks {
        buf.push(0); // [0] reserved
        buf.push(m.mark_type); // [1] mark_type
        buf.extend_from_slice(&m.play_item_ref.to_be_bytes()); // [2-3] play_item_ref
        buf.extend_from_slice(&m.timestamp.to_be_bytes()); // [4-7] timestamp
        buf.extend_from_slice(&[0u8; 6]); // [8-13] entry_ES_PID(2) + duration(4)
    }

    buf
}

// Build a stream entry (stream_entry part + stream_attributes part),
// type=0x01 (PlayItem stream). Video attrs: coding_type+format_rate
// [+hdr if HEVC]; audio adds language(3); PG: coding_type+language(3).
fn build_stream_entry_video(
    pid: u16,
    coding_type: u8,
    format: u8,
    rate: u8,
    hdr: Option<u8>,
) -> Vec<u8> {
    let mut out = Vec::new();
    // Stream entry: length(1) + sub_path_type(1) + pid(2)
    out.push(3); // se_len = 3 bytes (type + pid_hi + pid_lo)
    out.push(0x01); // type: PlayItem stream
    out.extend_from_slice(&pid.to_be_bytes());
    // Stream attributes
    let mut attrs = vec![coding_type, (format << 4) | rate];
    if let Some(h) = hdr {
        attrs.push(h);
    }
    out.push(attrs.len() as u8); // sa_len
    out.extend_from_slice(&attrs);
    out
}

fn build_stream_entry_audio(
    pid: u16,
    coding_type: u8,
    ch_layout: u8,
    sample_rate: u8,
    lang: &[u8; 3],
) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(3);
    out.push(STREAM_ENTRY_PLAYITEM_CLIP);
    out.extend_from_slice(&pid.to_be_bytes());
    // attrs: coding_type(1) + format_rate(1) + language(3)
    let attrs = vec![
        coding_type,
        (ch_layout << 4) | sample_rate,
        lang[0],
        lang[1],
        lang[2],
    ];
    out.push(attrs.len() as u8);
    out.extend_from_slice(&attrs);
    out
}

fn build_stream_entry_pg(pid: u16, coding_type: u8, lang: &[u8; 3]) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(3);
    out.push(STREAM_ENTRY_PLAYITEM_CLIP);
    out.extend_from_slice(&pid.to_be_bytes());
    // attrs: coding_type(1) + language(3)
    let attrs = vec![coding_type, lang[0], lang[1], lang[2]];
    out.push(attrs.len() as u8);
    out.extend_from_slice(&attrs);
    out
}

#[test]
fn parse_valid_mpls() {
    let in_time: u32 = 90000; // 2 seconds at 45kHz
    let out_time: u32 = 4500000; // 100 seconds

    let video = build_stream_entry_video(0x1011, 0x1B, 6, 1, None); // H264, 1080p, 23.976
    let audio = build_stream_entry_audio(0x1100, 0x83, 6, 1, b"eng"); // TrueHD, 5.1, 48kHz
    let pg = build_stream_entry_pg(0x1200, 0x90, b"eng"); // PGS subtitle

    let data = build_mpls(
        &[(b"00001", 1, in_time, out_time)],
        (1, 1, 1, 0, 0, 0, 0, 0),
        &[video, audio, pg],
    );

    let playlist = parse(&data).expect("should parse valid MPLS");
    assert_eq!(playlist.version, "0200");
    assert_eq!(playlist.play_items.len(), 1);
    assert_eq!(playlist.play_items[0].clip_id, "00001");
    assert_eq!(playlist.play_items[0].in_time, in_time);
    assert_eq!(playlist.play_items[0].out_time, out_time);
    assert_eq!(playlist.play_items[0].connection_condition, 1);
}

#[test]
fn parse_streams() {
    let video = build_stream_entry_video(0x1011, 0x24, 8, 1, Some(0x12)); // HEVC, 2160p, 23.976, HDR10+BT.2020
    let audio = build_stream_entry_audio(0x1100, 0x83, 6, 1, b"eng");
    let pg = build_stream_entry_pg(0x1200, 0x90, b"fra");

    let data = build_mpls(
        &[(b"00001", 1, 0, 9000000)],
        (1, 1, 1, 0, 0, 0, 0, 0),
        &[video, audio, pg],
    );

    let playlist = parse(&data).expect("should parse");
    assert_eq!(playlist.streams.len(), 3);

    // Video stream
    let v = &playlist.streams[0];
    assert_eq!(v.stream_type, 1);
    assert_eq!(v.pid, 0x1011);
    assert_eq!(v.coding_type, 0x24); // HEVC
    assert_eq!(v.video_format, 8); // 2160p
    assert_eq!(v.video_rate, 1); // 23.976
    assert_eq!(v.dynamic_range, 1); // HDR10
    assert_eq!(v.color_space, 2); // BT.2020
    assert!(!v.secondary);

    // Audio stream
    let a = &playlist.streams[1];
    assert_eq!(a.stream_type, 2);
    assert_eq!(a.pid, 0x1100);
    assert_eq!(a.coding_type, 0x83); // TrueHD
    assert_eq!(a.audio_format, 6); // 5.1
    assert_eq!(a.audio_rate, 1); // 48kHz
    assert_eq!(a.language, "eng");
    assert!(!a.secondary);

    // PG subtitle stream
    let s = &playlist.streams[2];
    assert_eq!(s.stream_type, 3);
    assert_eq!(s.pid, 0x1200);
    assert_eq!(s.coding_type, 0x90); // PGS
    assert_eq!(s.language, "fra");
    assert!(!s.secondary);
}

// A first play item that ends exactly at the 16-byte STN header (48 bytes) but
// declares non-zero counts: the header is read, no entry fits, nothing panics.
// (`>` vs `>=` at this gate is not observable: no entry can fit either way.)
#[test]
fn stn_header_ending_exactly_at_the_item_boundary_is_parsed() {
    let data = build_mpls(&[(b"00001", 1, 0, 9000000)], (1, 1, 1, 1, 1, 1, 1, 1), &[]);
    let playlist = parse(&data).expect("a boundary-length STN header must parse");
    assert_eq!(playlist.play_items.len(), 1);
    assert!(playlist.streams.is_empty());
}

// Regression for issue #45 (see module `//!` header): a multi-angle first
// PlayItem puts its STN past the angle block (2 angles ⇒ offset 44); a fixed
// offset of 32 misreads it and drops the video. Assert the video is found.
#[test]
fn multi_angle_first_play_item_stn_is_located_after_the_angle_block() {
    let in_time = 0u32;
    let out_time = 9_000_000u32;
    let number_of_angles = 2u8;

    let mut item = Vec::new();
    item.extend_from_slice(b"00001"); // [0..5] clip_id (angle 1 = primary)
    item.extend_from_slice(b"M2TS"); // [5..9] codec_id
    item.push(0); // [9] reserved
    item.push(0x10); // [10] is_multi_angle (bit 4), connection_condition = 0
    item.push(0); // [11] stc_id
    item.extend_from_slice(&in_time.to_be_bytes()); // [12..16]
    item.extend_from_slice(&out_time.to_be_bytes()); // [16..20]
    item.extend_from_slice(&[0u8; 8]); // [20..28] UO mask
    item.push(0); // [28] misc flags
    item.push(0); // [29] still_mode
    item.extend_from_slice(&[0u8; 2]); // [30..32] still_time
    // Angle block: number_of_angles(1) + flags(1) + (angles-1) refs * 10 bytes.
    item.push(number_of_angles); // [32]
    item.push(0); // [33] is_different_audios / is_seamless_angle_change
    item.extend_from_slice(b"00002"); // angle 2 clip_name(5)
    item.extend_from_slice(b"M2TS"); // angle 2 codec_id(4)
    item.push(0); // angle 2 ref_to_STC_id(1)  -> STN now begins at offset 44
    // STN table: length(2) + reserved(2) + counts(8) + reserved(4) + entries.
    let stn_start = item.len();
    item.extend_from_slice(&[0u8; 2]); // STN length placeholder
    item.extend_from_slice(&[0u8; 2]); // reserved
    item.extend_from_slice(&[1, 0, 0, 0, 0, 0, 0, 0]); // n_video=1, rest 0
    item.extend_from_slice(&[0u8; 4]); // reserved
    item.extend_from_slice(&build_stream_entry_video(0x1011, 0x24, 8, 1, Some(0x12)));
    let stn_len = (item.len() - stn_start - 2) as u16;
    item[stn_start..stn_start + 2].copy_from_slice(&stn_len.to_be_bytes());

    // Wrap in the file + PlayList headers.
    let mut buf = Vec::new();
    buf.extend_from_slice(b"MPLS0200");
    buf.extend_from_slice(&40u32.to_be_bytes()); // playlist_start
    buf.extend_from_slice(&[0u8; 28]); // mark_start(0) + padding to 40
    let pl_start = buf.len();
    buf.extend_from_slice(&[0u8; 4]); // PlayList length placeholder
    buf.extend_from_slice(&[0u8; 2]); // reserved
    buf.extend_from_slice(&1u16.to_be_bytes()); // num_play_items
    buf.extend_from_slice(&[0u8; 2]); // num_sub_paths
    buf.extend_from_slice(&(item.len() as u16).to_be_bytes());
    buf.extend_from_slice(&item);
    let pl_len = (buf.len() - pl_start - 4) as u32;
    buf[pl_start..pl_start + 4].copy_from_slice(&pl_len.to_be_bytes());

    let playlist = parse(&buf).expect("multi-angle MPLS must parse");
    assert_eq!(playlist.play_items.len(), 1);
    assert_eq!(
        playlist.streams.len(),
        1,
        "the primary video must be found past the angle block, not misread as garbage"
    );
    assert_eq!(playlist.streams[0].stream_type, 1, "it is a video stream");
    assert_eq!(playlist.streams[0].pid, 0x1011);
    assert_eq!(playlist.streams[0].coding_type, 0x24); // HEVC
}

#[test]
fn parse_invalid_magic() {
    let mut data = build_mpls(&[(b"00001", 1, 0, 9000000)], (0, 0, 0, 0, 0, 0, 0, 0), &[]);
    data[0] = b'X';
    data[1] = b'X';
    data[2] = b'X';
    data[3] = b'X';
    assert!(parse(&data).is_err());
}

#[test]
fn parse_truncated() {
    // Less than 40 bytes
    assert!(parse(&[0u8; 10]).is_err());
    assert!(parse(b"MPLS0200").is_err());
    assert!(parse(&[0u8; 39]).is_err());
}

#[test]
fn parse_truncated_play_item_keeps_the_items_before_it() {
    let video = build_stream_entry_video(0x1011, 0x1B, 6, 1, None);
    let data = build_mpls(
        &[
            (b"00001", 1, 90000, 4500000),
            (b"00002", 5, 4500000, 9000000),
        ],
        (1, 0, 0, 0, 0, 0, 0, 0),
        &[video],
    );
    let ps = u32::from_be_bytes([data[8], data[9], data[10], data[11]]) as usize;
    let first = u16::from_be_bytes([data[ps + 10], data[ps + 11]]) as usize;
    // Cut inside the second item: one whole item survives.
    let cut = &data[..ps + 10 + 2 + first + 10];
    let playlist = parse(cut).expect("damage must not drop the parsed items");
    assert_eq!(playlist.play_items.len(), 1);
    assert_eq!(playlist.play_items[0].clip_id, "00001");
}

#[test]
fn parse_multiple_play_items() {
    let video = build_stream_entry_video(0x1011, 0x1B, 6, 1, None);

    let data = build_mpls(
        &[
            (b"00001", 1, 90000, 4500000),
            (b"00002", 5, 4500000, 9000000),
            (b"00003", 6, 9000000, 13500000),
        ],
        (1, 0, 0, 0, 0, 0, 0, 0),
        &[video],
    );

    let playlist = parse(&data).expect("should parse multiple play items");
    assert_eq!(playlist.play_items.len(), 3);
    assert_eq!(playlist.play_items[0].clip_id, "00001");
    assert_eq!(playlist.play_items[0].connection_condition, 1);
    assert_eq!(playlist.play_items[1].clip_id, "00002");
    assert_eq!(playlist.play_items[1].connection_condition, 5);
    assert_eq!(playlist.play_items[1].in_time, 4500000);
    assert_eq!(playlist.play_items[2].clip_id, "00003");
    assert_eq!(playlist.play_items[2].connection_condition, 6);
    assert_eq!(playlist.play_items[2].out_time, 13500000);
}

#[test]
fn parse_secondary_streams() {
    // Primary video
    let video = build_stream_entry_video(0x1011, 0x1B, 6, 1, None);
    // Secondary audio (stream_type 5): build as audio, parser overrides type to 5
    let sec_audio_se = build_stream_entry_audio(0x1A00, 0x83, 3, 1, b"eng");
    // Need ref bytes after secondary audio: num_refs(1) + reserved(1) = 2 bytes min
    let mut sec_audio_with_refs = sec_audio_se;
    sec_audio_with_refs.push(0); // num_refs = 0
    sec_audio_with_refs.push(0); // reserved

    // Secondary video (stream_type 6): build as video, parser overrides type to 6
    let sec_video_se = build_stream_entry_video(0x1B00, 0x1B, 4, 1, None);
    // Need ref bytes: n_arefs(1) + reserved(1) + n_prefs(1) + reserved(1) = 4 bytes
    let mut sec_video_with_refs = sec_video_se;
    sec_video_with_refs.push(0); // n_arefs = 0
    sec_video_with_refs.push(0); // reserved
    sec_video_with_refs.push(0); // n_prefs = 0
    sec_video_with_refs.push(0); // reserved

    let data = build_mpls(
        &[(b"00001", 1, 0, 9000000)],
        (1, 0, 0, 0, 1, 1, 0, 0), // 1 video, 0 audio, 0 pg, 0 ig, 1 sec_audio, 1 sec_video
        &[video, sec_audio_with_refs, sec_video_with_refs],
    );

    let playlist = parse(&data).expect("should parse secondary streams");
    // Should have 3 streams: primary video, secondary audio, secondary video
    assert_eq!(playlist.streams.len(), 3);

    // Primary video
    assert_eq!(playlist.streams[0].stream_type, 1);
    assert!(!playlist.streams[0].secondary);

    // Secondary audio
    assert_eq!(playlist.streams[1].stream_type, 5);
    assert!(playlist.streams[1].secondary);
    assert_eq!(playlist.streams[1].pid, 0x1A00);

    // Secondary video
    assert_eq!(playlist.streams[2].stream_type, 6);
    assert!(playlist.streams[2].secondary);
    assert_eq!(playlist.streams[2].pid, 0x1B00);
}

#[test]
fn parse_secondary_video_then_dv_alignment() {
    // Regression: the secondary-video ref-skip must use the same
    // `next < item.len()` guard as the sibling secondary blocks so spos
    // stays aligned for a following stream (here a Dolby Vision EL).
    let video = build_stream_entry_video(0x1011, 0x24, 8, 1, Some(0x12));

    // Secondary video with audio-ref + PG-ref blocks present.
    let mut sec_video_with_refs = build_stream_entry_video(0x1B00, 0x1B, 4, 1, None);
    sec_video_with_refs.push(0); // n_arefs = 0
    sec_video_with_refs.push(0); // reserved
    sec_video_with_refs.push(0); // n_prefs = 0
    sec_video_with_refs.push(0); // reserved

    // Dolby Vision enhancement layer immediately after.
    let dv_el = build_stream_entry_video(0x1015, 0x24, 8, 1, Some(0x12));

    let data = build_mpls(
        &[(b"00001", 1, 0, 9000000)],
        (1, 0, 0, 0, 0, 1, 0, 1), // 1 video, 1 sec_video, 1 dv
        &[video, sec_video_with_refs, dv_el],
    );

    let playlist = parse(&data).expect("should parse");
    assert_eq!(playlist.streams.len(), 3);
    // Secondary video
    assert_eq!(playlist.streams[1].stream_type, 6);
    assert_eq!(playlist.streams[1].pid, 0x1B00);
    // DV EL parsed at the correct offset → correct PID and type 7.
    assert_eq!(playlist.streams[2].stream_type, 7);
    assert_eq!(playlist.streams[2].pid, 0x1015);
    assert!(playlist.streams[2].secondary);
}

#[test]
fn parse_marks_chapter_entries() {
    let video = build_stream_entry_video(0x1011, 0x1B, 6, 1, None);
    let marks = vec![
        TestMark {
            mark_type: 1,
            play_item_ref: 0,
            timestamp: 90000,
        },
        TestMark {
            mark_type: 1,
            play_item_ref: 0,
            timestamp: 4500000,
        },
        TestMark {
            mark_type: 1,
            play_item_ref: 0,
            timestamp: 9000000,
        },
    ];

    let data = build_mpls_with_marks(
        &[(b"00001", 1, 90000, 13500000)],
        (1, 0, 0, 0, 0, 0, 0, 0),
        &[video],
        &marks,
    );

    let playlist = parse(&data).expect("should parse marks");
    assert_eq!(playlist.marks.len(), 3);
    assert_eq!(playlist.marks[0].mark_type, 1);
    assert_eq!(playlist.marks[0].play_item_ref, 0);
    assert_eq!(playlist.marks[0].timestamp, 90000);
    assert_eq!(playlist.marks[1].timestamp, 4500000);
    assert_eq!(playlist.marks[2].timestamp, 9000000);
}

#[test]
fn parse_marks_chapter_timestamps_correct() {
    let video = build_stream_entry_video(0x1011, 0x1B, 6, 1, None);
    let in_time: u32 = 90000;

    // Chapters at 0s, 100s, 200s relative to in_time
    let marks = vec![
        TestMark {
            mark_type: 1,
            play_item_ref: 0,
            timestamp: in_time,
        },
        TestMark {
            mark_type: 1,
            play_item_ref: 0,
            timestamp: in_time + 45000 * 100,
        },
        TestMark {
            mark_type: 1,
            play_item_ref: 0,
            timestamp: in_time + 45000 * 200,
        },
        TestMark {
            mark_type: 2,
            play_item_ref: 0,
            timestamp: in_time + 45000 * 50,
        }, // non-chapter mark
    ];

    let data = build_mpls_with_marks(
        &[(b"00001", 1, in_time, in_time + 45000 * 300)],
        (1, 0, 0, 0, 0, 0, 0, 0),
        &[video],
        &marks,
    );

    let playlist = parse(&data).expect("should parse");
    // All 4 marks should be parsed
    assert_eq!(playlist.marks.len(), 4);
    // Chapter marks (type 1) are 3 of them
    let chapter_marks: Vec<_> = playlist.marks.iter().filter(|m| m.mark_type == 1).collect();
    assert_eq!(chapter_marks.len(), 3);
    // Non-chapter mark (type 2)
    assert_eq!(playlist.marks[3].mark_type, 2);

    // Verify timestamp conversion: (timestamp - in_time) / 45000
    let ch0_secs = (chapter_marks[0].timestamp as f64 - in_time as f64) / 45000.0;
    let ch1_secs = (chapter_marks[1].timestamp as f64 - in_time as f64) / 45000.0;
    let ch2_secs = (chapter_marks[2].timestamp as f64 - in_time as f64) / 45000.0;
    assert!((ch0_secs - 0.0).abs() < 0.001);
    assert!((ch1_secs - 100.0).abs() < 0.001);
    assert!((ch2_secs - 200.0).abs() < 0.001);
}

#[test]
fn mark_type_read_from_correct_offset() {
    // Regression for the mark_type off-by-one: PlayListMark is reserved(1)+mark_type(1)+...,
    // so the parser must read byte[1], not byte[0]. build_mpls_with_marks sets reserved=0
    // and mark_type=byte[1]; distinct non-zero/non-1 types make an offset error unmistakable.
    let video = build_stream_entry_video(0x1011, 0x1B, 6, 1, None);
    let marks = vec![
        TestMark {
            mark_type: 1, // entry mark (chapter)
            play_item_ref: 0,
            timestamp: 90000,
        },
        TestMark {
            mark_type: 2, // link point (not a chapter)
            play_item_ref: 0,
            timestamp: 180000,
        },
        TestMark {
            mark_type: 3, // arbitrary other type
            play_item_ref: 0,
            timestamp: 270000,
        },
    ];

    let data = build_mpls_with_marks(
        &[(b"00001", 1, 0, 9000000)],
        (1, 0, 0, 0, 0, 0, 0, 0),
        &[video],
        &marks,
    );

    let playlist = parse(&data).expect("should parse marks");
    assert_eq!(playlist.marks.len(), 3);
    // If the parser read the reserved byte (byte[0] == 0) these would all
    // be 0; reading byte[1] yields the real types.
    assert_eq!(playlist.marks[0].mark_type, 1);
    assert_eq!(playlist.marks[1].mark_type, 2);
    assert_eq!(playlist.marks[2].mark_type, 3);
    // Only the type-1 mark is a chapter under the corrected convention.
    let chapters = playlist.marks.iter().filter(|m| m.mark_type == 1).count();
    assert_eq!(chapters, 1);
}

#[test]
fn parse_no_marks_section() {
    // When mark_start is 0, no marks should be returned
    let video = build_stream_entry_video(0x1011, 0x1B, 6, 1, None);
    let data = build_mpls(
        &[(b"00001", 1, 90000, 4500000)],
        (1, 0, 0, 0, 0, 0, 0, 0),
        &[video],
    );
    let playlist = parse(&data).expect("should parse without marks");
    // build_mpls writes an empty mark section (0 marks)
    assert_eq!(playlist.marks.len(), 0);
}

// ─── Added hardening tests below, grounded in the BD-ROM MPLS spec byte layout ───

// Header guard: parse() requires `playlist_start + 10 <= data.len()` before
// reading the PlayList header. A playlist_start past EOF must error, not panic.
#[test]
fn playlist_start_past_eof_errs() {
    let mut data = build_mpls(&[(b"00001", 1, 0, 9000000)], (0, 0, 0, 0, 0, 0, 0, 0), &[]);
    // Overwrite PlayList_start_address (bytes 8..12) with a huge offset.
    data[8..12].copy_from_slice(&0xFFFF_0000u32.to_be_bytes());
    assert!(parse(&data).is_err());
}

// connection_condition is the LOW nibble of PlayItem byte[10] (high nibble
// is reserved + is_multi_angle flag). byte[9] is reserved and must not leak
// in; byte[10] = 0xE5 must yield 5 even with a non-zero byte[9].
#[test]
fn connection_condition_is_low_nibble_only() {
    let video = build_stream_entry_video(0x1011, 0x1B, 6, 1, None);
    let mut data = build_mpls(
        &[(b"00001", 0, 0, 9000000)],
        (1, 0, 0, 0, 0, 0, 0, 0),
        &[video],
    );
    // PlayItem base: header(40) + pl_header(10) + item_len(2).
    let item_base = 40 + 10 + 2;
    // byte[9] fully reserved — set it to prove it does not leak in.
    data[item_base + 9] = 0xFF;
    // byte[10] high nibble set — parser must mask to the low nibble.
    data[item_base + 10] = 0xE5; // multi-angle bit (0x10) clear
    let pl = parse(&data).expect("should parse");
    assert_eq!(pl.play_items[0].connection_condition, 0x05);
}

// stream_entry() PID location for type 0x02 (SubPath SubClip):
// subpath_id(1)+subclip_id(1) precede the PID, so PID is at +4. A parser
// reading +2 (type-1 layout) would wrongly pick up subpath/subclip bytes.
#[test]
fn stream_entry_type2_pid_at_offset_4() {
    // Build a primary-audio entry with stream_entry type 0x02.
    // se_len = 5: type(1) + subpath_id(1) + subclip_id(1) + pid(2)
    let mut se = vec![
        5,                            // se_len
        STREAM_ENTRY_SUBPATH_SUBCLIP, // type: SubPath SubClip
        0xAA,                         // subpath_id (must NOT be read as PID hi)
        0xBB,                         // subclip_id
    ];
    se.extend_from_slice(&0x1100u16.to_be_bytes()); // real PID at +4
    // stream_attributes: audio coding(1)+fmt(1)+lang(3)
    let attrs = vec![0x83u8, (6 << 4) | 1, b'e', b'n', b'g'];
    se.push(attrs.len() as u8);
    se.extend_from_slice(&attrs);

    let data = build_mpls(
        &[(b"00001", 1, 0, 9000000)],
        (0, 1, 0, 0, 0, 0, 0, 0),
        &[se],
    );
    let pl = parse(&data).expect("should parse");
    assert_eq!(pl.streams.len(), 1);
    assert_eq!(pl.streams[0].pid, 0x1100);
}

/// stream_entry() PID for type 0x03/0x04 (SubPath clip; 0x04 = DV EL):
/// subpath_id(1) precedes PID, so PID is at +3. Cited in source:
/// DV EL PID e.g. 0x1015.
#[test]
fn stream_entry_type4_pid_at_offset_3() {
    let mut se = Vec::new();
    se.push(4); // se_len: type(1)+subpath_id(1)+pid(2)
    se.push(STREAM_ENTRY_SUBPATH_DV_EL); // type 4 (DV EL)
    se.push(0x07); // subpath_id (not PID)
    se.extend_from_slice(&0x1015u16.to_be_bytes()); // PID at +3
    let attrs = vec![0x24u8, (8 << 4) | 1, 0x12]; // HEVC video attrs
    se.push(attrs.len() as u8);
    se.extend_from_slice(&attrs);

    // Put it in the primary-video slot so it's retained as a stream.
    let data = build_mpls(
        &[(b"00001", 1, 0, 9000000)],
        (1, 0, 0, 0, 0, 0, 0, 0),
        &[se],
    );
    let pl = parse(&data).expect("should parse");
    assert_eq!(pl.streams.len(), 1);
    assert_eq!(pl.streams[0].pid, 0x1015);
}

/// stream_entry() unknown type → pid_off match arm `_ => 0`, so PID is
/// left 0. A type byte of 0x09 (not 1/2/3/4) must yield pid 0, never an
/// out-of-spec read. Grounded in the explicit default arm in source.
#[test]
fn stream_entry_unknown_type_pid_zero() {
    let mut se = Vec::new();
    se.push(3);
    se.push(0x09); // unknown stream_entry type
    se.extend_from_slice(&0x1234u16.to_be_bytes());
    let attrs = vec![0x24u8, (8 << 4) | 1];
    se.push(attrs.len() as u8);
    se.extend_from_slice(&attrs);
    let data = build_mpls(
        &[(b"00001", 1, 0, 9000000)],
        (1, 0, 0, 0, 0, 0, 0, 0),
        &[se],
    );
    let pl = parse(&data).expect("should parse");
    assert_eq!(pl.streams.len(), 1);
    assert_eq!(pl.streams[0].pid, 0); // unknown type → PID not read
}

/// Video stream_attributes: byte[1] high nibble = video_format, low
/// nibble = video_rate (BD spec format/frame_rate packing). Verify the
/// split: 0x84 → format 8 (2160p), rate 4.
#[test]
fn video_attr_nibble_split() {
    let video = build_stream_entry_video(0x1011, 0x1B, 8, 4, None);
    let data = build_mpls(
        &[(b"00001", 1, 0, 9000000)],
        (1, 0, 0, 0, 0, 0, 0, 0),
        &[video],
    );
    let pl = parse(&data).expect("should parse");
    assert_eq!(pl.streams[0].video_format, 8);
    assert_eq!(pl.streams[0].video_rate, 4);
}

// HDR byte (HEVC only, coding_type 0x24): sa[2] hi nibble = dynamic_range,
// lo nibble = color_space. Non-HEVC video (e.g. H264 0x1B) must not
// consume it even if present, per the `coding_type == 0x24` guard.
#[test]
fn hdr_byte_only_for_hevc() {
    // H264 video with a third attr byte present — must stay SDR/unknown.
    let video = build_stream_entry_video(0x1011, 0x1B, 6, 1, Some(0x12));
    let data = build_mpls(
        &[(b"00001", 1, 0, 9000000)],
        (1, 0, 0, 0, 0, 0, 0, 0),
        &[video],
    );
    let pl = parse(&data).expect("should parse");
    assert_eq!(pl.streams[0].coding_type, 0x1B);
    assert_eq!(pl.streams[0].dynamic_range, 0); // not parsed for H264
    assert_eq!(pl.streams[0].color_space, 0);
}

// HEVC hdr_plus_flag is bit 6 of sa[3]; cr_flag (bit 7)
// alone must not read as HDR10+, and a 3-byte attribute block has no flag.
#[test]
fn hevc_hdr_plus_flag_is_bit_6_of_the_fourth_attribute_byte() {
    let entry = |attrs: &[u8]| {
        let mut out = vec![3u8, 0x01, 0x10, 0x11, attrs.len() as u8];
        out.extend_from_slice(attrs);
        let data = build_mpls(
            &[(b"00001", 1, 0, 9000000)],
            (1, 0, 0, 0, 0, 0, 0, 0),
            &[out],
        );
        parse(&data).expect("should parse").streams[0].hdr_plus
    };
    assert!(entry(&[0x24, 0x81, 0x12, 0x40]));
    assert!(!entry(&[0x24, 0x81, 0x12, 0x80]));
    assert!(!entry(&[0x24, 0x81, 0x12, 0x00]));
    assert!(!entry(&[0x24, 0x81, 0x12]));
    assert!(!entry(&[0x1B, 0x61, 0x12, 0x40]), "not read for non-HEVC");
}

// Audio language is at sa[2..5] normally, EXCEPT when the audio slot
// carries a PG coding_type (0x90/0x91): layout is coding_type(1)+lang(3),
// so lang is at sa[1..4]. Verify the PG-in-audio path.
#[test]
fn pg_coding_in_audio_slot_uses_pg_lang_offset() {
    // Audio-slot entry but coding_type 0x90 (PGS): attrs = 0x90 + lang(3).
    let mut se = Vec::new();
    se.push(3);
    se.push(STREAM_ENTRY_PLAYITEM_CLIP);
    se.extend_from_slice(&0x1100u16.to_be_bytes());
    let attrs = vec![0x90u8, b'j', b'p', b'n']; // PG layout: coding + lang
    se.push(attrs.len() as u8);
    se.extend_from_slice(&attrs);
    let data = build_mpls(
        &[(b"00001", 1, 0, 9000000)],
        (0, 1, 0, 0, 0, 0, 0, 0),
        &[se],
    );
    let pl = parse(&data).expect("should parse");
    assert_eq!(pl.streams[0].coding_type, 0x90);
    assert_eq!(pl.streams[0].language, "jpn"); // read from sa[1..4]
    // audio_format/rate not parsed in PG branch.
    assert_eq!(pl.streams[0].audio_format, 0);
}

// IG streams (n_ig, stream_type 4) are consumed to keep the STN cursor
// aligned but NEVER retained as StreamEntry. Placing a DV EL right after
// IG proves spos still advanced correctly past the dropped IG entry.
#[test]
fn ig_consumed_but_not_retained_and_dv_after_aligned() {
    // STN parse order is video, audio, PG+pip_pg, IG, sec_audio, sec_video, DV.
    // The IG entry must be consumed (advancing spos) but never retained; placing
    // a DV EL right after IG proves this — a wrong spos would misread the DV PID.
    let video = build_stream_entry_video(0x1011, 0x24, 8, 1, Some(0x12));
    let ig = build_stream_entry_pg(0x1400, 0x91, b"eng"); // IG entry bytes
    let dv = build_stream_entry_video(0x1015, 0x24, 8, 1, Some(0x12)); // DV EL
    let data = build_mpls(
        &[(b"00001", 1, 0, 9000000)],
        (1, 0, 0, 1, 0, 0, 0, 1), // 1 video, 1 ig, 1 dv
        &[video, ig, dv],
    );
    let pl = parse(&data).expect("should parse");
    // 2 retained streams: video + DV EL (IG dropped).
    assert_eq!(pl.streams.len(), 2);
    assert_eq!(pl.streams[0].stream_type, 1);
    assert_eq!(pl.streams[0].pid, 0x1011);
    // DV EL parsed at correct offset → IG advanced spos past 0x1400.
    assert_eq!(pl.streams[1].stream_type, 7);
    assert_eq!(pl.streams[1].pid, 0x1015);
    assert!(pl.streams.iter().all(|s| s.pid != 0x1400));
}

// parse_stream_entry short-circuits when the declared length runs past
// the item end (`se_end > item.len()` → None); the STN count loop then
// `break`s. Build n_video=2 but only enough bytes for 1 full entry.
#[test]
fn truncated_stream_entry_stops_without_panic() {
    let video = build_stream_entry_video(0x1011, 0x1B, 6, 1, None);
    // Second "entry" declares se_len=200 but supplies no body → None.
    let bad = vec![200u8, STREAM_ENTRY_PLAYITEM_CLIP];
    let data = build_mpls(
        &[(b"00001", 1, 0, 9000000)],
        (2, 0, 0, 0, 0, 0, 0, 0), // claims 2 video
        &[video, bad],
    );
    let pl = parse(&data).expect("should not panic on truncated entry");
    // Only the first parsed; second aborted the loop.
    assert_eq!(pl.streams.len(), 1);
    assert_eq!(pl.streams[0].pid, 0x1011);
}

// PID must be bounded by the entry's declared se_end, not item.len(): a
// short se_len must leave PID 0 rather than reading into stream_attributes.
// se_len=1 (type byte only) means the PID bytes at +2/+3 fall in attrs.
#[test]
fn short_se_len_does_not_read_pid_from_attrs() {
    // se_len = 1: just the type byte, no PID bytes within the entry.
    let mut se = Vec::new();
    se.push(1); // se_len = 1
    se.push(0x01); // type 1; PID would be at +2 but that's past se_end
    // stream_attributes follow immediately.
    let attrs = vec![0x1Bu8, (6 << 4) | 1];
    se.push(attrs.len() as u8);
    se.extend_from_slice(&attrs);
    let data = build_mpls(
        &[(b"00001", 1, 0, 9000000)],
        (1, 0, 0, 0, 0, 0, 0, 0),
        &[se],
    );
    let pl = parse(&data).expect("should parse");
    assert_eq!(pl.streams.len(), 1);
    // PID bytes lie outside the declared entry → must be 0, not attrs.
    assert_eq!(pl.streams[0].pid, 0);
}

/// parse_stream_entry rejects sa_len == 0 (`sa_len < 1` → None). A
/// zero-length stream_attributes block means the entry is unusable and
/// the STN loop must break, not push a degenerate StreamEntry.
#[test]
fn zero_length_stream_attributes_yields_no_stream() {
    let mut se = Vec::new();
    se.push(3);
    se.push(STREAM_ENTRY_PLAYITEM_CLIP);
    se.extend_from_slice(&0x1011u16.to_be_bytes());
    se.push(0); // sa_len = 0 → parse_stream_entry returns None
    let data = build_mpls(
        &[(b"00001", 1, 0, 9000000)],
        (1, 0, 0, 0, 0, 0, 0, 0),
        &[se],
    );
    let pl = parse(&data).expect("should parse");
    assert_eq!(pl.streams.len(), 0);
}

/// PlayListMark timestamp is a big-endian u32 at entry offset +4..+8
/// (after reserved(1)+mark_type(1)+ref(2)). Verify BE decode and that
/// ref_to_PlayItem_id is read from +2..+4.
#[test]
fn mark_timestamp_and_ref_offsets() {
    let video = build_stream_entry_video(0x1011, 0x1B, 6, 1, None);
    let marks = vec![TestMark {
        mark_type: 1,
        play_item_ref: 0x0203,
        timestamp: 0x0A0B0C0D,
    }];
    let data = build_mpls_with_marks(
        &[(b"00001", 1, 0, 9000000)],
        (1, 0, 0, 0, 0, 0, 0, 0),
        &[video],
        &marks,
    );
    let pl = parse(&data).expect("should parse");
    assert_eq!(pl.marks.len(), 1);
    assert_eq!(pl.marks[0].play_item_ref, 0x0203);
    assert_eq!(pl.marks[0].timestamp, 0x0A0B0C0D);
}

// num_marks is read from ms[4..6]; each entry is strictly 14 bytes. The
// loop must stop when fewer than 14 bytes remain (`mpos + 14 > ms.len()`
// → break) rather than panic, so an overshooting num_marks is safe.
#[test]
fn mark_count_overshoot_truncates_safely() {
    let video = build_stream_entry_video(0x1011, 0x1B, 6, 1, None);
    let marks = vec![
        TestMark {
            mark_type: 1,
            play_item_ref: 0,
            timestamp: 100,
        },
        TestMark {
            mark_type: 1,
            play_item_ref: 0,
            timestamp: 200,
        },
    ];
    let mut data = build_mpls_with_marks(
        &[(b"00001", 1, 0, 9000000)],
        (1, 0, 0, 0, 0, 0, 0, 0),
        &[video],
        &marks,
    );
    // Find mark_start (header bytes 12..16) and bump num_marks to 99.
    let mark_start = u32::from_be_bytes([data[12], data[13], data[14], data[15]]) as usize;
    // num_marks at ms[4..6].
    data[mark_start + 4] = 0;
    data[mark_start + 5] = 99;
    let pl = parse(&data).expect("should not panic on mark overshoot");
    // Only the 2 real marks fit; the loop broke at the 3rd.
    assert_eq!(pl.marks.len(), 2);
}

/// Mark section guard: `mark_start + 6 <= data.len()` is required before
/// reading num_marks at ms[4..6]. A mark_start pointing within 5 bytes
/// of EOF must yield zero marks, not panic.
#[test]
fn mark_start_near_eof_yields_no_marks() {
    let video = build_stream_entry_video(0x1011, 0x1B, 6, 1, None);
    let mut data = build_mpls(
        &[(b"00001", 1, 0, 9000000)],
        (1, 0, 0, 0, 0, 0, 0, 0),
        &[video],
    );
    // Point mark_start to len-3 (only 3 bytes remain < 6 needed).
    let near = (data.len() - 3) as u32;
    data[12..16].copy_from_slice(&near.to_be_bytes());
    let pl = parse(&data).expect("should parse");
    assert_eq!(pl.marks.len(), 0);
}

// A PlayItem whose item_length leaves fewer than 20 bytes of body is
// skipped (`item.len() < 20` → continue): clip_id/times aren't parsed,
// but the cursor advances and following items still parse.
#[test]
fn short_play_item_skipped_cursor_advances() {
    // Construct two items manually: a short (10-byte) first item, then
    // a valid second item. We can't use build_mpls (it always writes
    // ≥32-byte items), so assemble directly.
    let playlist_start: u32 = 40;
    let mut buf = Vec::new();
    buf.extend_from_slice(b"MPLS0200");
    buf.extend_from_slice(&playlist_start.to_be_bytes());
    buf.extend_from_slice(&[0u8; 28]); // mark_start=0 + padding

    let pl_start = buf.len();
    buf.extend_from_slice(&[0u8; 4]); // pl length placeholder
    buf.extend_from_slice(&[0u8; 2]); // reserved
    buf.extend_from_slice(&2u16.to_be_bytes()); // num_play_items = 2
    buf.extend_from_slice(&[0u8; 2]); // num_sub_paths

    // Item 0: length 10 (< 20) → skipped.
    let short = vec![0u8; 10];
    buf.extend_from_slice(&(short.len() as u16).to_be_bytes());
    buf.extend_from_slice(&short);

    // Item 1: a valid 32-byte item with clip_id "00009".
    let mut item = Vec::new();
    item.extend_from_slice(b"00009");
    item.extend_from_slice(b"M2TS");
    item.push(0); // [9] reserved
    item.push(0x01); // [10] connection_condition (low nibble)
    item.push(0); // [11] stc_id / reserved
    item.extend_from_slice(&90000u32.to_be_bytes()); // in_time
    item.extend_from_slice(&180000u32.to_be_bytes()); // out_time
    item.resize(32, 0); // pad through STN_OFFSET; item.len()==32 so no STN
    buf.extend_from_slice(&(item.len() as u16).to_be_bytes());
    buf.extend_from_slice(&item);

    let pl_len = (buf.len() - pl_start - 4) as u32;
    buf[pl_start..pl_start + 4].copy_from_slice(&pl_len.to_be_bytes());

    let pl = parse(&buf).expect("should parse with a short leading item");
    // Only the valid second item is retained.
    assert_eq!(pl.play_items.len(), 1);
    assert_eq!(pl.play_items[0].clip_id, "00009");
    assert_eq!(pl.play_items[0].in_time, 90000);
}

/// data.len() exactly 40 with valid magic but playlist_start past the
/// header: parse() must hit the `playlist_start + 10 > data.len()`
/// guard. A 40-byte buffer with playlist_start=40 has no PlayList body.
#[test]
fn exactly_40_bytes_no_playlist_body_errs() {
    let mut data = vec![0u8; 40];
    data[0..4].copy_from_slice(b"MPLS");
    data[4..8].copy_from_slice(b"0200");
    data[8..12].copy_from_slice(&40u32.to_be_bytes()); // playlist_start = 40 = len
    assert!(parse(&data).is_err());
}

// ─────────────────────────────────────────────────────────────────────
// Added: STN-table block alignment and section-boundary hardening.
// ─────────────────────────────────────────────────────────────────────

/// Build an MPLS from raw PlayItem bodies, with no PlayListMark section
/// (mark_start = 0). Lets a test control item_length exactly.
fn build_mpls_raw_items(items: &[Vec<u8>]) -> Vec<u8> {
    let playlist_start: u32 = 40;
    let mut buf = Vec::new();
    buf.extend_from_slice(b"MPLS0200");
    buf.extend_from_slice(&playlist_start.to_be_bytes());
    buf.extend_from_slice(&[0u8; 28]); // mark_start = 0, then padding
    let pl_start = buf.len();
    buf.extend_from_slice(&[0u8; 4]); // PlayList length placeholder
    buf.extend_from_slice(&[0u8; 2]); // reserved
    buf.extend_from_slice(&(items.len() as u16).to_be_bytes());
    buf.extend_from_slice(&[0u8; 2]); // num_sub_paths
    for it in items {
        buf.extend_from_slice(&(it.len() as u16).to_be_bytes());
        buf.extend_from_slice(it);
    }
    let pl_len = (buf.len() - pl_start - 4) as u32;
    buf[pl_start..pl_start + 4].copy_from_slice(&pl_len.to_be_bytes());
    buf
}

/// The 20 bytes a PlayItem needs for clip_id(5) + codec_id(4) +
/// reserved(1) + connection_condition byte(1) + stc_id/reserved(1) +
/// IN_time(4) + OUT_time(4).
fn play_item_20(clip: &[u8; 5], cc: u8, in_t: u32, out_t: u32) -> Vec<u8> {
    let mut it = Vec::new();
    it.extend_from_slice(clip);
    it.extend_from_slice(b"M2TS");
    it.push(0); // [9] reserved
    it.push(cc); // [10] connection_condition (low nibble)
    it.push(0); // [11] stc_id / reserved
    it.extend_from_slice(&in_t.to_be_bytes());
    it.extend_from_slice(&out_t.to_be_bytes());
    assert_eq!(it.len(), 20);
    it
}

/// A PlayItem body of exactly 20 bytes carries every field the parser
/// reads (the last is OUT_time at [16..20]), so it must be RECORDED,
/// not skipped — and it has no STN table, which starts at byte 32.
#[test]
fn play_item_of_exactly_20_bytes_is_recorded_without_stn() {
    let data = build_mpls_raw_items(&[play_item_20(b"00007", 5, 90_000, 180_000)]);
    let pl = parse(&data).expect("a 20-byte PlayItem must parse");
    assert_eq!(pl.play_items.len(), 1);
    assert_eq!(pl.play_items[0].clip_id, "00007");
    assert_eq!(pl.play_items[0].in_time, 90_000);
    assert_eq!(pl.play_items[0].out_time, 180_000);
    assert_eq!(pl.play_items[0].connection_condition, 5);
    assert!(pl.streams.is_empty(), "no STN table exists below byte 32");
}

// A 40-byte MPLS whose PlayList section is exactly its 10-byte header
// ending at EOF is structurally complete, not truncated: nothing the
// parser reads lies past the buffer, so it must parse to an empty playlist.
#[test]
fn minimum_size_mpls_with_empty_playlist_header_parses() {
    let mut data = vec![0u8; 40];
    data[0..4].copy_from_slice(b"MPLS");
    data[4..8].copy_from_slice(b"0200");
    data[8..12].copy_from_slice(&30u32.to_be_bytes()); // playlist_start + 10 == 40
    // mark_start (12..16) stays 0; num_play_items at data[36..38] is 0.
    let pl = parse(&data).expect("40-byte MPLS with a complete PlayList header must parse");
    assert!(pl.play_items.is_empty());
    assert!(pl.streams.is_empty());
    assert!(pl.marks.is_empty());
}

/// A mark_start of 0 means "no PlayListMark section". The file header
/// bytes at offset 0 must not be decoded as one — data[4..6] is the
/// version string "02", which as a big-endian num_marks would be 12338.
#[test]
fn mark_start_zero_does_not_parse_header_as_marks() {
    let data = build_mpls_raw_items(&[play_item_20(b"00007", 1, 0, 90_000)]);
    assert_eq!(
        &data[12..16],
        &[0, 0, 0, 0],
        "fixture must have mark_start 0"
    );
    let pl = parse(&data).expect("should parse");
    assert!(
        pl.marks.is_empty(),
        "mark_start == 0 must mean absent, got {} marks",
        pl.marks.len()
    );
}

#[test]
fn full_stn_table_block_alignment() {
    let mut entries: Vec<Vec<u8>> = vec![
        build_stream_entry_video(0x1011, 0x1B, 6, 1, None),
        build_stream_entry_audio(0x1100, 0x83, 6, 1, b"eng"),
        build_stream_entry_audio(0x1101, 0x86, 3, 1, b"fra"),
        build_stream_entry_pg(0x1200, 0x90, b"eng"),
        build_stream_entry_pg(0x1201, 0x90, b"fra"),
        build_stream_entry_pg(0x1202, 0x90, b"deu"),
        // PiP PG: straight after primary PG, no ref block
        build_stream_entry_pg(0x1C00, 0x90, b"jpn"),
    ];
    for i in 0..4u16 {
        entries.push(build_stream_entry_pg(0x1400 + i, 0x91, b"eng"));
    }
    // secondary audio + its secondary-audio ref block (1 ref → 1 pad)
    let mut sec_audio = build_stream_entry_audio(0x1A00, 0x83, 3, 1, b"spa");
    sec_audio.extend_from_slice(&[1, 0, 0x55, 0x00]);
    entries.push(sec_audio);
    // secondary video + audio-ref block + PiP-PG-ref block
    let mut sec_video = build_stream_entry_video(0x1B00, 0x1B, 4, 1, None);
    sec_video.extend_from_slice(&[1, 0, 0x55, 0x00]);
    sec_video.extend_from_slice(&[1, 0, 0x66, 0x00]);
    entries.push(sec_video);
    // Dolby Vision enhancement layer
    entries.push(build_stream_entry_video(0x1015, 0x24, 8, 1, Some(0x12)));

    let data = build_mpls(
        &[(b"00001", 1, 0, 9_000_000)],
        (1, 2, 3, 4, 1, 1, 1, 1),
        &entries,
    );
    let pl = parse(&data).expect("should parse");

    let got: Vec<(u8, u16, bool)> = pl
        .streams
        .iter()
        .map(|s| (s.stream_type, s.pid, s.secondary))
        .collect();
    assert_eq!(
        got,
        vec![
            (1, 0x1011, false), // primary video
            (2, 0x1100, false), // primary audio ×2
            (2, 0x1101, false),
            (3, 0x1200, false), // PG ×3
            (3, 0x1201, false),
            (3, 0x1202, false),
            (3, 0x1C00, true), // PiP PG
            // the 4 IG entries are consumed and discarded
            (5, 0x1A00, true), // secondary audio
            (6, 0x1B00, true), // secondary video
            (7, 0x1015, true), // Dolby Vision EL
        ]
    );
    // Languages prove each entry was decoded at its own offset.
    assert_eq!(pl.streams[1].language, "eng");
    assert_eq!(pl.streams[2].language, "fra");
    assert_eq!(pl.streams[6].language, "jpn");
    assert_eq!(pl.streams[7].language, "spa");
}

// BD STN_table order (libbluray _parse_stn, reverse-engineered): PiP PG entries
// follow the primary PG entries in one loop, BEFORE IG, and carry no ref block.
#[test]
fn pip_pg_entries_follow_primary_pg_before_ig() {
    let pg = build_stream_entry_pg(0x1200, 0x90, b"eng");
    let pip_pg = build_stream_entry_pg(0x1A01, 0x90, b"jpn");
    let ig = build_stream_entry_pg(0x1400, 0x91, b"eng");
    let mut sec_audio = build_stream_entry_audio(0x1A00, 0x83, 3, 1, b"spa");
    sec_audio.extend_from_slice(&[1, 0, 0x55, 0x00]);
    let data = build_mpls(
        &[(b"00001", 1, 0, 9_000_000)],
        (0, 0, 1, 1, 1, 0, 1, 0),
        &[pg, pip_pg, ig, sec_audio],
    );
    let pl = parse(&data).expect("should parse");
    let got: Vec<(u8, u16, bool, &str)> = pl
        .streams
        .iter()
        .map(|s| (s.stream_type, s.pid, s.secondary, s.language.as_str()))
        .collect();
    assert_eq!(
        got,
        vec![
            (3, 0x1200, false, "eng"),
            (3, 0x1A01, true, "jpn"),
            (5, 0x1A00, true, "spa"),
        ]
    );
}

// TextST (0x92) stream_attributes: coding_type(1) + character_code(1) + language(3).
#[test]
fn text_subtitle_language_follows_the_character_code() {
    let mut se = vec![3, STREAM_ENTRY_PLAYITEM_CLIP, 0x18, 0x00];
    let attrs = [0x92, 0x01, b'e', b'n', b'g'];
    se.push(attrs.len() as u8);
    se.extend_from_slice(&attrs);
    let data = build_mpls(
        &[(b"00001", 1, 0, 9_000_000)],
        (0, 0, 1, 0, 0, 0, 0, 0),
        &[se],
    );
    let pl = parse(&data).expect("should parse");
    assert_eq!(pl.streams[0].coding_type, 0x92);
    assert_eq!(pl.streams[0].language, "eng");
}

// A secondary block whose stream entry ends exactly at the end of the
// PlayItem has no reference block at all; the count byte must not be
// read from one-past-the-end. Covers both secondary ref blocks.
#[test]
fn secondary_ref_block_at_item_end_is_not_read() {
    let video = build_stream_entry_video(0x1011, 0x1B, 6, 1, None);

    // Secondary audio is the last entry, with no ref bytes following.
    let sec_audio = build_stream_entry_audio(0x1A00, 0x83, 3, 1, b"eng");
    let data = build_mpls(
        &[(b"00001", 1, 0, 9_000_000)],
        (1, 0, 0, 0, 1, 0, 0, 0),
        &[video.clone(), sec_audio],
    );
    let pl = parse(&data).expect("secondary audio at item end");
    assert_eq!(pl.streams.len(), 2);
    assert_eq!(pl.streams[1].pid, 0x1A00);

    // Secondary video is the last entry, with no ref bytes following.
    let sec_video = build_stream_entry_video(0x1B00, 0x1B, 4, 1, None);
    let data = build_mpls(
        &[(b"00001", 1, 0, 9_000_000)],
        (1, 0, 0, 0, 0, 1, 0, 0),
        &[video.clone(), sec_video.clone()],
    );
    let pl = parse(&data).expect("secondary video at item end");
    assert_eq!(pl.streams.len(), 2);
    assert_eq!(pl.streams[1].pid, 0x1B00);

    // Secondary video whose audio-ref block ends exactly at item end, so
    // the PiP-PG ref count byte would sit one past it.
    let mut sec_video_arefs = sec_video;
    sec_video_arefs.extend_from_slice(&[0, 0]); // n_arefs = 0, reserved
    let data = build_mpls(
        &[(b"00001", 1, 0, 9_000_000)],
        (1, 0, 0, 0, 0, 1, 0, 0),
        &[video, sec_video_arefs],
    );
    let pl = parse(&data).expect("secondary video aref block at item end");
    assert_eq!(pl.streams.len(), 2);
    assert_eq!(pl.streams[1].pid, 0x1B00);
}

// ─────────────────────────────────────────────────────────────────────
// parse_stream_entry bounds, exercised directly.
// ─────────────────────────────────────────────────────────────────────

/// Fewer than 2 bytes remain for the stream_entry header
/// (length(1) + stream_entry_type(1)) → None, without reading either.
#[test]
fn stream_entry_header_past_end_is_none() {
    let item = [0u8; 8];
    for pos in 7..12usize {
        assert!(
            parse_stream_entry(&item, pos, STREAM_CATEGORY_VIDEO).is_none(),
            "pos={pos}"
        );
    }
}

/// The stream_attributes header (length(1) + coding_type(1)) lies past
/// the end of the PlayItem → None, without reading the length byte.
#[test]
fn stream_attributes_header_past_end_is_none() {
    // se_len = 3 → se_end = 4 == item.len(); the sa length byte would be
    // at item[4] and the coding type at item[5].
    let item = [3u8, STREAM_ENTRY_PLAYITEM_CLIP, 0x10, 0x11];
    assert!(parse_stream_entry(&item, 0, STREAM_CATEGORY_VIDEO).is_none());
}

/// A declared stream_attributes length of 0 has no coding_type byte and
/// must be rejected — even when the (empty) attribute region is itself
/// in bounds.
#[test]
fn zero_length_attributes_in_bounds_is_none() {
    // se_len = 3 → se_end = 4; sa_len = item[4] = 0 → sa_end = 5 ≤ 6.
    let item = [3u8, STREAM_ENTRY_PLAYITEM_CLIP, 0x10, 0x11, 0, 0];
    assert!(parse_stream_entry(&item, 0, STREAM_CATEGORY_VIDEO).is_none());
}

// stream_attributes of exactly 1 byte carries only the coding_type — the
// minimum accepted, so the entry returns with no format-specific fields;
// for a PG stream the 3-byte language must NOT be read past attributes.
#[test]
fn one_byte_stream_attributes_yields_bare_entry() {
    // se_len = 3 → se_end = 4; sa_len = 1 → sa_end = 6 == item.len().
    let item = [3u8, STREAM_ENTRY_PLAYITEM_CLIP, 0x10, 0x11, 1, 0x1B];
    let (entry, next) =
        parse_stream_entry(&item, 0, STREAM_CATEGORY_VIDEO).expect("1-byte attrs are valid");
    assert_eq!(entry.pid, 0x1011);
    assert_eq!(entry.coding_type, 0x1B);
    assert_eq!(entry.video_format, 0);
    assert_eq!(entry.video_rate, 0);
    assert_eq!(next, 6);

    let pg = [3u8, STREAM_ENTRY_PLAYITEM_CLIP, 0x12, 0x00, 1, 0x90];
    let (entry, _) =
        parse_stream_entry(&pg, 0, STREAM_CATEGORY_PG_SUBTITLE).expect("1-byte PG attrs are valid");
    assert_eq!(entry.pid, 0x1200);
    assert_eq!(entry.coding_type, 0x90);
    assert_eq!(entry.language, "");
}

// Type 0 is reserved, type 2 a link point; neither is a chapter.
// `labels::collect_chapter_summary` once filtered on `mark_type <= 1`,
// inflating `chapter_count`. Both call sites now share this predicate.
#[test]
fn only_entry_marks_count_as_chapters() {
    let mk = |mark_type| PlaylistMark {
        mark_type,
        play_item_ref: 0,
        timestamp: 0,
    };
    assert!(
        !mk(0).is_chapter_mark(),
        "type 0 is reserved, not a chapter"
    );
    assert!(mk(1).is_chapter_mark());
    assert!(!mk(2).is_chapter_mark(), "type 2 is a link point");
}

// ---- audit tests ----

/// Item with a 32-byte PlayItem head followed by a single-angle STN
/// holding `entries` (counts: n_video only).
fn item_with_stn(n_video: u8, entries: &[Vec<u8>]) -> Vec<u8> {
    let mut it = play_item_20(b"00001", 1, 0, 9000);
    it.resize(32, 0);
    it.extend_from_slice(&[0u8; 4]);
    it.push(n_video);
    it.extend_from_slice(&[0u8; 11]);
    for e in entries {
        it.extend_from_slice(e);
    }
    it
}

#[test]
fn truncated_play_item_errors_only_when_nothing_is_left() {
    let full = build_mpls_raw_items(&[play_item_20(b"00001", 1, 0, 9000)]);
    // Cut into the item body: declared item_length runs past EOF.
    assert!(parse(&full[..full.len() - 3]).is_err());
    // num_play_items overshoots the items actually present.
    let mut over = full.clone();
    over[46..48].copy_from_slice(&2u16.to_be_bytes());
    assert_eq!(parse(&over).expect("kept").play_items.len(), 1);
}

#[test]
fn streams_come_only_from_first_play_item() {
    let v1 = build_stream_entry_video(0x1011, 0x1B, 6, 1, None);
    let v2 = build_stream_entry_video(0x1012, 0x1B, 6, 1, None);
    let data = build_mpls_raw_items(&[item_with_stn(1, &[v1]), item_with_stn(1, &[v2])]);
    let pl = parse(&data).expect("parse");
    assert_eq!(pl.play_items.len(), 2);
    assert_eq!(pl.streams.len(), 1);
    assert_eq!(pl.streams[0].pid, 0x1011);
}

#[test]
fn duration_ticks_sums_and_saturates_inverted_items() {
    let pi = |i, o| PlayItem {
        clip_id: String::new(),
        in_time: i,
        out_time: o,
        connection_condition: 1,
    };
    let pl = Playlist {
        version: String::new(),
        play_items: vec![pi(10, 110), pi(500, 100), pi(0, 50)],
        streams: vec![],
        marks: vec![],
    };
    assert_eq!(pl.duration_ticks(), 150);
}

#[test]
fn stream_entry_type3_pid_at_offset_3() {
    // se_len=4: type(1) + subpath_id(1) + pid(2); attrs: H264 + 1080p.
    let se = vec![
        4,
        STREAM_ENTRY_SUBPATH_CLIP,
        0xAA,
        0x12,
        0x34,
        2,
        0x1B,
        0x61,
    ];
    let data = build_mpls_raw_items(&[item_with_stn(1, &[se])]);
    let pl = parse(&data).expect("parse");
    assert_eq!(pl.streams[0].pid, 0x1234);
}

#[test]
fn stream_attributes_past_item_end_are_dropped() {
    // sa_len=200 runs far past the item end; must not slice out of bounds.
    let se = vec![3, STREAM_ENTRY_PLAYITEM_CLIP, 0x10, 0x11, 200, 0x1B];
    let data = build_mpls_raw_items(&[item_with_stn(1, &[se])]);
    let pl = parse(&data).expect("parse");
    assert!(pl.streams.is_empty());
}

#[test]
fn short_audio_attributes_do_not_panic() {
    let audio = |attrs: &[u8]| {
        let mut e = vec![3, STREAM_ENTRY_PLAYITEM_CLIP, 0x11, 0x00, attrs.len() as u8];
        e.extend_from_slice(attrs);
        e
    };
    let mut it = item_with_stn(
        0,
        &[audio(&[0x83, 0x61, b'e', b'n']), audio(&[0x90, b'e', b'n'])],
    );
    it[32 + 5] = 2; // n_audio
    let pl = parse(&build_mpls_raw_items(&[it])).expect("parse");
    assert_eq!(pl.streams.len(), 2);
    assert_eq!(pl.streams[0].audio_format, 6);
    assert_eq!(pl.streams[0].language, "");
    assert_eq!(pl.streams[1].language, "");
}
