use super::*;
use crate::disc::{
    AudioChannels, AudioStream, Codec, ContentFormat, DiscTitle, Extent, LabelPurpose, SampleRate,
};
use crate::sector::SectorSource;

// Builds a correctly-SIZED AC-3 frame (128 bytes, matching frmsizecod=0) whose
// `acmod`/`lfeon` encode a known channel count, via a bit writer so the test never
// hand-miscomputes the lfeon offset.
fn ac3_frame(acmod: u8, lfeon: bool) -> Vec<u8> {
    let mut bits: Vec<u8> = Vec::new();
    let push = |val: u32, n: usize, bits: &mut Vec<u8>| {
        for i in (0..n).rev() {
            bits.push(((val >> i) & 1) as u8);
        }
    };
    push(acmod as u32, 3, &mut bits);
    if (acmod & 0x1) != 0 && acmod != 0x1 {
        push(0, 2, &mut bits); // cmixlev
    }
    if (acmod & 0x4) != 0 {
        push(0, 2, &mut bits); // surmixlev
    }
    if acmod == 0x2 {
        push(0, 2, &mut bits); // dsurmod
    }
    push(lfeon as u32, 1, &mut bits);
    // Pack the bit vector MSB-first into bytes (byte6 onward).
    let mut tail = Vec::new();
    let mut cur = 0u8;
    for (i, b) in bits.iter().enumerate() {
        cur = (cur << 1) | b;
        if i % 8 == 7 {
            tail.push(cur);
            cur = 0;
        }
    }
    let rem = bits.len() % 8;
    if rem != 0 {
        cur <<= 8 - rem;
        tail.push(cur);
    }
    // AC-3 frame: 0x0B 0x77 crc(2) byte4(fscod=0,frmsizecod=0) bsid<<3 then BSI.
    let mut frame = vec![0x0B, 0x77, 0x00, 0x00, 0x00, 8u8 << 3];
    frame.extend_from_slice(&tail);
    // frmsizecod=0 @ 48kHz → 64 words = 128 bytes. Pad to the real size so
    // the frame-stepping in max_substream_channels lands on the next sync.
    frame.resize(128, 0);
    frame
}

// Builds a minimal `private_stream_1` PES carrying `frames` for `sub_id`, mirroring the
// on-disc layout the PS demux expects.
fn ps_ac3_frames(sub_id: u8, frames: &[Vec<u8>]) -> Vec<u8> {
    // PES sub-header for AC-3: sub_id + frame_count + 2-byte access ptr.
    let mut payload = vec![sub_id, frames.len() as u8, 0x00, 0x04];
    for f in frames {
        payload.extend_from_slice(f);
    }
    // PES packet: start code 00 00 01 BD, length(2), flags(2), hdr_len(0).
    let pes_payload_len = 3 + payload.len(); // flags(2)+hdrlen(1)+payload
    let mut pkt = vec![0x00, 0x00, 0x01, 0xBD];
    pkt.extend_from_slice(&(pes_payload_len as u16).to_be_bytes());
    pkt.extend_from_slice(&[0x80, 0x00, 0x00]); // no PTS, header_data_len=0
    pkt.extend_from_slice(&payload);
    pkt
}

/// Single-frame `private_stream_1` PES — the common case in existing tests.
fn ps_ac3(sub_id: u8, acmod: u8, lfeon: bool) -> Vec<u8> {
    ps_ac3_frames(sub_id, &[ac3_frame(acmod, lfeon)])
}

fn ac3_stream(pid: u16, channels: AudioChannels) -> Stream {
    Stream::Audio(AudioStream {
        pid,
        codec: Codec::Ac3,
        channels,
        language: "en".into(),
        sample_rate: SampleRate::S48,
        secondary: false,
        purpose: LabelPurpose::Normal,
        label: String::new(),
    })
}

/// The probe decodes the real channel count of each physical sub-stream.
/// 0x80 carries a 2.0 frame (acmod=2,no lfe → 2ch); 0x81 carries 5.1
/// (acmod=7 + lfe → 6ch).
#[test]
fn probe_decodes_per_substream_channels() {
    let mut bytes = ps_ac3(0x80, 2, false);
    bytes.extend(ps_ac3(0x81, 7, true));
    let probed = probe_ac3_substream_channels(&bytes);
    assert_eq!(probed.get(&0x80), Some(&2), "0x80 is the 2.0 down-mix");
    assert_eq!(probed.get(&0x81), Some(&6), "0x81 is the 5.1 main mix");
}

// Real-disc regression: the probe must read each sub-stream's TRUE (max-mix) channel count
// without cross-contaminating between sub-streams.
#[test]
fn probe_reads_max_channels_no_cross_contamination() {
    let mut bytes = Vec::new();
    // 0x80 opens with a 2.0 frame (the logo bed)...
    bytes.extend(ps_ac3_frames(0x80, &[ac3_frame(2, false)]));
    // ...0x81 interleaves a pure-2.0 PES (must NOT bleed 6 into 0x80)...
    bytes.extend(ps_ac3_frames(
        0x81,
        &[ac3_frame(2, false), ac3_frame(2, false)],
    ));
    // ...then 0x80 reaches its real 5.1 main mix (acmod=7 + lfe → 6 ch),
    // with a trailing 2.0 frame in the SAME PES to prove we take the max,
    // not the last frame.
    bytes.extend(ps_ac3_frames(
        0x80,
        &[ac3_frame(7, true), ac3_frame(2, false)],
    ));

    let probed = probe_ac3_substream_channels(&bytes);
    assert_eq!(
        probed.get(&0x80),
        Some(&6),
        "0x80's real 5.1 mix must win over its 2.0 head/tail frames"
    );
    assert_eq!(
        probed.get(&0x81),
        Some(&2),
        "0x81 is a pure 2.0 stream — must not absorb 0x80's 6-channel frame"
    );
}

// Mutation guard for `pos + rel` (not `pos - rel`, which could underflow `usize`) as the
// sync's true absolute position.
#[test]
fn max_substream_channels_locates_sync_after_leading_non_sync_bytes() {
    let mut data = vec![0xAA, 0xAA, 0xAA]; // no 0x0B77 pattern in here
    data.extend(ac3_frame(2, false)); // real 2.0 frame, sync at absolute offset 3
    assert_eq!(
        max_substream_channels(&data),
        Some(2),
        "must find and decode the frame whose sync is NOT at offset 0"
    );
}

// On an unmappable AC-3 size, must fall back to a forward `start + 2` rescan (not loop or
// overshoot).
#[test]
fn max_substream_channels_unmappable_size_steps_forward_by_two() {
    let mut real = ac3_frame(2, false);
    // Overwrite the real frame's (unchecked) CRC bytes, which double as byte4/5
    // of the bogus header at offset 4: 0xC0 (fscod=3 -> ac3_frame_size == 0,
    // unmappable) and 0xF8 (bsid=31 -> acmod_channels == None, no spurious count).
    real[2] = 0xC0;
    real[3] = 0xF8;
    let mut data = vec![0xAA, 0xAA, 0xAA, 0xAA]; // offsets 0..4, no sync
    data.push(0x0B); // offset 4: bogus header sync byte 0
    data.push(0x77); // offset 5: bogus header sync byte 1
    data.extend(real); // offset 6..: the real frame (also serves as the
    // bogus header's byte4/byte5 at offsets 8/9)
    assert_eq!(
        max_substream_channels(&data),
        Some(2),
        "must recover the real frame 2 bytes after the unmappable-size sync, not lose it"
    );
}

// Same fallback, sync at offset 0 (`start - 2` would underflow).
#[test]
fn max_substream_channels_unmappable_size_at_start_steps_forward_not_back() {
    let mut data = vec![0x0B, 0x77, 0x00, 0x00, 0xC0, 0xF8]; // bogus header, offsets 0..6
    data.extend(ac3_frame(2, false)); // real 2.0 frame at offset 6
    assert_eq!(
        max_substream_channels(&data),
        Some(2),
        "must step forward past the bogus header at offset 0 and find the real frame at offset 6"
    );
}

/// A `SectorSource` stub that hands back fixed bytes regardless of the
/// requested LBA/count, for exercising `probe_and_remap`'s end-to-end
/// wiring (format/AC-3/extent/count guards -> read -> probe -> remap).
struct FixedSource {
    data: Vec<u8>,
}

impl SectorSource for FixedSource {
    fn read_sectors(
        &mut self,
        _lba: u32,
        _count: u16,
        buf: &mut [u8],
        _recovery: bool,
    ) -> crate::error::Result<usize> {
        let n = self.data.len().min(buf.len());
        buf[..n].copy_from_slice(&self.data[..n]);
        Ok(n)
    }
}

// L084b: the scanner routes by PGC_AST_CTL, which every player follows, so the probe must
// not move a stream off it, even when the IFO channel count disagrees with the VOB.
#[test]
fn probe_never_overrides_the_ast_ctl_route() {
    let mut bytes = ps_ac3(0x80, 7, true); // 5.1 this PGC does not play
    bytes.extend(ps_ac3(0x81, 2, false)); // the AST_CTL-routed stream, really 2.0
    let mut title = DiscTitle {
        selection_evidence: Default::default(),
        playlist: "00001.ifo".into(),
        playlist_id: 1,
        duration_secs: 60.0,
        size_bytes: bytes.len() as u64,
        clips: Vec::new(),
        streams: vec![ac3_stream(0xBD81, AudioChannels::Surround51)],
        chapters: Vec::new(),
        extents: vec![Extent {
            start_lba: 0,
            sector_count: 2,
        }],
        content_format: ContentFormat::DvdPs,
        codec_privates: vec![None],
    };
    let ((), ev) =
        crate::testlog::capture(|| probe_and_remap(&mut FixedSource { data: bytes }, &mut title));
    let probed: Vec<&str> = ev.iter().map(|e| e.message()).collect();
    assert!(
        probed.iter().any(|m| m.contains("sub_id=0x80 channels=6")),
        "the probe still runs and reports the VOB's real layout: {probed:?}"
    );
    let Stream::Audio(a) = &title.streams[0] else {
        panic!("audio")
    };
    assert_eq!(a.pid, 0xBD81);
    assert_eq!(a.channels, AudioChannels::Surround51);
}
