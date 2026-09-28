//! AAC ADTS framing and validation.

use super::audio_frames::{AudioFrames, Header, Sync};
#[cfg(test)]
use super::pts_to_ns;
use super::{CodecParser, Frame, PesPacket};

/// ADTS `sampling_frequency_index` table (ISO/IEC 14496-3) — 13 valid entries;
/// indices 13/14/15 are 0 (reserved) and constitute a hard reject.
const ADTS_SAMPLE_RATE_VALID: [u32; 16] = [
    96000, 88200, 64000, 48000, 44100, 32000, 24000, 22050, 16000, 12000, 11025, 8000, 7350, 0, 0,
    0,
];

/// ADTS header verdict for the packet head.
enum AdtsVerdict {
    /// No 12-bit ADTS sync at the head — not an ADTS frame we can validate.
    NoSync,
    /// Sync present and the three structural fields are legal.
    Valid,
    /// Sync present but a reserved sample-rate index or a sub-header
    /// frame-length — structurally invalid per the ADTS spec.
    Invalid,
}

fn adts_verdict(data: &[u8]) -> AdtsVerdict {
    // Need the full 7-byte fixed+variable header to read frame_length.
    if data.len() < 7 {
        return AdtsVerdict::NoSync;
    }
    // 12-bit syncword 0xFFF: byte0 == 0xFF and top nibble of byte1 == 0xF.
    if data[0] != 0xFF || (data[1] & 0xF0) != 0xF0 {
        return AdtsVerdict::NoSync;
    }
    // layer is always 00; profile 3 is reserved when ID = 1 (MPEG-2 AAC).
    if data[1] & 0x06 != 0 || (data[1] & 0x08 != 0 && data[2] >> 6 == 3) {
        return AdtsVerdict::Invalid;
    }
    // sampling_frequency_index: byte2 bits 5..2.
    let sr_index = ((data[2] >> 2) & 0x0F) as usize;
    if ADTS_SAMPLE_RATE_VALID[sr_index] == 0 {
        return AdtsVerdict::Invalid;
    }
    // aac_frame_length: 13 bits = byte3[1:0] | byte4 | byte5[7:5].
    let frame_length =
        ((u32::from(data[3]) & 0x03) << 11) | (u32::from(data[4]) << 3) | (u32::from(data[5]) >> 5);
    // Floor depends on protection_absent: CRC-present (byte1 bit0 clear) adds a
    // 16-bit crc_check after the 7-byte header, so the min is 9, not a flat 7 —
    // else a CRC frame declaring length 7-8 wrongly passed as Valid.
    let header_bytes = if data[1] & 0x01 == 0 { 9 } else { 7 };
    if frame_length < header_bytes {
        return AdtsVerdict::Invalid;
    }
    AdtsVerdict::Valid
}

// Frame length of a valid ADTS header, with no parser state touched.
fn adts_frame_len(data: &[u8]) -> Option<usize> {
    matches!(adts_verdict(data), AdtsVerdict::Valid).then(|| {
        (usize::from(data[3] & 3) << 11) | (usize::from(data[4]) << 3) | usize::from(data[5] >> 5)
    })
}

pub struct AdtsParser {
    frames: AudioFrames,
    config: Option<Vec<u8>>,
    config_changes: u64,
    pid: u16,
}

impl Default for AdtsParser {
    fn default() -> Self {
        Self::new()
    }
}

impl AdtsParser {
    pub fn new() -> Self {
        Self {
            frames: AudioFrames::new(
                "aac",
                Sync {
                    mask: 0xf0,
                    frame_len: adts_frame_len,
                    // ID, layer, profile, sampling_frequency_index, channel_configuration.
                    fixed: [0, 0x0E, 0xFD, 0xC0],
                },
            ),
            config: None,
            config_changes: 0,
            pid: 0,
        }
    }
    pub fn dropped_frames(&self) -> u64 {
        self.frames.dropped_frames()
    }
    pub fn dropped_duration_ns(&self) -> u64 {
        self.frames.dropped_duration_ns()
    }
}

// Validate and size the ADTS frame at the slice head, recording its AudioSpecificConfig.
fn adts_header(
    data: &[u8],
    config: &mut Option<Vec<u8>>,
    changes: &mut u64,
    pid: u16,
) -> Option<Header> {
    if !matches!(adts_verdict(data), AdtsVerdict::Valid) {
        return None;
    }
    let rate_index = (data[2] >> 2) & 15;
    let object_type = (data[2] >> 6) + 1;
    let channels = ((data[2] & 1) << 2) | (data[3] >> 6);
    // The ASC replaces the ADTS header (payload carries no header/CRC).
    // CodecPrivate is fixed per track: a later config change keeps the
    // first ASC and is counted.
    let asc = [
        (object_type << 3) | (rate_index >> 1),
        (rate_index << 7) | (channels << 3),
    ];
    match config {
        None => *config = Some(asc.to_vec()),
        Some(first) if first[..] != asc => {
            if *changes == 0 {
                tracing::warn!(target: "mux", pid, "AAC config changed mid-stream; keeping the first");
            }
            *changes += 1;
        }
        Some(_) => {}
    }
    Some(Header {
        bytes: adts_frame_len(data)?,
        skip: if data[1] & 1 == 0 { 9 } else { 7 },
        samples: 1024 * (u32::from(data[6] & 3) + 1),
        rate: ADTS_SAMPLE_RATE_VALID[usize::from(rate_index)],
    })
}

impl CodecParser for AdtsParser {
    fn parse(&mut self, pes: &PesPacket) -> Vec<Frame> {
        let (config, changes) = (&mut self.config, &mut self.config_changes);
        let pid = pes.pid;
        let mut out = Vec::new();
        if pes.discontinuity {
            // A frame still awaiting its successor is emitted before the gap clears it.
            out = self
                .frames
                .drain_before_gap(7, |d| adts_header(d, config, changes, pid));
        }
        out.extend(
            self.frames
                .parse(pes, 7, |d| adts_header(d, config, changes, pid)),
        );
        self.pid = pid;
        out
    }

    fn flush(&mut self) -> Vec<Frame> {
        let (config, changes, pid) = (&mut self.config, &mut self.config_changes, self.pid);
        self.frames
            .flush_with(7, |d| adts_header(d, config, changes, pid))
    }
    fn codec_private(&self) -> Option<Vec<u8>> {
        self.config.clone()
    }
    fn config_changes(&self) -> u64 {
        self.config_changes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_pes(data: Vec<u8>, pts: Option<i64>) -> PesPacket {
        PesPacket {
            source: None,
            pid: 0x1100,
            pts,
            dts: None,
            data,
            discontinuity: false,
        }
    }

    // A header claiming a CRC (protection_absent=0) but declaring a frame length too short to
    // contain one.
    #[test]
    fn a_crc_present_header_shorter_than_its_own_crc_is_invalid() {
        for declared in [7u32, 8] {
            let mut f = adts_frame(16);
            f[1] = 0xF0; // sync + MPEG-4, protection_absent = 0 => CRC present
            f[3] = (f[3] & 0xFC) | ((declared >> 11) & 0x03) as u8;
            f[4] = ((declared >> 3) & 0xFF) as u8;
            f[5] = (f[5] & 0x1F) | ((declared & 0x07) << 5) as u8;
            assert!(
                matches!(adts_verdict(&f), AdtsVerdict::Invalid),
                "protection_absent=0 declaring {declared} bytes cannot hold its \
                 own 7-byte header plus a 2-byte CRC"
            );
        }

        // 9 is the smallest length that CAN hold header + CRC, so it must pass
        // the structural gate — the floor moved, it did not become stricter
        // than the spec.
        let mut ok = adts_frame(16);
        ok[1] = 0xF0;
        let nine = 9u32;
        ok[3] = (ok[3] & 0xFC) | ((nine >> 11) & 0x03) as u8;
        ok[4] = ((nine >> 3) & 0xFF) as u8;
        ok[5] = (ok[5] & 0x1F) | ((nine & 0x07) << 5) as u8;
        assert!(matches!(adts_verdict(&ok), AdtsVerdict::Valid));

        // And with NO CRC the floor is still 7, unchanged.
        let mut no_crc = adts_frame(16);
        no_crc[1] = 0xF1; // protection_absent = 1
        let seven = 7u32;
        no_crc[3] = (no_crc[3] & 0xFC) | ((seven >> 11) & 0x03) as u8;
        no_crc[4] = ((seven >> 3) & 0xFF) as u8;
        no_crc[5] = (no_crc[5] & 0x1F) | ((seven & 0x07) << 5) as u8;
        assert!(matches!(adts_verdict(&no_crc), AdtsVerdict::Valid));
    }

    /// A valid ADTS header (AAC-LC, 44.1 kHz, stereo) + payload, with
    /// aac_frame_length set to the total size.
    fn adts_frame(payload: usize) -> Vec<u8> {
        let total = 7 + payload;
        let mut f = vec![0u8; total];
        f[0] = 0xFF;
        f[1] = 0xF1; // sync + MPEG-4 + no CRC (protection_absent=1)
        f[2] = 0x50; // profile=AAC-LC, sr_index=4 (44.1 kHz)
        f[3] = 0x80; // channel_config low + start of frame_length
        // frame_length (13 bits) = total.
        let fl = total as u32;
        f[3] = (f[3] & 0xFC) | ((fl >> 11) & 0x03) as u8;
        f[4] = ((fl >> 3) & 0xFF) as u8;
        f[5] = (((fl & 0x07) << 5) as u8) | 0x1F; // low 3 bits of len + buffer-fullness bits
        f
    }

    #[test]
    fn valid_adts_is_kept() {
        let mut p = AdtsParser::new();
        let f = p.parse(&make_pes(adts_frame(400), Some(90000)));
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].pts_ns, pts_to_ns(90000));
        assert_eq!(p.dropped_frames(), 0);
    }

    #[test]
    fn pes_without_pts_advances_by_sample_count() {
        // A PES with no PTS (legal for audio, e.g. after a discontinuity) must
        // advance from the last timestamp by sample count — resetting to 0 would corrupt
        // A/V sync.
        let mut p = AdtsParser::new();
        p.parse(&make_pes(adts_frame(400), Some(90000)));
        let f = p.parse(&make_pes(adts_frame(400), None));
        assert_eq!(f.len(), 1);
        assert_eq!(
            f[0].pts_ns,
            pts_to_ns(90000) + 1024 * 1_000_000_000 / 44100,
            "continuation advances by one AAC frame"
        );
    }

    // A dropped frame's header failed validation, so duration is reported as zero rather than
    // guessed from untrustworthy fields.
    #[test]
    fn dropped_frames_are_counted_but_their_duration_is_not_invented() {
        let mut parser = AdtsParser::new();
        // Three frames whose sampling_frequency_index is a reserved value (13),
        // so `adts_verdict` rejects each one.
        let mut bad = adts_frame(32);
        bad[2] = (bad[2] & 0b1100_0011) | (13 << 2);
        for i in 0..3 {
            let out = parser.parse(&make_pes(bad.clone(), Some(i * 90_000)));
            assert!(out.is_empty(), "an invalid ADTS frame is not emitted");
        }
        assert_eq!(parser.dropped_frames(), 3, "every drop is counted");
        assert_eq!(
            parser.dropped_duration_ns(),
            0,
            "the duration comes from the header that just failed validation, so \
             it is reported as unmeasured rather than guessed"
        );
    }

    #[test]
    fn reserved_sample_rate_index_is_dropped() {
        // sr_index = 13 (reserved). byte2 bits5..2 = 1101 → 0x34.
        let mut p = AdtsParser::new();
        let mut f = adts_frame(400);
        f[2] = (f[2] & 0xC3) | (13 << 2); // set sr_index = 13
        assert!(p.parse(&make_pes(f, Some(0))).is_empty());
        assert_eq!(p.dropped_frames(), 1);
    }

    #[test]
    fn subheader_frame_length_is_dropped() {
        // frame_length < 7 (here 0) is a sub-header length → reject.
        let mut p = AdtsParser::new();
        let mut f = adts_frame(400);
        f[3] &= 0xFC; // clear len high bits
        f[4] = 0;
        f[5] &= 0x1F; // clear len low bits → frame_length = 0
        assert!(p.parse(&make_pes(f, Some(0))).is_empty());
        assert_eq!(p.dropped_frames(), 1);
    }

    #[test]
    fn raw_aac_without_sync_passes_through() {
        // No ADTS sync (e.g. raw AAC from mp4) → cannot validate → keep.
        let mut p = AdtsParser::new();
        let f = p.parse(&make_pes(
            vec![0x21, 0x00, 0x03, 0x40, 0x00, 0x00, 0x00],
            Some(0),
        ));
        assert_eq!(f.len(), 1);
        assert_eq!(p.dropped_frames(), 0);
    }

    #[test]
    fn drop_preserves_sync_via_own_pts() {
        let mut p = AdtsParser::new();
        let mut bad = adts_frame(400);
        bad[2] = (bad[2] & 0xC3) | (14 << 2); // reserved sr_index
        assert!(p.parse(&make_pes(bad, Some(90000))).is_empty());
        // The first frame after a drop waits for a successor to confirm it, here EOS.
        let mut f = p.parse(&make_pes(adts_frame(400), Some(96000)));
        f.extend(p.flush());
        assert_eq!(f.len(), 1);
        assert_eq!(
            f[0].pts_ns,
            pts_to_ns(96000),
            "next frame keeps its own PTS"
        );
    }

    #[test]
    fn short_sync_header_waits_for_continuation() {
        let mut p = AdtsParser::new();
        let f = p.parse(&make_pes(vec![0xFF, 0xF1, 0x50], Some(0)));
        assert!(f.is_empty(), "partial ADTS header must be buffered");
    }

    /// One PES is one unit here, so the frame carries that packet's offset.
    #[test]
    fn a_frame_carries_its_packets_source() {
        let mut parser = AdtsParser::new();
        let mut p = make_pes(adts_frame(64), Some(90_000));
        p.source = Some(crate::pes::SourcePos::at_byte(4_242));
        let frames = parser.parse(&p);
        assert!(!frames.is_empty(), "a valid ADTS frame is emitted");
        assert_eq!(frames[0].source.map(|s| s.byte), Some(4_242));
    }
    #[test]
    fn every_pes_split_reassembles_and_strips_adts() {
        let data = adts_frame(40);
        for split in 1..data.len() {
            let mut p = AdtsParser::new();
            assert!(
                p.parse(&make_pes(data[..split].to_vec(), Some(90000)))
                    .is_empty()
            );
            let frames = p.parse(&make_pes(data[split..].to_vec(), None));
            assert_eq!(frames.len(), 1, "split {split}");
            assert_eq!(frames[0].data, data[7..]);
            assert_eq!(frames[0].pts_ns, 1_000_000_000);
            assert!(p.flush().is_empty());
            assert_eq!(p.dropped_frames(), 0);
        }
    }

    #[test]
    fn multiple_adts_frames_in_one_pes_have_sample_timestamps() {
        let data = adts_frame(20);
        let mut p = AdtsParser::new();
        let frames = p.parse(&make_pes(data.repeat(3), Some(90000)));
        assert_eq!(frames.len(), 3);
        for (i, f) in frames.iter().enumerate() {
            assert_eq!(
                f.pts_ns,
                1_000_000_000 + i as i64 * (1024 * 1_000_000_000i64 / 44100)
            );
            assert_eq!(f.data, data[7..]);
        }
        assert_eq!(p.codec_private(), Some(vec![0x12, 0x10]));
    }

    // Matroska CodecPrivate is fixed per track: a later ADTS header with a
    // different config keeps the first ASC and is counted, not silently adopted.
    #[test]
    fn mid_stream_config_change_keeps_first_config_and_is_counted() {
        let stereo = adts_frame(20);
        let mut surround = adts_frame(20);
        surround[2] = (surround[2] & !1) | 1; // channel_configuration = 6 (5.1)
        surround[3] = (surround[3] & 0x3F) | (2 << 6);
        let mut p = AdtsParser::new();
        p.parse(&make_pes(stereo.clone(), Some(0)));
        assert_eq!(p.config_changes(), 0);
        let f = p.parse(&make_pes(surround.clone(), Some(9000)));
        assert_eq!(f.len(), 1, "the frame is still emitted");
        assert_eq!(p.codec_private(), Some(vec![0x12, 0x10]), "first ASC kept");
        assert_eq!(p.config_changes(), 1);
        p.parse(&make_pes(surround, Some(18000)));
        assert_eq!(p.config_changes(), 2, "every mismatching frame counts");
        p.parse(&make_pes(stereo, Some(27000)));
        assert_eq!(p.config_changes(), 2);
    }

    #[test]
    fn crc_bytes_are_removed_with_transport_header() {
        let mut data = adts_frame(20);
        data[1] &= !1;
        let mut p = AdtsParser::new();
        let frames = p.parse(&make_pes(data.clone(), Some(0)));
        assert_eq!(frames[0].data, data[9..]);
    }

    #[test]
    fn partial_tail_is_not_emitted_at_eof() {
        let data = adts_frame(20);
        let mut p = AdtsParser::new();
        assert!(p.parse(&make_pes(data[..10].to_vec(), Some(0))).is_empty());
        assert!(p.flush().is_empty());
    }

    // Deterministic noise with no 0xFF, so a scan through it meets only the syncs a test plants.
    fn noise(seed: &mut u64, n: usize) -> Vec<u8> {
        (0..n)
            .map(|_| {
                *seed ^= *seed << 13;
                *seed ^= *seed >> 7;
                *seed ^= *seed << 17;
                (*seed as u8).min(0xFE)
            })
            .collect()
    }

    fn noisy_frame(seed: &mut u64, payload: usize) -> Vec<u8> {
        let mut f = adts_frame(payload);
        f[7..].copy_from_slice(&noise(seed, payload));
        f
    }

    const AAC_FRAME_NS: i64 = 1024 * 1_000_000_000 / 44100;

    // L043: 0xFFE is MPEG-audio sync, not ADTS; garbage full of it must not poison a good track.
    #[test]
    fn eleven_bit_false_syncs_do_not_poison_a_good_track() {
        let mut p = AdtsParser::new();
        let mut seed = 7;
        assert_eq!(
            p.parse(&make_pes(noisy_frame(&mut seed, 200), Some(0)))
                .len(),
            1
        );
        p.parse(&make_pes([0xFF, 0xE5].repeat(2048), None));
        let kept: usize = (1..=300)
            .map(|i| {
                p.parse(&make_pes(noisy_frame(&mut seed, 200), Some(i * 2090)))
                    .len()
            })
            .sum();
        assert_eq!(kept, 300, "the good track survives");
        assert_eq!(
            p.frames.verified_dropped(),
            1,
            "one garbage run where a header was due"
        );
    }

    // L043: one resync run through a PES is one verified drop, not one per sync-looking byte.
    #[test]
    fn a_resync_run_is_one_verified_drop() {
        let mut bad = adts_frame(0);
        bad[2] = (bad[2] & 0xC3) | (13 << 2);
        let mut p = AdtsParser::new();
        let mut seed = 11;
        p.parse(&make_pes(noisy_frame(&mut seed, 200), Some(0)));
        p.parse(&make_pes(bad.repeat(600), None));
        let kept: usize = (1..=300)
            .map(|i| {
                p.parse(&make_pes(noisy_frame(&mut seed, 200), Some(i * 2090)))
                    .len()
            })
            .sum();
        assert_eq!(kept, 300, "the good track survives");
        assert_eq!(
            p.frames.verified_dropped(),
            1,
            "one fault, one verified drop"
        );
        assert_eq!(
            p.dropped_frames(),
            20,
            "4200 lost bytes are 20 frames of audio"
        );
    }

    // L042: a first PES starting mid-frame loses the fragment, not the frames after it.
    #[test]
    fn a_first_pes_starting_mid_frame_frames_from_the_first_header() {
        let mut seed = 3;
        let (a, b) = (noisy_frame(&mut seed, 300), noisy_frame(&mut seed, 250));
        let mut data = noise(&mut seed, 57);
        data.extend_from_slice(&a);
        data.extend_from_slice(&b);
        let mut p = AdtsParser::new();
        let f = p.parse(&make_pes(data, Some(90_000)));
        assert_eq!(f.len(), 2, "the leading fragment is not a frame");
        assert_eq!((&f[0].data[..], &f[1].data[..]), (&a[7..], &b[7..]));
        assert_eq!(
            f[0].pts_ns, 1_000_000_000,
            "the PTS is the first whole frame's"
        );
        assert_eq!(f[1].pts_ns, 1_000_000_000 + AAC_FRAME_NS);
        assert_eq!(p.codec_private(), Some(vec![0x12, 0x10]));
        // A lone whole frame is confirmed by ending exactly at the packet end.
        let mut data = noise(&mut seed, 30);
        data.extend_from_slice(&a);
        let f = AdtsParser::new().parse(&make_pes(data, Some(0)));
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].data, a[7..]);
    }

    // L052/L056: a corrupt header mid-PES, its noisy payload holding false syncs of both widths,
    // is one drop; the later frames keep their slots on the sample clock.
    #[test]
    fn a_corrupt_frame_mid_pes_keeps_later_frames_on_the_clock() {
        let mut seed = 5;
        let frames: Vec<Vec<u8>> = (0..4).map(|_| noisy_frame(&mut seed, 300)).collect();
        let mut bad = frames[1].clone();
        bad[2] = (bad[2] & 0xC3) | (13 << 2);
        bad[40..42].copy_from_slice(&[0xFF, 0xE3]);
        let false_header = bad[..7].to_vec();
        bad[90..97].copy_from_slice(&false_header);
        let data = [&frames[0][..], &bad, &frames[2], &frames[3]].concat();
        let mut p = AdtsParser::new();
        let f = p.parse(&make_pes(data, Some(90_000)));
        let pts: Vec<i64> = f.iter().map(|f| f.pts_ns).collect();
        let slots = [0, 2, 3].map(|i| 1_000_000_000 + i * AAC_FRAME_NS);
        assert_eq!(pts, slots, "the lost frame keeps its slot");
        assert!(
            f.iter()
                .zip([0, 2, 3])
                .all(|(f, i)| f.data == frames[i][7..])
        );
        assert_eq!(p.dropped_frames(), 1);
    }

    // Each corrupt frame in a PES is its own drop once a good frame ends the previous resync.
    #[test]
    fn two_corrupt_frames_in_one_pes_are_two_drops() {
        let mut seed = 13;
        let mut frames: Vec<Vec<u8>> = (0..5).map(|_| noisy_frame(&mut seed, 200)).collect();
        for i in [1, 3] {
            frames[i][2] = (frames[i][2] & 0xC3) | (13 << 2);
        }
        let mut p = AdtsParser::new();
        let mut f = p.parse(&make_pes(frames.concat(), Some(0)));
        f.extend(p.flush());
        let pts: Vec<i64> = f.iter().map(|f| f.pts_ns).collect();
        assert_eq!(
            pts,
            [0, 2, 4].map(|i| i * AAC_FRAME_NS),
            "the good frame between is kept"
        );
        assert!(
            f.iter()
                .zip([0, 2, 4])
                .all(|(f, i)| f.data == frames[i][7..])
        );
        assert_eq!(p.dropped_frames(), 2);
    }

    // L052: the PES PTS names its first AU; when that AU is corrupt the next is one frame later.
    #[test]
    fn a_corrupt_first_frame_keeps_the_pes_pts_for_itself() {
        let mut seed = 9;
        let mut p = AdtsParser::new();
        p.parse(&make_pes(noisy_frame(&mut seed, 300), Some(0)));
        let mut bad = noisy_frame(&mut seed, 300);
        bad[2] = (bad[2] & 0xC3) | (13 << 2);
        let good = noisy_frame(&mut seed, 300);
        let mut f = p.parse(&make_pes([&bad[..], &good].concat(), Some(90_000)));
        f.extend(p.flush());
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].pts_ns, 1_000_000_000 + AAC_FRAME_NS);
    }

    // An unchained header in a first non-sync PES is not trusted: the raw passthrough stands
    // and the probe leaves no AudioSpecificConfig behind.
    #[test]
    fn an_unconfirmed_header_in_a_first_pes_is_not_trusted() {
        let mut seed = 21;
        let mut data = noise(&mut seed, 40);
        data.extend_from_slice(&adts_frame(0)[..7]);
        data[44] = 0x40; // frame_length 512: neither chained nor ending the packet
        data.extend_from_slice(&noise(&mut seed, 60));
        let mut p = AdtsParser::new();
        let f = p.parse(&make_pes(data.clone(), Some(0)));
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].data, data);
        assert_eq!(p.codec_private(), None);
    }

    fn corrupt(f: &mut [u8]) {
        f[2] = (f[2] & 0xC3) | (13 << 2);
    }

    // After a gap the leading bytes are a fragment, not a lost AU: a false sync in them must
    // not push the PES's first real frame a slot late.
    #[test]
    fn a_false_sync_after_a_discontinuity_does_not_delay_the_next_frame() {
        let mut seed = 31;
        let mut p = AdtsParser::new();
        p.parse(&make_pes(noisy_frame(&mut seed, 300), Some(0)));
        let mut frag = noise(&mut seed, 50);
        frag[10..17].copy_from_slice(&adts_frame(0)[..7]);
        corrupt(&mut frag[10..]);
        let (g1, g2) = (noisy_frame(&mut seed, 300), noisy_frame(&mut seed, 300));
        let gap = PesPacket {
            discontinuity: true,
            ..make_pes([&frag[..], &g1, &g2].concat(), Some(900_000))
        };
        let pts: Vec<i64> = p.parse(&gap).iter().map(|f| f.pts_ns).collect();
        assert_eq!(pts, [10_000_000_000, 10_000_000_000 + AAC_FRAME_NS]);
    }

    // After a gap the PES may start mid-frame: that fragment is not a due header, so no drop.
    #[test]
    fn a_fragment_after_a_gap_is_not_a_drop() {
        let mut seed = 83;
        let mut p = AdtsParser::new();
        p.parse(&make_pes(noisy_frame(&mut seed, 300), Some(0)));
        let (g1, g2) = (noisy_frame(&mut seed, 300), noisy_frame(&mut seed, 300));
        let gap = PesPacket {
            discontinuity: true,
            ..make_pes(
                [&noise(&mut seed, 40)[..], &g1, &g2].concat(),
                Some(900_000),
            )
        };
        assert_eq!(p.parse(&gap).len(), 2);
        assert_eq!(p.dropped_frames(), 0);
    }

    // A gap ends an open resync run: the next PES's lone frame needs no successor.
    #[test]
    fn a_discontinuity_ends_a_resync_run() {
        let mut seed = 61;
        let mut bad = noisy_frame(&mut seed, 200);
        corrupt(&mut bad);
        let mut p = AdtsParser::new();
        p.parse(&make_pes(noisy_frame(&mut seed, 200), Some(0)));
        p.parse(&make_pes(bad, None));
        let gap = PesPacket {
            discontinuity: true,
            ..make_pes(noisy_frame(&mut seed, 200), Some(900_000))
        };
        let f = p.parse(&gap);
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].pts_ns, 10_000_000_000);
    }

    // A resync run spanning two lost frames advances the clock two slots (skipped bytes over
    // the last frame's size).
    #[test]
    fn a_resync_run_over_two_frames_advances_two_slots() {
        let mut seed = 37;
        let mut frames: Vec<Vec<u8>> = (0..5).map(|_| noisy_frame(&mut seed, 250)).collect();
        corrupt(&mut frames[1]);
        frames[2] = noisy_frame(&mut seed, 240); // shorter: 504 skipped bytes round to 2 frames
        frames[2][..2].copy_from_slice(&[0x12, 0x34]); // header lost: no sync at all
        let mut p = AdtsParser::new();
        let f = p.parse(&make_pes(frames.concat(), Some(0)));
        let pts: Vec<i64> = f.iter().map(|f| f.pts_ns).collect();
        assert_eq!(pts, [0, 3, 4].map(|i| i * AAC_FRAME_NS));
        assert_eq!(p.dropped_frames(), 2, "both lost frames are reported");
        assert_eq!(p.frames.verified_dropped(), 1);
    }

    // While resyncing, a frame ending at a sync-shaped (corrupt) header is kept only if its own
    // fixed header matches the last good frame's; a stray header that differs is payload.
    #[test]
    fn a_mismatched_header_ending_at_a_sync_is_not_kept() {
        let mut seed = 67;
        let frames: Vec<Vec<u8>> = (0..4).map(|_| noisy_frame(&mut seed, 300)).collect();
        let mut bad = frames[1].clone();
        corrupt(&mut bad);
        let mut fake = adts_frame(13);
        fake[2] |= 0x01; // channel configuration 4..7: differs from the stream's stereo
        fake[7..].copy_from_slice(&noise(&mut seed, 13));
        bad[50..70].copy_from_slice(&fake);
        bad[70..72].copy_from_slice(&[0xFF, 0xF3]);
        let data = [&frames[0][..], &bad, &frames[2], &frames[3]].concat();
        let f = AdtsParser::new().parse(&make_pes(data, Some(0)));
        let pts: Vec<i64> = f.iter().map(|f| f.pts_ns).collect();
        assert_eq!(pts, [0, 2, 3].map(|i| i * AAC_FRAME_NS));
    }

    // At EOS a good frame after a drop is kept even when a truncated header trails it.
    #[test]
    fn a_resync_frame_before_a_short_tail_is_kept_at_eos() {
        let mut seed = 71;
        let (g0, g2) = (noisy_frame(&mut seed, 200), noisy_frame(&mut seed, 200));
        let mut bad = noisy_frame(&mut seed, 200);
        corrupt(&mut bad);
        let mut p = AdtsParser::new();
        p.parse(&make_pes(g0.clone(), Some(0)));
        assert!(
            p.parse(&make_pes([&bad[..], &g2, &g0[..3]].concat(), None))
                .is_empty()
        );
        let f = p.flush();
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].data, g2[7..]);
    }

    // Mid-run, a sync-shaped byte that is no valid header does not re-anchor the run: the next
    // PES's PTS still names its first whole frame.
    #[test]
    fn a_run_restarts_only_on_a_valid_header() {
        let mut seed = 73;
        let mut p = AdtsParser::new();
        p.parse(&make_pes(noisy_frame(&mut seed, 300), Some(0)));
        let mut bad = noisy_frame(&mut seed, 300);
        corrupt(&mut bad);
        p.parse(&make_pes(bad, None));
        let mut tail = noise(&mut seed, 30);
        tail[10..12].copy_from_slice(&[0xFF, 0xF3]); // sync-shaped, invalid layer
        let (g1, g2) = (noisy_frame(&mut seed, 300), noisy_frame(&mut seed, 300));
        let f = p.parse(&make_pes([&tail[..], &g1, &g2].concat(), Some(90_000)));
        let pts: Vec<i64> = f.iter().map(|f| f.pts_ns).collect();
        assert_eq!(pts, [0, 1].map(|i| 1_000_000_000 + i * AAC_FRAME_NS));
    }

    // Mid-run, a valid (if unchained) header in a new PES does restart the run there.
    #[test]
    fn a_run_restarts_at_a_valid_unchained_header() {
        let mut seed = 89;
        let mut p = AdtsParser::new();
        p.parse(&make_pes(noisy_frame(&mut seed, 300), Some(0)));
        let mut bad = noisy_frame(&mut seed, 300);
        corrupt(&mut bad);
        p.parse(&make_pes(bad, None));
        let cut = noisy_frame(&mut seed, 300)[..157].to_vec(); // its length runs past the cut
        let (g1, g2) = (noisy_frame(&mut seed, 300), noisy_frame(&mut seed, 300));
        let data = [&noise(&mut seed, 30)[..], &cut, &g1, &g2].concat();
        let f = p.parse(&make_pes(data, Some(90_000)));
        let pts: Vec<i64> = f.iter().map(|f| f.pts_ns).collect();
        assert_eq!(pts, [1, 2].map(|i| 1_000_000_000 + i * AAC_FRAME_NS));
    }

    // A frame whose sync is lost where a header was due opens a resync run: a false header in
    // its payload is not emitted, and the loss is counted.
    #[test]
    fn a_lost_sync_where_a_header_was_due_opens_a_run() {
        let mut seed = 79;
        let frames: Vec<Vec<u8>> = (0..4).map(|_| noisy_frame(&mut seed, 300)).collect();
        let mut bad = frames[1].clone();
        bad[..2].copy_from_slice(&[0x00, 0x00]);
        let mut fake = adts_frame(6);
        fake[7..].copy_from_slice(&noise(&mut seed, 6));
        bad[50..63].copy_from_slice(&fake);
        let data = [&frames[0][..], &bad, &frames[2], &frames[3]].concat();
        let mut p = AdtsParser::new();
        let f = p.parse(&make_pes(data, Some(0)));
        let pts: Vec<i64> = f.iter().map(|f| f.pts_ns).collect();
        assert_eq!(pts, [0, 2, 3].map(|i| i * AAC_FRAME_NS), "no junk AU");
        assert_eq!(p.dropped_frames(), 1);
    }

    // A verified drop costs at least one slot, however few bytes the run skipped.
    #[test]
    fn a_short_lost_frame_still_costs_a_slot() {
        let mut seed = 47;
        let (g0, g2, g3) = (
            noisy_frame(&mut seed, 300),
            noisy_frame(&mut seed, 300),
            noisy_frame(&mut seed, 300),
        );
        let mut bad = noisy_frame(&mut seed, 60);
        corrupt(&mut bad);
        let f = AdtsParser::new().parse(&make_pes([&g0[..], &bad, &g2, &g3].concat(), Some(0)));
        let pts: Vec<i64> = f.iter().map(|f| f.pts_ns).collect();
        assert_eq!(pts, [0, 2, 3].map(|i| i * AAC_FRAME_NS));
    }

    // A new PES timestamp met mid-run names the AU lost there, so skipped bytes restart.
    #[test]
    fn a_resync_run_restarts_under_a_new_timestamp() {
        let mut seed = 53;
        let mut p = AdtsParser::new();
        p.parse(&make_pes(noisy_frame(&mut seed, 300), Some(0)));
        let mut bad = noisy_frame(&mut seed, 300);
        corrupt(&mut bad);
        p.parse(&make_pes(bad.clone(), None));
        let (g1, g2) = (noisy_frame(&mut seed, 300), noisy_frame(&mut seed, 300));
        let f = p.parse(&make_pes([&bad[..], &g1, &g2].concat(), Some(90_000)));
        let pts: Vec<i64> = f.iter().map(|f| f.pts_ns).collect();
        assert_eq!(pts, [1, 2].map(|i| 1_000_000_000 + i * AAC_FRAME_NS));
    }

    // A frame awaiting its successor is emitted before a discontinuity clears the buffer.
    #[test]
    fn a_pending_resync_frame_survives_a_discontinuity() {
        let mut seed = 59;
        let (g0, g1, g2) = (
            noisy_frame(&mut seed, 200),
            noisy_frame(&mut seed, 200),
            noisy_frame(&mut seed, 200),
        );
        let mut bad = noisy_frame(&mut seed, 200);
        corrupt(&mut bad);
        let mut p = AdtsParser::new();
        p.parse(&make_pes(g0, Some(0)));
        assert!(
            p.parse(&make_pes([&bad[..], &g1].concat(), None))
                .is_empty()
        );
        let gap = PesPacket {
            discontinuity: true,
            ..make_pes(g2.clone(), Some(900_000))
        };
        let f = p.parse(&gap);
        assert_eq!(f.len(), 2);
        assert_eq!((&f[0].data[..], &f[1].data[..]), (&g1[7..], &g2[7..]));
    }

    #[test]
    fn a_nonzero_layer_or_mpeg2_profile_3_is_invalid() {
        let mut layer = adts_frame(16);
        layer[1] = 0xF3; // layer 01
        assert!(matches!(adts_verdict(&layer), AdtsVerdict::Invalid));
        let mut mpeg2 = adts_frame(16);
        mpeg2[1] = 0xF9; // ID = 1 (MPEG-2)
        assert!(matches!(adts_verdict(&mpeg2), AdtsVerdict::Valid));
        mpeg2[2] |= 0xC0; // profile 3: reserved in MPEG-2 AAC
        assert!(matches!(adts_verdict(&mpeg2), AdtsVerdict::Invalid));
        let mut mpeg4 = adts_frame(16);
        mpeg4[2] |= 0xC0; // profile 3 (LTP) is legal for MPEG-4
        assert!(matches!(adts_verdict(&mpeg4), AdtsVerdict::Valid));
    }

    // While resyncing, a valid-looking header that does not chain to another is payload.
    #[test]
    fn an_unchained_header_inside_a_resync_run_is_not_emitted() {
        let mut seed = 41;
        let frames: Vec<Vec<u8>> = (0..4).map(|_| noisy_frame(&mut seed, 300)).collect();
        let mut bad = frames[1].clone();
        corrupt(&mut bad);
        let mut fake = adts_frame(13);
        fake[7..].copy_from_slice(&noise(&mut seed, 13));
        bad[50..70].copy_from_slice(&fake);
        bad[70..72].copy_from_slice(&[0xFF, 0x12]); // 0xFF, but no sync after it
        let mut p = AdtsParser::new();
        let f = p.parse(&make_pes(
            [&frames[0][..], &bad, &frames[2], &frames[3]].concat(),
            Some(0),
        ));
        let pts: Vec<i64> = f.iter().map(|f| f.pts_ns).collect();
        assert_eq!(pts, [0, 2, 3].map(|i| i * AAC_FRAME_NS), "no junk AU");
        assert!(
            f.iter()
                .zip([0, 2, 3])
                .all(|(f, i)| f.data == frames[i][7..])
        );
        assert_eq!(p.dropped_frames(), 1);
    }

    // The first header after a resync waits for its successor, or for EOS if it ends the data.
    #[test]
    fn a_resync_header_waits_for_its_successor_or_eos() {
        let mut seed = 43;
        let fr: Vec<Vec<u8>> = (0..3).map(|_| noisy_frame(&mut seed, 200)).collect();
        let mut bad = noisy_frame(&mut seed, 200);
        corrupt(&mut bad);
        let lost = [&bad[..], &fr[1]].concat();
        let mut p = AdtsParser::new();
        p.parse(&make_pes(fr[0].clone(), Some(0)));
        assert!(
            p.parse(&make_pes(lost.clone(), None)).is_empty(),
            "unconfirmed yet"
        );
        let f = p.parse(&make_pes(fr[2].clone(), None));
        let pts: Vec<i64> = f.iter().map(|f| f.pts_ns).collect();
        assert_eq!(pts, [2, 3].map(|i| i * AAC_FRAME_NS));
        let mut p = AdtsParser::new();
        p.parse(&make_pes(fr[0].clone(), Some(0)));
        assert!(p.parse(&make_pes(lost, None)).is_empty());
        let f = p.flush();
        assert_eq!(f.len(), 1, "EOS: a frame ending the data is accepted");
        assert_eq!(f[0].data, fr[1][7..]);
    }

    // L056: random payloads (false syncs and all) are never scanned while framing is locked.
    #[test]
    fn random_payload_frames_pass_through_locked_framing() {
        let mut seed = 0x9E37_79B9_7F4A_7C15;
        let mut p = AdtsParser::new();
        let mut want = Vec::new();
        let mut data = Vec::new();
        for i in 0..64 {
            let mut f = noisy_frame(&mut seed, 100 + i * 7);
            f[20 + i..22 + i].copy_from_slice(&[0xFF, 0xF1]);
            want.push(f[7..].to_vec());
            data.extend_from_slice(&f);
        }
        let mut got = Vec::new();
        for chunk in data.chunks(1000) {
            got.extend(
                p.parse(&make_pes(chunk.to_vec(), None))
                    .into_iter()
                    .map(|f| f.data),
            );
        }
        assert_eq!(got, want);
        assert_eq!(p.dropped_frames(), 0);
    }

    #[test]
    fn discontinuity_drops_partial_frame_and_reanchors() {
        let data = adts_frame(20);
        let mut p = AdtsParser::new();
        p.parse(&make_pes(data[..10].to_vec(), Some(0)));
        let mut fresh = make_pes(data.clone(), Some(180000));
        fresh.discontinuity = true;
        fresh.source = Some(crate::pes::SourcePos::at_byte(4096));
        let frames = p.parse(&fresh);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].pts_ns, 2_000_000_000);
        assert_eq!(frames[0].source, fresh.source);
        assert!(frames[0].discontinuity);
    }
}
