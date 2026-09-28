//! AAC ADTS framing and validation.
//!
//! Spec text is quoted from ISO/IEC 13818-7:2004 (MPEG-2 AAC, ADTS in §6.2 and §8.1) and
//! ISO/IEC 11172-3 (the public CD text of §2.4.2.3, which 13818-7 refers to). MPEG-4 ADTS
//! (ID '0', 7350 Hz at index 0xc) follows ISO/IEC 14496-3, which is not quoted here.

use super::audio_frames::{AudioFrames, Header, Sync};
#[cfg(test)]
use super::pts_to_ns;
use super::{CodecParser, Frame, PesPacket};

/// [13818-7 §8.1.1.2 Table 35] `sampling_frequency_index` 0x0-0xb in Hz, "0xc reserved" to
/// "0xf reserved"; 0xc is 7350 Hz for MPEG-4 (14496-3). Zero marks a reserved index.
const ADTS_SAMPLE_RATE_VALID: [u32; 16] = [
    96000, 88200, 64000, 48000, 44100, 32000, 24000, 22050, 16000, 12000, 11025, 8000, 7350, 0, 0,
    0,
];

/// ADTS header verdict for the packet head.
enum AdtsVerdict {
    /// No 12-bit ADTS sync at the head — not an ADTS frame we can validate.
    NoSync,
    /// Sync present and the structural fields are legal.
    Valid,
    /// Sync present but a field is reserved or the frame length is shorter than its headers.
    Invalid,
}

fn adts_verdict(data: &[u8]) -> AdtsVerdict {
    // Need the full 7-byte fixed+variable header to read frame_length.
    if data.len() < 7 {
        return AdtsVerdict::NoSync;
    }
    // [13818-7 §8.1.1.2] syncword: "The bit string '1111 1111 1111'."
    if data[0] != 0xFF || (data[1] & 0xF0) != 0xF0 {
        return AdtsVerdict::NoSync;
    }
    // [§8.1.1.2] layer: "Set to '00'." ID: "MPEG identifier, set to '1'" (MPEG-2), where
    // [§7.1 Table 31] profile "3 (reserved)" and [Table 35] "0xc reserved".
    let mpeg2 = data[1] & 0x08 != 0;
    let sr_index = usize::from((data[2] >> 2) & 0x0F);
    if data[1] & 0x06 != 0 || (mpeg2 && (data[2] >> 6 == 3 || sr_index == 0xc)) {
        return AdtsVerdict::Invalid;
    }
    if ADTS_SAMPLE_RATE_VALID[sr_index] == 0 {
        return AdtsVerdict::Invalid;
    }
    // [§8.1.1.2] frame_length: "Length of the frame including headers and error_check in
    // bytes", so at least 7, or 9 with the CRC ("protection_absent" '0').
    let frame_length =
        ((u32::from(data[3]) & 0x03) << 11) | (u32::from(data[4]) << 3) | (u32::from(data[5]) >> 5);
    let header_bytes = if data[1] & 0x01 == 0 { 9 } else { 7 };
    if frame_length < header_bytes {
        return AdtsVerdict::Invalid;
    }
    AdtsVerdict::Valid
}

// Frame length of a valid ADTS header, with no parser state touched.
pub(super) fn adts_frame_len(data: &[u8]) -> Option<usize> {
    matches!(adts_verdict(data), AdtsVerdict::Valid).then(|| {
        (usize::from(data[3] & 3) << 11) | (usize::from(data[4]) << 3) | usize::from(data[5] >> 5)
    })
}

// [13818-7 §8.1.1.1] adts_fixed_header(): "does not change from frame to frame." Keyed on its
// ID, layer, profile, sampling_frequency_index, channel_configuration; protection_absent,
// private_bit, original_copy and home are left out (no effect on decoding the payload).
fn adts_fixed_key(d: &[u8]) -> u32 {
    u32::from_be_bytes([d[0], d[1], d[2], d[3]]) & 0x000E_FDC0
}

// Duration of a valid ADTS frame: 1024 samples per raw_data_block() (see adts_header).
fn adts_frame_ns(d: &[u8]) -> Option<u64> {
    adts_frame_len(d)?;
    let rate = ADTS_SAMPLE_RATE_VALID[usize::from((d[2] >> 2) & 15)];
    Some(1024 * (u64::from(d[6] & 3) + 1) * 1_000_000_000 / u64::from(rate))
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
                    fixed: adts_fixed_key,
                    frame_ns: adts_frame_ns,
                    // [13818-7 §8.1.1.2] frame_length "including headers": at least 7 bytes.
                    min_frame: 7,
                },
            ),
            config: None,
            config_changes: 0,
            pid: 0,
        }
    }
    /// Access units dropped as undecodable: a lower bound where no later timestamp measured a
    /// corrupt run (at EOS or a gap), which counts the fewest AUs it allows.
    pub fn dropped_frames(&self) -> u64 {
        self.frames.dropped_frames()
    }
    #[cfg(test)]
    pub(crate) fn verified_dropped(&self) -> u64 {
        self.frames.verified_dropped()
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
    // [§8.1.1.2] "Number of raw_data_block()'s ... is equal to number_of_raw_data_blocks_in_frame
    // + 1", and [§8.2.1.1] each holds "audio data for a time period of 1024 samples".
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
        corrupt(&mut bad);
        let mut p = AdtsParser::new();
        let mut seed = 11;
        p.parse(&make_pes(frame48(&mut seed, 200), Some(0)));
        p.parse(&make_pes(bad.repeat(600), None));
        let kept: usize = (1..=300)
            .map(|i| {
                p.parse(&make_pes(frame48(&mut seed, 200), Some(i * SLOT48)))
                    .len()
            })
            .sum();
        assert_eq!(kept, 300, "the good track survives");
        assert_eq!(
            p.frames.verified_dropped(),
            1,
            "one fault, one verified drop"
        );
        // The next PTS is one slot on: the garbage took no audio time, so one AU (the fault).
        assert_eq!(p.dropped_frames(), 1);
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
        let mut f = p.parse(&make_pes(data, Some(90_000)));
        f.extend(p.flush()); // I3: output after a run waits for the next PTS, or EOS
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
        let mut f = p.parse(&gap);
        f.extend(p.flush()); // the lock waits for the next PTS, or EOS
        assert_eq!(pts(&f), [10_000_000_000, 10_000_000_000 + AAC_FRAME_NS]);
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

    // A run still open at EOS reports its lost slots too, not just the first.
    #[test]
    fn a_run_open_at_eos_reports_every_lost_slot() {
        let mut bad = adts_frame(0);
        corrupt(&mut bad);
        let mut seed = 97;
        let mut p = AdtsParser::new();
        p.parse(&make_pes(noisy_frame(&mut seed, 200), Some(0)));
        p.parse(&make_pes(bad.repeat(600), None));
        assert!(p.flush().is_empty());
        assert_eq!(p.dropped_frames(), 20, "4194 scanned bytes are 20 frames");
        assert_eq!(p.frames.verified_dropped(), 1);
    }

    // So does a run that a discontinuity cuts short.
    #[test]
    fn a_run_cut_by_a_gap_reports_every_lost_slot() {
        let mut bad = adts_frame(0);
        corrupt(&mut bad);
        let mut seed = 101;
        let mut p = AdtsParser::new();
        p.parse(&make_pes(noisy_frame(&mut seed, 200), Some(0)));
        p.parse(&make_pes(bad.repeat(600), None));
        let gap = PesPacket {
            discontinuity: true,
            ..make_pes(noisy_frame(&mut seed, 200), Some(900_000))
        };
        let mut f = p.parse(&gap);
        f.extend(p.flush()); // after a gap the first frame chains (the PES may start in a fragment): EOS confirms it
        assert_eq!(f.len(), 1);
        assert_eq!(p.dropped_frames(), 20);
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
        let mut f = p.parse(&gap);
        f.extend(p.flush()); // after a gap the first frame chains (the PES may start in a fragment): EOS confirms it
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].pts_ns, 10_000_000_000);
    }

    // A resync run spanning two lost frames: the next PTS places the lock two slots on. At EOS
    // it takes one slot (the fewest the run allows): early by one, never late (I5).
    #[test]
    fn a_resync_run_over_two_frames_advances_two_slots() {
        for eos in [false, true] {
            let mut seed = 37;
            let mut frames: Vec<Vec<u8>> = (0..5).map(|_| frame48(&mut seed, 250)).collect();
            corrupt(&mut frames[1]);
            frames[2] = frame48(&mut seed, 240);
            frames[2][..2].copy_from_slice(&[0x12, 0x34]); // header lost: no sync at all
            let mut p = AdtsParser::new();
            let mut f = p.parse(&make_pes(frames.concat(), Some(0)));
            if eos {
                f.extend(p.flush());
                assert_slots(&f, &[0, 2, 3]);
            } else {
                f.extend(p.parse(&make_pes(frame48(&mut seed, 250), Some(5 * SLOT48))));
                assert_slots(&f, &[0, 3, 4, 5]);
                assert_eq!(p.dropped_frames(), 2, "both lost frames are reported");
            }
            assert_eq!(p.frames.verified_dropped(), 1);
        }
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
        let mut p = AdtsParser::new();
        let mut f = p.parse(&make_pes(data, Some(0)));
        f.extend(p.flush()); // I3: output after a run waits for the next PTS, or EOS
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

    // A false sync in the fragment a new PES carries over (a random 0xFF Fx turns up every
    // 2-4 KB) is not the corruption event starting again (I1), nor the AU the PES PTS names.
    // Supersedes round 3's one-slot-late limit; the corrupt-AU case keeps its own test below.
    #[test]
    fn a_false_sync_in_a_carried_fragment_is_one_fault_and_keeps_the_pts() {
        for flush in [false, true] {
            let mut seed = 73;
            let mut p = AdtsParser::new();
            p.parse(&make_pes(frame48(&mut seed, 300), Some(0)));
            p.parse(&make_pes(bad48(&mut seed, 300), None));
            let mut tail = noise(&mut seed, 30);
            tail[10..12].copy_from_slice(&[0xFF, 0xF3]); // sync-shaped, invalid layer
            let c = [tail, frame48(&mut seed, 300), frame48(&mut seed, 300)].concat();
            let mut f = p.parse(&make_pes(c, Some(2 * SLOT48)));
            if flush {
                f.extend(p.flush()); // EOS: no later PTS, the byte rule decides
            } else {
                f.extend(p.parse(&make_pes(frame48(&mut seed, 300), Some(4 * SLOT48))));
            }
            let p2 = pts_to_ns(2 * SLOT48);
            assert_eq!(pts(&f)[..2], [p2, p2 + D48], "flush {flush}");
            assert_eq!(p.dropped_frames(), 1, "flush {flush}");
            assert_eq!(p.frames.verified_dropped(), 1, "flush {flush}");
        }
    }

    // Mid-run, a PES carrying a fragment and then a valid (if unchained) header: its PTS names
    // that AU, so the next PTS places g1 one slot after it; at EOS g1 takes the PES PTS (I5).
    #[test]
    fn a_valid_unchained_header_in_a_new_pes_is_the_au_its_pts_names() {
        for eos in [false, true] {
            let mut seed = 89;
            let mut p = AdtsParser::new();
            p.parse(&make_pes(frame48(&mut seed, 300), Some(0)));
            p.parse(&make_pes(bad48(&mut seed, 300), None));
            let cut = frame48(&mut seed, 300)[..157].to_vec(); // its length runs past the cut
            let (g1, g2) = (frame48(&mut seed, 300), frame48(&mut seed, 300));
            let data = [&noise(&mut seed, 30)[..], &cut, &g1, &g2].concat();
            let mut f = p.parse(&make_pes(data, Some(2 * SLOT48)));
            if eos {
                f.extend(p.flush());
                assert_slots(&f, &[2, 3]);
            } else {
                f.extend(p.parse(&make_pes(frame48(&mut seed, 300), Some(5 * SLOT48))));
                assert_slots(&f, &[3, 4, 5]);
            }
            assert_eq!(p.frames.verified_dropped(), 1, "eos {eos}");
        }
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
        let mut f = p.parse(&make_pes(data, Some(0)));
        f.extend(p.flush()); // I3: output after a run waits for the next PTS, or EOS
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
        let mut p = AdtsParser::new();
        let mut f = p.parse(&make_pes([&g0[..], &bad, &g2, &g3].concat(), Some(0)));
        f.extend(p.flush()); // I3: output after a run waits for the next PTS, or EOS
        let pts: Vec<i64> = f.iter().map(|f| f.pts_ns).collect();
        assert_eq!(pts, [0, 2, 3].map(|i| i * AAC_FRAME_NS));
    }

    // A run crossing a PES whose PTS names a corrupt AU: the next PTS places the lock exactly
    // and the lost AUs are the clock skip (4). At EOS the lock takes the PES PTS (I5 limit).
    #[test]
    fn a_resync_run_restarts_under_a_new_timestamp() {
        for eos in [false, true] {
            let mut seed = 53;
            let mut p = AdtsParser::new();
            p.parse(&make_pes(frame48(&mut seed, 300), Some(0)));
            let bad = bad48(&mut seed, 300);
            p.parse(&make_pes(bad.repeat(3), None));
            let (g1, g2) = (frame48(&mut seed, 300), frame48(&mut seed, 300));
            let mut f = p.parse(&make_pes([&bad[..], &g1, &g2].concat(), Some(4 * SLOT48)));
            if eos {
                f.extend(p.flush());
                assert_slots(&f, &[4, 5]);
                assert_eq!(p.dropped_frames(), 3, "the clock skip taken");
            } else {
                f.extend(p.parse(&make_pes(frame48(&mut seed, 300), Some(7 * SLOT48))));
                assert_slots(&f, &[5, 6, 7]);
                assert_eq!(
                    p.dropped_frames(),
                    4,
                    "the old run's 3 lost AUs and the new one"
                );
            }
            assert_eq!(p.frames.verified_dropped(), 1, "I1: one run, one fault");
        }
    }

    // After a gap, a garbage run opening at the PES's first byte (the AU its PTS names) has a
    // clock: the next PTS places the lock at its true slot and every lost AU is counted.
    #[test]
    fn a_garbage_run_after_a_gap_reports_every_lost_slot() {
        let mut seed = 109;
        let bad = bad48(&mut seed, 193);
        let mut p = AdtsParser::new();
        p.parse(&make_pes(frame48(&mut seed, 193), Some(0)));
        let (g1, g2) = (frame48(&mut seed, 193), frame48(&mut seed, 193));
        let gap = PesPacket {
            discontinuity: true,
            ..make_pes([&bad.repeat(5)[..], &g1, &g2].concat(), Some(100 * SLOT48))
        };
        let mut f = p.parse(&gap);
        f.extend(p.parse(&make_pes(frame48(&mut seed, 193), Some(107 * SLOT48))));
        assert_slots(&f, &[105, 106, 107]);
        assert_eq!(p.dropped_frames(), 5);
        assert_eq!(p.frames.verified_dropped(), 1);
    }

    // One run spanning PES without a PTS is one fault: later PES starts are not new drops.
    #[test]
    fn a_run_spanning_several_pes_is_one_fault() {
        let mut seed = 107;
        let bad = bad48(&mut seed, 193);
        let mut p = AdtsParser::new();
        p.parse(&make_pes(frame48(&mut seed, 193), Some(0)));
        p.parse(&make_pes(bad.repeat(10), None));
        p.parse(&make_pes(bad.repeat(10), None));
        let good = [frame48(&mut seed, 193), frame48(&mut seed, 193)].concat();
        assert_eq!(p.parse(&make_pes(good, Some(21 * SLOT48))).len(), 2);
        assert_eq!(p.dropped_frames(), 20, "20 AUs lost");
        assert_eq!(p.frames.verified_dropped(), 1, "one fault");
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
        let mut f = p.parse(&gap);
        f.extend(p.flush()); // after a gap the first frame chains (the PES may start in a fragment): EOS confirms it
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
        let mut f = p.parse(&make_pes(
            [&frames[0][..], &bad, &frames[2], &frames[3]].concat(),
            Some(0),
        ));
        f.extend(p.flush()); // I3: output after a run waits for the next PTS, or EOS
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
        let mut f = p.parse(&make_pes(fr[2].clone(), None));
        f.extend(p.flush()); // I3: output after a run waits for the next PTS, or EOS
        let pts: Vec<i64> = f.iter().map(|f| f.pts_ns).collect();
        assert_eq!(pts, [2, 3].map(|i| i * AAC_FRAME_NS));
        let mut p = AdtsParser::new();
        p.parse(&make_pes(fr[0].clone(), Some(0)));
        assert!(p.parse(&make_pes(lost, None)).is_empty());
        let f = p.flush();
        assert_eq!(f.len(), 1, "EOS: a frame ending the data is accepted");
        assert_eq!(f[0].data, fr[1][7..]);
    }

    // ADTS header fields, laid out per [13818-7 §6.2.1 Table 8] adts_fixed_header() and
    // [§6.2.2 Table 9] adts_variable_header(); `len` bytes long, zero payload.
    struct Adts {
        id: u8,
        layer: u8,
        protection_absent: u8,
        profile: u8,
        sfi: u8,
        channels: u8,
        len: usize,
        fullness: u16,
        blocks: u8,
    }

    const LC_44K_STEREO: Adts = Adts {
        id: 1,
        layer: 0,
        protection_absent: 1,
        profile: 1,
        sfi: 4,
        channels: 2,
        len: 64,
        fullness: 0x7FF,
        blocks: 0,
    };

    impl Adts {
        fn bytes(&self) -> Vec<u8> {
            let mut f = vec![0u8; self.len.max(7)];
            f[0] = 0xFF;
            f[1] = 0xF0 | self.id << 3 | self.layer << 1 | self.protection_absent;
            f[2] = self.profile << 6 | self.sfi << 2 | self.channels >> 2;
            f[3] = (self.channels & 3) << 6 | (self.len >> 11) as u8;
            f[4] = (self.len >> 3) as u8;
            f[5] = ((self.len & 7) as u8) << 5 | (self.fullness >> 6) as u8;
            f[6] = ((self.fullness & 0x3F) as u8) << 2 | self.blocks;
            f
        }
        fn verdict(&self) -> AdtsVerdict {
            adts_verdict(&self.bytes())
        }
        fn header(&self) -> Option<Header> {
            adts_header(&self.bytes(), &mut None, &mut 0, 0)
        }
    }

    // [13818-7 §8.1.1.2] syncword: "The bit string '1111 1111 1111'."
    #[test]
    fn spec_syncword_is_twelve_ones() {
        let f = LC_44K_STEREO.bytes();
        assert!(matches!(adts_verdict(&f), AdtsVerdict::Valid));
        for bit in 0..4 {
            let mut g = f.clone();
            g[1] &= !(0x80 >> bit);
            assert!(
                matches!(adts_verdict(&g), AdtsVerdict::NoSync),
                "sync bit {bit}"
            );
        }
    }

    // [13818-7 §8.1.1.2] layer: "Indicates which layer is used. Set to '00'."
    #[test]
    fn spec_layer_is_00() {
        for layer in 1..4 {
            let a = Adts {
                layer,
                ..LC_44K_STEREO
            };
            assert!(matches!(a.verdict(), AdtsVerdict::Invalid), "layer {layer}");
        }
    }

    // [13818-7 §7.1 Table 31] profile: "0 Main profile", "1 Low Complexity profile (LC)",
    // "2 Scalable Sampling Rate profile (SSR)", "3 (reserved)".
    #[test]
    fn spec_mpeg2_profile_3_is_reserved() {
        for profile in 0..3 {
            let a = Adts {
                profile,
                ..LC_44K_STEREO
            };
            assert!(
                matches!(a.verdict(), AdtsVerdict::Valid),
                "profile {profile}"
            );
        }
        let a = Adts {
            profile: 3,
            ..LC_44K_STEREO
        };
        assert!(matches!(a.verdict(), AdtsVerdict::Invalid));
    }

    // [13818-7 §8.1.1.2 Table 35] sampling_frequency_index 0x0..0xb: 96000 88200 64000 48000
    // 44100 32000 24000 22050 16000 12000 11025 8000 Hz; "0xc reserved" to "0xf reserved".
    #[test]
    fn spec_sampling_frequency_index_table() {
        let hz = [
            96000, 88200, 64000, 48000, 44100, 32000, 24000, 22050, 16000, 12000, 11025, 8000,
        ];
        for (sfi, want) in hz.into_iter().enumerate() {
            let a = Adts {
                sfi: sfi as u8,
                ..LC_44K_STEREO
            };
            assert_eq!(a.header().map(|h| h.rate), Some(want), "index {sfi:#x}");
        }
        for sfi in 0xc..=0xf {
            let a = Adts {
                sfi,
                ..LC_44K_STEREO
            };
            assert!(
                matches!(a.verdict(), AdtsVerdict::Invalid),
                "index {sfi:#x}"
            );
        }
    }

    // [13818-7 §8.1.1.2] protection_absent: "Indicates whether error_check() data is present or
    // not." [11172-3 §2.4.2.3] protection_bit: "'0' if redundancy has been added".
    #[test]
    fn spec_protection_absent_selects_the_crc() {
        for (absent, header) in [(1, 7), (0, 9)] {
            let a = Adts {
                protection_absent: absent,
                ..LC_44K_STEREO
            };
            let h = a.header().unwrap();
            assert_eq!(
                (h.skip, h.bytes),
                (header, 64),
                "protection_absent {absent}"
            );
        }
    }

    // [13818-7 §8.1.1.2] frame_length: "Length of the frame including headers and error_check
    // in bytes". So it can never be shorter than those headers.
    #[test]
    fn spec_frame_length_includes_headers_and_error_check() {
        let mut p = AdtsParser::new();
        let f = p.parse(&make_pes(LC_44K_STEREO.bytes(), Some(0)));
        assert_eq!(f[0].data.len(), 64 - 7, "payload = frame_length - header");
        for (absent, floor) in [(1, 7), (0, 9)] {
            let short = Adts {
                protection_absent: absent,
                len: floor - 1,
                ..LC_44K_STEREO
            };
            assert!(matches!(adts_verdict(&short.bytes()), AdtsVerdict::Invalid));
            let ok = Adts {
                protection_absent: absent,
                len: floor,
                ..LC_44K_STEREO
            };
            assert!(matches!(ok.verdict(), AdtsVerdict::Valid));
        }
    }

    // [13818-7 §8.1.1.2] "Number of raw_data_block()'s that are multiplexed in the adts_frame()
    // is equal to number_of_raw_data_blocks_in_frame + 1." [§8.2.1.1] raw_data_block(): "block
    // of raw data that contains audio data for a time period of 1024 samples".
    #[test]
    fn spec_each_raw_data_block_is_1024_samples() {
        for blocks in 0..4u8 {
            let a = Adts {
                blocks,
                ..LC_44K_STEREO
            };
            assert_eq!(a.header().unwrap().samples, 1024 * (u32::from(blocks) + 1));
        }
    }

    // [13818-7 §8.1.1.2] channel_configuration: "If channel_configuration equals 0, the channel
    // configuration is not specified in the header" (a PCE carries it): legal, not rejected.
    #[test]
    fn spec_channel_configuration_0_is_legal() {
        for channels in 0..8 {
            let a = Adts {
                channels,
                ..LC_44K_STEREO
            };
            assert!(
                matches!(a.verdict(), AdtsVerdict::Valid),
                "config {channels}"
            );
        }
    }

    // [13818-7 §8.1.1.2] adts_buffer_fullness: "A value of hexadecimal 7FF signals that the
    // bitstream is a variable rate bitstream." Any fullness is legal.
    #[test]
    fn spec_buffer_fullness_is_not_validated() {
        for fullness in [0, 0x123, 0x7FF] {
            let a = Adts {
                fullness,
                ..LC_44K_STEREO
            };
            assert!(
                matches!(a.verdict(), AdtsVerdict::Valid),
                "fullness {fullness:#x}"
            );
        }
    }

    // [13818-7 §8.1.1.1] adts_fixed_header(): "The information in this header does not change
    // from frame to frame." The resync key covers it and ignores the variable header.
    #[test]
    fn spec_fixed_key_is_the_fixed_header() {
        let key = |a: Adts| adts_fixed_key(&a.bytes());
        let base = key(LC_44K_STEREO);
        assert_eq!(
            base,
            key(Adts {
                len: 300,
                fullness: 0,
                blocks: 3,
                ..LC_44K_STEREO
            })
        );
        assert_ne!(
            base,
            key(Adts {
                id: 0,
                ..LC_44K_STEREO
            })
        );
        assert_ne!(
            base,
            key(Adts {
                profile: 0,
                ..LC_44K_STEREO
            })
        );
        assert_ne!(
            base,
            key(Adts {
                sfi: 3,
                ..LC_44K_STEREO
            })
        );
        assert_ne!(
            base,
            key(Adts {
                channels: 6,
                ..LC_44K_STEREO
            })
        );
        assert_ne!(
            base,
            key(Adts {
                channels: 3,
                ..LC_44K_STEREO
            })
        );
    }

    // 48 kHz frames: one 1024-sample slot is exactly 1920 PTS ticks.
    fn frame48(seed: &mut u64, payload: usize) -> Vec<u8> {
        let mut f = noisy_frame(seed, payload);
        f[2] = (f[2] & 0xC3) | (3 << 2);
        f
    }
    const SLOT48: i64 = 1920;
    const D48: i64 = 1024 * 1_000_000_000 / 48000;
    fn bad48(seed: &mut u64, payload: usize) -> Vec<u8> {
        let mut f = frame48(seed, payload);
        corrupt(&mut f);
        f
    }
    fn pts(f: &[Frame]) -> Vec<i64> {
        f.iter().map(|f| f.pts_ns).collect()
    }

    // [13818-1 §2.4.3.7] a PES's PTS belongs to the first AU starting in it: mid-run, b3 after the
    // carried fragment, so the next PTS puts g1 a slot after it. At EOS g1 takes the fewest slots
    // the run allows, the PES PTS: one early, never late (I5).
    #[test]
    fn a_new_pes_pts_names_the_first_au_starting_in_it() {
        for eos in [false, true] {
            let mut seed = 113;
            let mut p = AdtsParser::new();
            p.parse(&make_pes(frame48(&mut seed, 300), Some(0)));
            let (b1, b2, b3) = (
                bad48(&mut seed, 300),
                bad48(&mut seed, 300),
                bad48(&mut seed, 300),
            );
            p.parse(&make_pes([&b1[..], &b2[..257]].concat(), None));
            let (g1, g2) = (frame48(&mut seed, 300), frame48(&mut seed, 300));
            let mut f = p.parse(&make_pes(
                [&b2[257..], &b3, &g1, &g2].concat(),
                Some(3 * SLOT48),
            ));
            if eos {
                f.extend(p.flush());
                assert_slots(&f, &[3, 4]);
            } else {
                f.extend(p.parse(&make_pes(frame48(&mut seed, 300), Some(6 * SLOT48))));
                assert_slots(&f, &[4, 5, 6]);
                assert_eq!(p.dropped_frames(), 3);
            }
            assert_eq!(p.frames.verified_dropped(), 1, "I1: one run, one fault");
        }
    }

    // Timestamps count the AUs a run lost, whatever their size: VBR frames larger than the
    // last good one (small then large).
    #[test]
    fn lost_aus_come_from_timestamps_small_then_large() {
        let mut seed = 127;
        let mut p = AdtsParser::new();
        p.parse(&make_pes(frame48(&mut seed, 50), Some(0)));
        p.parse(&make_pes(
            [bad48(&mut seed, 400), bad48(&mut seed, 400)].concat(),
            None,
        ));
        let g = [frame48(&mut seed, 400), frame48(&mut seed, 400)].concat();
        let f = p.parse(&make_pes(g, Some(3 * SLOT48)));
        let p3 = pts_to_ns(3 * SLOT48);
        assert_eq!(pts(&f), [p3, p3 + D48]);
        assert_eq!(p.dropped_frames(), 2);
        assert_eq!(
            p.dropped_duration_ns(),
            2 * D48 as u64,
            "timestamps measure the loss"
        );
    }

    // ... and smaller than it (large then small).
    #[test]
    fn lost_aus_come_from_timestamps_large_then_small() {
        let mut seed = 131;
        let mut p = AdtsParser::new();
        p.parse(&make_pes(frame48(&mut seed, 800), Some(0)));
        p.parse(&make_pes(
            [bad48(&mut seed, 50), bad48(&mut seed, 50)].concat(),
            None,
        ));
        let g = [frame48(&mut seed, 50), frame48(&mut seed, 50)].concat();
        let f = p.parse(&make_pes(g, Some(3 * SLOT48)));
        let p3 = pts_to_ns(3 * SLOT48);
        assert_eq!(pts(&f), [p3, p3 + D48]);
        assert_eq!(p.dropped_frames(), 2);
    }

    // With no later PTS the lock takes the fewest slots the run allows (I5): VBR makes a byte
    // yardstick unreliable, and early is corrected by the next PTS where late would be carried
    // by the I3 backstop. With a later PTS it is exact.
    #[test]
    fn without_a_pts_lost_aus_use_the_mean_frame_size() {
        for eos in [false, true] {
            let mut seed = 137;
            let mut p = AdtsParser::new();
            let mut data: Vec<u8> = (0..10)
                .flat_map(|i| frame48(&mut seed, [100, 500][i % 2]))
                .collect();
            data.extend([bad48(&mut seed, 300), bad48(&mut seed, 300)].concat());
            data.extend([frame48(&mut seed, 300), frame48(&mut seed, 300)].concat());
            let mut f = p.parse(&make_pes(data, Some(0)));
            if eos {
                f.extend(p.flush());
                assert_eq!(f[10].pts_ns, 11 * D48, "one slot: early by one, never late");
                assert_eq!(p.dropped_frames(), 1, "the clock skip taken");
            } else {
                f.extend(p.parse(&make_pes(frame48(&mut seed, 300), Some(14 * SLOT48))));
                assert_eq!(
                    f[10].pts_ns,
                    pts_to_ns(14 * SLOT48) - 2 * D48,
                    "two lost slots"
                );
                assert_eq!(p.dropped_frames(), 2);
            }
        }
    }

    // At EOS there is no lock at all: the open run is sized by the mean frame size too.
    #[test]
    fn at_eos_lost_aus_use_the_mean_frame_size() {
        let mut seed = 139;
        let mut p = AdtsParser::new();
        let mut data: Vec<u8> = (0..10)
            .flat_map(|i| frame48(&mut seed, [100, 500][i % 2]))
            .collect();
        data.extend((0..3).flat_map(|_| bad48(&mut seed, 300)));
        p.parse(&make_pes(data, Some(0)));
        p.flush();
        assert_eq!(p.dropped_frames(), 3);
    }

    // The clock never runs past a timestamp already buffered after the lock.
    #[test]
    fn a_byte_estimate_never_passes_a_known_next_pts() {
        let mut seed = 149;
        let mut p = AdtsParser::new();
        p.parse(&make_pes(frame48(&mut seed, 50), Some(0)));
        let run = [
            bad48(&mut seed, 400),
            bad48(&mut seed, 400),
            frame48(&mut seed, 400),
        ]
        .concat();
        assert!(
            p.parse(&make_pes(run, None)).is_empty(),
            "the lock awaits its successor"
        );
        let d = [frame48(&mut seed, 400), frame48(&mut seed, 400)].concat();
        let f = p.parse(&make_pes(d, Some(4 * SLOT48)));
        let p4 = pts_to_ns(4 * SLOT48);
        assert_eq!(pts(&f), [p4 - D48, p4, p4 + D48]);
        assert_eq!(
            p.dropped_frames(),
            2,
            "lost AUs from the clamped delta (I2, I4)"
        );
    }

    // VBR run (57-byte corrupt AUs after a 407-byte frame) crossing a PES whose PTS names a
    // corrupt AU: the next PTS places g1 at slot 4 exactly. At EOS it takes the PES PTS (slot
    // 3): one slot early, where a byte yardstick could not tell a 57-byte AU from a fragment.
    #[test]
    fn a_vbr_run_across_a_pes_pts_is_counted_by_timestamps() {
        for eos in [false, true] {
            let mut seed = 157;
            let mut p = AdtsParser::new();
            p.parse(&make_pes(frame48(&mut seed, 400), Some(0)));
            p.parse(&make_pes(
                [bad48(&mut seed, 50), bad48(&mut seed, 50)].concat(),
                None,
            ));
            let c = [
                bad48(&mut seed, 50),
                frame48(&mut seed, 400),
                frame48(&mut seed, 400),
            ];
            let mut f = p.parse(&make_pes(c.concat(), Some(3 * SLOT48)));
            if eos {
                f.extend(p.flush());
                assert_slots(&f, &[3, 4]);
                assert_eq!(p.dropped_frames(), 2, "the clock skip taken");
            } else {
                f.extend(p.parse(&make_pes(frame48(&mut seed, 400), Some(6 * SLOT48))));
                assert_slots(&f, &[4, 5, 6]);
                assert_eq!(p.dropped_frames(), 3);
            }
            assert_eq!(p.frames.verified_dropped(), 1);
        }
    }

    // A sync-shaped byte left from the previous PES is not the AU the new PES's PTS names: the
    // PTS belongs to a byte of the new PES (the corrupt AU there), placed by the next PTS.
    #[test]
    fn a_sync_left_from_the_previous_pes_does_not_take_the_new_pts() {
        let mut seed = 163;
        let mut p = AdtsParser::new();
        p.parse(&make_pes(frame48(&mut seed, 300), Some(0)));
        let b = [&bad48(&mut seed, 300)[..], &[0xFF, 0xF3, 0x00, 0x00]].concat();
        p.parse(&make_pes(b, None));
        let c = [
            bad48(&mut seed, 300),
            frame48(&mut seed, 300),
            frame48(&mut seed, 300),
        ];
        let mut f = p.parse(&make_pes(c.concat(), Some(2 * SLOT48)));
        f.extend(p.parse(&make_pes(frame48(&mut seed, 300), Some(5 * SLOT48))));
        assert_slots(&f, &[3, 4, 5]);
    }

    // The mean frame size (which sizes a run no frame ends) restarts with the stream key, so a
    // run open at EOS after a change is counted in the new stream's frames.
    #[test]
    fn the_mean_frame_size_restarts_with_the_stream_key() {
        let mut seed = 167;
        let mono = |mut f: Vec<u8>| {
            f[2] &= !1;
            f[3] = (f[3] & 0x3F) | 1 << 6;
            f
        };
        let mut data: Vec<u8> = (0..10).flat_map(|_| frame48(&mut seed, 100)).collect();
        data.extend((0..2).flat_map(|_| mono(frame48(&mut seed, 500))));
        data.extend((0..2).flat_map(|_| mono(bad48(&mut seed, 500))));
        let mut p = AdtsParser::new();
        p.parse(&make_pes(data, Some(0)));
        p.flush();
        assert_eq!(
            p.dropped_frames(),
            2,
            "two 507-byte AUs, not six of the old mean"
        );
    }

    // I3: a lock whose successor starts before the next PTS's PES is placed k frames before it,
    // not one (slots [3,4,5,6], not [4,5,5,6]).
    #[test]
    fn a_lock_is_placed_by_every_frame_before_the_next_pts() {
        let mut seed = 173;
        let mut p = AdtsParser::new();
        p.parse(&make_pes(frame48(&mut seed, 50), Some(0)));
        let (g1, g2) = (frame48(&mut seed, 400), frame48(&mut seed, 400));
        let a = [
            &bad48(&mut seed, 400)[..],
            &bad48(&mut seed, 400),
            &g1,
            &g2[..4],
        ]
        .concat();
        let mut f = p.parse(&make_pes(a, None));
        let b = [&g2[4..], &frame48(&mut seed, 400), &frame48(&mut seed, 400)].concat();
        f.extend(p.parse(&make_pes(b, Some(5 * SLOT48))));
        let p5 = pts_to_ns(5 * SLOT48);
        assert_eq!(pts(&f), [p5 - 2 * D48, p5 - D48, p5, p5 + D48]);
        assert_eq!(p.dropped_frames(), 2);
    }

    // I3: with no PTS buffered at the lock, a VBR byte estimate (57-byte mean, 407-byte AUs)
    // must not stamp ahead of the next PTS and then jump back.
    #[test]
    fn pts_never_go_backwards_after_a_vbr_run() {
        let mut seed = 179;
        let mut p = AdtsParser::new();
        let mut f = p.parse(&make_pes(frame48(&mut seed, 50), Some(0)));
        let a = [
            bad48(&mut seed, 400),
            bad48(&mut seed, 400),
            frame48(&mut seed, 400),
        ];
        f.extend(p.parse(&make_pes(
            [&a.concat()[..], &frame48(&mut seed, 400)].concat(),
            None,
        )));
        f.extend(p.parse(&make_pes(frame48(&mut seed, 400), Some(5 * SLOT48))));
        f.extend(p.parse(&make_pes(frame48(&mut seed, 400), None)));
        let got = pts(&f);
        assert!(
            got.windows(2).all(|w| w[0] < w[1]),
            "strictly increasing: {got:?}"
        );
        let p5 = pts_to_ns(5 * SLOT48);
        assert_eq!(got[1..], [p5 - 2 * D48, p5 - D48, p5, p5 + D48]);
    }

    // I2: a PTS jump the skipped bytes cannot hold is an unflagged discontinuity: the clock
    // follows it, but the lost count stays what the bytes allow, logged once.
    #[test]
    fn a_pts_jump_beyond_the_bytes_is_not_counted_as_lost_aus() {
        let mut seed = 181;
        let mut p = AdtsParser::new();
        p.parse(&make_pes(frame48(&mut seed, 300), Some(0)));
        p.parse(&make_pes(bad48(&mut seed, 300), None));
        let far = 1_000_000 * SLOT48;
        let c = [frame48(&mut seed, 300), frame48(&mut seed, 300)].concat();
        let f = p.parse(&make_pes(c, Some(far)));
        assert_eq!(f[0].pts_ns, pts_to_ns(far));
        assert_eq!(
            p.dropped_frames(),
            1,
            "307 bytes hold one AU, not a million"
        );
    }

    // I2 mid-run: a forward PES timestamp jump the run's bytes cannot hold is followed, not
    // counted: the lock takes the jump PTS (the fewest slots, I5), and the lost count stays what
    // the bytes allow, one run and one fault (I1).
    #[test]
    fn a_mid_run_pts_jump_is_followed_but_not_counted() {
        let mut seed = 191;
        let mut p = AdtsParser::new();
        p.parse(&make_pes(frame48(&mut seed, 300), Some(0)));
        p.parse(&make_pes(bad48(&mut seed, 300), None));
        let far = 1_000_000 * SLOT48;
        let c = [
            bad48(&mut seed, 300),
            frame48(&mut seed, 300),
            frame48(&mut seed, 300),
        ];
        let mut f = p.parse(&make_pes(c.concat(), Some(far)));
        f.extend(p.flush());
        assert_eq!(pts(&f), [pts_to_ns(far), pts_to_ns(far) + D48]);
        assert_eq!(p.dropped_frames(), 2, "what 614 bytes hold, not a million");
        assert_eq!(p.frames.verified_dropped(), 1);
    }

    // I3: a timestamp behind the run's start (unflagged, backwards) does not move the clock back.
    #[test]
    fn a_backwards_pts_after_a_run_keeps_the_clock() {
        let mut seed = 193;
        let mut p = AdtsParser::new();
        let mut f = p.parse(&make_pes(frame48(&mut seed, 300), Some(5 * SLOT48)));
        p.parse(&make_pes(bad48(&mut seed, 300), None));
        let c = [frame48(&mut seed, 300), frame48(&mut seed, 300)].concat();
        f.extend(p.parse(&make_pes(c, Some(2 * SLOT48))));
        let got = pts(&f);
        assert!(
            got.windows(2).all(|w| w[0] < w[1]),
            "strictly increasing: {got:?}"
        );
    }

    // A non-conformant stream with no further PTS is not held without bound: past MAX_HOLD the
    // lock takes the fewest slots possible (one, the verified AU).
    #[test]
    fn a_lock_is_not_held_without_bound() {
        let mut seed = 197;
        let mut p = AdtsParser::new();
        p.parse(&make_pes(frame48(&mut seed, 300), Some(0)));
        let run: Vec<u8> = [bad48(&mut seed, 300)]
            .into_iter()
            .chain((0..250).map(|_| frame48(&mut seed, 300)))
            .flatten()
            .collect();
        let f = p.parse(&make_pes(run, None));
        assert!(!f.is_empty(), "released before EOS");
        assert_eq!(f[0].pts_ns, 2 * D48);
    }

    // An implied AU size below a tenth of the stream mean is not believed: a timestamp that
    // claims 42 seven-byte AUs in 307 bytes cannot push the next frame 27 slots on.
    #[test]
    fn an_implausible_implied_au_size_is_floored() {
        let mut seed = 199;
        let mut p = AdtsParser::new();
        p.parse(&make_pes(noisy_frame(&mut seed, 300), Some(0)));
        p.parse(&make_pes(bad48(&mut seed, 300), None));
        let data = [
            &noise(&mut seed, 187)[..],
            &noisy_frame(&mut seed, 300),
            &noisy_frame(&mut seed, 300),
        ]
        .concat();
        let mut f = p.parse(&make_pes(data, Some(90_000)));
        f.extend(p.flush());
        assert!(
            f[0].pts_ns <= 1_000_000_000 + 10 * AAC_FRAME_NS,
            "{}",
            f[0].pts_ns
        );
    }

    // A lock whose header walk to the next PTS leaves the chain (a second corruption with tiny
    // VBR/silence frames after it) is not placed from that PTS: the byte estimate is no
    // measurement. The second run's lock, which does reach the PTS, is placed exactly.
    #[test]
    fn a_walk_that_leaves_the_chain_does_not_place_the_lock() {
        let mut seed = 211;
        let mut c2 = frame48(&mut seed, 10);
        corrupt(&mut c2);
        let pes1 = [
            frame48(&mut seed, 393),
            frame48(&mut seed, 393),
            bad48(&mut seed, 393),
            frame48(&mut seed, 393),
            c2,
            frame48(&mut seed, 3),
            frame48(&mut seed, 3),
        ];
        let mut p = AdtsParser::new();
        let mut f = p.parse(&make_pes(pes1.concat(), Some(0)));
        f.extend(p.parse(&make_pes(frame48(&mut seed, 393), Some(7 * SLOT48))));
        let p7 = pts_to_ns(7 * SLOT48);
        let want = [0, D48, 3 * D48, p7 - 2 * D48, p7 - D48, p7];
        assert_eq!(pts(&f), want, "F0 F1 F3 F5 F6 F7");
        assert_eq!(p.dropped_frames(), 2);
        assert_eq!(p.frames.verified_dropped(), 2, "two runs");
    }

    // The same walk break with a run far larger than the stream's frames: the byte estimate
    // overshoots, so the lock is capped below q by the walked frames, a lost AU and the next
    // chain's frames that reach q; nothing repeats or goes backwards (I3).
    #[test]
    fn a_walk_break_leaves_room_for_the_next_chain() {
        let mut seed = 227;
        let mut c2 = frame48(&mut seed, 10);
        corrupt(&mut c2);
        let pes1 = [
            frame48(&mut seed, 10),
            frame48(&mut seed, 10),
            bad48(&mut seed, 400),
            frame48(&mut seed, 10),
            c2,
            frame48(&mut seed, 10),
            frame48(&mut seed, 10),
        ];
        let mut p = AdtsParser::new();
        let mut f = p.parse(&make_pes(pes1.concat(), Some(0)));
        f.extend(p.parse(&make_pes(frame48(&mut seed, 10), Some(7 * SLOT48))));
        let got = pts(&f);
        assert!(
            got.windows(2).all(|w| w[0] < w[1]),
            "strictly increasing: {got:?}"
        );
        let p7 = pts_to_ns(7 * SLOT48);
        assert_eq!(
            got[3..],
            [p7 - 2 * D48, p7 - D48, p7],
            "F5 F6 F7 placed exactly"
        );
        assert!(got[2] <= p7 - 4 * D48, "F3 leaves room for C2, F5, F6");
    }

    // A walk break with no later chain before q (the corruption runs up to the next PES): the
    // lock needs room only for itself and one lost AU.
    #[test]
    fn a_walk_break_with_no_next_chain_reserves_one_lost_au() {
        let mut seed = 233;
        let pes1 = [
            frame48(&mut seed, 400),
            frame48(&mut seed, 400),
            bad48(&mut seed, 400),
            frame48(&mut seed, 400),
            bad48(&mut seed, 400),
        ];
        let mut p = AdtsParser::new();
        let mut f = p.parse(&make_pes(pes1.concat(), Some(0)));
        f.extend(p.parse(&make_pes(frame48(&mut seed, 400), Some(5 * SLOT48))));
        f.extend(p.flush()); // run 2's lock has no successor until EOS
        assert_eq!(pts(&f), [0, D48, 3 * D48, pts_to_ns(5 * SLOT48)]);
    }

    // With timestamps too close to hold the walked frames, the capped lock still never goes
    // behind the run's start (I3).
    #[test]
    fn a_capped_lock_never_goes_behind_the_run_start() {
        let mut seed = 229;
        let mut c2 = frame48(&mut seed, 10);
        corrupt(&mut c2);
        let pes1 = [
            frame48(&mut seed, 100),
            frame48(&mut seed, 100),
            bad48(&mut seed, 100),
            frame48(&mut seed, 100),
            c2,
            frame48(&mut seed, 3),
            frame48(&mut seed, 3),
        ];
        let mut p = AdtsParser::new();
        let mut f = p.parse(&make_pes(pes1.concat(), Some(0)));
        f.extend(p.parse(&make_pes(frame48(&mut seed, 100), Some(3 * SLOT48))));
        assert!(
            f[2].pts_ns >= 2 * D48,
            "F3 at or after the run's first lost slot"
        );
        assert!(f[2].pts_ns > f[1].pts_ns);
    }

    // Stamps within a few ns of the given slots (the clock adds whole frame durations; the
    // PTS ticks round to ns).
    fn assert_slots(f: &[Frame], slots: &[i64]) {
        let got = pts(f);
        let ok = got.len() == slots.len()
            && got
                .iter()
                .zip(slots)
                .all(|(g, s)| (g - pts_to_ns(s * SLOT48)).abs() <= 16);
        assert!(ok, "got {got:?}, want slots {slots:?}");
    }

    fn vbr_stream(spec: &[i64], seed: &mut u64) -> Vec<u8> {
        spec.iter()
            .flat_map(|&n| {
                if n < 0 {
                    bad48(seed, -n as usize)
                } else {
                    frame48(seed, n as usize)
                }
            })
            .collect()
    }

    // Regression A: several breaks between a lock and the next PTS. Room is reserved for every
    // walked frame and one AU per break; the chain reaching q is exact (I3, I4, I2).
    #[test]
    fn several_breaks_before_the_next_pts_stay_ordered() {
        let mut seed = 239;
        let pes1 = vbr_stream(&[10, 10, -400, 10, -10, 10, 10, -10, 10], &mut seed);
        let mut p = AdtsParser::new();
        let mut f = p.parse(&make_pes(pes1, Some(0)));
        f.extend(p.parse(&make_pes(
            vbr_stream(&[10, 10], &mut seed),
            Some(9 * SLOT48),
        )));
        assert_slots(&f, &[0, 1, 3, 5, 6, 8, 9, 10]);
        assert_eq!(p.dropped_frames(), 3, "the clock skips three slots");
        assert_eq!(p.frames.verified_dropped(), 3);
    }

    // Regression B: VBR payloads with three corruptions and a single later PTS.
    #[test]
    fn a_vbr_stream_with_three_breaks_stays_ordered() {
        let mut seed = 241;
        let spec = [3, 2, 2, 5, -382, 104, -1406, 5, 155, 691, -928, 883];
        let mut p = AdtsParser::new();
        let mut f = p.parse(&make_pes(vbr_stream(&spec, &mut seed), Some(0)));
        f.extend(p.parse(&make_pes(
            vbr_stream(&[100, 100], &mut seed),
            Some(12 * SLOT48),
        )));
        assert_slots(&f, &[0, 1, 2, 3, 5, 7, 8, 9, 11, 12, 13]);
        assert_eq!(p.dropped_frames(), 3);
    }

    // A lock with a break before the next PTS takes the fewest slots its run allows (slot 3):
    // early, never late, as a byte split can err late and push later frames on (I3, I5). The
    // chain reaching the PTS is exact.
    #[test]
    fn a_lock_before_a_break_takes_the_fewest_slots() {
        let mut seed = 263;
        let pes1 = vbr_stream(
            &[300, 300, -300, -300, -300, 300, -300, 300, 300],
            &mut seed,
        );
        let mut p = AdtsParser::new();
        let mut f = p.parse(&make_pes(pes1, Some(0)));
        f.extend(p.parse(&make_pes(frame48(&mut seed, 300), Some(9 * SLOT48))));
        assert_slots(&f, &[0, 1, 3, 7, 8, 9]);
    }

    // A mid-run PTS names an AU at or before the lock, so the lock is never placed before it,
    // even where bytes (tiny run AUs, a big break) would give the run fewer slots.
    #[test]
    fn a_split_lock_is_never_before_a_mid_run_pts() {
        let mut seed = 269;
        let mut p = AdtsParser::new();
        let mut f = p.parse(&make_pes(frame48(&mut seed, 300), Some(0)));
        p.parse(&make_pes(vbr_stream(&[-50, -50], &mut seed), None));
        let c = vbr_stream(&[-50, 300, -1400, 300, 300], &mut seed);
        f.extend(p.parse(&make_pes(c, Some(3 * SLOT48))));
        f.extend(p.parse(&make_pes(frame48(&mut seed, 300), Some(8 * SLOT48))));
        let (got, lost) = (pts(&f), p.dropped_frames());
        assert!(
            f[1].pts_ns >= pts_to_ns(3 * SLOT48),
            "lock before the mid PTS: {got:?} {lost}"
        );
        assert!(pts(&f).windows(2).all(|w| w[0] < w[1]));
    }

    // Contradicting timestamps (a mid-run PTS ahead of what the next PTS allows): the lock is
    // still kept below the next PTS by every later frame and break, so the chain reaching it
    // lands exactly there rather than being pushed on by the backstop.
    #[test]
    fn a_split_lock_stays_below_the_next_pts() {
        let mut seed = 257;
        let mut p = AdtsParser::new();
        let mut f = p.parse(&make_pes(frame48(&mut seed, 300), Some(0)));
        p.parse(&make_pes(bad48(&mut seed, 300), None));
        let c = vbr_stream(&[-300, 300, -300, 300, 300], &mut seed);
        f.extend(p.parse(&make_pes(c, Some(6 * SLOT48))));
        f.extend(p.parse(&make_pes(frame48(&mut seed, 300), Some(7 * SLOT48))));
        let got = pts(&f);
        assert!(
            got.windows(2).all(|w| w[0] < w[1]),
            "strictly increasing: {got:?}"
        );
        assert_eq!(
            *got.last().unwrap(),
            pts_to_ns(7 * SLOT48),
            "the frame at the next PTS is exact"
        );
    }

    // A false header in corrupt payload whose frame_len runs past the data is no lock at EOS:
    // scanning continues, so the good frames after it are kept, not cleared with it.
    #[test]
    fn a_false_header_running_past_eos_does_not_swallow_the_frames_after_it() {
        let mut seed = 271;
        let mut bad = bad48(&mut seed, 300);
        let mut fake = adts_frame(0);
        fake[3] = (fake[3] & 0xFC) | 0x01; // frame_length 2048 + 7: past everything buffered
        bad[50..57].copy_from_slice(&fake);
        let (g2, g3) = (frame48(&mut seed, 300), frame48(&mut seed, 300));
        let mut p = AdtsParser::new();
        let mut f = p.parse(&make_pes(frame48(&mut seed, 300), Some(0)));
        f.extend(p.parse(&make_pes([&bad[..], &g2, &g3].concat(), None)));
        f.extend(p.flush());
        assert_eq!(f.len(), 3, "g0, g2 and g3");
        assert_eq!((&f[1].data[..], &f[2].data[..]), (&g2[7..], &g3[7..]));
    }

    // I3 backstop: whatever the source timestamps do, a stream key never emits a PTS at or
    // before its last one; a PES PTS behind the clock cannot rewind it.
    #[test]
    fn a_backwards_pes_pts_never_rewinds_the_output() {
        let mut seed = 251;
        let mut p = AdtsParser::new();
        let mut f = p.parse(&make_pes(
            vbr_stream(&[100, 100], &mut seed),
            Some(5 * SLOT48),
        ));
        f.extend(p.parse(&make_pes(
            vbr_stream(&[100, 100], &mut seed),
            Some(2 * SLOT48),
        )));
        f.extend(p.parse(&make_pes(vbr_stream(&[100], &mut seed), Some(3 * SLOT48))));
        assert_slots(&f, &[5, 6, 7, 8, 9]);
    }

    // ---- Property fuzz: seeded, deterministic; no dependency beyond the xorshift in `noise`.
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next() % n.max(1)
        }
        fn chance(&mut self, per_mille: u64) -> bool {
            self.below(1000) < per_mille
        }
    }

    #[derive(Default)]
    struct FuzzOutcome {
        violations: Vec<String>,
        max_err_slots: f64,
        dense: bool,
    }

    // One generated case: good VBR frames (7 B to 2 KiB) with corrupt runs of 1-4 AUs (invalid
    // header or destroyed sync) each followed by at least two good frames, random PES splits,
    // PTS true to the first AU starting in the PES, random gaps inside good stretches, and EOS.
    fn fuzz_case(seed: u64) -> FuzzOutcome {
        // splitmix64: distinct, well-mixed streams for consecutive case seeds.
        let mix = |mut z: u64| {
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            (z ^ (z >> 31)) | 1
        };
        let mut rng = Rng(mix(seed.wrapping_add(0x9E37_79B9_7F4A_7C15)));
        let mut noise_seed = mix(seed ^ 0xD1B5_4A32_D192_ED03);
        let n = 20 + rng.below(60) as usize;
        let mut good = vec![true; n];
        let mut i = 3;
        while i + 3 < n {
            if rng.chance(150) {
                let len = 1 + rng.below(4) as usize;
                for g in good.iter_mut().skip(i).take(len.min(n - 3 - i)) {
                    *g = false;
                }
                i += len + 2;
            } else {
                i += 1;
            }
        }
        let frames: Vec<Vec<u8>> = (0..n)
            .map(|i| {
                // Good frames carry at least one raw_data_block byte (8 B, the smallest decodable
                // frame), whose value identifies the frame; corrupt ones may be header-only.
                let payload =
                    [rng.below(12), rng.below(400), rng.below(2041)][rng.below(3) as usize];
                let payload = payload.max(u64::from(good[i])) as usize;
                let mut f = frame48(&mut noise_seed, payload);
                if payload > 0 {
                    f[7] = i as u8 & 0x7F;
                }
                if !good[i] {
                    if rng.chance(500) {
                        corrupt(&mut f)
                    } else {
                        f[..2].copy_from_slice(&[0, 0])
                    }
                }
                f
            })
            .collect();
        // Gaps: drop bytes from inside frame j to inside frame j+1, both in a good stretch.
        let mut lost = vec![false; n];
        let mut cut_at = Vec::new(); // (drop start, drop end) in original offsets
        let starts: Vec<usize> = frames
            .iter()
            .scan(0, |o, f| {
                let s = *o;
                *o += f.len();
                Some(s)
            })
            .collect();
        for _ in 0..rng.below(3) {
            let j = 3 + rng.below((n - 7) as u64) as usize;
            if (j - 2..=j + 3).all(|k| good[k] && !lost[k]) && !(j - 1..=j + 2).any(|k| lost[k]) {
                let a = starts[j] + 1 + rng.below(frames[j].len() as u64 - 1) as usize;
                let b = starts[j + 1] + 1 + rng.below(frames[j + 1].len() as u64 - 1) as usize;
                lost[j] = true;
                lost[j + 1] = true;
                cut_at.push((a, b));
            }
        }
        cut_at.sort();
        // Build the delivered byte stream with frame starts (slot) and forced gap boundaries.
        let mut stream = Vec::new();
        let mut frame_at = Vec::new();
        let mut gap_at = Vec::new();
        for (i, f) in frames.iter().enumerate() {
            for (k, &b) in f.iter().enumerate() {
                let off = starts[i] + k;
                if cut_at.iter().any(|&(a, e)| off >= a && off < e) {
                    continue;
                }
                if cut_at.iter().any(|&(_, e)| off == e) {
                    gap_at.push(stream.len());
                }
                if k == 0 {
                    frame_at.push((stream.len(), i as i64));
                }
                stream.push(b);
            }
        }
        let dense = rng.chance(400);
        let pts_rate = if dense {
            1000
        } else {
            [700, 300][rng.below(2) as usize]
        };
        let mut cuts: Vec<usize> = gap_at.clone();
        let mut o = 0;
        loop {
            o += 1 + rng.below(3000) as usize;
            if o >= stream.len() {
                break;
            }
            cuts.push(o);
        }
        cuts.push(stream.len());
        cuts.sort();
        cuts.dedup();
        let mut p = AdtsParser::new();
        let mut out = Vec::new();
        let mut named = Vec::new();
        let mut need_pts = false;
        let mut from = 0;
        for &to in &cuts {
            if to <= from {
                continue;
            }
            let gap = gap_at.contains(&from);
            need_pts |= gap; // after a discontinuity the first AU to start carries a PTS
            let first = frame_at
                .iter()
                .find(|&&(at, _)| at >= from && at < to)
                .map(|&(_, s)| s);
            let pts = first
                .filter(|_| need_pts || from == 0 || rng.chance(pts_rate))
                .map(|s| s * SLOT48);
            need_pts &= first.is_none();
            if let Some(t) = pts {
                named.push(t / SLOT48);
            }
            let pes = PesPacket {
                discontinuity: gap,
                ..make_pes(stream[from..to].to_vec(), pts)
            };
            out.extend(p.parse(&pes));
            from = to;
        }
        out.extend(p.flush());

        let mut r = FuzzOutcome {
            dense,
            ..Default::default()
        };
        let mut v = |m: String| r.violations.push(m);
        let got = pts(&out);
        if !got.windows(2).all(|w| w[0] < w[1]) {
            v(format!("I3: not strictly increasing: {got:?}"));
        }
        // Match every emitted frame to a good frame, in order, once (no junk, no repeats).
        let mut next = 0;
        let mut emitted = vec![false; n];
        let mut max_err: f64 = 0.0;
        for fr in &out {
            match (next..n).find(|&i| good[i] && frames[i][7..] == fr.data[..]) {
                Some(i) => {
                    emitted[i] = true;
                    next = i + 1;
                    let err = (fr.pts_ns - pts_to_ns(i as i64 * SLOT48)).abs() as f64 / D48 as f64;
                    max_err = max_err.max(err);
                    // Bound: exact (within rounding) unless AUs were lost to corruption or a gap
                    // between the timestamps bracketing the frame; then at most that many slots.
                    let before = named
                        .iter()
                        .rev()
                        .find(|&&t| t <= i as i64)
                        .copied()
                        .unwrap_or(0);
                    let after = named
                        .iter()
                        .find(|&&t| t > i as i64)
                        .copied()
                        .unwrap_or(n as i64);
                    let lost_between = (before..after)
                        .filter(|&k| !good[k as usize] || lost[k as usize])
                        .count();
                    if err > lost_between as f64 + 1e-3 {
                        v(format!(
                            "PTS error {err:.2} slots at frame {i} > {lost_between} lost AUs"
                        ));
                    }
                }
                None => v(format!("junk or repeated frame at {}", fr.pts_ns)),
            }
        }
        r.max_err_slots = max_err;
        for i in 0..n {
            let safe =
                good[i] && !lost[i] && (i == 0 || good[i - 1]) && (i + 1 == n || good[i + 1]);
            if safe && !emitted[i] {
                v(format!("safe good frame {i} not emitted"));
            }
        }
        let runs = (0..n)
            .filter(|&i| !good[i] && (i == 0 || good[i - 1]))
            .count() as u64;
        if p.frames.verified_dropped() != runs {
            v(format!(
                "I1: verified {} != runs {runs}",
                p.frames.verified_dropped()
            ));
        }
        let bad_bytes: usize = (0..n).filter(|&i| !good[i]).map(|i| frames[i].len()).sum();
        let cap = bad_bytes as u64 / 7 + runs;
        let skip: i64 = got
            .windows(2)
            .map(|w| ((w[1] - w[0] + D48 / 2) / D48 - 1).max(0))
            .sum();
        if p.dropped_frames() > cap {
            v(format!(
                "I2: dropped {} > byte cap {cap}",
                p.dropped_frames()
            ));
        }
        if p.dropped_frames() as i64 > skip + runs as i64 {
            v(format!(
                "I2/I4: dropped {} > clock skip {skip} + faults {runs}",
                p.dropped_frames()
            ));
        }
        r
    }

    fn fuzz_cases() -> u64 {
        std::env::var("AUDIO_FUZZ_CASES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(300)
    }

    // Property fuzz over seeded cases (AUDIO_FUZZ_CASES, default 300 for CI; 100k run locally):
    // I1, I2, I3, no junk or repeat, every good frame between good neighbours emitted, and PTS
    // error at most the AUs lost between the frame's bracketing timestamps (I5).
    #[test]
    fn resync_properties_hold_on_generated_streams() {
        let (mut worst_dense, mut worst_sparse) = (0f64, 0f64);
        for case in 0..fuzz_cases() {
            let r = fuzz_case(0x5EED_0000 + case);
            assert!(r.violations.is_empty(), "case {case}: {:?}", r.violations);
            if r.dense {
                worst_dense = worst_dense.max(r.max_err_slots)
            } else {
                worst_sparse = worst_sparse.max(r.max_err_slots)
            }
        }
        eprintln!("max PTS error: dense {worst_dense:.3} slots, sparse {worst_sparse:.3} slots");
    }

    // Per project principle ("rip bad discs"; do not change without a user decision): a track
    // 90% lost in bursts is kept, never poisoned, and every lost AU is counted. One verified
    // fault per burst (I1) cannot outnumber the good frames between bursts.
    #[test]
    fn a_ninety_percent_lost_track_keeps_its_good_frames() {
        let mut seed = 223;
        let mut p = AdtsParser::new();
        let mut f = Vec::new();
        for g in 0..30 {
            let group: Vec<u8> = [frame48(&mut seed, 100)]
                .into_iter()
                .chain((0..9).map(|_| bad48(&mut seed, 100)))
                .flatten()
                .collect();
            f.extend(p.parse(&make_pes(group, Some(g * 10 * SLOT48))));
        }
        f.extend(p.flush());
        assert_eq!(f.len(), 30, "every genuine frame is emitted");
        assert_eq!(p.dropped_frames(), 270, "every lost AU is counted");
        assert_eq!(p.frames.verified_dropped(), 30, "one fault per burst");
    }

    // [dropgate] Once poisoned, "the caller should drop every remaining AU": frames after the
    // poisoning drop in the same PES are not emitted, and each framed AU is counted. With no
    // frame measured, each PES timestamp ends a run, so 200 corrupt PES are 200 faults (I1).
    #[test]
    fn nothing_is_emitted_once_poisoned() {
        let mut seed = 151;
        let mut p = AdtsParser::new();
        for i in 0..200 {
            p.parse(&make_pes(bad48(&mut seed, 100), Some(i * SLOT48)));
        }
        assert_eq!(p.frames.verified_dropped(), 200);
        let last = [
            bad48(&mut seed, 100),
            frame48(&mut seed, 100),
            frame48(&mut seed, 100),
        ];
        assert!(
            p.parse(&make_pes(last.concat(), Some(200 * SLOT48)))
                .is_empty()
        );
        let more: Vec<u8> = (0..3).flat_map(|_| frame48(&mut seed, 100)).collect();
        assert!(p.parse(&make_pes(more, Some(203 * SLOT48))).is_empty());
        let before = p.dropped_frames();
        assert_eq!(
            before, 206,
            "200 faults, then 1 corrupt + 2 + 3 good AUs as collateral"
        );
        let more: Vec<u8> = (0..3).flat_map(|_| frame48(&mut seed, 100)).collect();
        assert!(p.parse(&make_pes(more, Some(206 * SLOT48))).is_empty());
        assert_eq!(
            p.dropped_frames(),
            before + 3,
            "one per framed AU, not one per PES"
        );
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
        let mut frames = p.parse(&fresh);
        frames.extend(p.flush()); // after a gap the first frame chains (the PES may start in a fragment): EOS confirms it
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].pts_ns, 2_000_000_000);
        assert_eq!(frames[0].source, fresh.source);
        assert!(frames[0].discontinuity);
    }
}
