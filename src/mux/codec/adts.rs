//! AAC ADTS framing and validation.
//!
//! Spec text is quoted from ISO/IEC 13818-7:2004 (MPEG-2 AAC, ADTS in §6.2 and §8.1) and
//! ISO/IEC 11172-3 (the public CD text of §2.4.2.3, which 13818-7 refers to). MPEG-4 ADTS
//! (ID '0', 7350 Hz at index 0xc) follows ISO/IEC 14496-3, which is not quoted here.

use super::audio_frames::{AudioFrames, Header, SyncSpec};
#[cfg(test)]
use super::pts_to_ns;
use super::{CodecParser, Frame, PesPacket};

/// [13818-7 §8.1.1.2 Table 35] `sampling_frequency_index` 0x0-0xb in Hz, "0xc reserved" to
/// "0xf reserved"; 0xc is 7350 Hz for MPEG-4 (14496-3). Zero marks a reserved index.
const ADTS_SAMPLE_RATE_VALID: [u32; 16] = [
    96000, 88200, 64000, 48000, 44100, 32000, 24000, 22050, 16000, 12000, 11025, 8000, 7350, 0, 0,
    0,
];

/// Length of the fixed ADTS header without CRC.
const ADTS_HEADER_BYTES: usize = 7;

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
    if data.len() < ADTS_HEADER_BYTES {
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

/// The ADTS header (MPEG-4 ID, no CRC, one raw_data_block, frame_length 0) for the access
/// units an AudioSpecificConfig describes (14496-3 §1.6.2.1); `None` when ADTS cannot signal
/// it: no 2-bit profile, an explicit rate, an in-band PCE layout or 960-sample frames
/// (GASpecificConfig frameLengthFlag). SBR/PS signal the core.
pub(crate) fn adts_template(asc: &[u8]) -> Option<[u8; ADTS_HEADER_BYTES]> {
    let (b0, b1) = (*asc.first()?, *asc.get(1)?);
    let rate = ((b0 & 7) << 1) | (b1 >> 7);
    let channels = (b1 >> 3) & 0x0F;
    let (object, frame_960) = match b0 >> 3 {
        // extensionSamplingFrequencyIndex (4 bits), then the core audioObjectType.
        5 | 29 => {
            let b2 = *asc.get(2)?;
            let ext = ((b1 & 7) << 1) | (b2 >> 7);
            (ext != 0x0F).then_some(((b2 >> 2) & 0x1F, b2 & 0x02 != 0))?
        }
        o => (o, b1 & 0x04 != 0),
    };
    if frame_960 || !(1..=4).contains(&object) || rate > 0x0B || !(1..=7).contains(&channels) {
        return None;
    }
    Some([
        0xFF,
        0xF1,
        ((object - 1) << 6) | (rate << 2) | (channels >> 2),
        (channels & 3) << 6,
        0,
        0x1F,
        0xFC,
    ])
}

/// raw_data_block()s in an access unit lasting `duration_ns` at `template`'s rate: an
/// ADTS source frame keeps all of its 1..=4 blocks. 1 unless the duration is within a
/// sample of a whole number of 1024-sample blocks.
pub(crate) fn raw_data_blocks(template: [u8; ADTS_HEADER_BYTES], duration_ns: Option<u64>) -> u8 {
    let rate = u64::from(ADTS_SAMPLE_RATE_VALID[usize::from((template[2] >> 2) & 15)]);
    let Some(scaled) = duration_ns.and_then(|d| d.checked_mul(rate)) else {
        return 1;
    };
    let n = scaled.saturating_add(512_000_000_000) / 1_024_000_000_000;
    if (1..=4).contains(&n) && scaled.abs_diff(n * 1_024_000_000_000) <= 1_000_000_000 {
        n as u8
    } else {
        1
    }
}

/// `raw` (`blocks` raw_data_block()s) as one ADTS frame under `template`; `None` past the
/// 13-bit frame_length.
pub(crate) fn adts_frame(
    template: [u8; ADTS_HEADER_BYTES],
    raw: &[u8],
    blocks: u8,
) -> Option<Vec<u8>> {
    let len = ADTS_HEADER_BYTES + raw.len();
    if len >= 1 << 13 || !(1..=4).contains(&blocks) {
        return None;
    }
    let mut h = template;
    h[6] |= blocks - 1;
    h[3] |= (len >> 11) as u8;
    h[4] = (len >> 3) as u8;
    h[5] |= ((len & 7) << 5) as u8;
    Some([&h[..], raw].concat())
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
                SyncSpec {
                    mask: 0xf0,
                    frame_len: adts_frame_len,
                    fixed: adts_fixed_key,
                    frame_ns: adts_frame_ns,
                    // [13818-7 §8.1.1.2] frame_length "including headers": at least 7 bytes.
                    min_frame: ADTS_HEADER_BYTES,
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
    let skip = if data[1] & 1 == 0 { 9 } else { 7 };
    let blocks = data[6] & 3;
    // A Matroska AAC block is one raw_data_block: with CRC, multi-block frames keep per-block
    // CRC words in the payload, so refuse them.
    if blocks > 0 && skip == 9 {
        return None;
    }
    let bytes = adts_frame_len(data)?;
    // The ASC replaces the ADTS header (payload carries no header/CRC).
    // CodecPrivate is fixed per track: a later config change keeps the
    // first ASC and is counted.
    let base = [
        (object_type << 3) | (rate_index >> 1),
        (rate_index << 7) | (channels << 3),
    ];
    // channel_configuration 0: the layout is an in-band PCE, which the ASC must carry.
    let pce = if channels == 0 && data.len() >= bytes {
        adts_pce(&data[skip..bytes])
    } else {
        None
    };
    let full = |pce: &Option<Vec<u8>>| [&base[..], pce.as_deref().unwrap_or(&[])].concat();
    match config {
        None => *config = Some(full(&pce)),
        Some(first)
            if first[..2] != base
                || (first.len() > 2 && pce.as_ref().is_some_and(|p| first[2..] != p[..])) =>
        {
            if *changes == 0 {
                tracing::warn!(target: "mux", pid, "AAC config changed mid-stream; keeping the first");
            }
            *changes += 1;
        }
        // Seeded before the PCE was in view: upgrade to the PCE-bearing form.
        Some(first) if first.len() == 2 && pce.is_some() => *first = full(&pce),
        Some(_) => {}
    }
    // [§8.1.1.2] "Number of raw_data_block()'s ... is equal to number_of_raw_data_blocks_in_frame
    // + 1", and [§8.2.1.1] each holds "audio data for a time period of 1024 samples".
    Some(Header {
        bytes,
        skip,
        samples: 1024 * (u32::from(blocks) + 1),
        rate: ADTS_SAMPLE_RATE_VALID[usize::from(rate_index)],
    })
}

// Bit copier from a raw_data_block into an ASC-side buffer.
struct PceCopy<'a> {
    block: &'a [u8],
    src: usize,
    out: Vec<u8>,
    nbits: usize,
}

impl PceCopy<'_> {
    fn copy(&mut self, n: usize) -> Option<usize> {
        let mut v = 0;
        for _ in 0..n {
            let byte = *self.block.get(self.src / 8)?;
            let bit = usize::from(byte >> (7 - self.src % 8) & 1);
            self.src += 1;
            if self.nbits & 7 == 0 {
                self.out.push(0);
            }
            *self.out.last_mut()? |= (bit as u8) << (7 - self.nbits % 8);
            self.nbits += 1;
            v = v << 1 | bit;
        }
        Some(v)
    }
}

// The program_config_element() opening a raw_data_block (ID_PCE, [14496-3 §4.4.1.1]), re-packed
// for the ASC: alignment is relative to the ASC start there, to the block start here.
fn adts_pce(block: &[u8]) -> Option<Vec<u8>> {
    let mut c = PceCopy {
        block,
        src: 0,
        out: Vec::new(),
        nbits: 0,
    };
    if c.copy(3)? != 5 {
        return None;
    }
    c.out.clear();
    c.nbits = 0;
    c.copy(10)?;
    let five = c.copy(4)? + c.copy(4)? + c.copy(4)?;
    let four = c.copy(2)? + c.copy(3)?;
    let cc = c.copy(4)?;
    for extra in [4, 4, 3] {
        if c.copy(1)? == 1 {
            c.copy(extra)?;
        }
    }
    c.copy(5 * (five + cc) + 4 * four)?;
    c.src += (8 - c.src % 8) % 8;
    c.nbits += (8 - c.nbits % 8) % 8;
    let comment = c.copy(8)?;
    c.copy(comment * 8)?;
    Some(c.out)
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
                .drain_before_gap(ADTS_HEADER_BYTES, |d| adts_header(d, config, changes, pid));
        }
        out.extend(self.frames.parse(pes, ADTS_HEADER_BYTES, |d| {
            adts_header(d, config, changes, pid)
        }));
        self.pid = pid;
        out
    }

    fn flush(&mut self) -> Vec<Frame> {
        let (config, changes, pid) = (&mut self.config, &mut self.config_changes, self.pid);
        self.frames
            .flush_with(ADTS_HEADER_BYTES, |d| adts_header(d, config, changes, pid))
    }
    fn codec_private(&self) -> Option<Vec<u8>> {
        self.config.clone()
    }
    fn config_changes(&self) -> u64 {
        self.config_changes
    }
}

#[cfg(test)]
#[path = "adts_tests.rs"]
mod tests;
