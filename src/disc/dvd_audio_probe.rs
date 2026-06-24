//! Physical AC-3 sub-stream probing for DVD audio routing.
//!
//! ## Why this exists (Silence-of-the-Lambs wrong-substream bug)
//!
//! A DVD VTS IFO declares its audio streams in a fixed table, and freemkv's
//! scan assigns each declared stream a `private_stream_1` sub-stream id purely
//! by per-codec ordinal — the first AC-3 stream becomes `0x80`, the second
//! `0x81`, and so on (`ifo::assign_audio_sub_stream_ids`). That assumes the
//! physical sub-stream order on the wire matches the IFO declaration order.
//!
//! On some discs it does NOT. The R2 PAL "The Silence of the Lambs" feature
//! declares ONE AC-3 audio stream the IFO nibble marks as 5.1 (6 channels), but
//! the physical VOB carries the 5.1 main mix and a 2.0 down-mix on DIFFERENT
//! `0x8x` sub-stream ids, and the 2.0 is the one that happens to land at the
//! ordinal `0x80` slot. Routing the declared 5.1 stream to `0x80` by ordinal
//! therefore muxes the 2.0 down-mix while labelling it 5.1 — the wrong physical
//! track.
//!
//! The robust fix is data-driven and codec/disc agnostic: read each physical
//! AC-3 sub-stream's REAL channel count from the VOB (the `acmod`/`lfeon` of its
//! first frame after the `0x0B77` sync) and route each IFO-declared AC-3 stream
//! to the physical sub-stream whose actual channel count matches the IFO's
//! declared count — instead of trusting the ordinal. This never re-reads the
//! disc beyond a bounded head-of-feature probe and degrades to the original
//! ordinal mapping when the probe yields nothing (unreadable/short VOB).

use crate::disc::Stream;
use crate::mux::codec::ac3;
use crate::mux::ps::PsDemuxer;
use crate::sector::SectorSource;
use std::collections::BTreeMap;

/// How many 2048-byte sectors of the first feature extent to probe. The head of
/// a DVD feature opens with logos/warnings whose audio is frequently a thin 2.0
/// bed on the FIRST sub-stream only — the other physical `0x8x` sub-streams and
/// the main 5.1 mix do not appear until a sector or two further in. 512 sectors
/// (1 MiB) was too short: on Greenland it saw ONLY `0x80`, and only its opening
/// 2.0 frames. 1024 sectors (2 MiB) reliably contains at least one frame of
/// every physical AC-3 sub-stream AND enough of `0x80` to reach its 5.1 frames.
/// Still bounded so a live drive is never hammered (see the project "don't
/// hammer the live drive" rule).
const PROBE_SECTORS: u16 = 1024;

/// Decode the real per-sub-stream AC-3 channel count from a buffer of decrypted
/// MPEG-PS (DVD VOB) bytes.
///
/// Demuxes `private_stream_1` (0xBD), and for each AC-3 sub-stream id
/// (`0x80..=0x87`) records the MAXIMUM channel count seen across EVERY decodable
/// frame in the probe window (`acmod` + `lfeon` at each `0x0B77` sync). Pure and
/// unit-testable — takes the already-read bytes, never touches the disc.
///
/// ## Why the maximum, not the first frame
///
/// The first frame of a sub-stream at the head of a feature is NOT
/// representative. A DVD opens with logos/warnings, and the main `0x80`
/// sub-stream there frequently carries a thin 2.0 bed before transitioning to
/// its real 5.1 main mix a fraction of a second later (observed on Greenland:
/// `0x80`'s first frames are acmod=2 → 2 channels, then it becomes acmod=7+lfe →
/// 6 channels within the same 2 MiB window). Recording only the FIRST frame read
/// `0x80=2` and missed the 5.1 entirely, defeating the channel-match routing.
/// The 5.1 capability of a sub-stream is the *maximum* channel count any of its
/// frames carries, so we scan them all and keep the max.
///
/// Returns a map `sub_id -> max channels`. Sub-streams whose frames are all too
/// short to carry the BSI bits, or that never appear in the buffer, are absent
/// from the map.
pub fn probe_ac3_substream_channels(ps_bytes: &[u8]) -> BTreeMap<u8, u8> {
    let mut found: BTreeMap<u8, u8> = BTreeMap::new();
    let mut demux = PsDemuxer::new();
    let mut packets = demux.feed(ps_bytes);
    packets.extend(demux.flush());
    for p in packets {
        // Only private_stream_1 AC-3 sub-streams (0x80..=0x87).
        let Some(sub) = p.sub_stream_id else { continue };
        if !(0x80..=0x87).contains(&sub) {
            continue;
        }
        // The PS demux strips the 4-byte AC-3 sub-header but does not align to a
        // frame. Walk EVERY 0x0B77 sync in this sub-stream's payload, decode
        // each frame's channel count, and keep the largest — the sub-stream's
        // real (main-mix) channel capability. See the doc comment above for why
        // the first frame alone is unreliable.
        if let Some(ch) = max_substream_channels(&p.data) {
            let slot = found.entry(sub).or_insert(0);
            *slot = (*slot).max(ch);
        }
    }
    found
}

/// Largest AC-3 channel count over every decodable frame in a single
/// sub-stream's payload. Returns `None` when no frame carries enough BSI bits.
///
/// Each frame is advanced by its real `ac3_frame_size` so a frame's compressed
/// body (which can contain stray `0x0B77` byte pairs) cannot be mistaken for a
/// new frame; only when a size is unmappable do we fall back to a +2 byte
/// rescan to re-lock the next genuine sync.
fn max_substream_channels(data: &[u8]) -> Option<u8> {
    let mut best: Option<u8> = None;
    let mut pos = 0;
    while pos < data.len() {
        let Some(rel) = ac3::find_ac3_sync(&data[pos..]) else {
            break;
        };
        let start = pos + rel;
        let frame = &data[start..];
        if let Some(ch) = ac3::acmod_channels(frame) {
            if ch > 0 {
                best = Some(best.map_or(ch, |b| b.max(ch)));
            }
        }
        // Advance past this frame by its declared size when that is mappable;
        // otherwise step 2 bytes past the sync and re-scan for the next one.
        let size = ac3::ac3_frame_size(frame);
        pos = if (6..=8192).contains(&size) {
            start + size
        } else {
            start + 2
        };
    }
    best
}

/// Re-route the title's declared AC-3 audio streams onto the physical
/// sub-stream ids whose REAL channel counts match, using a probed
/// `sub_id -> channels` map.
///
/// For each declared AC-3 audio stream (in IFO order), it picks the physical
/// `0x8x` sub-stream whose probed channel count equals the stream's declared
/// channel count, never re-using a sub-stream already claimed by an earlier
/// stream. The chosen sub-stream's PID (`0xBD00 | sub_id`) is written back onto
/// the `Stream::Audio` so BOTH mux demux paths (`DiscStream` and the file-backed
/// highway) route by it.
///
/// Conservative — it only ever REASSIGNS among the physical sub-streams the
/// probe actually saw, and only when a better (exact-channel) match exists than
/// the stream's current assignment. A stream whose current sub-stream already
/// matches is left alone; a stream with no matching physical sub-stream keeps
/// its ordinal assignment. So a normal disc (physical order == IFO order) is a
/// no-op.
///
/// Returns the number of streams whose PID was changed (for diagnostics).
pub fn remap_audio_pids(streams: &mut [Stream], probed: &BTreeMap<u8, u8>) -> usize {
    if probed.is_empty() {
        return 0;
    }
    // Sub-streams already claimed by a remapped (or matching) earlier stream,
    // so two declared streams never collide on one physical sub-stream.
    let mut claimed: Vec<u8> = Vec::new();
    let mut changed = 0usize;

    for s in streams.iter_mut() {
        let Stream::Audio(a) = s else { continue };
        if a.codec != crate::disc::Codec::Ac3 {
            continue;
        }
        let declared = a.channels.count();
        // The sub-id this stream currently routes by (low byte of its PID).
        let current_sub = (a.pid & 0x00FF) as u8;

        // If the stream's current physical sub-stream already matches its
        // declared channel count, keep it and claim it.
        if probed.get(&current_sub) == Some(&declared) {
            claimed.push(current_sub);
            continue;
        }

        // Otherwise find an unclaimed physical sub-stream whose REAL channel
        // count equals the declared count.
        let pick = probed
            .iter()
            .find(|(sub, ch)| **ch == declared && !claimed.contains(*sub))
            .map(|(sub, _)| *sub);

        if let Some(sub) = pick {
            let new_pid = 0xBD00 | sub as u16;
            if new_pid != a.pid {
                tracing::debug!(
                    target: "freemkv::scan",
                    old_pid = a.pid,
                    new_pid,
                    declared_channels = declared,
                    "dvd: re-routed AC-3 audio to physical sub-stream matching channel count"
                );
                a.pid = new_pid;
                changed += 1;
            }
            claimed.push(sub);
        } else {
            // No physical match — leave the ordinal assignment, but claim its
            // current sub so later streams don't steal a slot it may still use.
            claimed.push(current_sub);
        }
    }
    changed
}

/// Probe the first feature extent of a DVD title through a (decrypted) sector
/// source and re-route its AC-3 audio PIDs to the physically-correct
/// sub-streams. A bounded, best-effort scan: any read error or empty probe
/// leaves the ordinal assignment untouched.
///
/// `reader` MUST yield PLAINTEXT VOB bytes (i.e. a `DecryptingSectorSource` on a
/// CSS disc) — probing scrambled sectors yields no AC-3 syncs and is a safe
/// no-op. Returns the number of audio streams whose PID changed.
pub fn probe_and_remap<S: SectorSource + ?Sized>(
    reader: &mut S,
    title: &mut crate::disc::DiscTitle,
) {
    // Only DVD (MPEG-PS) titles carry private_stream_1 AC-3 sub-streams.
    if title.content_format != crate::disc::ContentFormat::MpegPs {
        return;
    }
    // Nothing to disambiguate unless there is at least one AC-3 audio stream.
    let has_ac3 = title
        .streams
        .iter()
        .any(|s| matches!(s, Stream::Audio(a) if a.codec == crate::disc::Codec::Ac3));
    if !has_ac3 {
        return;
    }
    let Some(ext) = title.extents.first() else {
        return;
    };
    let count: u16 = ext.sector_count.min(PROBE_SECTORS as u32) as u16;
    if count == 0 {
        return;
    }
    let mut buf = vec![0u8; count as usize * 2048];
    // `recovery=false`: a single best-effort attempt — the probe must never
    // stall the mux or hammer a marginal drive. On any error, bail to ordinal.
    let n = match reader.read_sectors(ext.start_lba, count, &mut buf, false) {
        Ok(n) => n,
        Err(_) => return,
    };
    buf.truncate(n);
    let probed = probe_ac3_substream_channels(&buf);
    crate::diag::dump_dvd_substream_probe(title.playlist_id, &probed);
    remap_audio_pids(&mut title.streams, &probed);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disc::{AudioChannels, AudioStream, Codec, LabelPurpose, SampleRate};

    /// Build a single, correctly-SIZED AC-3 frame whose `acmod`/`lfeon` encode a
    /// known channel count. `byte4` is `fscod=0 | frmsizecod=0`, so
    /// `ac3_frame_size` reports 128 bytes and the frame is zero-padded to exactly
    /// that — this lets `max_substream_channels` advance frame-by-frame over a
    /// multi-frame payload exactly as it does on real VOB data. The BSI bits are
    /// laid down with a writer so the test never hand-miscomputes the lfeon
    /// offset, matching `acmod_channels`' reader.
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

    /// Build a minimal `private_stream_1` PES carrying `frames` for `sub_id`,
    /// each preceded only by the 4-byte AC-3 sub-header at the PES head. Mirrors
    /// the on-disc layout the PS demux expects: PES start `0x000001BD`, length,
    /// PES header (no PTS), sub-header `[sub_id, frame_count, ptr_hi, ptr_lo]`,
    /// then the concatenated AC-3 frames.
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

    /// GREENLAND regression — the probe must read each sub-stream's TRUE
    /// (max-mix) channel count, not be poisoned by an unrepresentative head
    /// frame, and must NOT cross-contaminate between sub-streams.
    ///
    /// Mirrors the real on-disc layout that caused the mis-read: the feature
    /// head carries `0x80` opening with a 2.0 frame and THEN a 5.1 frame (its
    /// real main mix), interleaved with `0x81` carrying only 2.0. The old
    /// first-frame probe read `0x80=2` (the logo bed) and missed the 5.1; the
    /// max-over-frames probe must report `0x80=6` and `0x81=2`.
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

    /// SILENCE-OF-THE-LAMBS regression: the IFO declares ONE 5.1 AC-3 stream and
    /// the ordinal mapping put it at 0x80, but physically 0x80 is the 2.0
    /// down-mix and the 5.1 lives at 0x81. After probe+remap the declared 5.1
    /// stream must route to 0x81 (PID 0xBD81), NOT the ordinal 0x80.
    #[test]
    fn remap_routes_declared_51_to_physical_51_substream() {
        // Physical layout: 0x80 = 2.0, 0x81 = 5.1 (reversed vs ordinal).
        let mut probed = BTreeMap::new();
        probed.insert(0x80u8, 2u8);
        probed.insert(0x81u8, 6u8);

        // Declared: one 5.1 stream, ordinally assigned 0x80 (PID 0xBD80).
        let mut streams = vec![ac3_stream(0xBD80, AudioChannels::Surround51)];
        let changed = remap_audio_pids(&mut streams, &probed);
        assert_eq!(changed, 1, "the one 5.1 stream must be re-routed");
        let Stream::Audio(a) = &streams[0] else {
            panic!("audio")
        };
        assert_eq!(
            a.pid, 0xBD81,
            "declared 5.1 must route to physical 0x81 (the real 5.1), not ordinal 0x80"
        );
    }

    /// Conservative no-op: when the physical order already matches the IFO
    /// order (0x80 = 5.1 as declared), remap changes nothing.
    #[test]
    fn remap_noop_when_physical_matches_ordinal() {
        let mut probed = BTreeMap::new();
        probed.insert(0x80u8, 6u8); // 0x80 really is the 5.1
        let mut streams = vec![ac3_stream(0xBD80, AudioChannels::Surround51)];
        let changed = remap_audio_pids(&mut streams, &probed);
        assert_eq!(changed, 0, "matching physical order is a no-op");
        let Stream::Audio(a) = &streams[0] else {
            panic!()
        };
        assert_eq!(a.pid, 0xBD80);
    }

    /// Two declared streams (5.1 + 2.0) where the physical order is reversed:
    /// 0x80=2.0, 0x81=5.1. The 5.1 declaration must claim 0x81 and the 2.0
    /// declaration must claim 0x80 — no collision, both correct.
    #[test]
    fn remap_two_streams_no_collision() {
        let mut probed = BTreeMap::new();
        probed.insert(0x80u8, 2u8);
        probed.insert(0x81u8, 6u8);
        // Declared order: 5.1 first (ordinal 0x80), 2.0 second (ordinal 0x81).
        let mut streams = vec![
            ac3_stream(0xBD80, AudioChannels::Surround51),
            ac3_stream(0xBD81, AudioChannels::Stereo),
        ];
        remap_audio_pids(&mut streams, &probed);
        let pids: Vec<u16> = streams
            .iter()
            .filter_map(|s| match s {
                Stream::Audio(a) => Some(a.pid),
                _ => None,
            })
            .collect();
        assert_eq!(
            pids,
            vec![0xBD81, 0xBD80],
            "5.1→0x81, 2.0→0x80, no collision"
        );
    }

    /// Empty probe (unreadable / scrambled VOB) is a no-op — the ordinal
    /// assignment survives so behaviour never regresses below today's.
    #[test]
    fn remap_empty_probe_is_noop() {
        let probed = BTreeMap::new();
        let mut streams = vec![ac3_stream(0xBD80, AudioChannels::Surround51)];
        let changed = remap_audio_pids(&mut streams, &probed);
        assert_eq!(changed, 0);
        let Stream::Audio(a) = &streams[0] else {
            panic!()
        };
        assert_eq!(a.pid, 0xBD80, "no probe data → keep ordinal");
    }
}
