//! Physical AC-3 sub-stream probing for DVD audio routing.
//!
//! On some discs the physical `private_stream_1` sub-stream order does not
//! match the IFO's declared audio stream order, so ordinal assignment
//! (`ifo::assign_audio_sub_stream_ids`) can mux the wrong physical track
//! (e.g. a 2.0 down-mix labelled 5.1). This module probes each physical
//! AC-3 sub-stream's REAL channel count from the VOB and re-routes each
//! declared stream to the matching sub-stream, falling back to ordinal
//! mapping when the probe yields nothing. See docs/dvd-audio-probe.md.

use crate::disc::Stream;
use crate::mux::codec::ac3;
use crate::mux::ps::PsDemuxer;
use crate::sector::SectorSource;
use std::collections::BTreeMap;

// Sectors of the first feature extent to probe. 512 (1 MiB) was too short on
// a real disc; 1024 (2 MiB) reliably reaches every physical AC-3 sub-stream.
// See docs/dvd-audio-probe.md#probe_sectors-sizing.
const PROBE_SECTORS: u16 = 1024;

/// Decode the real per-sub-stream AC-3 channel count from a buffer of decrypted
/// MPEG-PS (DVD VOB) bytes. Demuxes `private_stream_1` (0xBD), and for each
/// AC-3 sub-stream id (`0x80..=0x87`) records the MAXIMUM channel count seen
/// across EVERY decodable frame — the max, not the first frame, because a
/// sub-stream's opening frames are often an unrepresentative logo/warning bed
/// (see docs/dvd-audio-probe.md). Pure — takes the already-read bytes, never
/// touches the disc. Returns a map `sub_id -> max channels`; absent for
/// sub-streams that never appear or carry no decodable BSI bits.
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
        // The PS demux strips the AC-3 sub-header but doesn't align to a frame, so
        // walk every 0x0B77 sync in the payload and keep the largest decoded
        // channel count — the sub-stream's real main-mix capability (see doc above).
        if let Some(ch) = max_substream_channels(&p.data) {
            let slot = found.entry(sub).or_insert(0);
            *slot = (*slot).max(ch);
        }
    }
    found
}

// Largest AC-3 channel count over every decodable frame in a sub-stream's
// payload; None when no frame carries enough BSI bits. Advances by the real
// `ac3_frame_size`; falls back to a +2 byte rescan when unmappable.
fn max_substream_channels(data: &[u8]) -> Option<u8> {
    let mut best: Option<u8> = None;
    let mut pos = 0;
    while pos < data.len() {
        let Some(rel) = ac3::find_ac3_sync(&data[pos..]) else {
            break;
        };
        let start = pos + rel;
        let frame = &data[start..];
        if let Some(ch) = ac3::acmod_channels(frame)
            && ch > 0
        {
            best = Some(best.map_or(ch, |b| b.max(ch)));
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
/// `sub_id -> channels` map. For each declared AC-3 audio stream (in IFO
/// order), picks the physical `0x8x` sub-stream whose probed channel count
/// equals the declared count, never reusing a claimed sub-stream, and writes
/// its PID (`0xBD00 | sub_id`) back onto the `Stream::Audio`. Conservative:
/// only reassigns when a better match exists (see docs/dvd-audio-probe.md).
/// Returns the number of streams whose PID was changed.
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
/// no-op. Remaps `title`'s AC-3 streams in place; returns nothing.
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
    use crate::disc::{
        AudioChannels, AudioStream, Codec, ContentFormat, DiscTitle, Extent, LabelPurpose,
        SampleRate,
    };
    use crate::sector::SectorSource;

    // Builds a correctly-SIZED AC-3 frame (128 bytes, matching frmsizecod=0)
    // whose `acmod`/`lfeon` encode a known channel count, via a bit writer so
    // the test never hand-miscomputes the lfeon offset. See docs/dvd-audio-probe.md.
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

    // Builds a minimal `private_stream_1` PES carrying `frames` for `sub_id`,
    // mirroring the on-disc layout the PS demux expects. See
    // docs/dvd-audio-probe.md#test-helper-notes.
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

    // Real-disc regression: the probe must read each sub-stream's TRUE
    // (max-mix) channel count without cross-contaminating between sub-streams.
    // See docs/dvd-audio-probe.md#probe_reads_max_channels_no_cross_contamination.
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

    // Real-disc regression: IFO declares ONE 5.1 AC-3 stream ordinally mapped
    // to 0x80, but physically 0x80 is the 2.0 down-mix and 5.1 lives at 0x81.
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

    // Mutation guard for `pos + rel` (not `pos - rel`, which could underflow
    // `usize`) as the sync's true absolute position. See
    // docs/dvd-audio-probe.md#max_substream_channels_locates_sync_after_leading_non_sync_bytes.
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

    // On an unmappable AC-3 size, must fall back to a forward `start + 2`
    // rescan (not loop or overshoot). See
    // docs/dvd-audio-probe.md#unmappable-size-fallback-tests.
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

    // Same fallback, sync at offset 0 (`start - 2` would underflow). See
    // docs/dvd-audio-probe.md#unmappable-size-fallback-tests.
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

    // Mutation guard: current sub-stream must be read via `pid & 0x00FF`, not
    // `|`/`^`. See docs/dvd-audio-probe.md#remap_reads_current_substream_via_and_not_or_or_xor.
    #[test]
    fn remap_reads_current_substream_via_and_not_or_or_xor() {
        let mut probed = BTreeMap::new();
        probed.insert(0x80u8, 6u8);
        probed.insert(0x81u8, 6u8); // ambiguous: two physical 6ch sub-streams
        let mut streams = vec![ac3_stream(0xBD81, AudioChannels::Surround51)];
        let changed = remap_audio_pids(&mut streams, &probed);
        assert_eq!(
            changed, 0,
            "already sitting on a matching physical sub-stream (0x81) must be left alone"
        );
        let Stream::Audio(a) = &streams[0] else {
            panic!()
        };
        assert_eq!(
            a.pid, 0xBD81,
            "must not be bumped to the other matching sub-stream (0x80)"
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

    // End-to-end `probe_and_remap`: real-disc-shaped MpegPs title, 0x80=2.0
    // down-mix / 0x81=real 5.1, must re-route to 0xBD81. See
    // docs/dvd-audio-probe.md#probe_and_remap_reroutes_swapped_substream_scenario_end_to_end.
    #[test]
    fn probe_and_remap_reroutes_swapped_substream_scenario_end_to_end() {
        let mut bytes = ps_ac3(0x80, 2, false); // physical 0x80 = 2.0 down-mix
        bytes.extend(ps_ac3(0x81, 7, true)); // physical 0x81 = 5.1 main mix
        let mut title = DiscTitle {
            playlist: "00001.ifo".into(),
            playlist_id: 1,
            duration_secs: 60.0,
            size_bytes: bytes.len() as u64,
            clips: Vec::new(),
            streams: vec![ac3_stream(0xBD80, AudioChannels::Surround51)],
            chapters: Vec::new(),
            extents: vec![Extent {
                start_lba: 0,
                sector_count: 2,
            }],
            content_format: ContentFormat::MpegPs,
            codec_privates: vec![None],
        };
        let mut source = FixedSource { data: bytes };
        probe_and_remap(&mut source, &mut title);
        let Stream::Audio(a) = &title.streams[0] else {
            panic!("audio")
        };
        assert_eq!(
            a.pid, 0xBD81,
            "declared 5.1 stream must be re-routed to the physical 5.1 sub-stream 0x81"
        );
    }
}
