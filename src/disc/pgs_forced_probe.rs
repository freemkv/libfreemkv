//! Content-based forced-subtitle detection for Blu-ray/UHD PGS tracks.
//!
//! `freemkv info` and the muxer must agree on which subtitle tracks are forced.
//! The muxer derives it from the PGS `forced_on_flag` while muxing a rip; this
//! module gives `info` the SAME verdict up front by reading the title's PGS
//! streams and feeding them through the one shared classifier
//! ([`crate::mux::codec::pgs::ForcedTracker`]) — so the two never diverge.
//!
//! Cost: a track is only confirmed forced once EVERY display set is seen to be
//! forced, so a disc that has a forced track is read through — the
//! accuracy-over-speed tradeoff `info` opts into. Full tracks early-exit as soon
//! as they show a single non-forced subtitle, and a whole run stops early once
//! every track has settled.
//!
//! Encrypted content: the probe reuses whatever [`SectorSource`] the scan holds.
//! With a decrypting source it sees real PGS; without keys it reads ciphertext
//! and observes no display sets, in which case it leaves each track's existing
//! (vendor-label-derived) forced flag untouched rather than asserting anything.

use crate::disc::{Codec, DiscTitle, Stream};
use crate::mux::codec::CodecParser;
use crate::mux::codec::pgs::{ForcedTracker, PgsParser};
use crate::mux::ts::TsDemuxer;
use crate::sector::SectorSource;
use std::collections::HashMap;

const SECTOR_BYTES: usize = 2048;
/// Read the clip in 2 MiB chunks.
const CHUNK_SECTORS: u16 = 1024;

/// Read the title's PGS streams and set `SubtitleStream::forced` from their
/// content. Best-effort: any read error ends the probe with whatever verdicts
/// have accumulated. Only PGS tracks are touched (DVD VobSub forced comes from
/// the IFO/vendor path).
pub(crate) fn probe_and_set_forced<S: SectorSource + ?Sized>(
    reader: &mut S,
    title: &mut DiscTitle,
) {
    let pg_pids: Vec<u16> = title
        .streams
        .iter()
        .filter_map(|s| match s {
            Stream::Subtitle(sub) if sub.codec == Codec::Pgs => Some(sub.pid),
            _ => None,
        })
        .collect();
    if pg_pids.is_empty() {
        return;
    }

    let mut demux = TsDemuxer::new(&pg_pids);
    let mut parsers: HashMap<u16, PgsParser> =
        pg_pids.iter().map(|&p| (p, PgsParser::new())).collect();
    let mut trackers: HashMap<u16, ForcedTracker> =
        pg_pids.iter().map(|&p| (p, ForcedTracker::new())).collect();

    let extents = title.extents.clone();
    let mut buf = vec![0u8; CHUNK_SECTORS as usize * SECTOR_BYTES];
    'outer: for ext in &extents {
        let mut lba = ext.start_lba;
        let mut remaining = ext.sector_count;
        while remaining > 0 {
            let count = remaining.min(CHUNK_SECTORS as u32) as u16;
            let want = count as usize * SECTOR_BYTES;
            let n = match reader.read_sectors(lba, count, &mut buf[..want], false) {
                Ok(n) => n,
                Err(_) => break 'outer, // best-effort — stop, keep what we have
            };
            if n == 0 {
                break 'outer;
            }
            for pes in demux.feed(&buf[..n]) {
                if let (Some(parser), Some(tracker)) =
                    (parsers.get_mut(&pes.pid), trackers.get_mut(&pes.pid))
                {
                    for frame in parser.parse(&pes) {
                        tracker.observe(&frame.data);
                    }
                }
            }
            // Every track has already shown a non-forced set → nothing left to
            // learn; stop reading the (huge) clip.
            if trackers.values().all(ForcedTracker::settled_not_forced) {
                break 'outer;
            }
            lba += count as u32;
            remaining -= count as u32;
        }
    }

    // Drain any buffered final display set.
    for (pid, parser) in parsers.iter_mut() {
        if let Some(tracker) = trackers.get_mut(pid) {
            for frame in parser.flush() {
                tracker.observe(&frame.data);
            }
        }
    }

    // Apply verdicts. Only override a track we actually saw content for — an
    // undecrypted/unread track keeps its vendor-derived flag.
    for s in &mut title.streams {
        if let Stream::Subtitle(sub) = s {
            if sub.codec == Codec::Pgs {
                if let Some(t) = trackers.get(&sub.pid) {
                    if t.observed() {
                        sub.forced = t.is_forced();
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disc::{ContentFormat, Extent, LabelQualifier, SubtitleStream};

    /// A reader that yields all-zeros (an encrypted / unreadable clip) for a
    /// bounded span, then EOF.
    struct ZeroReader {
        served: u32,
        cap: u32,
    }
    impl SectorSource for ZeroReader {
        fn read_sectors(
            &mut self,
            _lba: u32,
            count: u16,
            buf: &mut [u8],
            _recovery: bool,
        ) -> crate::error::Result<usize> {
            if self.served >= self.cap {
                return Ok(0);
            }
            self.served += count as u32;
            buf.fill(0);
            Ok(buf.len())
        }
        fn capacity_sectors(&self) -> u32 {
            self.cap
        }
    }

    fn pgs_title(pid: u16, vendor_forced: bool) -> DiscTitle {
        DiscTitle {
            playlist: String::new(),
            playlist_id: 0,
            duration_secs: 0.0,
            size_bytes: 0,
            clips: vec![],
            streams: vec![Stream::Subtitle(SubtitleStream {
                pid,
                codec: Codec::Pgs,
                language: "eng".into(),
                forced: vendor_forced,
                qualifier: LabelQualifier::None,
                codec_data: None,
            })],
            chapters: vec![],
            extents: vec![Extent {
                start_lba: 0,
                sector_count: 4,
            }],
            content_format: ContentFormat::BdTs,
            codec_privates: vec![None],
        }
    }

    #[test]
    fn no_observed_content_preserves_vendor_forced() {
        // An unreadable/encrypted clip yields no PGS display sets — the probe must
        // leave the existing vendor-derived forced flag untouched, never assert
        // "not forced" from having seen nothing.
        let mut reader = ZeroReader { served: 0, cap: 4 };
        let mut title = pgs_title(0x1200, true);
        probe_and_set_forced(&mut reader, &mut title);
        let Stream::Subtitle(s) = &title.streams[0] else {
            panic!()
        };
        assert!(s.forced, "no content observed → vendor forced preserved");
    }

    /// A reader that serves a fixed BD-TS byte stream once (across sequential
    /// `read_sectors` calls), then EOF — so the probe's demux→parse→observe→apply
    /// path runs on real synthetic PGS content.
    struct TsReader {
        data: Vec<u8>,
        pos: usize,
    }
    impl SectorSource for TsReader {
        fn read_sectors(
            &mut self,
            _lba: u32,
            _count: u16,
            buf: &mut [u8],
            _recovery: bool,
        ) -> crate::error::Result<usize> {
            if self.pos >= self.data.len() {
                return Ok(0);
            }
            let n = buf.len().min(self.data.len() - self.pos);
            buf[..n].copy_from_slice(&self.data[self.pos..self.pos + n]);
            self.pos += n;
            Ok(n)
        }
        fn capacity_sectors(&self) -> u32 {
            self.data.len().div_ceil(SECTOR_BYTES) as u32
        }
    }

    // PGS PCS layout (matches the private constants in mux::codec::pgs): a
    // display-set frame begins with a PCS (segment type 0x16); byte 13 is
    // number_of_composition_objects; byte 17 is the first object's flags, whose
    // 0x40 bit is forced_on_flag.
    const PCS_SEG: u8 = 0x16;
    const PCS_NUM_OBJECTS_OFF: usize = 13;
    const PCS_FLAGS_OFF: usize = 17;
    const PCS_FORCED_FLAG: u8 = 0x40;

    /// One PGS display-set elementary payload with a single composition object;
    /// `forced` sets forced_on_flag.
    fn pcs_display(forced: bool) -> Vec<u8> {
        let mut d = vec![0u8; 18];
        d[0] = PCS_SEG;
        d[PCS_NUM_OBJECTS_OFF] = 1;
        d[PCS_FLAGS_OFF] = if forced { PCS_FORCED_FLAG } else { 0 };
        d
    }

    /// Wrap an elementary payload in one 192-byte BD-TS PES packet (PUSI, PTS
    /// present) on `pid`. `cc` is the 4-bit continuity counter.
    fn bd_pes_packet(pid: u16, cc: u8, es: &[u8]) -> Vec<u8> {
        let mut pkt = vec![0u8; 192];
        // pkt[0..4] = TP_extra_header (zeros). TS packet starts at pkt[4].
        pkt[4] = 0x47; // sync
        pkt[5] = 0x40 | ((pid >> 8) & 0x1F) as u8; // PUSI + PID high 5 bits
        pkt[6] = (pid & 0xFF) as u8; // PID low 8 bits
        pkt[7] = 0x10 | (cc & 0x0F); // adaptation=payload-only + continuity counter
        // PES header (at ts payload = pkt[8..]): 00 00 01 stream_id len flags.
        let p = 8;
        pkt[p] = 0x00;
        pkt[p + 1] = 0x00;
        pkt[p + 2] = 0x01;
        pkt[p + 3] = 0xBD; // private_stream_1 (carries the standard PES extension)
        pkt[p + 4] = 0x00; // PES packet length hi (0 = unbounded; ignored by demux)
        pkt[p + 5] = 0x00; // PES packet length lo
        pkt[p + 6] = 0x80; // flags1 ('10' marker)
        pkt[p + 7] = 0x80; // flags2 → PTS present
        pkt[p + 8] = 0x05; // PES_header_data_length = 5 (one PTS)
        // 5-byte PTS with the mandatory marker bits (bytes 0,2,4 low bit = 1).
        pkt[p + 9] = 0x21;
        pkt[p + 10] = 0x00;
        pkt[p + 11] = 0x01;
        pkt[p + 12] = 0x00;
        pkt[p + 13] = 0x01;
        let es_off = p + 14; // ES data follows the 14-byte PES header
        let n = es.len().min(192 - es_off);
        pkt[es_off..es_off + n].copy_from_slice(&es[..n]);
        pkt
    }

    /// Two BD-TS PES on `pid`: the FIRST carries `es` (the observed display set);
    /// the second (a fresh PUSI) exists only to flush the first PES out of the
    /// demuxer — the probe never calls `TsDemuxer::flush`, so an open PES stays
    /// buffered until the next PES start arrives.
    fn ts_stream(pid: u16, es: &[u8]) -> Vec<u8> {
        let mut s = bd_pes_packet(pid, 0, es);
        s.extend_from_slice(&bd_pes_packet(pid, 1, &pcs_display(false)));
        s
    }

    #[test]
    fn forced_display_sets_apply_forced_verdict() {
        // Feed REAL synthetic PGS bytes through the full demux→parse→observe→apply
        // path: a forced display set must flip a vendor-not-forced PGS track to
        // forced. Mutation guard: inverting ForcedTracker::is_forced flips this.
        let pid = 0x1200u16;
        let mut reader = TsReader {
            data: ts_stream(pid, &pcs_display(true)),
            pos: 0,
        };
        let mut title = pgs_title(pid, false); // vendor label says NOT forced
        probe_and_set_forced(&mut reader, &mut title);
        let Stream::Subtitle(s) = &title.streams[0] else {
            panic!()
        };
        assert!(
            s.forced,
            "an all-forced PGS track → forced verdict applied onto the stream"
        );
    }

    #[test]
    fn nonforced_display_sets_clear_forced_verdict() {
        // A non-forced display set observed on the wire overrides a vendor-forced
        // label → the track settles as not-forced.
        let pid = 0x1200u16;
        let mut reader = TsReader {
            data: ts_stream(pid, &pcs_display(false)),
            pos: 0,
        };
        let mut title = pgs_title(pid, true); // vendor label says forced
        probe_and_set_forced(&mut reader, &mut title);
        let Stream::Subtitle(s) = &title.streams[0] else {
            panic!()
        };
        assert!(
            !s.forced,
            "a non-forced display set observed → forced verdict cleared"
        );
    }

    #[test]
    fn no_pgs_streams_is_noop() {
        // A title with no PGS subtitle streams is a no-op (the reader is never
        // touched — a DVD/VobSub or audio-only title).
        let mut reader = ZeroReader { served: 0, cap: 0 };
        let mut title = pgs_title(0x1200, false);
        // Swap the PGS sub for an audio stream so there are no PGS PIDs.
        title.streams.clear();
        probe_and_set_forced(&mut reader, &mut title);
        assert_eq!(reader.served, 0, "no PGS PIDs → no reads");
    }
}
