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
