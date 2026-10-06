//! Physical AC-3 sub-stream channel probe for DVD bug logs.
//!
//! Reads the head of a title's feature and reports each physical `private_stream_1` AC-3
//! sub-stream's REAL channel count under `--log-level 3`, so a log alone shows whether the
//! IFO's declared layout matches the VOB. It no longer re-routes anything: the scanner routes
//! audio by the PGC's AST_CTL, the map every player follows, and a channel-count heuristic
//! layered on top could only move a stream off the track the disc actually plays.

use crate::disc::Stream;
use crate::mux::codec::ac3;
use crate::mux::ps::PsDemuxer;
use crate::sector::SectorSource;
use std::collections::BTreeMap;

// Sectors of the first feature extent to probe. 512 (1 MiB) was too short on a real disc; 1024
// (2 MiB) reliably reaches every physical AC-3 sub-stream.
const PROBE_SECTORS: u16 = 1024;

/// Decode the real per-sub-stream AC-3 channel count from a buffer of decrypted MPEG-PS (DVD
/// VOB) bytes. Demuxes `private_stream_1` (0xBD), and for each AC-3 sub-stream id
/// (`0x80..=0x87`) records the MAXIMUM channel count seen across EVERY decodable frame — the
/// max, not the first frame, because a sub-stream's opening frames are often an
/// unrepresentative logo/warning bed. Pure — takes the already-read bytes, never touches the
/// disc. Returns a map `sub_id -> max channels`; absent for sub-streams that never appear or
/// carry no decodable BSI bits.
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

/// Probe the first feature extent of a DVD title through a (decrypted) sector source and
/// log each physical AC-3 sub-stream's real channel count. Diagnostics only: reads nothing
/// unless `freemkv::diag` is enabled, and never changes `title` (the name predates L084b).
///
/// `reader` MUST yield PLAINTEXT VOB bytes (a `DecryptingSectorSource` on a CSS disc);
/// scrambled sectors yield no AC-3 syncs and log `probed=0`.
pub fn probe_and_remap<S: SectorSource + ?Sized>(
    reader: &mut S,
    title: &mut crate::disc::DiscTitle,
) {
    if !tracing::enabled!(target: "freemkv::diag", tracing::Level::DEBUG) {
        return;
    }
    // Only DVD-Video titles (`DvdPs`) carry these private_stream_1 AC-3 sub-streams.
    if title.content_format != crate::disc::ContentFormat::DvdPs {
        return;
    }
    // Nothing to report unless there is at least one AC-3 audio stream.
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
    // stall the mux or hammer a marginal drive. On any error, log nothing.
    reader.set_unit_base(ext.start_lba); // HD DVD AACS: anchor at the title head
    let n = match reader.read_sectors(ext.start_lba, count, &mut buf, false) {
        Ok(n) => n,
        Err(_) => return,
    };
    buf.truncate(n);
    let probed = probe_ac3_substream_channels(&buf);
    crate::diag::dump_dvd_substream_probe(title.playlist_id, &probed);
}

#[cfg(test)]
#[path = "dvd_audio_probe_tests.rs"]
mod tests;
