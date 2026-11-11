//! Physical layer layout as the drive reports it: READ DISC STRUCTURE format 00h.
//! DVD-family media (DVD, HD DVD) return the Physical Format Information (MMC-6
//! §6.22.3.2.1, Table 398), which carries the layer count, the track path and the
//! layer-0 end sector, so it gives the layer break. BD returns Disc Information (DI)
//! units (§6.22.3.3.1). Only the first unit's layer count is read: the rest of the
//! unit body is defined in BD-Ref1, which isn't public.

use crate::drive::Drive;
use crate::scsi::{DataDirection, SCSI_READ_DISC_STRUCTURE};

/// How the two layers of a dual-layer DVD-family disc are addressed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrackPath {
    /// Each layer has its own lead-in, user area and lead-out.
    Parallel,
    /// One spiral runs out on layer 0 and back in on layer 1 (nearly every DVD-9).
    Opposite,
}

/// The drive-reported layer layout of a disc.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LayerMap {
    /// Recording layers on the read side (1-4).
    pub layers: u8,
    /// Sectors on layer 0, which is also the LBA of the first layer-1 sector: the
    /// value burners take as the layer break. DVD-family dual-layer discs only.
    pub layer_break: Option<u32>,
    /// DVD-family dual-layer discs only.
    pub track_path: Option<TrackPath>,
}

/// MMC media-type codes for READ DISC STRUCTURE byte 1 (Table 382).
const MEDIA_DVD: u8 = 0x00;
const MEDIA_BD: u8 = 0x01;
/// Header (4) plus one PFI sector (2048); BD DI answers with 4100.
const ALLOC: usize = 4100;

/// Ask the drive for the layer layout. `dvd_family` picks the media type (DVD/HD DVD
/// vs BD). Any failure — refused CDB, short or unparsable answer — is `None`, so the
/// caller keeps its capacity estimate.
pub(crate) fn read(drive: &mut Drive, dvd_family: bool, capacity: u32) -> Option<LayerMap> {
    let media = if dvd_family { MEDIA_DVD } else { MEDIA_BD };
    let cdb = [
        SCSI_READ_DISC_STRUCTURE,
        media,
        0,
        0,
        0,
        0,
        0, // layer 0
        0x00,
        (ALLOC >> 8) as u8,
        (ALLOC & 0xFF) as u8,
        0,
        0,
    ];
    let mut buf = vec![0u8; ALLOC];
    let r = match drive.scsi_execute(&cdb, DataDirection::FromDevice, &mut buf, 10_000) {
        Ok(r) => r,
        Err(e) => {
            tracing::info!(target: "freemkv::scan", error = %e, "layer layout not reported by drive");
            return None;
        }
    };
    let got = &buf[..r.bytes_transferred.min(buf.len())];
    let map = if dvd_family {
        parse_dvd_pfi(got, capacity)
    } else {
        parse_bd_di(got)
    };
    tracing::info!(target: "freemkv::scan", ?map, "drive layer layout");
    map
}

/// Parse a format-00h answer for DVD media (4-byte header, then Table 398).
/// `capacity` (sectors) bounds the layer break: a break at or past it is bogus.
pub fn parse_dvd_pfi(buf: &[u8], capacity: u32) -> Option<LayerMap> {
    if buf.len() < 4 + 16 || u16::from_be_bytes([buf[0], buf[1]]) < 2 + 16 {
        return None;
    }
    let pfi = &buf[4..];
    let layers = match (pfi[2] >> 5) & 0x03 {
        0 => 1,
        1 => 2,
        _ => return None,
    };
    if layers == 1 {
        return Some(LayerMap {
            layers,
            layer_break: None,
            track_path: None,
        });
    }
    let be24 = |o: usize| u32::from_be_bytes([0, pfi[o + 1], pfi[o + 2], pfi[o + 3]]);
    let start = be24(4);
    let end_data = be24(8);
    let end_l0 = be24(12);
    let track_path = if pfi[2] & 0x10 != 0 {
        TrackPath::Opposite
    } else {
        TrackPath::Parallel
    };
    // OTP: layer 0 ends at its own field. PTP leaves that field zero, and the layer-0
    // PFI's data-area end is layer 0's end (each layer has its own data area).
    let last_l0 = match track_path {
        TrackPath::Opposite => end_l0,
        TrackPath::Parallel => end_data,
    };
    let layer_break = last_l0
        .checked_sub(start)
        .map(|n| n + 1)
        .filter(|&b| b < capacity);
    Some(LayerMap {
        layers,
        layer_break,
        track_path: Some(track_path),
    })
}

/// Parse a format-00h answer for BD media (4-byte header, then DI units). Reads the
/// first DI unit's layer count (body byte 12, high nibble).
pub fn parse_bd_di(buf: &[u8]) -> Option<LayerMap> {
    if buf.len() < 4 + 13 {
        return None;
    }
    let di = &buf[4..];
    if &di[0..2] != b"DI" || !matches!(&di[8..11], b"BDO" | b"BDR" | b"BDW" | b"BDU") {
        return None;
    }
    let layers = di[12] >> 4;
    if !(1..=4).contains(&layers) {
        return None;
    }
    Some(LayerMap {
        layers,
        layer_break: None,
        track_path: None,
    })
}

/// The ImgBurn layer-break descriptor (`.dvd`) for a dual-layer DVD image named
/// `iso_name`: ImgBurn burns the image with this break when it opens the file.
pub fn imgburn_dvd_descriptor(layer_break: u32, iso_name: &str) -> String {
    format!("LayerBreak={layer_break}\r\n{iso_name}\r\n")
}

#[cfg(test)]
#[path = "layer_map_tests.rs"]
mod tests;
