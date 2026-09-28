//! Test-only: an independent parser and P-STD replayer for `mpg://` output (design §7).
//! It re-derives every byte's arrival from SCR and `program_mux_rate` (MS-4 eq. 2-21) and
//! checks the normative constraints, allowing exactly the anomalies the sink counted.

use super::pack;
use std::collections::BTreeMap;

pub(super) type Key = (u8, Option<u8>);

#[derive(Debug, Clone)]
pub(super) struct Pack {
    pub off: usize,
    pub scr: u64,
    pub rate: u32,
}

#[derive(Debug, Clone)]
pub(super) struct Pes {
    pub pack: usize,
    pub key: Key,
    pub pts: Option<u64>,
    pub dts: Option<u64>,
    pub pstd: Option<(bool, u16)>,
    /// Absolute offset of the first `PES_packet_data_byte` and of the ES after any
    /// sub-stream header.
    pub data_off: usize,
    pub es_off: usize,
    pub sub_hdr: Vec<u8>,
    pub es: Vec<u8>,
    pub end: usize,
}

#[derive(Debug, Default)]
pub(super) struct Parsed {
    pub packs: Vec<Pack>,
    pub system_headers: Vec<(usize, Vec<u8>)>,
    pub psms: Vec<(usize, Vec<u8>)>,
    pub pes: Vec<Pes>,
    pub end_code: bool,
}

fn ts(b: &[u8]) -> u64 {
    (u64::from(b[0] >> 1) & 7) << 30
        | u64::from(b[1]) << 22
        | u64::from(b[2] >> 1) << 15
        | u64::from(b[3]) << 7
        | u64::from(b[4] >> 1)
}

/// Parse `data` as 2048-byte packs (MS-1: every PES lies inside its pack).
pub(super) fn parse(data: &[u8]) -> Result<Parsed, String> {
    let mut p = Parsed::default();
    let mut off = 0;
    while off < data.len() {
        if data[off..].starts_with(&pack::PROGRAM_END) {
            if off + 4 != data.len() {
                return Err("bytes after MPEG_program_end_code".into());
            }
            p.end_code = true;
            break;
        }
        let pk = data
            .get(off..off + pack::PACK_BYTES)
            .ok_or("a short pack")?;
        if pk[..4] != [0, 0, 1, 0xBA] || pk[4] >> 6 != 1 {
            return Err(format!("no MPEG-2 pack header at {off}"));
        }
        let b = |i: usize| u64::from(pk[i]);
        let base = ((b(4) >> 3) & 7) << 30
            | (b(4) & 3) << 28
            | b(5) << 20
            | (b(6) >> 3) << 15
            | (b(6) & 3) << 13
            | b(7) << 5
            | b(8) >> 3;
        let ext = (b(8) & 3) << 7 | b(9) >> 1;
        let rate = (u32::from(pk[10]) << 14) | (u32::from(pk[11]) << 6) | (u32::from(pk[12]) >> 2);
        if rate == 0 {
            return Err("program_mux_rate 0 is forbidden (MS-3)".into());
        }
        let stuffing = usize::from(pk[13] & 7);
        let pi = p.packs.len();
        p.packs.push(Pack {
            off,
            scr: base * 300 + ext,
            rate,
        });
        let mut i = pack::PACK_HEADER_BYTES + stuffing;
        while i < pack::PACK_BYTES {
            let h = &pk[i..];
            if h.len() < 6 || h[..3] != [0, 0, 1] {
                return Err(format!("no start code at {}", off + i));
            }
            let len = usize::from(u16::from_be_bytes([h[4], h[5]]));
            let end = i + 6 + len;
            if end > pack::PACK_BYTES {
                return Err(format!("a packet crosses its pack at {} (MS-1)", off + i));
            }
            match h[3] {
                0xBB => p.system_headers.push((pi, pk[i..end].to_vec())),
                0xBC => p.psms.push((pi, pk[i..end].to_vec())),
                0xBE => {}
                sid => {
                    let hdl = usize::from(h[8]);
                    let flags = h[7];
                    let mut q = 9;
                    let (mut pts, mut dts) = (None, None);
                    if flags & 0x80 != 0 {
                        pts = Some(ts(&h[q..]));
                        q += 5;
                    }
                    if flags & 0xC0 == 0xC0 {
                        dts = Some(ts(&h[q..]));
                        q += 5;
                    }
                    let mut pstd = None;
                    if flags & 0x01 != 0 && h[q] & 0x10 != 0 {
                        pstd = Some((
                            h[q + 1] & 0x20 != 0,
                            u16::from(h[q + 1] & 0x1F) << 8 | u16::from(h[q + 2]),
                        ));
                    }
                    let data_off = off + i + 9 + hdl;
                    let payload = &pk[i + 9 + hdl..end];
                    let (key, hdr) = if sid == pack::PRIVATE_STREAM_1 {
                        let sub = payload[0];
                        let n = match sub {
                            0x80..=0x8F => 4,
                            0xA0..=0xA7 => 7,
                            _ => 1,
                        };
                        ((sid, Some(sub)), n)
                    } else {
                        ((sid, None), 0)
                    };
                    p.pes.push(Pes {
                        pack: pi,
                        key,
                        pts,
                        dts,
                        pstd,
                        data_off,
                        es_off: data_off + hdr,
                        sub_hdr: payload[..hdr].to_vec(),
                        es: payload[hdr..].to_vec(),
                        end: off + end,
                    });
                }
            }
            i = end;
        }
        off += pack::PACK_BYTES;
    }
    Ok(p)
}

/// Arrival time (27 MHz, floor) of absolute byte `x` of pack `k` (MS-4 eq. 2-21).
pub(super) fn arrival(p: &Parsed, k: usize, x: usize) -> i128 {
    let pk = &p.packs[k];
    let i0 = (pk.off + pack::SCR_BASE_LAST_BYTE) as i128;
    let d = x as i128 - i0;
    i128::from(pk.scr) + (d * 540_000).div_euclid(i128::from(pk.rate))
}

/// One stream's access units, as the fixture knows them: `(size, mark)`.
pub(super) type Aus = BTreeMap<Key, Vec<(usize, usize)>>;

/// What the replay found beyond hard failures.
#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct Found {
    pub late_aus: u64,
    pub pts_gaps: u64,
}

/// Replay `p` through the P-STD. Hard errors are structural and normative failures that no
/// counter excuses; `Found` holds what the sink must have counted.
pub(super) fn replay(p: &Parsed, aus: &Aus) -> Result<Found, String> {
    // MS-21 §2.7.8 / MS-7: one system header, in the first pack, each stream bound once.
    let [(0, sh)] = &p.system_headers[..] else {
        return Err(format!(
            "system headers: {:?}",
            p.system_headers.iter().map(|s| s.0).collect::<Vec<_>>()
        ));
    };
    let mut bounds: BTreeMap<u8, (bool, u16)> = BTreeMap::new();
    for e in sh[12..].chunks(3) {
        if bounds
            .insert(
                e[0],
                (
                    e[1] & 0x20 != 0,
                    u16::from(e[1] & 0x1F) << 8 | u16::from(e[2]),
                ),
            )
            .is_some()
        {
            return Err(format!("stream {:#04x} bound twice (MS-7)", e[0]));
        }
    }
    let rate_bound =
        (u32::from(sh[6]) & 0x7F) << 15 | u32::from(sh[7]) << 7 | u32::from(sh[8]) >> 1;
    // MS-8/MS-9: one map in the first pack, CRC_32 zero residue, length ≤ 1018.
    let [(0, psm)] = &p.psms[..] else {
        return Err("the program stream map is not (only) in the first pack".into());
    };
    if pack::crc32(psm) != 0 || psm.len() - 6 > pack::MAX_PSM_LENGTH {
        return Err("PSM CRC_32 or length".into());
    }
    if !p.end_code {
        return Err("no MPEG_program_end_code".into());
    }
    let mut found = Found::default();
    // MS-17, MS-4: SCR spacing ≤ 0.7 s; packs never overlap.
    for w in p.packs.windows(2) {
        if w[1].scr.saturating_sub(w[0].scr) > super::pstd::MAX_SCR_GAP27 {
            return Err(format!("SCR gap {} (MS-17)", w[1].scr - w[0].scr));
        }
        if w[0].rate > rate_bound || w[1].rate > rate_bound {
            return Err("program_mux_rate above rate_bound (MS-6)".into());
        }
    }
    for k in 0..p.packs.len().saturating_sub(1) {
        let last = arrival(p, k, p.packs[k].off + pack::PACK_BYTES - 1);
        let first = arrival(p, k + 1, p.packs[k + 1].off);
        if last > first {
            return Err(format!("pack {k} overlaps the next (MS-4)"));
        }
    }
    // Per stream: P-STD fields, timestamps and AU bytes.
    let keys: std::collections::BTreeSet<Key> = p.pes.iter().map(|x| x.key).collect();
    // Buffer events: (time, buffer stream_id, +bytes / −bytes).
    let mut events: Vec<(i128, u8, i64, i128)> = Vec::new();
    for key in &keys {
        let pes: Vec<&Pes> = p.pes.iter().filter(|x| x.key == *key).collect();
        let bound = bounds
            .get(&key.0)
            .ok_or(format!("{key:?} has no system-header bound (MS-7)"))?;
        match pes[0].pstd {
            // MS-21 §2.7.7, MS-13: the first PES states the buffer; scale 0 audio, 1 video.
            Some(f) if f == *bound => {}
            other => {
                return Err(format!(
                    "{key:?} first PES P-STD fields {other:?} vs bound {bound:?}"
                ));
            }
        }
        let audio = (0xC0..=0xDF).contains(&key.0);
        if (audio && bound.0) || (key.0 >= 0xE0 && !bound.0) {
            return Err(format!("{key:?} P-STD scale (MS-13)"));
        }
        let mut last_pts: Option<u64> = None;
        let mut last_dts: Option<u64> = None;
        for x in &pes {
            if let Some(d) = x.dts {
                // MS-19: DTS only with a PTS and only where it differs; monotone, ≤ PTS.
                let pts = x.pts.ok_or("DTS without PTS")?;
                if d >= pts || last_dts.is_some_and(|l| d <= l) {
                    return Err(format!(
                        "{key:?} DTS {d} vs PTS {pts} / previous {last_dts:?}"
                    ));
                }
                last_dts = Some(d);
            } else if let Some(pts) = x.pts {
                last_dts = Some(last_dts.map_or(pts, |l| l.max(pts)));
            }
            if let Some(pts) = x.pts {
                if key.1.is_none_or(|s| !(0x20..=0x3F).contains(&s))
                    && last_pts.is_some_and(|l| pts.abs_diff(l) > 63_000)
                {
                    found.pts_gaps += 1;
                }
                last_pts = Some(pts);
            }
        }
        // AU boundaries: the fixture's sizes, or the first_access_unit_pointer (MS-29).
        let es_total: usize = pes.iter().map(|x| x.es.len()).sum();
        let sizes: Vec<(usize, usize)> = match aus.get(key) {
            Some(s) => s.clone(),
            None => {
                let mut pos = 0usize;
                let mut abs = Vec::new();
                for x in &pes {
                    if x.sub_hdr.len() >= 4 && x.sub_hdr[1] > 0 {
                        let ptr = usize::from(u16::from_be_bytes([x.sub_hdr[2], x.sub_hdr[3]]));
                        let skip = if key.1.is_some_and(|s| (0xA0..=0xA7).contains(&s)) {
                            4
                        } else {
                            1
                        };
                        abs.push(pos + ptr - skip);
                    } else if x.sub_hdr.len() == 1 && x.pts.is_some() {
                        abs.push(pos);
                    }
                    pos += x.es.len();
                }
                abs.push(es_total);
                abs.windows(2).map(|w| (w[1] - w[0], 0)).collect()
            }
        };
        if sizes.iter().map(|s| s.0).sum::<usize>() != es_total {
            return Err(format!(
                "{key:?}: ES bytes {es_total} vs AU sizes {}",
                sizes.iter().map(|s| s.0).sum::<usize>()
            ));
        }
        // Walk bytes: byte j of the stream's ES lives in some PES at a known file offset.
        let mut idx = Vec::with_capacity(pes.len()); // (es start, pes)
        let mut acc = 0;
        for x in &pes {
            idx.push(acc);
            acc += x.es.len();
        }
        let locate = |j: usize| {
            let pi = idx.partition_point(|&s| s <= j) - 1;
            (pi, pes[pi].es_off + (j - idx[pi]))
        };
        let pts_pes: Vec<usize> = (0..pes.len()).filter(|&i| pes[i].pts.is_some()).collect();
        if pts_pes.len() != sizes.len() {
            return Err(format!(
                "{key:?}: {} PTS-bearing PES for {} AUs (MS-19 one PTS per AU)",
                pts_pes.len(),
                sizes.len()
            ));
        }
        // Each PES's sub-stream header leaves Bn with the AU holding its first ES byte.
        let mut au_of_pes_hdr = vec![0i64; sizes.len()];
        {
            let mut ends = Vec::with_capacity(sizes.len());
            let mut e = 0;
            for s in &sizes {
                e += s.0;
                ends.push(e);
            }
            for (pi, x) in pes.iter().enumerate() {
                let a = ends
                    .partition_point(|&end| end <= idx[pi])
                    .min(sizes.len() - 1);
                au_of_pes_hdr[a] += x.sub_hdr.len() as i64;
            }
        }
        let mut j = 0;
        for (a, &(size, mark)) in sizes.iter().enumerate() {
            let x = pes[pts_pes[a]];
            // MS-15: the PTS names the AU whose commencement byte is in this PES.
            let (cpi, _) = locate(j + mark);
            if cpi != pts_pes[a] {
                return Err(format!(
                    "{key:?} AU {a}: PTS in PES {} but it commences in PES {cpi} (MS-15)",
                    pts_pes[a]
                ));
            }
            // The AU's first byte (a sequence header ahead of the picture) shares that PES, as
            // a DVD encoder writes it and PS readers expect.
            if locate(j).0 != cpi {
                return Err(format!(
                    "{key:?} AU {a}: first byte in PES {} but PTS in PES {cpi}",
                    locate(j).0
                ));
            }
            let dec = i128::from(x.dts.or(x.pts).unwrap()) * 300;
            let (fpi, first_off) = locate(j);
            let (lpi, last_off) = locate(j + size - 1);
            let t_first = arrival(p, pes[fpi].pack, first_off);
            let t_last = arrival(p, pes[lpi].pack, last_off);
            // MS-16: "less than or equal to one second" delay; complete by tdn(j).
            if dec - t_first > 27_000_000 {
                return Err(format!(
                    "{key:?} AU {a}: buffered {} s (MS-16)",
                    (dec - t_first) as f64 / 27e6
                ));
            }
            if t_last > dec {
                found.late_aus += 1;
            }
            events.push((dec, key.0, -(size as i64) - au_of_pes_hdr[a], 0));
            j += size;
        }
        for x in &pes {
            // Payload bytes (sub-stream header included) enter Bn; checked at the PES's
            // first byte against removals up to then (MS-16).
            let bytes = (x.end - x.data_off) as i64;
            events.push((
                arrival(p, x.pack, x.data_off),
                key.0,
                bytes,
                arrival(p, x.pack, x.end - 1),
            ));
        }
    }
    // MS-16: 0 ≤ Fn(t) ≤ BSn, one buffer per stream_id (0xBD shared).
    events.sort_by_key(|e| (e.0, e.2 > 0));
    let mut fill: BTreeMap<u8, i64> = BTreeMap::new();
    for (_, sid, delta, _) in &events {
        let f = fill.entry(*sid).or_default();
        *f += delta;
        let (scale, size) = bounds[sid];
        let bs = i64::from(size) * if scale { 1024 } else { 128 };
        if *f > bs {
            return Err(format!("B({sid:#04x}) overflows: {f} > {bs} (MS-16)"));
        }
    }
    Ok(found)
}
