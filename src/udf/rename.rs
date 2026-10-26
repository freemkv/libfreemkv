//! Same-length UDF FID renames for decrypted whole-disc outputs.

use super::{
    Error, Result, SECTOR_BYTES, SectorSource, UdfFs, descriptor_crc16, parse_udf_name, read_sector,
};

/// One path policy. FID name lengths must match so ISO sectors and their layout stay fixed.
#[derive(Clone, Copy)]
pub(crate) struct NameRule {
    pub dir: &'static str,
    pub from: &'static str,
    pub to: &'static str,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SectorBytePatch {
    pub lba: u32,
    pub offset: usize,
    pub value: u8,
}

#[derive(Clone, Copy)]
struct Span {
    logical: usize,
    len: usize,
    lba: u32,
    offset: usize,
}

fn bad_sector(lba: u32) -> Error {
    Error::DiscRead {
        sector: lba as u64,
        status: None,
        sense: None,
    }
}

fn encoded_like(old: &[u8], replacement: &str, lba: u32) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(old.len());
    match old.first().copied() {
        Some(8) if replacement.is_ascii() => {
            out.push(8);
            out.extend_from_slice(replacement.as_bytes());
        }
        Some(16) => {
            out.push(16);
            for unit in replacement.encode_utf16() {
                out.extend_from_slice(&unit.to_be_bytes());
            }
        }
        _ => return Err(bad_sector(lba)),
    }
    if out.len() != old.len() {
        return Err(bad_sector(lba));
    }
    Ok(out)
}

fn location(spans: &[Span], byte: usize, lba: u32) -> Result<(u32, usize)> {
    let span = spans
        .iter()
        .find(|span| byte >= span.logical && byte < span.logical + span.len)
        .ok_or_else(|| bad_sector(lba))?;
    Ok((span.lba, span.offset + byte - span.logical))
}

impl UdfFs {
    /// Patch the on-disc FID only; payload bytes and UDF allocation never move.
    /// The same prepared reader feeds both ISO writing and folder extraction.
    pub(crate) fn name_patches(
        &self,
        reader: &mut dyn SectorSource,
        rules: &[NameRule],
    ) -> Result<Vec<SectorBytePatch>> {
        let mut all = Vec::new();
        for &rule in rules {
            let Some(dir) = self.find_dir(rule.dir) else {
                continue;
            };
            let count = dir
                .entries
                .iter()
                .filter(|e| e.name.eq_ignore_ascii_case(rule.from))
                .count();
            if count == 0 {
                continue;
            }
            if count != 1
                || dir
                    .entries
                    .iter()
                    .any(|e| e.name.eq_ignore_ascii_case(rule.to))
            {
                return Err(Error::DirNameCollision {
                    host: format!("{}/{}", rule.dir, rule.to),
                });
            }
            let (data, spans, icb_lba) = self.directory_bytes(reader, dir.meta_lba)?;
            let mut pos = 0;
            let mut found = 0;
            while pos + 38 <= data.len() {
                let tag = u16::from_le_bytes([data[pos], data[pos + 1]]);
                if tag == 0 {
                    break;
                }
                if tag != 257 {
                    return Err(bad_sector(icb_lba));
                }
                let flags = data[pos + 18];
                let name_len = data[pos + 19] as usize;
                let iu_len = u16::from_le_bytes([data[pos + 36], data[pos + 37]]) as usize;
                let desc_len = 38usize
                    .checked_add(iu_len)
                    .and_then(|n| n.checked_add(name_len))
                    .ok_or_else(|| bad_sector(icb_lba))?;
                let end = pos
                    .checked_add(desc_len)
                    .ok_or_else(|| bad_sector(icb_lba))?;
                if end > data.len() {
                    return Err(bad_sector(icb_lba));
                }
                let name_at = 38 + iu_len;
                let old = &data[pos + name_at..end];
                if flags & 0x0c == 0 && parse_udf_name(old).eq_ignore_ascii_case(rule.from) {
                    found += 1;
                    let crc_len = u16::from_le_bytes([data[pos + 10], data[pos + 11]]) as usize;
                    let covered_len = 16usize
                        .checked_add(crc_len)
                        .ok_or_else(|| bad_sector(icb_lba))?;
                    let aligned_len = (desc_len + 3) & !3;
                    if covered_len < desc_len
                        || covered_len > aligned_len
                        || pos + covered_len > data.len()
                    {
                        return Err(bad_sector(icb_lba));
                    }
                    let mut fid = data[pos..pos + covered_len].to_vec();
                    let replacement = encoded_like(old, rule.to, icb_lba)?;
                    fid[name_at..name_at + name_len].copy_from_slice(&replacement);
                    let crc = descriptor_crc16(&fid[16..16 + crc_len]);
                    fid[8..10].copy_from_slice(&crc.to_le_bytes());
                    fid[4] = fid
                        .iter()
                        .take(16)
                        .enumerate()
                        .filter(|(i, _)| *i != 4)
                        .map(|(_, byte)| *byte as u32)
                        .sum::<u32>() as u8;
                    for (i, (&before, &after)) in
                        data[pos..pos + covered_len].iter().zip(&fid).enumerate()
                    {
                        if before != after {
                            let (lba, offset) = location(&spans, pos + i, icb_lba)?;
                            all.push(SectorBytePatch {
                                lba,
                                offset,
                                value: after,
                            });
                        }
                    }
                }
                pos = pos
                    .checked_add((desc_len + 3) & !3)
                    .ok_or_else(|| bad_sector(icb_lba))?;
            }
            if found != 1 {
                return Err(bad_sector(icb_lba));
            }
        }
        all.sort_by_key(|p| (p.lba, p.offset));
        Ok(all)
    }

    fn directory_bytes(
        &self,
        reader: &mut dyn SectorSource,
        meta_lba: u32,
    ) -> Result<(Vec<u8>, Vec<Span>, u32)> {
        let icb_lba = self.meta_to_abs(meta_lba)?;
        let mut icb = [0u8; SECTOR_BYTES];
        read_sector(reader, icb_lba, &mut icb)?;
        let tag = u16::from_le_bytes([icb[0], icb[1]]);
        let ea_at = match tag {
            261 => 168,
            266 => 208,
            _ => return Err(bad_sector(icb_lba)),
        };
        let ea = u32::from_le_bytes([icb[ea_at], icb[ea_at + 1], icb[ea_at + 2], icb[ea_at + 3]])
            as usize;
        let ad_len = u32::from_le_bytes([
            icb[ea_at + 4],
            icb[ea_at + 5],
            icb[ea_at + 6],
            icb[ea_at + 7],
        ]) as usize;
        let ad_at = (ea_at + 8usize)
            .checked_add(ea)
            .ok_or_else(|| bad_sector(icb_lba))?;
        if icb[34] & 7 == 3 {
            let end = ad_at
                .checked_add(ad_len)
                .ok_or_else(|| bad_sector(icb_lba))?;
            if end > SECTOR_BYTES {
                return Err(bad_sector(icb_lba));
            }
            return Ok((
                icb[ad_at..end].to_vec(),
                vec![Span {
                    logical: 0,
                    len: ad_len,
                    lba: icb_lba,
                    offset: ad_at,
                }],
                icb_lba,
            ));
        }

        let mut data = Vec::new();
        let mut spans = Vec::new();
        for extent in self.read_icb_extents(reader, meta_lba)? {
            if !extent.recorded {
                return Err(bad_sector(icb_lba));
            }
            let mut left = extent.len as usize;
            for index in 0..left.div_ceil(SECTOR_BYTES) {
                let rel = extent
                    .lba
                    .checked_add(index as u32)
                    .ok_or_else(|| bad_sector(icb_lba))?;
                let lba = self.meta_to_abs(rel)?;
                let mut sector = [0u8; SECTOR_BYTES];
                read_sector(reader, lba, &mut sector)?;
                let len = left.min(SECTOR_BYTES);
                spans.push(Span {
                    logical: data.len(),
                    len,
                    lba,
                    offset: 0,
                });
                data.extend_from_slice(&sector[..len]);
                left -= len;
                if data.len() > super::MAX_DIR_BYTES as usize {
                    return Err(bad_sector(icb_lba));
                }
            }
        }
        Ok((data, spans, icb_lba))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dirimage::DirImage;
    use crate::sector::SectorSource;

    const RULE: &[NameRule] = &[NameRule {
        dir: "/AACS",
        from: "Unit_Key_RO.inf",
        to: "Unit_Key_RO.bak",
    }];

    // Polynomial long division, independent of the production CRC implementation.
    fn crc_oracle(bytes: &[u8]) -> u16 {
        let mut remainder = 0u32;
        for &byte in bytes {
            remainder = (remainder << 8) ^ (u32::from(byte) << 16);
            for shift in (0..8).rev() {
                if remainder & (1 << (16 + shift)) != 0 {
                    remainder ^= 0x1_1021 << shift;
                }
            }
        }
        remainder as u16
    }

    fn fid(name: &str, icb: u32, flags: u8, block: u32) -> Vec<u8> {
        let mut bytes = vec![0; (38 + 1 + name.len() + 3) & !3];
        bytes[..2].copy_from_slice(&257u16.to_le_bytes());
        bytes[2..4].copy_from_slice(&3u16.to_le_bytes());
        bytes[12..16].copy_from_slice(&block.to_le_bytes());
        bytes[18] = flags;
        bytes[19] = (name.len() + 1) as u8;
        bytes[20..24].copy_from_slice(&(SECTOR_BYTES as u32).to_le_bytes());
        bytes[24..28].copy_from_slice(&icb.to_le_bytes());
        bytes[28..30].copy_from_slice(&1u16.to_le_bytes());
        bytes[38] = 8;
        bytes[39..39 + name.len()].copy_from_slice(name.as_bytes());
        let crc_len = bytes.len() - 16;
        bytes[10..12].copy_from_slice(&(crc_len as u16).to_le_bytes());
        let crc = crc_oracle(&bytes[16..]);
        bytes[8..10].copy_from_slice(&crc.to_le_bytes());
        bytes[4] = bytes[..16].iter().copied().fold(0u8, u8::wrapping_add);
        bytes
    }

    fn mapped_directory(second: u32, crosses: bool) -> (super::super::fixture::MemDisc, usize) {
        use super::super::fixture::{MemDisc, build_file_icb, build_file_icb_ads};

        let mut disc = MemDisc::new();
        let mut sector = [0; SECTOR_BYTES];
        sector[..2].copy_from_slice(&2u16.to_le_bytes());
        disc.put_bytes(256, &sector);
        sector.fill(0);
        sector[..2].copy_from_slice(&5u16.to_le_bytes());
        sector[188..192].copy_from_slice(&128u32.to_le_bytes());
        disc.put_bytes(32, &sector);
        sector.fill(0);
        sector[..2].copy_from_slice(&6u16.to_le_bytes());
        sector[268..272].copy_from_slice(&2u32.to_le_bytes());
        sector[440..442].copy_from_slice(&[1, 6]);
        sector[446..448].copy_from_slice(&[2, 64]);
        sector[451..474].copy_from_slice(b"*UDF Metadata Partition");
        disc.put_bytes(33, &sector);
        sector.fill(0);
        sector[..2].copy_from_slice(&8u16.to_le_bytes());
        disc.put_bytes(34, &sector);
        disc.put_bytes(
            128,
            &build_file_icb_ads(&[(512 - 128, 32 * 2048), (second - 128, 32 * 2048)], false),
        );
        sector.fill(0);
        sector[..2].copy_from_slice(&256u16.to_le_bytes());
        sector[404..408].copy_from_slice(&1u32.to_le_bytes());
        disc.put_bytes(512, &sector);
        let root = fid("AACS", 3, 2, 2);
        disc.put_bytes(513, &build_file_icb(root.len() as u64, 2, false));
        disc.put_bytes(514, &root);
        disc.put_bytes(516, &build_file_icb(11, 2800 - 128, false));
        disc.put_bytes(517, &build_file_icb(9, 2801 - 128, false));
        disc.put_bytes(2800, b"key payload");
        disc.put_bytes(2801, b"unrelated");

        let mut directory = Vec::new();
        for i in 0..if crosses { 30 } else { 32 } {
            directory.extend(fid(&format!("padding-{i:017}"), 5, 0, 31));
        }
        if crosses {
            directory.extend(fid(&"x".repeat(37), 5, 0, 31));
        }
        let target = directory.len();
        assert_eq!(target, if crosses { 1996 } else { 2048 });
        directory.extend(fid("Unit_Key_RO.inf", 4, 0, if crosses { 31 } else { 32 }));
        disc.put_bytes(515, &build_file_icb(directory.len() as u64, 31, false));
        disc.put_bytes(543, &directory[..SECTOR_BYTES]);
        // Both locations contain plausible stale FIDs/tails. The old planner can
        // succeed while silently modifying the physical gap instead of block 32.
        disc.put_bytes(544, &directory[SECTOR_BYTES..]);
        disc.put_bytes(second, &directory[SECTOR_BYTES..]);
        (disc, target)
    }

    fn image(reader: &mut dyn SectorSource) -> Vec<u8> {
        let mut bytes = vec![0; 3072 * SECTOR_BYTES];
        assert_eq!(
            reader.read_sectors(0, 3072, &mut bytes, false).unwrap(),
            bytes.len()
        );
        bytes
    }

    fn mapped_rename(second: u32, crosses: bool) -> Vec<SectorBytePatch> {
        assert_eq!(crc_oracle(b"123456789"), 0x31c3);
        let (mut disc, target) = mapped_directory(second, crosses);
        let before = image(&mut disc);
        let fs = super::super::read_filesystem(&mut disc).unwrap();
        assert!(
            fs.find_dir("/AACS")
                .unwrap()
                .entries
                .iter()
                .any(|e| e.name == RULE[0].from)
        );
        assert!(fs.name_patches(&mut disc, &[]).unwrap().is_empty());
        let absent = [NameRule {
            from: "absent.inf",
            ..RULE[0]
        }];
        assert!(fs.name_patches(&mut disc, &absent).unwrap().is_empty());
        let patches = fs.name_patches(&mut disc, RULE).unwrap();
        assert!(!patches.is_empty());
        assert!(
            patches.iter().any(|p| p.lba == second),
            "target block 32 must be patched"
        );
        assert!(
            patches
                .iter()
                .all(|p| p.lba == second || (crosses && p.lba == 543))
        );

        let copy = disc.scoped(&[(0, 3072)]);
        let mut clear = crate::whole_disc::UnitAligned::new(copy, Vec::new())
            .with_name_patches(patches.clone());
        let after = image(&mut clear);
        let mut expected = before.clone();
        let expected_fid = fid(RULE[0].to, 4, 0, if crosses { 31 } else { 32 });
        for (i, &byte) in expected_fid.iter().enumerate() {
            let logical = target + i;
            let (lba, offset) = if logical < SECTOR_BYTES {
                (543, logical)
            } else {
                (second, logical - SECTOR_BYTES)
            };
            expected[lba as usize * SECTOR_BYTES + offset] = byte;
        }
        // Full-image equality checks the physical gap, payload, ICBs, unrelated
        // FIDs, partial-sector padding, and every byte outside the target FID.
        assert_eq!(after, expected);
        let mut directory = after[543 * SECTOR_BYTES..544 * SECTOR_BYTES].to_vec();
        directory.extend_from_slice(
            &after[second as usize * SECTOR_BYTES..(second as usize + 1) * SECTOR_BYTES],
        );
        let changed = &directory[target..target + expected_fid.len()];
        let crc_len = u16::from_le_bytes([changed[10], changed[11]]) as usize;
        assert_eq!(
            u16::from_le_bytes([changed[8], changed[9]]),
            crc_oracle(&changed[16..16 + crc_len])
        );
        assert_eq!(
            changed[4],
            changed[..16]
                .iter()
                .enumerate()
                .filter(|(i, _)| *i != 4)
                .fold(0u8, |sum, (_, byte)| sum.wrapping_add(*byte))
        );
        assert_eq!(
            &changed[12..16],
            &if crosses { 31u32 } else { 32u32 }.to_le_bytes()
        );
        let renamed = super::super::read_filesystem(&mut clear).unwrap();
        assert!(
            renamed
                .find_dir("/AACS")
                .unwrap()
                .entries
                .iter()
                .any(|e| e.name == RULE[0].to)
        );
        assert!(
            !renamed
                .find_dir("/AACS")
                .unwrap()
                .entries
                .iter()
                .any(|e| e.name == RULE[0].from)
        );
        assert_eq!(
            renamed
                .read_file(&mut clear, "/AACS/Unit_Key_RO.bak")
                .unwrap(),
            b"key payload"
        );
        let mut raw = crate::whole_disc::raw_whole_disc_reader(disc);
        assert_eq!(
            image(&mut raw),
            before,
            "raw output must retain the original image"
        );
        patches
    }

    #[test]
    fn fragmented_directory_rename_ignores_wrong_sector_decoy() {
        mapped_rename(2048, false);
    }

    #[test]
    fn fragmented_directory_rename_maps_cross_boundary_fid_and_checksums() {
        mapped_rename(2048, true);
    }

    #[test]
    fn contiguous_directory_rename_has_the_same_relative_patches() {
        for crosses in [false, true] {
            let contiguous = mapped_rename(544, crosses);
            let mut fragmented = mapped_rename(2048, crosses);
            for patch in &mut fragmented {
                if patch.lba == 2048 {
                    patch.lba = 544;
                }
            }
            assert_eq!(fragmented, contiguous);
        }
    }

    #[test]
    fn one_udf_fid_rename_serves_iso_and_folder_views_without_changing_payload() {
        let temp = tempfile::tempdir().unwrap();
        std::fs::create_dir(temp.path().join("AACS")).unwrap();
        std::fs::create_dir(temp.path().join("BDMV")).unwrap();
        let key_bytes = b"same content under its new name";
        std::fs::write(temp.path().join("AACS/Unit_Key_RO.inf"), key_bytes).unwrap();
        std::fs::write(temp.path().join("AACS/MKB_RO.inf"), b"other file").unwrap();
        let mut raw = DirImage::open(temp.path()).unwrap();
        let fs = super::super::read_filesystem(&mut raw).unwrap();
        let patches = fs.name_patches(&mut raw, RULE).unwrap();
        assert!(!patches.is_empty());
        assert!(fs.read_file(&mut raw, "/AACS/Unit_Key_RO.inf").is_ok());

        let mut clear =
            crate::whole_disc::UnitAligned::new(raw, Vec::new()).with_name_patches(patches.clone());
        let renamed = super::super::read_filesystem(&mut clear).unwrap();
        let aacs = renamed.find_dir("/AACS").unwrap();
        assert!(aacs.entries.iter().any(|e| e.name == "Unit_Key_RO.bak"));
        assert!(!aacs.entries.iter().any(|e| e.name == "Unit_Key_RO.inf"));
        assert_eq!(
            renamed
                .read_file(&mut clear, "/AACS/Unit_Key_RO.bak")
                .unwrap(),
            key_bytes
        );
        assert_eq!(
            renamed.read_file(&mut clear, "/AACS/MKB_RO.inf").unwrap(),
            b"other file"
        );
        let mut sector = [0u8; SECTOR_BYTES];
        for lba in patches.iter().map(|p| p.lba) {
            assert_eq!(
                clear.read_sectors(lba, 1, &mut sector, false).unwrap(),
                SECTOR_BYTES
            );
        }
    }

    #[test]
    fn a_preexisting_target_refuses_the_rename_before_output() {
        let temp = tempfile::tempdir().unwrap();
        std::fs::create_dir(temp.path().join("AACS")).unwrap();
        std::fs::create_dir(temp.path().join("BDMV")).unwrap();
        std::fs::write(temp.path().join("AACS/Unit_Key_RO.inf"), b"original").unwrap();
        std::fs::write(temp.path().join("AACS/Unit_Key_RO.bak"), b"other").unwrap();
        let mut raw = DirImage::open(temp.path()).unwrap();
        let fs = super::super::read_filesystem(&mut raw).unwrap();
        assert!(matches!(
            fs.name_patches(&mut raw, RULE),
            Err(Error::DirNameCollision { .. })
        ));
    }
}
