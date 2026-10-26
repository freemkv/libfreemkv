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

struct InlineOwner {
    meta_lba: u32,
    sector: [u8; SECTOR_BYTES],
    descriptor_end: usize,
}

type DirectoryBytes = (Vec<u8>, Vec<Span>, u32, Option<InlineOwner>);

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
    /// Patch FID names and descriptor checksums; payload bytes and allocation never move.
    /// The same prepared reader feeds both ISO writing and folder extraction.
    pub(crate) fn name_patches(
        &self,
        reader: &mut dyn SectorSource,
        rules: &[NameRule],
    ) -> Result<Vec<SectorBytePatch>> {
        let mut all = Vec::new();
        let mut inline_owners: Vec<InlineOwner> = Vec::new();
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
            let (data, spans, icb_lba, inline_owner) =
                self.directory_bytes(reader, dir.meta_lba)?;
            let first_patch = all.len();
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
            if all.len() > first_patch
                && let Some(owner) = inline_owner
                && !inline_owners.iter().any(|o| o.meta_lba == owner.meta_lba)
            {
                inline_owners.push(owner);
            }
        }
        // Embedded FIDs are part of their FE/EFE descriptor body. Finalize its
        // tag only after every rule, so its CRC includes all child edits.
        for owner in inline_owners {
            let icb_lba = self.meta_to_abs(owner.meta_lba)?;
            let crc_len = u16::from_le_bytes([owner.sector[10], owner.sector[11]]) as usize;
            // ECMA-167 permits an omitted CRC. Preserve that policy, as well as
            // shorter declared coverage, rather than repairing/expanding it.
            if crc_len == 0 {
                continue;
            }
            let crc_end = 16 + crc_len;
            if crc_end > owner.descriptor_end || crc_end > SECTOR_BYTES {
                return Err(bad_sector(icb_lba));
            }
            let mut final_sector = owner.sector;
            for patch in all.iter().filter(|patch| patch.lba == icb_lba) {
                final_sector[patch.offset] = patch.value;
            }
            let crc = descriptor_crc16(&final_sector[16..crc_end]);
            final_sector[8..10].copy_from_slice(&crc.to_le_bytes());
            final_sector[4] = final_sector[..16]
                .iter()
                .enumerate()
                .filter(|(i, _)| *i != 4)
                .map(|(_, byte)| *byte as u32)
                .sum::<u32>() as u8;
            for offset in [4, 8, 9] {
                if owner.sector[offset] != final_sector[offset] {
                    all.push(SectorBytePatch {
                        lba: icb_lba,
                        offset,
                        value: final_sector[offset],
                    });
                }
            }
        }
        all.sort_by_key(|p| (p.lba, p.offset));
        Ok(all)
    }

    fn directory_bytes(
        &self,
        reader: &mut dyn SectorSource,
        meta_lba: u32,
    ) -> Result<DirectoryBytes> {
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
                Some(InlineOwner {
                    meta_lba,
                    sector: icb,
                    descriptor_end: end,
                }),
            ));
        }

        let mut data = Vec::new();
        let mut spans = Vec::new();
        for extent in self.read_icb_extents(reader, meta_lba)? {
            if !extent.recorded {
                return Err(bad_sector(icb_lba));
            }
            let base = self.meta_to_abs(extent.lba)?;
            let mut left = extent.len as usize;
            for index in 0..left.div_ceil(SECTOR_BYTES) {
                let lba = base
                    .checked_add(index as u32)
                    .ok_or_else(|| bad_sector(base))?;
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
        Ok((data, spans, icb_lba, None))
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

    const TWO_RULES: &[NameRule] = &[
        RULE[0],
        NameRule {
            dir: "/AACS",
            from: "MKB_RO.inf",
            to: "MKB_RO.bak",
        },
    ];

    // Polynomial long division with explicit zero augmentation, independent of
    // the production byte-at-a-time CRC implementation.
    fn oracle_crc(bytes: &[u8]) -> u16 {
        let mut remainder = 0u32;
        for bit in bytes
            .iter()
            .flat_map(|byte| (0..8).rev().map(move |bit| (byte >> bit) & 1))
            .chain(std::iter::repeat_n(0, 16))
        {
            remainder = (remainder << 1) | u32::from(bit);
            if remainder & 0x10000 != 0 {
                remainder ^= 0x11021;
            }
        }
        remainder as u16
    }

    fn oracle_finish(bytes: &mut [u8], end: usize) {
        bytes[10..12].copy_from_slice(&((end - 16) as u16).to_le_bytes());
        let crc = oracle_crc(&bytes[16..end]);
        bytes[8..10].copy_from_slice(&crc.to_le_bytes());
        bytes[4] = bytes[..16]
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != 4)
            .fold(0u8, |sum, (_, byte)| sum.wrapping_add(*byte));
    }

    fn assert_descriptor_valid(bytes: &[u8]) {
        let crc_len = u16::from_le_bytes(bytes[10..12].try_into().unwrap()) as usize;
        assert_eq!(
            u16::from_le_bytes(bytes[8..10].try_into().unwrap()),
            oracle_crc(&bytes[16..16 + crc_len]),
            "descriptor CRC must cover the final bytes"
        );
        assert_eq!(
            bytes[4],
            bytes[..16]
                .iter()
                .enumerate()
                .filter(|(i, _)| *i != 4)
                .fold(0u8, |sum, (_, byte)| sum.wrapping_add(*byte)),
            "descriptor tag checksum"
        );
    }

    struct RenameFixture {
        image: Vec<u8>,
        owner_lba: u32,
        data_at: usize,
        data_len: usize,
        fid_lengths: Vec<usize>,
    }

    fn rename_fixture(inline_tag: Option<u16>) -> RenameFixture {
        assert_eq!(oracle_crc(b"123456789"), 0x31c3);
        let temp = tempfile::tempdir().unwrap();
        std::fs::create_dir(temp.path().join("AACS")).unwrap();
        std::fs::create_dir(temp.path().join("BDMV")).unwrap();
        for name in ["Unit_Key_RO.inf", "MKB_RO.inf", "other.dat"] {
            std::fs::write(temp.path().join("AACS").join(name), name.as_bytes()).unwrap();
        }
        let mut raw = DirImage::open(temp.path()).unwrap();
        let fs = super::super::read_filesystem(&mut raw).unwrap();
        let meta_lba = fs.find_dir("/AACS").unwrap().meta_lba;
        let (mut data, _, owner_lba, _) = fs.directory_bytes(&mut raw, meta_lba).unwrap();
        let mut image = vec![0; raw.capacity_sectors() as usize * SECTOR_BYTES];
        for (lba, sector) in image.chunks_mut(SECTOR_BYTES).enumerate() {
            raw.read_sectors(lba as u32, 1, sector, false).unwrap();
        }
        let mut fid_lengths = Vec::new();
        let mut pos = 0;
        while pos < data.len() {
            let name_len = data[pos + 19] as usize;
            let iu_len = u16::from_le_bytes(data[pos + 36..pos + 38].try_into().unwrap()) as usize;
            let len = (38 + iu_len + name_len + 3) & !3;
            fid_lengths.push(len);
            if inline_tag.is_some() {
                data[pos + 12..pos + 16].copy_from_slice(&meta_lba.to_le_bytes());
                oracle_finish(&mut data[pos..pos + len], 38 + iu_len + name_len);
            }
            assert_descriptor_valid(&data[pos..pos + len]);
            pos += len;
        }
        let data_at = if let Some(tag) = inline_tag {
            let owner_at = owner_lba as usize * SECTOR_BYTES;
            let owner = &mut image[owner_at..owner_at + SECTOR_BYTES];
            let mut inline = [0u8; SECTOR_BYTES];
            inline[..64].copy_from_slice(&owner[..64]);
            inline[0..2].copy_from_slice(&tag.to_le_bytes());
            inline[34..36].copy_from_slice(&3u16.to_le_bytes());
            if tag == 266 {
                inline[64..72].copy_from_slice(&(data.len() as u64).to_le_bytes());
            }
            let ea_at = if tag == 261 { 168 } else { 208 };
            let ea_start = ea_at + 8;
            let ad_at = ea_start + 24;
            let end = ad_at + data.len();
            inline[ea_at..ea_at + 4].copy_from_slice(&24u32.to_le_bytes());
            inline[ea_at + 4..ea_at + 8].copy_from_slice(&(data.len() as u32).to_le_bytes());
            let ea = &mut inline[ea_start..ad_at];
            ea[0..2].copy_from_slice(&262u16.to_le_bytes());
            ea[2..4].copy_from_slice(&3u16.to_le_bytes());
            ea[12..16].copy_from_slice(&meta_lba.to_le_bytes());
            ea[16..20].copy_from_slice(&u32::MAX.to_le_bytes());
            ea[20..24].copy_from_slice(&u32::MAX.to_le_bytes());
            oracle_finish(ea, 24);
            inline[ad_at..end].copy_from_slice(&data);
            // Bytes beyond the descriptor are deliberately nonzero, and must
            // neither enter its CRC nor be changed by the rename.
            inline[end..].fill(0xa5);
            oracle_finish(&mut inline, end);
            owner.copy_from_slice(&inline);
            assert_descriptor_valid(owner);
            owner_at + ad_at
        } else {
            let extents = fs.read_icb_extents(&mut raw, meta_lba).unwrap();
            assert_eq!(extents.len(), 1);
            fs.meta_to_abs(extents[0].lba).unwrap() as usize * SECTOR_BYTES
        };
        RenameFixture {
            image,
            owner_lba,
            data_at,
            data_len: data.len(),
            fid_lengths,
        }
    }

    fn assert_rename_integrity(inline_tag: Option<u16>) {
        let fixture = rename_fixture(inline_tag);
        let mut raw = crate::test_util::MemSource::new(fixture.image.clone());
        let fs = super::super::read_filesystem(&mut raw).unwrap();
        let patches = fs.name_patches(&mut raw, TWO_RULES).unwrap();
        assert!(!patches.is_empty());
        let mut prepared =
            crate::whole_disc::UnitAligned::new(raw, Vec::new()).with_name_patches(patches.clone());
        let mut output = vec![0; fixture.image.len()];
        for (lba, sector) in output.chunks_mut(SECTOR_BYTES).enumerate() {
            prepared.read_sectors(lba as u32, 1, sector, false).unwrap();
        }
        let owner_at = fixture.owner_lba as usize * SECTOR_BYTES;
        let owner = &output[owner_at..owner_at + SECTOR_BYTES];
        assert_descriptor_valid(owner);
        let mut expected = fixture.image.clone();
        let mut pos = fixture.data_at;
        for &len in &fixture.fid_lengths {
            assert_descriptor_valid(&output[pos..pos + len]);
            let name_len = expected[pos + 19] as usize;
            let iu_len =
                u16::from_le_bytes(expected[pos + 36..pos + 38].try_into().unwrap()) as usize;
            let name_at = pos + 38 + iu_len;
            if let Some(rule) = TWO_RULES
                .iter()
                .find(|rule| parse_udf_name(&expected[name_at..name_at + name_len]) == rule.from)
            {
                expected[name_at + 1..name_at + name_len].copy_from_slice(rule.to.as_bytes());
                let end = 16
                    + u16::from_le_bytes(expected[pos + 10..pos + 12].try_into().unwrap()) as usize;
                oracle_finish(&mut expected[pos..pos + len], end);
            }
            pos += len;
        }
        assert_eq!(pos, fixture.data_at + fixture.data_len);
        if inline_tag.is_some() {
            let end = fixture.data_at + fixture.data_len - owner_at;
            oracle_finish(&mut expected[owner_at..owner_at + SECTOR_BYTES], end);
        } else {
            assert_eq!(owner, &fixture.image[owner_at..owner_at + SECTOR_BYTES]);
            assert!(patches.iter().all(|patch| patch.lba != fixture.owner_lba));
        }
        assert_eq!(
            output, expected,
            "only names and required checksums may change"
        );
        let renamed = super::super::read_filesystem(&mut prepared).unwrap();
        for rule in TWO_RULES {
            assert_eq!(
                renamed
                    .read_file(&mut prepared, &format!("/AACS/{}", rule.to))
                    .unwrap(),
                rule.from.as_bytes()
            );
        }
        assert_eq!(
            renamed.read_file(&mut prepared, "/AACS/other.dat").unwrap(),
            b"other.dat"
        );
        let mut raw = crate::whole_disc::UnitAligned::new(
            crate::test_util::MemSource::new(fixture.image.clone()),
            Vec::new(),
        );
        for (lba, original) in fixture.image.chunks(SECTOR_BYTES).enumerate() {
            let mut sector = [0; SECTOR_BYTES];
            raw.read_sectors(lba as u32, 1, &mut sector, false).unwrap();
            assert_eq!(sector.as_slice(), original, "raw bytes must stay unchanged");
        }
    }

    #[test]
    fn inline_fe_crc_covers_two_final_renames() {
        assert_rename_integrity(Some(261));
    }

    #[test]
    fn inline_efe_crc_covers_two_final_renames() {
        assert_rename_integrity(Some(266));
    }

    #[test]
    fn extent_backed_rename_leaves_owner_unchanged() {
        assert_rename_integrity(None);
    }

    #[test]
    fn inline_owner_invalid_crc_bounds_fail_only_for_an_actual_rename() {
        for tag in [261, 266] {
            let fixture = rename_fixture(Some(tag));
            let owner_at = fixture.owner_lba as usize * SECTOR_BYTES;
            let end = fixture.data_at + fixture.data_len - owner_at;
            // First runs past the declared descriptor, second past the sector.
            for crc_len in [end - 16 + 1, SECTOR_BYTES] {
                let mut image = fixture.image.clone();
                image[owner_at + 10..owner_at + 12]
                    .copy_from_slice(&(crc_len as u16).to_le_bytes());
                let mut raw = crate::test_util::MemSource::new(image);
                let fs = super::super::read_filesystem(&mut raw).unwrap();
                assert!(fs.name_patches(&mut raw, &[]).unwrap().is_empty());
                let absent = [NameRule {
                    from: "missing.inf",
                    ..RULE[0]
                }];
                assert!(fs.name_patches(&mut raw, &absent).unwrap().is_empty());
                assert!(matches!(
                    fs.name_patches(&mut raw, TWO_RULES),
                    Err(Error::DiscRead { sector, .. }) if sector == fixture.owner_lba as u64
                ));
            }
        }
    }

    #[test]
    fn inline_owner_zero_crc_length_stays_omitted() {
        for tag in [261, 266] {
            let fixture = rename_fixture(Some(tag));
            let owner_at = fixture.owner_lba as usize * SECTOR_BYTES;
            let mut image = fixture.image.clone();
            oracle_finish(&mut image[owner_at..owner_at + SECTOR_BYTES], 16);
            let mut raw = crate::test_util::MemSource::new(image.clone());
            let fs = super::super::read_filesystem(&mut raw).unwrap();
            let patches = fs.name_patches(&mut raw, TWO_RULES).unwrap();
            for patch in patches {
                image[patch.lba as usize * SECTOR_BYTES + patch.offset] = patch.value;
            }
            assert_eq!(&image[owner_at + 8..owner_at + 12], &[0, 0, 0, 0]);
            assert_descriptor_valid(&image[owner_at..owner_at + SECTOR_BYTES]);
        }
    }

    #[test]
    fn inline_owner_short_crc_coverage_is_preserved() {
        for tag in [261, 266] {
            let fixture = rename_fixture(Some(tag));
            let owner_at = fixture.owner_lba as usize * SECTOR_BYTES;
            let data_at = fixture.data_at - owner_at;
            // Cover just the unchanged parent FID, then a prefix ending inside
            // the first renamed name. Preserve both short declared intervals.
            for end in [
                data_at + fixture.fid_lengths[0],
                data_at + fixture.fid_lengths[0] + 47,
            ] {
                assert!(end < data_at + fixture.data_len);
                let mut image = fixture.image.clone();
                oracle_finish(&mut image[owner_at..owner_at + SECTOR_BYTES], end);
                let mut raw = crate::test_util::MemSource::new(image.clone());
                let fs = super::super::read_filesystem(&mut raw).unwrap();
                let patches = fs.name_patches(&mut raw, TWO_RULES).unwrap();
                for patch in patches {
                    image[patch.lba as usize * SECTOR_BYTES + patch.offset] = patch.value;
                }
                assert_eq!(
                    u16::from_le_bytes(image[owner_at + 10..owner_at + 12].try_into().unwrap()),
                    (end - 16) as u16
                );
                assert_descriptor_valid(&image[owner_at..owner_at + SECTOR_BYTES]);
            }
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
