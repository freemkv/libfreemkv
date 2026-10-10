use super::binary::{MAX_RECORDS, Reader, count};
use super::{Reject, Result};

#[derive(Debug)]
pub(super) struct Resource<'a> {
    pub id: usize,
    pub kind: u8,
    pub flags: u16,
    #[cfg(test)]
    pub name: &'a [u8],
    pub locator: &'a [u8],
    /// Kind-specific trailing fields, excluding the optional flags64 byte.
    pub parameters: &'a [u8],
}

/// The reviewed runtime uses these fields for frame/time seek conversion,
/// not playlist identity or an automatic authored end boundary.
pub(super) fn whole_resource_parameters(resource: &Resource<'_>) -> Result<()> {
    if resource.kind != 64 || resource.flags != 0 || resource.parameters.len() != 14 {
        return Err(Reject::Unsupported);
    }
    let p = resource.parameters;
    let numerator = u16::from_be_bytes([p[2], p[3]]);
    let denominator = u16::from_be_bytes([p[4], p[5]]);
    let duration = u32::from_be_bytes([p[6], p[7], p[8], p[9]]);
    if numerator == 0 || denominator == 0 || duration > i32::MAX as u32 {
        return Err(Reject::Unsupported);
    }
    Ok(())
}

/// aat's first qe configuration argument is the fixed h stack capacity.
/// The versioned configuration block precedes the next header section; never
/// accept a pointer into a resource string/entry as a configuration value.
pub(super) fn stack_capacity(data: &[u8]) -> Result<usize> {
    let mut r = Reader::new(data)?;
    r.magic(b"QCDF", 4)?;
    let config = r.at(8)?.u32()? as usize;
    let following = r.at(12)?.u32()? as usize;
    let table = r.at(16)?.u32()? as usize;
    if config < 32
        || following < config.checked_add(12).ok_or(Reject::Budget)?
        || following > table
        || table > data.len()
    {
        return Err(Reject::Invalid);
    }
    r.at(config)?.take(12)?;
    let slots = r.at(config + 4)?.u16()? as usize;
    if slots == 0 {
        return Err(Reject::Invalid);
    }
    Ok(slots)
}

pub(super) fn parse(data: &[u8]) -> Result<Vec<Resource<'_>>> {
    let mut r = Reader::new(data)?;
    r.magic(b"QCDF", 4)?;
    let table = r.at(16)?.u32()? as usize;
    let entries = count(r.at(26)?.u16()? as usize, MAX_RECORDS)?;
    if table < 28 {
        return Err(Reject::Invalid);
    }
    let mut index = r.at(table)?;
    index.take(entries * 4)?;
    index.pos = table;
    let lower = table.checked_add(entries * 4).ok_or(Reject::Budget)?;
    let mut offsets = Vec::with_capacity(entries + 1);
    for _ in 0..entries {
        let offset = (index.u32()? as usize)
            .checked_add(8)
            .ok_or(Reject::Budget)?;
        if offset < lower || offset >= data.len() || offsets.last().is_some_and(|p| *p >= offset) {
            return Err(Reject::Invalid);
        }
        offsets.push(offset);
    }
    offsets.push(data.len());
    let mut resources = Vec::with_capacity(entries);
    for i in 0..entries {
        let mut entry = Reader::new(&data[offsets[i]..offsets[i + 1]])?;
        let kind = entry.u8()?;
        let flags = entry.u16()?;
        // Flags are resource semantics, not an entry-width mask. Retain them
        // for the consumer; only bit 64 adds a byte to this version's grammar.
        if kind == 16 {
            entry.take(24)?;
        }
        let name = entry.string(entry.pos)?;
        entry.take(name.len() + 1)?;
        let locator = entry.string(entry.pos)?;
        entry.take(locator.len() + 1)?;
        let parameters_start = entry.pos;
        match kind {
            64 => {
                entry.take(14)?;
            }
            1 => {
                entry.take(1)?;
            }
            2 | 3 | 4 | 5 | 8 | 16 | 32 | 33 | 65 => {}
            _ => return Err(Reject::Unsupported),
        }
        let parameters = &entry.data[parameters_start..entry.pos];
        if flags & 64 != 0 {
            entry.take(1)?;
        }
        if entry.pos != entry.data.len() {
            return Err(Reject::Invalid);
        }
        resources.push(Resource {
            id: i + 1,
            kind,
            flags,
            #[cfg(test)]
            name,
            locator,
            parameters,
        });
    }
    Ok(resources)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seek_metadata_does_not_define_playlist_identity_or_authored_range() {
        let mut parameters = [0_u8; 14];
        parameters[2..4].copy_from_slice(&24000_u16.to_be_bytes());
        parameters[4..6].copy_from_slice(&1001_u16.to_be_bytes());
        parameters[6..10].copy_from_slice(&3_000_000_u32.to_be_bytes());
        let check = |p: &[u8]| {
            whole_resource_parameters(&Resource {
                id: 5,
                kind: 64,
                flags: 0,
                name: b"",
                locator: b"bd://PLAYLIST:00042.ITEM:0.V1:1",
                parameters: p,
            })
        };
        check(&parameters).unwrap();
        for duration in [0_u32, 1, 123456, i32::MAX as u32] {
            parameters[6..10].copy_from_slice(&duration.to_be_bytes());
            check(&parameters).unwrap();
        }
        parameters[0..2].fill(255);
        parameters[10..14].fill(255);
        check(&parameters).unwrap();
        for mutation in 0..3 {
            let mut bad = parameters;
            match mutation {
                0 => bad[2..4].fill(0),
                1 => bad[4..6].fill(0),
                _ => bad[6..10].copy_from_slice(&0x8000_0000_u32.to_be_bytes()),
            }
            assert!(check(&bad).is_err());
        }
        for length in 0..14 {
            assert!(check(&parameters[..length]).is_err());
        }
    }

    #[test]
    fn capacity_comes_from_bounded_config_not_resource_or_header_alias() {
        let mut data = vec![0; 64];
        data[..4].copy_from_slice(b"QCDF");
        data[4..8].copy_from_slice(&4_u32.to_be_bytes());
        data[8..12].copy_from_slice(&32_u32.to_be_bytes());
        data[12..16].copy_from_slice(&44_u32.to_be_bytes());
        data[16..20].copy_from_slice(&60_u32.to_be_bytes());
        for capacity in [1_u16, 100, 772, 4096, 65535] {
            data[36..38].copy_from_slice(&capacity.to_be_bytes());
            assert_eq!(stack_capacity(&data).unwrap(), usize::from(capacity));
        }
        for end in 0..44 {
            assert!(stack_capacity(&data[..end]).is_err());
        }
        for config in [0_u32, 8, 28, 40, 60, u32::MAX] {
            let mut bad = data.clone();
            bad[8..12].copy_from_slice(&config.to_be_bytes());
            assert!(stack_capacity(&bad).is_err());
        }
        data[36..38].fill(0);
        assert!(stack_capacity(&data).is_err());
    }
}
