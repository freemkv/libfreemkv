//! Check raw ZIP entries before zip's name index can coalesce duplicates.
use super::{Reject, Result};
use std::collections::BTreeSet;

fn short(bytes: &[u8], at: usize) -> Result<usize> {
    let b = bytes.get(at..at + 2).ok_or(Reject::Truncated)?;
    Ok(u16::from_le_bytes([b[0], b[1]]) as usize)
}
fn word(bytes: &[u8], at: usize) -> Result<usize> {
    let b = bytes.get(at..at + 4).ok_or(Reject::Truncated)?;
    Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]) as usize)
}

fn extra(bytes: &[u8], sizes: Option<(usize, usize)>) -> Result<()> {
    let mut at = 0;
    let mut seen = BTreeSet::new();
    while at < bytes.len() {
        let kind = short(bytes, at)?;
        let size = short(bytes, at + 2)?;
        if !seen.insert(kind) || matches!(kind, 0x7075 | 0x9901) {
            return Err(Reject::Unsupported); // alternate path or encryption
        }
        if kind == 1 {
            // Some authoring tools emit redundant 64-bit local sizes despite
            // ordinary non-sentinel 32-bit headers. Accept only exact agreement,
            // never a ZIP64 central-directory/offset override.
            let (uncompressed, compressed) = sizes.ok_or(Reject::Unsupported)?;
            if size != 16
                || word(bytes, at + 4)? != uncompressed
                || word(bytes, at + 8)? != 0
                || word(bytes, at + 12)? != compressed
                || word(bytes, at + 16)? != 0
            {
                return Err(Reject::Invalid);
            }
        }
        at = at.checked_add(4 + size).ok_or(Reject::Budget)?;
        if at > bytes.len() {
            return Err(Reject::Truncated);
        }
    }
    Ok(())
}

/// Classic single-disk ZIP plus matching redundant local 64-bit sizes.
/// No ZIP64 directory/offset overrides, encryption or ambiguous names.
pub(super) fn entries(bytes: &[u8], limit: usize) -> Result<usize> {
    if bytes.len() > 32 * 1024 * 1024 {
        return Err(Reject::Budget);
    }
    let end = bytes.len().checked_sub(22).ok_or(Reject::Truncated)?;
    let lower = end.saturating_sub(u16::MAX as usize);
    let eocd = (lower..=end)
        .rev()
        .find(|&at| {
            bytes.get(at..at + 4) == Some(b"PK\x05\x06")
                && short(bytes, at + 20).is_ok_and(|n| at + 22 + n == bytes.len())
        })
        .ok_or(Reject::Invalid)?;
    let count = short(bytes, eocd + 10)?;
    let size = word(bytes, eocd + 12)?;
    let start = word(bytes, eocd + 16)?;
    if short(bytes, eocd + 4)? != 0
        || short(bytes, eocd + 6)? != 0
        || short(bytes, eocd + 8)? != count
        || count > limit
        || start.checked_add(size) != Some(eocd)
    {
        return Err(Reject::Unsupported);
    }
    let mut at = start;
    let mut names = BTreeSet::new();
    let mut extents = Vec::new();
    for _ in 0..count {
        if bytes.get(at..at + 4) != Some(b"PK\x01\x02") {
            return Err(Reject::Invalid);
        }
        let flags = short(bytes, at + 8)?;
        let method = short(bytes, at + 10)?;
        let compressed = word(bytes, at + 20)?;
        let uncompressed = word(bytes, at + 24)?;
        let name_len = short(bytes, at + 28)?;
        let extra_len = short(bytes, at + 30)?;
        let comment_len = short(bytes, at + 32)?;
        let local_header = word(bytes, at + 42)?;
        if flags & (1 | 0x40 | 0x2000) != 0
            || !matches!(method, 0 | 8)
            || short(bytes, at + 34)? != 0
            || [compressed, uncompressed, local_header].contains(&(u32::MAX as usize))
            || local_header >= start
        {
            return Err(Reject::Unsupported);
        }
        let next = at + 46 + name_len + extra_len + comment_len;
        if next > eocd {
            return Err(Reject::Truncated);
        }
        let name = bytes
            .get(at + 46..at + 46 + name_len)
            .ok_or(Reject::Truncated)?;
        extra(
            &bytes[at + 46 + name_len..at + 46 + name_len + extra_len],
            None,
        )?;
        if name.is_empty()
            || !name.is_ascii()
            || name.contains(&0)
            || !names.insert(name)
            || bytes.get(local_header..local_header + 4) != Some(b"PK\x03\x04")
        {
            return Err(Reject::Invalid);
        }
        let local_name = short(bytes, local_header + 26)?;
        let local_extra = short(bytes, local_header + 28)?;
        let payload = local_header
            .checked_add(30 + local_name + local_extra)
            .ok_or(Reject::Budget)?;
        let payload_end = payload.checked_add(compressed).ok_or(Reject::Budget)?;
        if flags != short(bytes, local_header + 6)?
            || method != short(bytes, local_header + 8)?
            || bytes.get(local_header + 30..local_header + 30 + local_name) != Some(name)
            || payload_end > start
            || (flags & 8 == 0
                && (word(bytes, local_header + 14)? != word(bytes, at + 16)?
                    || word(bytes, local_header + 18)? != compressed
                    || word(bytes, local_header + 22)? != uncompressed))
        {
            return Err(Reject::Invalid);
        }
        extra(
            bytes
                .get(local_header + 30 + local_name..payload)
                .ok_or(Reject::Truncated)?,
            Some((uncompressed, compressed)),
        )?;
        let entry_end = if flags & 8 != 0 {
            let descriptor = if bytes.get(payload_end..payload_end + 4) == Some(b"PK\x07\x08") {
                payload_end + 4
            } else {
                payload_end
            };
            if word(bytes, descriptor)? != word(bytes, at + 16)?
                || word(bytes, descriptor + 4)? != compressed
                || word(bytes, descriptor + 8)? != uncompressed
                || descriptor + 12 > start
            {
                return Err(Reject::Invalid);
            }
            descriptor + 12
        } else {
            payload_end
        };
        extents.push((local_header, entry_end));
        at = next;
    }
    if at != eocd {
        return Err(Reject::Invalid);
    }
    extents.sort_unstable();
    if extents.windows(2).any(|pair| pair[0].1 > pair[1].0) {
        return Err(Reject::Invalid);
    }
    Ok(count)
}
