use super::{Reject, Result};

pub(super) const MAX_BYTES: usize = 2 * 1024 * 1024;
pub(super) const MAX_RECORDS: usize = 4096;
pub(super) const MAX_CELLS: usize = 65536;

pub(super) struct Reader<'a> {
    pub data: &'a [u8],
    pub pos: usize,
}

impl<'a> Reader<'a> {
    pub fn new(data: &'a [u8]) -> Result<Self> {
        if data.len() > MAX_BYTES {
            return Err(Reject::Budget);
        }
        Ok(Self { data, pos: 0 })
    }
    pub fn take(&mut self, size: usize) -> Result<&'a [u8]> {
        let end = self.pos.checked_add(size).ok_or(Reject::Budget)?;
        let bytes = self.data.get(self.pos..end).ok_or(Reject::Truncated)?;
        self.pos = end;
        Ok(bytes)
    }
    pub fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    pub fn u16(&mut self) -> Result<u16> {
        let b = self.take(2)?;
        Ok(u16::from_be_bytes([b[0], b[1]]))
    }
    pub fn u32(&mut self) -> Result<u32> {
        let b = self.take(4)?;
        Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }
    pub fn at(&self, pos: usize) -> Result<Self> {
        if pos > self.data.len() {
            return Err(Reject::Truncated);
        }
        Ok(Self {
            data: self.data,
            pos,
        })
    }
    pub fn string(&self, pos: usize) -> Result<&'a [u8]> {
        if pos == 0 {
            return Ok(&[]);
        }
        let bytes = self.data.get(pos..).ok_or(Reject::Truncated)?;
        let len = bytes
            .iter()
            .take(4097)
            .position(|b| *b == 0)
            .ok_or(Reject::Invalid)?;
        if len > 4096 {
            return Err(Reject::Budget);
        }
        Ok(&bytes[..len])
    }
    pub fn magic(&mut self, magic: &[u8], version: u32) -> Result<()> {
        if self.take(magic.len())? != magic || self.u32()? != version {
            return Err(Reject::Unsupported);
        }
        Ok(())
    }
}

pub(super) fn count(value: usize, limit: usize) -> Result<usize> {
    if value > limit {
        Err(Reject::Budget)
    } else {
        Ok(value)
    }
}
