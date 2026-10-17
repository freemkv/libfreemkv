//! BD+ CPU memory semantics and checked host transfers.

use super::{BdPlusError, BdPlusResult};

/// Size of the BD+ virtual address space.
pub const MEMORY_SIZE: usize = 0x40_0000;
const ADDRESS_MASK: u32 = (MEMORY_SIZE - 1) as u32;

/// Owned big-endian VM memory, isolated from the application's memory.
/// CPU accesses mask the address and align it down to the access width. Host
/// transfers require an explicit, non-wrapping in-range address.
pub struct BdPlusMemory {
    bytes: Vec<u8>,
}

impl Default for BdPlusMemory {
    fn default() -> Self {
        Self {
            bytes: vec![0; MEMORY_SIZE],
        }
    }
}

impl std::fmt::Debug for BdPlusMemory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BdPlusMemory")
            .field("length", &self.bytes.len())
            .finish()
    }
}

impl BdPlusMemory {
    /// Read a byte with architectural address wrapping.
    pub fn load_u8(&self, address: u32) -> u8 {
        self.bytes[(address & ADDRESS_MASK) as usize]
    }
    /// Read a big-endian halfword, masking and aligning down the address.
    pub fn load_u16(&self, address: u32) -> u16 {
        let start = (address & ADDRESS_MASK & !1) as usize;
        u16::from_be_bytes(self.bytes[start..start + 2].try_into().unwrap())
    }
    /// Read a big-endian word, masking and aligning down the address.
    pub fn load_u32(&self, address: u32) -> u32 {
        let start = (address & ADDRESS_MASK & !3) as usize;
        u32::from_be_bytes(self.bytes[start..start + 4].try_into().unwrap())
    }
    /// Store a byte with architectural address wrapping.
    pub fn store_u8(&mut self, address: u32, value: u8) {
        self.bytes[(address & ADDRESS_MASK) as usize] = value;
    }
    /// Store a big-endian halfword with architectural address masking/alignment.
    pub fn store_u16(&mut self, address: u32, value: u16) {
        let start = (address & ADDRESS_MASK & !1) as usize;
        self.bytes[start..start + 2].copy_from_slice(&value.to_be_bytes());
    }
    /// Store a big-endian word with architectural address masking/alignment.
    pub fn store_u32(&mut self, address: u32, value: u32) {
        let start = (address & ADDRESS_MASK & !3) as usize;
        self.bytes[start..start + 4].copy_from_slice(&value.to_be_bytes());
    }
    fn range(address: u32, length: usize) -> BdPlusResult<std::ops::Range<usize>> {
        let start = address as usize;
        let end = start.checked_add(length).filter(|end| *end <= MEMORY_SIZE);
        match end {
            Some(end) if start <= MEMORY_SIZE => Ok(start..end),
            _ => Err(BdPlusError::MemoryRange { address, length }),
        }
    }
    /// Borrow an exact host range without implicit address wrapping.
    pub fn read(&self, address: u32, length: usize) -> BdPlusResult<&[u8]> {
        Ok(&self.bytes[Self::range(address, length)?])
    }
    /// Borrow a mutable host range after checking the entire transfer.
    pub fn read_mut(&mut self, address: u32, length: usize) -> BdPlusResult<&mut [u8]> {
        Ok(&mut self.bytes[Self::range(address, length)?])
    }
    /// Copy an exact host transfer after validating its complete range.
    pub fn write(&mut self, address: u32, bytes: &[u8]) -> BdPlusResult<()> {
        self.read_mut(address, bytes.len())?.copy_from_slice(bytes);
        Ok(())
    }
}

#[cfg(test)]
#[path = "memory_tests.rs"]
mod tests;
