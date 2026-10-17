//! Persistent player storage used by BD+ content code.
//!
//! A [BD+ media-transform patent] identifies Player Storage Slots as the
//! Security VM's nonvolatile storage facility.  This type deliberately models
//! the portable record image without choosing a filesystem, database or AACS
//! provider; the application owns loading and saving it.
//!
//! [BD+ media-transform patent]: ../docs/kr20080012724_bdplus_fixup_media_transform.pdf "upstream: https://patentimages.storage.googleapis.com/5e/dd/22/1ceccd7eef9801/KR20080012724A.pdf"

use super::{BdPlusError, BdPlusResult};

pub const SLOT_COUNT: usize = 500;
pub const SLOT_SIZE: usize = 256;
pub const SLOT_IMAGE_SIZE: usize = SLOT_COUNT * SLOT_SIZE;

const AUTH_HASH: std::ops::Range<usize> = 48..68;

/// Provider-neutral persistent image of the player's BD+ storage slots.
#[derive(Clone, PartialEq, Eq)]
pub struct BdPlusSlotStorage {
    records: Vec<[u8; SLOT_SIZE]>,
}

impl std::fmt::Debug for BdPlusSlotStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BdPlusSlotStorage")
            .field("records", &self.records.len())
            .finish()
    }
}

impl Default for BdPlusSlotStorage {
    fn default() -> Self {
        Self {
            records: vec![[0; SLOT_SIZE]; SLOT_COUNT],
        }
    }
}

impl BdPlusSlotStorage {
    /// Decode the stable concatenation of all 500 256-byte records.
    pub fn from_image(image: &[u8]) -> BdPlusResult<Self> {
        if image.len() != SLOT_IMAGE_SIZE {
            return Err(BdPlusError::SlotImageLength {
                length: image.len(),
            });
        }
        let mut records = vec![[0; SLOT_SIZE]; SLOT_COUNT];
        let (source_records, remainder) = image.as_chunks::<SLOT_SIZE>();
        debug_assert!(remainder.is_empty());
        for (destination, source) in records.iter_mut().zip(source_records) {
            *destination = *source;
        }
        Ok(Self { records })
    }

    /// Encode a portable image for storage by the application.
    pub fn to_image(&self) -> Vec<u8> {
        let mut image = Vec::with_capacity(SLOT_IMAGE_SIZE);
        for record in &self.records {
            image.extend_from_slice(record);
        }
        image
    }

    pub fn record(&self, index: usize) -> Option<&[u8; SLOT_SIZE]> {
        self.records.get(index)
    }

    pub(crate) fn record_mut(&mut self, index: usize) -> Option<&mut [u8; SLOT_SIZE]> {
        self.records.get_mut(index)
    }
}

/// Per-session privileged attachment state; never serialized as slot content.
#[derive(Clone, Debug)]
pub(crate) struct SlotSession {
    pub(crate) storage: BdPlusSlotStorage,
    attached: usize,
    next_allocation: Option<usize>,
    status: [u32; 2],
}

impl Default for SlotSession {
    fn default() -> Self {
        Self::new(BdPlusSlotStorage::default())
    }
}

impl SlotSession {
    pub(crate) fn new(storage: BdPlusSlotStorage) -> Self {
        Self {
            storage,
            attached: 0,
            next_allocation: Some(SLOT_COUNT - 1),
            status: [0, 7],
        }
    }

    pub(crate) fn reset_attachment(&mut self) {
        self.attached = 0;
        self.status[0] = 0;
    }

    pub(crate) fn allocate(&mut self, volume_id: [u8; 16]) -> Option<u32> {
        let index = self.next_allocation?;
        let record = self.storage.record_mut(index)?;
        record.fill(0);
        record[..16].copy_from_slice(&volume_id);
        record[16..32].copy_from_slice(&volume_id);
        self.attached = index;
        self.next_allocation = index.checked_sub(1);
        if self.status[1] > 2 {
            self.status[1] -= 1;
        }
        self.status[0] = self.status[1];
        Some(index as u32)
    }

    pub(crate) fn authenticate(&mut self, index: usize, digest: &[u8; 20]) -> bool {
        let authenticated = self
            .storage
            .record(index)
            .is_some_and(|record| record[AUTH_HASH] == digest[..]);
        self.attached = if authenticated { index } else { 0 };
        authenticated
    }

    pub(crate) fn read(&self, index: usize) -> Option<[u8; SLOT_SIZE]> {
        let mut record = *self.storage.record(index)?;
        if index != self.attached {
            record[AUTH_HASH].fill(0);
        }
        Some(record)
    }

    pub(crate) fn status_bytes(&self) -> [u8; 12] {
        let mut bytes = [0; 12];
        bytes[..4].copy_from_slice(&(self.attached as u32).to_be_bytes());
        bytes[4..8].copy_from_slice(&self.status[0].to_be_bytes());
        bytes[8..].copy_from_slice(&self.status[1].to_be_bytes());
        bytes
    }

    pub(crate) fn write_attached(&mut self, source: &[u8; SLOT_SIZE]) {
        let record = &mut self.storage.records[self.attached];
        record[16..68].copy_from_slice(&source[16..68]);
        let sequence = u32::from_be_bytes([record[72], record[73], record[74], record[75]]);
        record[72..76].copy_from_slice(&sequence.wrapping_add(1).to_be_bytes());
        record[76..].copy_from_slice(&source[76..]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn image_round_trip_is_exact_and_debug_redacts_records() {
        let mut image = vec![0; SLOT_IMAGE_SIZE];
        image[17] = 0x5a;
        let storage = BdPlusSlotStorage::from_image(&image).unwrap();
        assert_eq!(storage.to_image(), image);
        assert_eq!(format!("{storage:?}"), "BdPlusSlotStorage { records: 500 }");
        assert_eq!(
            BdPlusSlotStorage::from_image(&image[..image.len() - 1]),
            Err(BdPlusError::SlotImageLength {
                length: SLOT_IMAGE_SIZE - 1
            })
        );
    }

    #[test]
    fn attachment_controls_hash_visibility_and_write_preserves_owner_fields() {
        let mut image = vec![0; SLOT_IMAGE_SIZE];
        let record = &mut image[SLOT_SIZE..SLOT_SIZE * 2];
        record[..16].fill(0x11);
        record[48..68].fill(0x22);
        record[68..72].fill(0x33);
        record[72..76].copy_from_slice(&9_u32.to_be_bytes());
        let mut slots = SlotSession::new(BdPlusSlotStorage::from_image(&image).unwrap());

        assert_eq!(slots.read(1).unwrap()[48..68], [0; 20]);
        assert!(slots.authenticate(1, &[0x22; 20]));
        assert_eq!(slots.read(1).unwrap()[48..68], [0x22; 20]);

        let mut update = [0; SLOT_SIZE];
        update[16..68].fill(0x44);
        update[68..72].fill(0x55);
        update[76..].fill(0x66);
        slots.write_attached(&update);
        let stored = slots.storage.record(1).unwrap();
        assert_eq!(stored[..16], [0x11; 16]);
        assert_eq!(stored[16..68], [0x44; 52]);
        assert_eq!(stored[68..72], [0x33; 4]);
        assert_eq!(stored[72..76], 10_u32.to_be_bytes());
        assert_eq!(stored[76..], [0x66; 180]);
    }
}
