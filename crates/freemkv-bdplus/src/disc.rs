//! Explicit disc and clock inputs; absence is never encoded as a secret value.

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BdPlusTimestamp {
    pub year: u16,
    pub month: u8,
    pub day: u8,
    pub hour: u8,
    pub minute: u8,
    pub second: u8,
    pub centisecond: u8,
}

/// Disc and dynamic discovery inputs supplied by the embedding application.
#[derive(Clone, Default)]
pub struct BdPlusDiscContext {
    pub volume_id: Option<[u8; 16]>,
    pub media_key: Option<[u8; 16]>,
    pub timestamp: Option<BdPlusTimestamp>,
    /// Player environment fields at Discovery(1,3) offsets 8..24.
    pub discovery_environment: Option<[u8; 16]>,
    pub discovery_tail: Option<[u8; 20]>,
}

impl std::fmt::Debug for BdPlusDiscContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BdPlusDiscContext")
            .field("volume_id_present", &self.volume_id.is_some())
            .field("media_key_present", &self.media_key.is_some())
            .field("timestamp_present", &self.timestamp.is_some())
            .field(
                "discovery_environment_present",
                &self.discovery_environment.is_some(),
            )
            .field("discovery_tail_present", &self.discovery_tail.is_some())
            .finish()
    }
}

impl BdPlusDiscContext {
    /// Assemble Discovery(1,3) only when all of its inputs were supplied.
    pub fn discovery_record(&self) -> Option<[u8; 60]> {
        let timestamp = self.timestamp?;
        let volume_id = self.volume_id?;
        let tail = self.discovery_tail?;
        let environment = self.discovery_environment?;
        let mut record = [0; 60];
        record[..2].copy_from_slice(&timestamp.year.to_be_bytes());
        record[2..8].copy_from_slice(&[
            timestamp.month,
            timestamp.day,
            timestamp.hour,
            timestamp.minute,
            timestamp.second,
            timestamp.centisecond,
        ]);
        record[8..24].copy_from_slice(&environment);
        record[24..40].copy_from_slice(&volume_id);
        record[40..60].copy_from_slice(&tail);
        Some(record)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_fields_cannot_synthesize_successful_discovery() {
        let mut disc = BdPlusDiscContext::default();
        assert_eq!(disc.discovery_record(), None);
        disc.volume_id = Some([0; 16]);
        disc.timestamp = Some(BdPlusTimestamp::default());
        assert_eq!(disc.discovery_record(), None);
        disc.discovery_tail = Some([0; 20]);
        assert_eq!(disc.discovery_record(), None);
        disc.discovery_environment = Some([0; 16]);
        assert_eq!(disc.discovery_record(), Some([0; 60]));
    }

    #[test]
    fn supplied_environment_fields_are_preserved_instead_of_zero_filled() {
        let disc = BdPlusDiscContext {
            volume_id: Some([0x44; 16]),
            timestamp: Some(BdPlusTimestamp {
                year: 2026,
                ..Default::default()
            }),
            discovery_environment: Some([0x55; 16]),
            discovery_tail: Some([0x66; 20]),
            ..Default::default()
        };
        let record = disc.discovery_record().unwrap();
        assert_eq!(&record[..2], &2026u16.to_be_bytes());
        assert_eq!(&record[8..24], &[0x55; 16]);
        assert_eq!(&record[24..40], &[0x44; 16]);
        assert_eq!(&record[40..], &[0x66; 20]);
    }

    #[test]
    fn debug_does_not_expose_disc_material() {
        let disc = BdPlusDiscContext {
            volume_id: Some([0xa5; 16]),
            media_key: Some([0xb6; 16]),
            ..Default::default()
        };
        let debug = format!("{disc:?}");
        assert!(debug.contains("media_key_present: true"));
        assert!(!debug.contains("165"));
        assert!(!debug.contains("182"));
    }
}
