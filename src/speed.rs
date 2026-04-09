//! Drive speed management — zone-based speed table.
//!
//! Every DriveSession has a SpeedTable. Default: max speed everywhere.
//! After init(): calibrated per-zone speeds from disc surface probes.
//! One u32 comparison per read on the hot path.

/// Speed table — maps disc positions to optimal read speeds.
#[derive(Debug, Clone)]
pub struct SpeedTable {
    zones: Vec<(u32, u16)>,  // (start_lba, speed_kbs), sorted by lba
    current_speed: u16,
    next_boundary: u32,
}

impl SpeedTable {
    /// Default: max speed, whole disc. Drive manages itself.
    pub fn new() -> Self {
        SpeedTable {
            zones: vec![(0, 0xFFFF)],
            current_speed: 0,       // force first SET_CD_SPEED
            next_boundary: 0,       // force first lookup
        }
    }

    /// Hot path: has the speed zone changed for this LBA?
    /// Returns Some(speed_kbs) only when a SET_CD_SPEED is needed.
    #[inline]
    pub fn speed_for(&mut self, lba: u32) -> Option<u16> {
        if lba < self.next_boundary {
            return None;
        }
        self.transition(lba)
    }

    /// Zone transition — lookup + precompute next boundary.
    fn transition(&mut self, lba: u32) -> Option<u16> {
        let mut zone_idx = 0;
        for (i, &(start, _)) in self.zones.iter().enumerate() {
            if start <= lba {
                zone_idx = i;
            } else {
                break;
            }
        }

        let speed = self.zones[zone_idx].1;

        self.next_boundary = if zone_idx + 1 < self.zones.len() {
            self.zones[zone_idx + 1].0
        } else {
            u32::MAX
        };

        if speed == self.current_speed {
            return None;
        }

        self.current_speed = speed;
        Some(speed)
    }

    /// Load calibrated zones. Converts from platform probe data to generic (lba, kbs).
    /// `disc_sectors`: total disc capacity from READ CAPACITY.
    /// `probes`: (probe_address, speed_index) pairs from calibration scan.
    /// `probe_range`: max probe address space (0x10000 for MT1959).
    /// `speed_multiplier`: KB/s per speed unit (4500 for BD 1x).
    pub fn load_calibration(
        &mut self,
        disc_sectors: u32,
        probes: &[(u16, u8)],
        probe_range: u32,
        speed_multiplier: u16,
    ) {
        if probes.is_empty() || disc_sectors == 0 {
            return;
        }

        let mut zones: Vec<(u32, u16)> = Vec::new();

        for &(probe_addr, speed_idx) in probes {
            let lba = (probe_addr as u64 * disc_sectors as u64 / probe_range as u64) as u32;
            let kbs = speed_idx as u16 * speed_multiplier;
            zones.push((lba, kbs));
        }

        zones.sort_by_key(|&(lba, _)| lba);

        // Deduplicate: keep only zone boundaries where speed changes
        let mut deduped: Vec<(u32, u16)> = Vec::new();
        for &(lba, kbs) in &zones {
            if deduped.last().map_or(true, |&(_, prev_kbs)| prev_kbs != kbs) {
                deduped.push((lba, kbs));
            }
        }

        if deduped.is_empty() {
            return;
        }

        self.zones = deduped;
        self.current_speed = 0;
        self.next_boundary = 0;
    }

    /// Temporarily reduce speed for error recovery.
    pub fn reduce(&mut self) -> u16 {
        let speed = (self.current_speed / 2).max(4500);
        self.current_speed = speed;
        speed
    }

    /// Resume table-driven speed at this LBA.
    pub fn resume(&mut self, lba: u32) {
        self.current_speed = 0;
        self.next_boundary = 0;
        self.transition(lba);
    }

    /// Current speed in KB/s.
    pub fn current(&self) -> u16 {
        self.current_speed
    }

    /// Number of zones.
    pub fn zone_count(&self) -> usize {
        self.zones.len()
    }
}

// Keep DriveSpeed enum for CLI display
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum DriveSpeed {
    BD1x, BD2x, BD4x, BD6x, BD8x, BD10x, BD12x,
    DVD1x, DVD2x, DVD4x, DVD8x, DVD16x,
    Max,
}

impl DriveSpeed {
    pub fn to_kbps(self) -> u16 {
        match self {
            DriveSpeed::BD1x   => 4_500,
            DriveSpeed::BD2x   => 9_000,
            DriveSpeed::BD4x   => 18_000,
            DriveSpeed::BD6x   => 27_000,
            DriveSpeed::BD8x   => 36_000,
            DriveSpeed::BD10x  => 45_000,
            DriveSpeed::BD12x  => 54_000,
            DriveSpeed::DVD1x  => 1_385,
            DriveSpeed::DVD2x  => 2_770,
            DriveSpeed::DVD4x  => 5_540,
            DriveSpeed::DVD8x  => 11_080,
            DriveSpeed::DVD16x => 22_160,
            DriveSpeed::Max    => 0xFFFF,
        }
    }
}

impl std::fmt::Display for DriveSpeed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?} ({} KB/s)", self, self.to_kbps())
    }
}
