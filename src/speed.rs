//! Drive speed control — query and set read speeds.
//!
//! Uses MMC-6 SET CD SPEED (0xBB) command.
//! Reference: MMC-6 §6.30

/// Disc read speed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum DriveSpeed {
    /// Blu-ray 1x = 4,500 KB/s
    BD1x,
    /// Blu-ray 2x = 9,000 KB/s
    BD2x,
    /// Blu-ray 4x = 18,000 KB/s
    BD4x,
    /// Blu-ray 6x = 27,000 KB/s
    BD6x,
    /// Blu-ray 8x = 36,000 KB/s
    BD8x,
    /// Blu-ray 10x = 45,000 KB/s
    BD10x,
    /// Blu-ray 12x = 54,000 KB/s
    BD12x,
    /// DVD 1x = 1,385 KB/s
    DVD1x,
    /// DVD 2x = 2,770 KB/s
    DVD2x,
    /// DVD 4x = 5,540 KB/s
    DVD4x,
    /// DVD 8x = 11,080 KB/s
    DVD8x,
    /// DVD 16x = 22,160 KB/s
    DVD16x,
    /// Maximum speed — drive decides
    Max,
}

impl DriveSpeed {
    /// Convert to KB/s for MMC-6 SET CD SPEED command.
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

    /// Create from KB/s value, rounding to nearest standard speed.
    pub fn from_kbps(kbps: u16) -> Self {
        match kbps {
            0..=2_000         => DriveSpeed::DVD1x,
            2_001..=4_000     => DriveSpeed::DVD2x,
            4_001..=6_000     => DriveSpeed::BD1x,
            6_001..=13_000    => DriveSpeed::BD2x,
            13_001..=22_000   => DriveSpeed::BD4x,
            22_001..=31_000   => DriveSpeed::BD6x,
            31_001..=40_000   => DriveSpeed::BD8x,
            40_001..=49_000   => DriveSpeed::BD10x,
            49_001..=0xFFFE   => DriveSpeed::BD12x,
            0xFFFF            => DriveSpeed::Max,
            _                 => DriveSpeed::Max,
        }
    }

    /// Human-readable label.
    pub fn label(&self) -> &'static str {
        match self {
            DriveSpeed::BD1x   => "BD 1x",
            DriveSpeed::BD2x   => "BD 2x",
            DriveSpeed::BD4x   => "BD 4x",
            DriveSpeed::BD6x   => "BD 6x",
            DriveSpeed::BD8x   => "BD 8x",
            DriveSpeed::BD10x  => "BD 10x",
            DriveSpeed::BD12x  => "BD 12x",
            DriveSpeed::DVD1x  => "DVD 1x",
            DriveSpeed::DVD2x  => "DVD 2x",
            DriveSpeed::DVD4x  => "DVD 4x",
            DriveSpeed::DVD8x  => "DVD 8x",
            DriveSpeed::DVD16x => "DVD 16x",
            DriveSpeed::Max    => "Max",
        }
    }

    /// All standard Blu-ray speeds.
    pub fn all_bd() -> &'static [DriveSpeed] {
        &[DriveSpeed::BD1x, DriveSpeed::BD2x, DriveSpeed::BD4x,
          DriveSpeed::BD6x, DriveSpeed::BD8x, DriveSpeed::BD10x, DriveSpeed::BD12x]
    }

    /// All standard DVD speeds.
    pub fn all_dvd() -> &'static [DriveSpeed] {
        &[DriveSpeed::DVD1x, DriveSpeed::DVD2x, DriveSpeed::DVD4x,
          DriveSpeed::DVD8x, DriveSpeed::DVD16x]
    }
}

impl std::fmt::Display for DriveSpeed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({} KB/s)", self.label(), self.to_kbps())
    }
}

/// Build SET CD SPEED CDB — MMC-6 §6.30
pub fn set_cd_speed_cdb(read_speed: DriveSpeed) -> [u8; 12] {
    let kbps = read_speed.to_kbps();
    [
        0xBB,                       // SET CD SPEED opcode
        0x00,                       // reserved
        (kbps >> 8) as u8,          // read speed MSB
        kbps as u8,                 // read speed LSB
        0xFF,                       // write speed MSB (0xFFFF = don't change)
        0xFF,                       // write speed LSB
        0x00, 0x00, 0x00, 0x00,    // reserved
        0x00, 0x00,                 // reserved
    ]
}
