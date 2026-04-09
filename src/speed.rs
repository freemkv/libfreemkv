//! Drive speed constants.

/// Common optical drive speeds with KB/s values for SET_CD_SPEED.
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
