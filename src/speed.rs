//! Drive speed constants.

/// Common optical drive speeds with KB/s values for SET_CD_SPEED.
///
/// Ordering is by [`to_kbps`](Self::to_kbps) throughput, not declaration
/// order — `PartialOrd`/`Ord` are implemented manually so e.g.
/// `DVD1x < BD1x` (1385 < 4500 KB/s) holds. A naive derive would have
/// ordered by variant position, making the slow DVD speeds sort above the
/// fast BD speeds. `Max` (0xFFFF) sorts highest, as intended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DriveSpeed {
    BD1x,
    BD2x,
    BD4x,
    BD6x,
    BD8x,
    BD10x,
    BD12x,
    DVD1x,
    DVD2x,
    DVD4x,
    DVD8x,
    DVD16x,
    Max,
}

impl DriveSpeed {
    /// Throughput in KB/s for the SET_CD_SPEED CDB. `Max` maps to the
    /// 0xFFFF sentinel that tells the drive to use its maximum speed.
    pub fn to_kbps(self) -> u16 {
        match self {
            DriveSpeed::BD1x => 4_500,
            DriveSpeed::BD2x => 9_000,
            DriveSpeed::BD4x => 18_000,
            DriveSpeed::BD6x => 27_000,
            DriveSpeed::BD8x => 36_000,
            DriveSpeed::BD10x => 45_000,
            DriveSpeed::BD12x => 54_000,
            DriveSpeed::DVD1x => 1_385,
            DriveSpeed::DVD2x => 2_770,
            DriveSpeed::DVD4x => 5_540,
            DriveSpeed::DVD8x => 11_080,
            DriveSpeed::DVD16x => 22_160,
            DriveSpeed::Max => 0xFFFF,
        }
    }
}

impl PartialOrd for DriveSpeed {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DriveSpeed {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.to_kbps().cmp(&other.to_kbps())
    }
}

impl std::fmt::Display for DriveSpeed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // `Max` is the "let the drive pick its maximum" sentinel; printing
        // its 0xFFFF KB/s value would read as a real (absurd) throughput.
        match self {
            DriveSpeed::Max => write!(f, "Max"),
            _ => write!(f, "{:?} ({} KB/s)", self, self.to_kbps()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordering_is_by_throughput_not_declaration() {
        assert!(DriveSpeed::DVD1x < DriveSpeed::BD1x);
        assert!(DriveSpeed::DVD16x < DriveSpeed::BD8x);
        assert!(DriveSpeed::BD12x < DriveSpeed::Max);
        let mut v = [DriveSpeed::Max, DriveSpeed::DVD1x, DriveSpeed::BD4x];
        v.sort();
        assert_eq!(v, [DriveSpeed::DVD1x, DriveSpeed::BD4x, DriveSpeed::Max]);
    }

    #[test]
    fn max_display_omits_sentinel_value() {
        assert_eq!(DriveSpeed::Max.to_string(), "Max");
        assert!(DriveSpeed::BD1x.to_string().contains("4500 KB/s"));
    }
}
