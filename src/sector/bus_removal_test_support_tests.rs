use super::*;

/// A leaf source reporting a fixed unmapped list; reads return zeroed sectors.
pub(crate) struct Reports(pub Vec<UnmappedStreamFile>);

impl SectorSource for Reports {
    fn capacity_sectors(&self) -> u32 {
        1 << 20
    }
    fn read_sectors(&mut self, _: u32, count: u16, buf: &mut [u8], _: bool) -> Result<usize> {
        let n = count as usize * SECTOR_BYTES;
        buf[..n].fill(0);
        Ok(n)
    }
    fn unmapped_stream_files(&self) -> &[UnmappedStreamFile] {
        &self.0
    }
}

pub(crate) fn m2ts1() -> UnmappedStreamFile {
    UnmappedStreamFile::new(
        "/BDMV/STREAM/00001.m2ts".into(),
        40,
        &Error::UdfAdChainTooLong,
    )
}

/// Through a generic bound, so the forwarding impl (not the vtable) answers.
pub(crate) fn unmapped_paths<S: SectorSource + ?Sized>(s: &S) -> Vec<String> {
    s.unmapped_stream_files()
        .iter()
        .map(|u| u.path.clone())
        .collect()
}

/// Every wrapper must relay its inner list, or an image built on it drops the
/// refusal. New SectorSource wrapper → assert it here.
pub(crate) fn assert_forwards<S: SectorSource>(over_reports: S) {
    assert_eq!(unmapped_paths(&over_reports), ["/BDMV/STREAM/00001.m2ts"]);
}
