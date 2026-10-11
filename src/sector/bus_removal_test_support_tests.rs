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

/// Test harness: reads `inner`, then de-busses the landed bytes through
/// `debus_read` exactly as `Drive::remove_bus_encryption` does.
pub(crate) struct BusRemovalSectorSource<S: SectorSource> {
    inner: S,
    stage: BusStage,
    gate: Option<BusGate>,
}

impl<S: SectorSource> BusRemovalSectorSource<S> {
    pub(crate) fn new(inner: S, stage: BusStage) -> Self {
        Self {
            inner,
            stage,
            gate: None,
        }
    }

    pub(crate) fn with_bus_map(mut self, map: Arc<BusMap>) -> Self {
        self.gate = Some(BusGate::new(map));
        self
    }

    pub(crate) fn with_content_ranges(self, ranges: Arc<[(u32, u32)]>) -> Self {
        self.with_bus_map(Arc::new(BusMap::from_ranges(&ranges)))
    }

    pub(crate) fn inner(&self) -> &S {
        &self.inner
    }
}

impl<S: SectorSource> SectorSource for BusRemovalSectorSource<S> {
    fn capacity_sectors(&self) -> u32 {
        self.inner.capacity_sectors()
    }

    fn read_sectors(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        recovery: bool,
    ) -> Result<usize> {
        let n = self.inner.read_sectors(lba, count, buf, recovery)?;
        if let BusStage::AacsHostKey(rdk) = self.stage.clone() {
            let mut gate = self.gate.take();
            let inner = &mut self.inner;
            debus_read(gate.as_mut(), &mut buf[..n], &rdk, lba, &mut |h| {
                let mut s = [0u8; SECTOR_BYTES];
                match inner.read_sectors_fua(h, 1, &mut s, false, false) {
                    Ok(got) if got >= SECTOR_BYTES => Some(s[0]),
                    _ => None,
                }
            });
            self.gate = gate;
        }
        Ok(n)
    }
}
