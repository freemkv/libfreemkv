use crate::sector::SectorSource;
use std::collections::HashMap;

/// PART_START == META_START: file LBAs (partition-relative) and ICB/dir LBAs
/// (metadata-relative) share one address space (abs = PART_START + lba), so
/// `read_filesystem` takes the single-partition path.
pub(crate) const PART_START: u32 = 2000;

/// In-memory `SectorSource` (absolute-LBA → 2048-byte sector map); unmapped
/// sectors read as zeroes.
pub(crate) struct MemDisc {
    sectors: HashMap<u32, [u8; 2048]>,
}

impl MemDisc {
    pub(crate) fn new() -> Self {
        Self {
            sectors: HashMap::new(),
        }
    }
    fn put(&mut self, lba: u32, data: [u8; 2048]) {
        self.sectors.insert(lba, data);
    }
    /// Write arbitrary-length bytes at `lba`, split across 2048-byte sectors.
    pub(crate) fn put_bytes(&mut self, lba: u32, bytes: &[u8]) {
        for (i, chunk) in bytes.chunks(2048).enumerate() {
            let mut s = [0u8; 2048];
            s[..chunk.len()].copy_from_slice(chunk);
            self.put(lba + i as u32, s);
        }
    }
}

impl SectorSource for MemDisc {
    fn read_sectors(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        _recovery: bool,
    ) -> crate::error::Result<usize> {
        let need = count as usize * 2048;
        for i in 0..count as u32 {
            let off = i as usize * 2048;
            let s = self.sectors.get(&(lba + i)).copied().unwrap_or([0u8; 2048]);
            buf[off..off + 2048].copy_from_slice(&s);
        }
        Ok(need)
    }
}

/// One file's placement: ICB metadata LBA, data-extent LBA, byte length,
/// Long-AD (16-byte, real BD-ROM layout) vs Short-AD, optional contents.
pub(crate) struct FileSpec {
    pub(crate) name: String,
    pub(crate) icb_lba: u32,
    pub(crate) data_lba: u32,
    pub(crate) size: u64,
    pub(crate) long_ad: bool,
    pub(crate) contents: Vec<u8>,
    /// When non-empty, the file's ICB carries exactly these `(partition-relative
    /// LBA, byte length)` allocation descriptors (in order) instead of the single
    /// `data_lba`/`size` one. A zero length terminates the list, as on a real disc.
    pub(crate) ads: Vec<(u32, u32)>,
}

/// A directory node: ICB LBA, FID-list LBA, child files and subdirectories.
pub(crate) struct DirSpec {
    pub(crate) name: String,
    pub(crate) icb_lba: u32,
    pub(crate) dir_data_lba: u32,
    pub(crate) files: Vec<FileSpec>,
    pub(crate) subdirs: Vec<DirSpec>,
}

/// Build an Extended File Entry ICB (tag 266) with one allocation descriptor.
pub(crate) fn build_file_icb(size: u64, data_lba: u32, long_ad: bool) -> [u8; 2048] {
    let mut s = [0u8; 2048];
    s[0..2].copy_from_slice(&266u16.to_le_bytes()); // Extended File Entry
    if long_ad {
        s[34..36].copy_from_slice(&1u16.to_le_bytes()); // ICB flags → Long AD
    }
    s[56..64].copy_from_slice(&size.to_le_bytes()); // info_length
    s[208..212].copy_from_slice(&0u32.to_le_bytes()); // l_ea
    let ad_size: u32 = if long_ad { 16 } else { 8 };
    s[212..216].copy_from_slice(&ad_size.to_le_bytes()); // l_ad
    // AD length field is 32-bit; a size beyond u32 can't be backed by it,
    // which is the disc-vs-reality mismatch a hostile fixture must express.
    let ad_len = (size.min(u32::MAX as u64) as u32) & 0x3FFF_FFFF;
    s[216..220].copy_from_slice(&ad_len.to_le_bytes());
    s[220..224].copy_from_slice(&data_lba.to_le_bytes());
    s
}

/// Extended File Entry ICB carrying every `(lba, byte_len)` in `ads` as a recorded AD.
pub(crate) fn build_file_icb_ads(ads: &[(u32, u32)], long_ad: bool) -> [u8; 2048] {
    let mut s = [0u8; 2048];
    s[0..2].copy_from_slice(&266u16.to_le_bytes());
    if long_ad {
        s[34..36].copy_from_slice(&1u16.to_le_bytes());
    }
    let total: u64 = ads.iter().map(|a| a.1 as u64).sum();
    s[56..64].copy_from_slice(&total.to_le_bytes());
    let ad_size: usize = if long_ad { 16 } else { 8 };
    s[212..216].copy_from_slice(&((ads.len() * ad_size) as u32).to_le_bytes());
    let mut off = 216;
    for &(lba, len) in ads {
        s[off..off + 4].copy_from_slice(&(len & 0x3FFF_FFFF).to_le_bytes());
        s[off + 4..off + 8].copy_from_slice(&lba.to_le_bytes());
        off += ad_size;
    }
    s
}

fn build_dir_icb(dir_data_lba: u32, dir_data_len: u32) -> [u8; 2048] {
    build_file_icb(dir_data_len as u64, dir_data_lba, false)
}

/// Append one File Identifier Descriptor (tag 257) to `buf`.
fn push_fid(buf: &mut Vec<u8>, name: &str, icb_lba: u32, is_dir: bool, is_parent: bool) {
    let start = buf.len();
    let name_field: Vec<u8> = if is_parent {
        Vec::new()
    } else {
        let mut v = vec![0x08u8];
        v.extend_from_slice(name.as_bytes());
        v
    };
    let l_fi = name_field.len();
    let mut fid = vec![0u8; 38];
    fid[0..2].copy_from_slice(&257u16.to_le_bytes()); // FID tag
    let mut file_chars = 0u8;
    if is_dir {
        file_chars |= 0x02;
    }
    if is_parent {
        file_chars |= 0x08;
    }
    fid[18] = file_chars;
    fid[19] = l_fi as u8;
    fid[24..28].copy_from_slice(&icb_lba.to_le_bytes()); // ICB long_ad LBA @24
    fid[36..38].copy_from_slice(&0u16.to_le_bytes()); // l_iu @36
    buf.extend_from_slice(&fid);
    buf.extend_from_slice(&name_field);
    let used = buf.len() - start;
    let pad = (used + 3) & !3;
    buf.resize(start + pad, 0);
}

/// Recursively lay a [`DirSpec`] into the [`MemDisc`].
pub(crate) fn lay_dir(disc: &mut MemDisc, dir: &DirSpec) {
    let mut fids = Vec::new();
    push_fid(&mut fids, "", dir.icb_lba, true, true);
    for f in &dir.files {
        push_fid(&mut fids, &f.name, f.icb_lba, false, false);
        disc.put(
            PART_START + f.icb_lba,
            if f.ads.is_empty() {
                build_file_icb(f.size, f.data_lba, f.long_ad)
            } else {
                build_file_icb_ads(&f.ads, f.long_ad)
            },
        );
        if !f.contents.is_empty() {
            disc.put_bytes(PART_START + f.data_lba, &f.contents);
        }
    }
    for sub in &dir.subdirs {
        push_fid(&mut fids, &sub.name, sub.icb_lba, true, false);
    }
    disc.put(
        PART_START + dir.icb_lba,
        build_dir_icb(dir.dir_data_lba, fids.len() as u32),
    );
    disc.put_bytes(PART_START + dir.dir_data_lba, &fids);
    for sub in &dir.subdirs {
        lay_dir(disc, sub);
    }
}

/// Build the static UDF anchor/VDS/FSD so `read_filesystem` reaches
/// `root_icb_lba` (single partition map → metadata_start == PART_START).
pub(crate) fn build_udf_skeleton(disc: &mut MemDisc, root_icb_lba: u32) {
    let mut avdp = [0u8; 2048];
    avdp[0..2].copy_from_slice(&2u16.to_le_bytes());
    disc.put(256, avdp);

    let mut pd = [0u8; 2048];
    pd[0..2].copy_from_slice(&5u16.to_le_bytes());
    pd[188..192].copy_from_slice(&PART_START.to_le_bytes());
    disc.put(32, pd);

    let mut lvd = [0u8; 2048];
    lvd[0..2].copy_from_slice(&6u16.to_le_bytes());
    lvd[268..272].copy_from_slice(&1u32.to_le_bytes());
    disc.put(33, lvd);

    let mut td = [0u8; 2048];
    td[0..2].copy_from_slice(&8u16.to_le_bytes());
    disc.put(34, td);

    let mut fsd = [0u8; 2048];
    fsd[0..2].copy_from_slice(&256u16.to_le_bytes());
    fsd[404..408].copy_from_slice(&root_icb_lba.to_le_bytes());
    disc.put(PART_START, fsd);
}

pub(crate) fn file(name: &str, icb_lba: u32, data_lba: u32, size: u64, long_ad: bool) -> FileSpec {
    FileSpec {
        name: name.to_string(),
        icb_lba,
        data_lba,
        size,
        long_ad,
        contents: Vec::new(),
        ads: Vec::new(),
    }
}

pub(crate) fn file_with(
    name: &str,
    icb_lba: u32,
    data_lba: u32,
    contents: Vec<u8>,
    long_ad: bool,
) -> FileSpec {
    FileSpec {
        name: name.to_string(),
        icb_lba,
        data_lba,
        size: contents.len() as u64,
        long_ad,
        contents,
        ads: Vec::new(),
    }
}

/// A file whose ICB lists exactly `ads` as `(partition-relative LBA, byte length)`.
pub(crate) fn file_ads(name: &str, icb_lba: u32, ads: &[(u32, u32)], long_ad: bool) -> FileSpec {
    FileSpec {
        name: name.to_string(),
        icb_lba,
        data_lba: 0,
        size: 0,
        long_ad,
        contents: Vec::new(),
        ads: ads.to_vec(),
    }
}
