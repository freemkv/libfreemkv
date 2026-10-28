//! A duplicated UDF 2.50 metadata mirror added to an [`encrypted_bd_image`].

use super::{BdFile, EncryptedBdImage, MemSource, encrypted_bd_image};
use crate::consts::SECTOR_BYTES;

/// Where [`add_metadata_mirror`] put the metadata partition's two copies (absolute LBAs).
#[derive(Debug, Clone, Copy)]
pub struct MetadataMirrorLayout {
    /// First sector of the main copy: the partition start.
    pub main: u32,
    /// First sector of the mirror copy, appended after the image.
    pub mirror: u32,
    /// Blocks in each copy.
    pub blocks: u32,
    /// The main sector holding `/AACS`'s file identifiers.
    pub aacs_dir: u32,
}

impl MetadataMirrorLayout {
    /// The mirror copy of main metadata sector `lba`.
    pub fn mirror_of(&self, lba: u32) -> u32 {
        lba - self.main + self.mirror
    }
}

/// Give `img` a duplicated metadata partition: main maps its existing metadata in place,
/// and two File Entries plus a byte-identical mirror copy are appended to the image.
pub fn add_metadata_mirror(img: &mut EncryptedBdImage) -> MetadataMirrorLayout {
    let mut src = MemSource::new(img.image.clone());
    let fs = crate::udf::read_filesystem(&mut src).expect("fixture UDF");
    let main = fs.partition_start();
    let aacs = fs.find_dir("/AACS").expect("fixture /AACS");
    let aacs_dir = fs.extents_abs_at(&mut src, aacs.meta_lba).expect("/AACS")[0].lba;
    // DirImage lays every metadata block before any file data.
    let inf = fs
        .file_extents(&mut src, "/AACS/Unit_Key_RO.inf")
        .expect("fixture Unit_Key_RO.inf");
    let data = img.files.iter().chain(&inf).map(|e| e.0).min().unwrap();
    let blocks = data - main;
    let fe = (img.image.len() / SECTOR_BYTES) as u32;
    let mirror = fe + 2;
    let mut extra = vec![0u8; (2 + blocks as usize) * SECTOR_BYTES];
    extra[..SECTOR_BYTES].copy_from_slice(&metadata_fe(0, blocks));
    extra[SECTOR_BYTES..2 * SECTOR_BYTES].copy_from_slice(&metadata_fe(mirror - main, blocks));
    extra[2 * SECTOR_BYTES..]
        .copy_from_slice(&img.image[main as usize * SECTOR_BYTES..data as usize * SECTOR_BYTES]);
    for bytes in [&mut img.image, &mut img.plain] {
        for lvd in bytes[..main as usize * SECTOR_BYTES].chunks_mut(SECTOR_BYTES) {
            if lvd[..2] == 6u16.to_le_bytes() {
                metadata_map(lvd, fe - main);
            }
        }
        bytes.extend_from_slice(&extra);
    }
    MetadataMirrorLayout {
        main,
        mirror,
        blocks,
        aacs_dir,
    }
}

/// [`encrypted_bd_image`] with [`add_metadata_mirror`] applied.
pub fn mirrored_aacs_image(
    files: &[BdFile],
    uk_ro: &[u8],
) -> (EncryptedBdImage, MetadataMirrorLayout) {
    let mut img = encrypted_bd_image(files, uk_ro);
    let layout = add_metadata_mirror(&mut img);
    (img, layout)
}

// An Extended File Entry with one short_ad of `blocks` at partition block `at`.
fn metadata_fe(at: u32, blocks: u32) -> [u8; SECTOR_BYTES] {
    let mut s = [0u8; SECTOR_BYTES];
    s[0..2].copy_from_slice(&266u16.to_le_bytes());
    let len = blocks * SECTOR_BYTES as u32;
    s[56..64].copy_from_slice(&u64::from(len).to_le_bytes());
    s[212..216].copy_from_slice(&8u32.to_le_bytes());
    s[216..220].copy_from_slice(&len.to_le_bytes());
    s[220..224].copy_from_slice(&at.to_le_bytes());
    s
}

// A second, Type 2 "*UDF Metadata Partition" map (UDF 2.50 2.2.10) with its duplicate flag.
fn metadata_map(lvd: &mut [u8], file: u32) {
    lvd[264..268].copy_from_slice(&(6u32 + 64).to_le_bytes());
    lvd[268..272].copy_from_slice(&2u32.to_le_bytes());
    let map = 440 + lvd[441] as usize;
    lvd[map] = 2;
    lvd[map + 1] = 64;
    lvd[map + 5..map + 28].copy_from_slice(b"*UDF Metadata Partition");
    lvd[map + 40..map + 44].copy_from_slice(&file.to_le_bytes());
    lvd[map + 44..map + 48].copy_from_slice(&(file + 1).to_le_bytes());
    lvd[map + 58] = 1;
}
