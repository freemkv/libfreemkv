//! ECMA-167 / UDF 1.02 descriptor encoder.
//!
//! Turns a [`Layout`](super::layout::Layout) — a directory tree with every
//! ICB, directory-data and file-data block already assigned — into the set of
//! metadata sectors a real UDF volume would carry. Nothing here touches the
//! filesystem: it is a pure function from layout to sectors, which is what
//! makes it testable against the production parser in `udf.rs`.
//!
//! What is emitted, in volume order:
//!
//! | sector | descriptor |
//! |---|---|
//! | 16, 17, 18 | Volume Recognition Sequence — `BEA01`, `NSR02`, `TEA01` (ECMA-167 2/9.1) |
//! | 32… | Main Volume Descriptor Sequence — PVD, IUVD, PD, LVD, USD, TD |
//! | 48… | Reserve VDS (byte-identical but for the tag locations) |
//! | 64, 65 | Logical Volume Integrity Sequence — LVID, TD |
//! | 256 | Anchor Volume Descriptor Pointer |
//! | `part_start` + 0, +1 | File Set Descriptor, TD |
//! | `part_start` + … | File Entries (ICBs) and directory data (FIDs) |
//! | last sector | Anchor Volume Descriptor Pointer (copy) |
//!
//! UDF revision 1.02 with a single Type-1 partition map is deliberate: it is
//! the DVD-Video profile, it is the shape `read_filesystem` takes when
//! `num_partition_maps < 2`, and it avoids the UDF 2.50 Metadata Partition
//! entirely. That also means a synthetic image never exercises the Metadata
//! Partition path in `udf.rs` (`:946-991`) — see the module docs on `dirimage`.

use super::layout::{DirNode, Layout};
use crate::error::{Error, Result};
use std::collections::BTreeMap;

/// Logical block / sector size. Fixed for every optical profile this crate
/// reads, and the same quantity as [`crate::consts::SECTOR_BYTES`] — aliased
/// rather than re-declared so the two cannot drift apart. The short name is
/// kept because it appears in ~25 extent and offset expressions across
/// `dirimage`, where the longer one would bury the arithmetic.
pub(super) use crate::consts::SECTOR_BYTES as SECTOR;

/// Descriptor version recorded in every tag. 2 = ECMA-167 2nd edition, which
/// is what UDF revisions up to and including 2.00 require.
const DESC_VERSION: u16 = 2;

/// UDF revision recorded in the domain EntityID suffix (1.02, BCD-ish u16).
const UDF_REVISION: u16 = 0x0102;

/// A fixed recording timestamp, so an image synthesized from the same folder
/// twice is byte-identical. Real mtimes would make every test golden-file
/// comparison and every `dir:// -> iso://` re-run differ for no benefit.
const FIXED_TIME: Timestamp = Timestamp {
    year: 2000,
    month: 1,
    day: 1,
};

struct Timestamp {
    year: i16,
    month: u8,
    day: u8,
}

/// The synthesized metadata: absolute LBA → sector contents. Data sectors are
/// NOT here; they are served from the backing files.
pub(super) type MetaSectors = BTreeMap<u32, Box<[u8; SECTOR]>>;

/// The descriptor-tag CRC of ECMA-167 7.2.4: polynomial 0x1021, initial value
/// ZERO, no reflection, no final XOR — the variant catalogued as CRC-16/XMODEM
/// (check value 0x31C3), NOT CCITT-FALSE, which seeds at 0xFFFF and would make
/// every descriptor this crate writes fail a conformant driver's validation.
fn crc16(data: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for &b in data {
        crc ^= (b as u16) << 8;
        for _ in 0..8 {
            crc = if crc & 0x8000 != 0 {
                (crc << 1) ^ 0x1021
            } else {
                crc << 1
            };
        }
    }
    crc
}

/// Write an ECMA-167 3/7.2 descriptor tag over `buf[0..16]`.
///
/// `tag_loc` is the block number of the sector holding the descriptor —
/// ABSOLUTE for the volume-space descriptors (AVDP, VDS, LVID) and
/// PARTITION-RELATIVE for everything inside the partition (FSD, File Entries).
/// Getting that wrong is the classic reason a hand-built volume mounts nowhere:
/// a driver that validates the tag location rejects the descriptor outright.
///
/// `desc_len` is the descriptor's total length including the tag; the CRC
/// covers `buf[16..desc_len]`.
fn finish_tag(buf: &mut [u8], tag_id: u16, tag_loc: u32, desc_len: usize) {
    buf[0..2].copy_from_slice(&tag_id.to_le_bytes());
    buf[2..4].copy_from_slice(&DESC_VERSION.to_le_bytes());
    buf[4] = 0; // checksum, filled below
    buf[5] = 0; // reserved
    buf[6..8].copy_from_slice(&0u16.to_le_bytes()); // tag serial number
    let crc_len = desc_len - 16;
    let crc = crc16(&buf[16..desc_len]);
    buf[8..10].copy_from_slice(&crc.to_le_bytes());
    buf[10..12].copy_from_slice(&(crc_len as u16).to_le_bytes());
    buf[12..16].copy_from_slice(&tag_loc.to_le_bytes());
    // ECMA-167 3/7.2.3: sum of bytes 0..16 EXCLUDING byte 4, modulo 256.
    let sum: u32 = buf[0..16]
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != 4)
        .map(|(_, b)| *b as u32)
        .sum();
    buf[4] = (sum % 256) as u8;
}

/// ECMA-167 1/7.2.1 charspec: type 0 (CS0) + "OSTA Compressed Unicode".
fn put_charspec(buf: &mut [u8]) {
    buf[0] = 0;
    let id = b"OSTA Compressed Unicode";
    buf[1..1 + id.len()].copy_from_slice(id);
}

/// ECMA-167 1/7.4 EntityID: flags byte, 23 identifier bytes, 8 suffix bytes.
fn put_entity_id(buf: &mut [u8], id: &[u8], suffix: &[u8]) {
    buf[0] = 0;
    let n = id.len().min(23);
    buf[1..1 + n].copy_from_slice(&id[..n]);
    let m = suffix.len().min(8);
    buf[24..24 + m].copy_from_slice(&suffix[..m]);
}

/// The `*OSTA UDF Compliant` domain EntityID suffix: UDF revision, domain
/// flags (0 = neither hard nor soft write-protected), reserved.
fn domain_suffix() -> [u8; 8] {
    let mut s = [0u8; 8];
    s[0..2].copy_from_slice(&UDF_REVISION.to_le_bytes());
    s
}

/// This crate's implementation EntityID suffix: OS class / OS identifier
/// (0 = undefined, deliberately — the image is not OS-specific) + 6 free bytes.
fn impl_suffix() -> [u8; 8] {
    [0u8; 8]
}

fn put_impl_id(buf: &mut [u8]) {
    put_entity_id(buf, b"*freemkv", &impl_suffix());
}

fn put_domain_id(buf: &mut [u8]) {
    put_entity_id(buf, b"*OSTA UDF Compliant", &domain_suffix());
}

/// OSTA CS0 d-string: a compression-ID byte, the characters, then the used
/// length in the FIELD'S LAST byte (ECMA-167 1/7.2.12 + UDF 2.1.3). An
/// all-zero field is the empty string.
fn put_dstring(buf: &mut [u8], s: &str) {
    if s.is_empty() {
        return;
    }
    let encoded = encode_cs0(s);
    // Leave room for the trailing length byte.
    let room = buf.len() - 1;
    let n = encoded.len().min(room);
    buf[..n].copy_from_slice(&encoded[..n]);
    buf[buf.len() - 1] = n as u8;
}

/// OSTA CS0: compression ID 8 (one byte per character) when every character
/// is ASCII, otherwise compression ID 16 (UTF-16BE).
///
/// ASCII rather than Latin-1 for the 8-bit form on purpose: `parse_udf_name`
/// (`udf.rs:1467`) decodes a compression-8 name with `from_utf8_lossy`, so a
/// 0x80-0xFF byte — legal CS0 — would come back as U+FFFD. Every character
/// above 0x7F therefore takes the 16-bit form, which that parser decodes
/// correctly.
pub(super) fn encode_cs0(s: &str) -> Vec<u8> {
    if s.is_ascii() {
        let mut v = Vec::with_capacity(1 + s.len());
        v.push(8u8);
        v.extend_from_slice(s.as_bytes());
        v
    } else {
        let mut v = vec![16u8];
        for u in s.encode_utf16() {
            v.extend_from_slice(&u.to_be_bytes());
        }
        v
    }
}

/// ECMA-167 1/7.3 timestamp, 12 bytes. Type 1 (local time) with a zero
/// offset, i.e. UTC.
fn put_timestamp(buf: &mut [u8]) {
    buf[0..2].copy_from_slice(&0x1000u16.to_le_bytes());
    buf[2..4].copy_from_slice(&FIXED_TIME.year.to_le_bytes());
    buf[4] = FIXED_TIME.month;
    buf[5] = FIXED_TIME.day;
}

/// ECMA-167 3/7.1 extent_ad: length in BYTES, then location.
fn put_extent_ad(buf: &mut [u8], len_bytes: u32, lba: u32) {
    buf[0..4].copy_from_slice(&len_bytes.to_le_bytes());
    buf[4..8].copy_from_slice(&lba.to_le_bytes());
}

/// ECMA-167 4/14.14.2 long_ad: length+type, then lb_addr (block, partition
/// reference), then 6 implementation-use bytes.
fn put_long_ad(buf: &mut [u8], len_bytes: u32, lba: u32) {
    buf[0..4].copy_from_slice(&len_bytes.to_le_bytes());
    buf[4..8].copy_from_slice(&lba.to_le_bytes());
    buf[8..10].copy_from_slice(&0u16.to_le_bytes()); // partition reference 0
}

/// ECMA-167 4/14.14.1 short_ad. The top two bits of the length word are the
/// extent TYPE (0 = recorded and allocated), which is exactly why `udf.rs`
/// masks with `0x3FFF_FFFF` when it reads one back — the mask is the field
/// boundary, not a truncation bug.
fn put_short_ad(buf: &mut [u8], len_bytes: u32, lba: u32) {
    debug_assert!(len_bytes <= 0x3FFF_FFFF, "AD length must fit 30 bits");
    buf[0..4].copy_from_slice(&len_bytes.to_le_bytes());
    buf[4..8].copy_from_slice(&lba.to_le_bytes());
}

fn blank() -> Box<[u8; SECTOR]> {
    Box::new([0u8; SECTOR])
}

// ── Volume-space descriptors ────────────────────────────────────────────────

/// ECMA-167 2/9.1 Volume Structure Descriptor: the three-sector recognition
/// sequence an OS looks for before it will even consider the volume UDF.
fn volume_recognition(id: &[u8; 5]) -> Box<[u8; SECTOR]> {
    let mut s = blank();
    s[0] = 0; // structure type
    s[1..6].copy_from_slice(id);
    s[6] = 1; // structure version
    s
}

/// ECMA-167 3/10.1 Primary Volume Descriptor.
fn primary_volume(volume_id: &str, lba: u32, seq: u32) -> Box<[u8; SECTOR]> {
    let mut s = blank();
    s[16..20].copy_from_slice(&seq.to_le_bytes());
    s[20..24].copy_from_slice(&0u32.to_le_bytes()); // PVD number
    put_dstring(&mut s[24..56], volume_id);
    s[56..58].copy_from_slice(&1u16.to_le_bytes()); // volume sequence number
    s[58..60].copy_from_slice(&1u16.to_le_bytes()); // max volume sequence number
    s[60..62].copy_from_slice(&2u16.to_le_bytes()); // interchange level
    s[62..64].copy_from_slice(&2u16.to_le_bytes()); // max interchange level
    s[64..68].copy_from_slice(&1u32.to_le_bytes()); // character set list
    s[68..72].copy_from_slice(&1u32.to_le_bytes()); // max character set list
    // UDF 2.2.2.5: the first 8 characters of the volume set identifier must be
    // unique. A fixed hex prefix plus the volume id is sufficient here — the
    // image is single-volume and never joins a real volume set.
    put_dstring(&mut s[72..200], &format!("46524D4B{volume_id}"));
    put_charspec(&mut s[200..264]); // descriptor character set
    put_charspec(&mut s[264..328]); // explanatory character set
    put_timestamp(&mut s[376..388]);
    put_impl_id(&mut s[388..420]);
    finish_tag(&mut s[..], 1, lba, 512);
    s
}

/// ECMA-167 3/10.4 + UDF 2.2.7 Implementation Use Volume Descriptor
/// (`*UDF LV Info`). Not read by `udf.rs`, required by the spec.
fn impl_use_volume(volume_id: &str, lba: u32, seq: u32) -> Box<[u8; SECTOR]> {
    let mut s = blank();
    s[16..20].copy_from_slice(&seq.to_le_bytes());
    put_entity_id(&mut s[20..52], b"*UDF LV Info", &domain_suffix());
    put_charspec(&mut s[52..116]); // LVI charset
    put_dstring(&mut s[116..244], volume_id); // logical volume identifier
    put_impl_id(&mut s[352..384]);
    finish_tag(&mut s[..], 4, lba, 512);
    s
}

/// ECMA-167 3/10.5 Partition Descriptor — the descriptor `read_filesystem`
/// takes `partition_start` from (offset 188).
fn partition(part_start: u32, part_sectors: u32, lba: u32, seq: u32) -> Box<[u8; SECTOR]> {
    let mut s = blank();
    s[16..20].copy_from_slice(&seq.to_le_bytes());
    s[20..22].copy_from_slice(&1u16.to_le_bytes()); // partition flags: allocated
    s[22..24].copy_from_slice(&0u16.to_le_bytes()); // partition number
    put_entity_id(&mut s[24..56], b"+NSR02", &[]);
    // s[56..184] partition contents use = Partition Header Descriptor. All
    // zero: a read-only partition records no unallocated/freed space tables.
    s[184..188].copy_from_slice(&1u32.to_le_bytes()); // access type: read only
    s[188..192].copy_from_slice(&part_start.to_le_bytes());
    s[192..196].copy_from_slice(&part_sectors.to_le_bytes());
    put_impl_id(&mut s[196..228]);
    finish_tag(&mut s[..], 5, lba, 512);
    s
}

/// ECMA-167 3/10.6 Logical Volume Descriptor. Carries the FSD long_ad and the
/// partition map table; `read_filesystem` reads `num_partition_maps` at 268
/// and takes the single-partition path when it is 1.
fn logical_volume(
    volume_id: &str,
    fsd_lba: u32,
    integrity_lba: u32,
    integrity_sectors: u32,
    lba: u32,
    seq: u32,
) -> Box<[u8; SECTOR]> {
    let mut s = blank();
    s[16..20].copy_from_slice(&seq.to_le_bytes());
    put_charspec(&mut s[20..84]);
    put_dstring(&mut s[84..212], volume_id);
    s[212..216].copy_from_slice(&(SECTOR as u32).to_le_bytes()); // logical block size
    put_domain_id(&mut s[216..248]);
    // Logical volume contents use = long_ad of the File Set Descriptor,
    // partition-relative. One sector.
    put_long_ad(&mut s[248..264], SECTOR as u32, fsd_lba);
    s[264..268].copy_from_slice(&6u32.to_le_bytes()); // map table length
    s[268..272].copy_from_slice(&1u32.to_le_bytes()); // number of partition maps
    put_impl_id(&mut s[272..304]);
    put_extent_ad(
        &mut s[432..440],
        integrity_sectors * SECTOR as u32,
        integrity_lba,
    );
    // ECMA-167 3/10.7.2 Type 1 partition map.
    s[440] = 1; // map type
    s[441] = 6; // map length
    s[442..444].copy_from_slice(&1u16.to_le_bytes()); // volume sequence number
    s[444..446].copy_from_slice(&0u16.to_le_bytes()); // partition number
    finish_tag(&mut s[..], 6, lba, 446);
    s
}

/// ECMA-167 3/10.8 Unallocated Space Descriptor with zero extents — the whole
/// volume is accounted for by the partition.
fn unallocated_space(lba: u32, seq: u32) -> Box<[u8; SECTOR]> {
    let mut s = blank();
    s[16..20].copy_from_slice(&seq.to_le_bytes());
    s[20..24].copy_from_slice(&0u32.to_le_bytes());
    finish_tag(&mut s[..], 7, lba, 24);
    s
}

/// ECMA-167 3/10.9 Terminating Descriptor.
fn terminating(lba: u32) -> Box<[u8; SECTOR]> {
    let mut s = blank();
    finish_tag(&mut s[..], 8, lba, 512);
    s
}

/// ECMA-167 3/10.10 + UDF 2.2.6 Logical Volume Integrity Descriptor, closed.
fn integrity(
    part_sectors: u32,
    files: u32,
    dirs: u32,
    next_uid: u64,
    lba: u32,
) -> Box<[u8; SECTOR]> {
    let mut s = blank();
    put_timestamp(&mut s[16..28]);
    s[28..32].copy_from_slice(&1u32.to_le_bytes()); // integrity type: close
    // s[32..40] next integrity extent: none.
    s[40..48].copy_from_slice(&next_uid.to_le_bytes()); // logical volume contents use: next unique id
    s[72..76].copy_from_slice(&1u32.to_le_bytes()); // number of partitions
    s[76..80].copy_from_slice(&46u32.to_le_bytes()); // length of implementation use
    s[80..84].copy_from_slice(&0u32.to_le_bytes()); // free space: none (read-only)
    s[84..88].copy_from_slice(&part_sectors.to_le_bytes()); // size table
    put_impl_id(&mut s[88..120]);
    s[120..124].copy_from_slice(&files.to_le_bytes());
    s[124..128].copy_from_slice(&dirs.to_le_bytes());
    s[128..130].copy_from_slice(&UDF_REVISION.to_le_bytes()); // min read revision
    s[130..132].copy_from_slice(&UDF_REVISION.to_le_bytes()); // min write revision
    s[132..134].copy_from_slice(&UDF_REVISION.to_le_bytes()); // max write revision
    finish_tag(&mut s[..], 9, lba, 134);
    s
}

/// ECMA-167 3/10.2 Anchor Volume Descriptor Pointer. `read_filesystem` reads
/// the main VDS extent from offsets 16..24 and sweeps it.
fn anchor(main_lba: u32, reserve_lba: u32, vds_sectors: u32, lba: u32) -> Box<[u8; SECTOR]> {
    let mut s = blank();
    put_extent_ad(&mut s[16..24], vds_sectors * SECTOR as u32, main_lba);
    put_extent_ad(&mut s[24..32], vds_sectors * SECTOR as u32, reserve_lba);
    finish_tag(&mut s[..], 2, lba, 512);
    s
}

/// ECMA-167 4/14.1 File Set Descriptor. `read_filesystem` requires tag 256 at
/// the first block of the (metadata =) partition and reads the root ICB block
/// from offset 404.
fn file_set(volume_id: &str, root_icb: u32, lba: u32) -> Box<[u8; SECTOR]> {
    let mut s = blank();
    put_timestamp(&mut s[16..28]);
    s[28..30].copy_from_slice(&3u16.to_le_bytes()); // interchange level
    s[30..32].copy_from_slice(&3u16.to_le_bytes()); // max interchange level
    s[32..36].copy_from_slice(&1u32.to_le_bytes()); // character set list
    s[36..40].copy_from_slice(&1u32.to_le_bytes()); // max character set list
    s[40..44].copy_from_slice(&0u32.to_le_bytes()); // file set number
    s[44..48].copy_from_slice(&0u32.to_le_bytes()); // file set descriptor number
    put_charspec(&mut s[48..112]);
    put_dstring(&mut s[112..240], volume_id);
    put_charspec(&mut s[240..304]);
    put_dstring(&mut s[304..336], volume_id);
    put_long_ad(&mut s[400..416], SECTOR as u32, root_icb);
    put_domain_id(&mut s[416..448]);
    finish_tag(&mut s[..], 256, lba, 512);
    s
}

// ── Partition-space descriptors ─────────────────────────────────────────────

/// UDF permission word: read + execute for owner, group and other. No write
/// bit anywhere — the volume is read-only.
const PERM_R_X: u32 = 0x0000_1000 | 0x0000_0400 | 0x0000_0080 | 0x0000_0020 | 0x4 | 0x1;

/// ECMA-167 4/14.9 File Entry (tag 261).
///
/// Tag 261 rather than the Extended File Entry (266) real BD-ROMs use: an EFE
/// requires UDF 2.00+, and this image declares 1.02. `udf.rs` reads both — the
/// 261 field offsets it uses (l_ea 168, l_ad 172, ADs at 176 + l_ea) are the
/// ones written here.
///
/// `extents` are partition-relative (block, byte-length) pairs, already split
/// so no single one exceeds the 30-bit AD length field.
fn file_entry(
    is_dir: bool,
    info_len: u64,
    extents: &[(u32, u32)],
    link_count: u16,
    unique_id: u64,
    lba: u32,
) -> Result<Box<[u8; SECTOR]>> {
    let mut s = blank();
    // ICB tag (ECMA-167 4/14.6) at offset 16.
    s[16..20].copy_from_slice(&0u32.to_le_bytes()); // prior recorded direct entries
    s[20..22].copy_from_slice(&4u16.to_le_bytes()); // strategy type 4
    s[24..26].copy_from_slice(&1u16.to_le_bytes()); // max number of entries
    s[27] = if is_dir { 4 } else { 5 }; // file type: directory / byte sequence
    // s[28..34] parent ICB location: not recorded (permitted).
    // s[34..36] ICB flags: 0 => short allocation descriptors. `udf.rs:601`
    // reads exactly this word to pick its AD stride.
    s[34..36].copy_from_slice(&0u16.to_le_bytes());
    // UDF's sentinel for "not specified" is 0xFFFFFFFF, not 0 — 0 is a real
    // uid/gid (root). A synthesized image has no meaningful owner, and a driver
    // that maps these through would otherwise report every file as root-owned.
    s[36..40].copy_from_slice(&u32::MAX.to_le_bytes()); // uid: not specified
    s[40..44].copy_from_slice(&u32::MAX.to_le_bytes()); // gid: not specified
    s[44..48].copy_from_slice(&PERM_R_X.to_le_bytes());
    s[48..50].copy_from_slice(&link_count.to_le_bytes());
    s[56..64].copy_from_slice(&info_len.to_le_bytes());
    let blocks: u64 = extents
        .iter()
        .map(|(_, len)| (*len as u64).div_ceil(SECTOR as u64))
        .sum();
    s[64..72].copy_from_slice(&blocks.to_le_bytes()); // logical blocks recorded
    put_timestamp(&mut s[72..84]); // access
    put_timestamp(&mut s[84..96]); // modification
    put_timestamp(&mut s[96..108]); // attribute
    s[108..112].copy_from_slice(&1u32.to_le_bytes()); // checkpoint
    put_impl_id(&mut s[128..160]);
    s[160..168].copy_from_slice(&unique_id.to_le_bytes());
    s[168..172].copy_from_slice(&0u32.to_le_bytes()); // length of EAs
    let l_ad = extents.len() * 8;
    // A short AD is 8 bytes and the entry has 2048 - 176 = 1872 bytes for
    // them, i.e. 234 extents — over 200 GiB at the per-AD ceiling. Beyond
    // that an Allocation Extent Descriptor chain would be required; refuse
    // rather than write a truncated list.
    if 176 + l_ad > SECTOR {
        return Err(Error::DirImageTooLarge);
    }
    s[172..176].copy_from_slice(&(l_ad as u32).to_le_bytes());
    for (i, (elba, len)) in extents.iter().enumerate() {
        let off = 176 + i * 8;
        put_short_ad(&mut s[off..off + 8], *len, *elba);
    }
    finish_tag(&mut s[..], 261, lba, 176 + l_ad);
    Ok(s)
}

/// ECMA-167 4/14.4 File Identifier Descriptor, appended to `buf`.
///
/// FIDs are packed with no inter-descriptor padding beyond the 4-byte
/// alignment the spec mandates, and they are allowed to span logical blocks —
/// which is also what `read_directory` (`udf.rs:1312`) assumes: it walks the
/// directory extent as one flat byte run and STOPS at the first non-257 tag,
/// so any block-alignment gap would truncate the directory.
fn push_fid(buf: &mut Vec<u8>, name: &str, icb_lba: u32, is_dir: bool, is_parent: bool) {
    let start = buf.len();
    let name_field: Vec<u8> = if is_parent {
        Vec::new()
    } else {
        encode_cs0(name)
    };
    let l_fi = name_field.len();
    let mut fid = vec![0u8; 38];
    fid[16..18].copy_from_slice(&1u16.to_le_bytes()); // file version number
    let mut chars = 0u8;
    if is_dir {
        chars |= 0x02;
    }
    if is_parent {
        chars |= 0x08;
    }
    fid[18] = chars;
    // The planner refuses any name whose encoding exceeds what this byte can
    // hold (`layout::MAX_CS0_NAME_BYTES`), so this cannot wrap in practice. The
    // assert states the invariant where it is relied on rather than trusting a
    // check three files away; a wrap here would desynchronise the directory.
    debug_assert!(
        l_fi <= u8::MAX as usize,
        "FID name length must fit one byte"
    );
    fid[19] = l_fi as u8;
    put_long_ad(&mut fid[20..36], SECTOR as u32, icb_lba);
    fid[36..38].copy_from_slice(&0u16.to_le_bytes()); // length of implementation use
    buf.extend_from_slice(&fid);
    buf.extend_from_slice(&name_field);
    let unpadded = buf.len() - start;
    let padded = unpadded.div_ceil(4) * 4;
    buf.resize(start + padded, 0);
    // The tag is written last: its CRC covers the descriptor body, which the
    // padding is not part of (ECMA-167 4/14.4.9 counts padding outside the
    // CRC'd length).
    let tag_loc_placeholder = 0;
    finish_tag(
        &mut buf[start..start + unpadded],
        257,
        tag_loc_placeholder,
        unpadded,
    );
}

/// Serialize one directory's FID list (parent entry first, then children).
pub(super) fn dir_fids(dir: &DirNode) -> Vec<u8> {
    let mut buf = Vec::new();
    push_fid(&mut buf, "", dir.parent_icb_lba, true, true);
    for sub in &dir.dirs {
        push_fid(&mut buf, &sub.name, sub.icb_lba, true, false);
    }
    for f in &dir.files {
        push_fid(&mut buf, &f.name, f.icb_lba, false, false);
    }
    buf
}

/// Patch every FID's tag location to the block it actually lands in. ECMA-167
/// 3/7.2.2 makes the tag location the block of the descriptor, and a FID that
/// spans two blocks records the block it STARTS in.
fn fix_fid_tag_locations(buf: &mut [u8], first_block: u32) {
    let mut pos = 0usize;
    while pos + 38 <= buf.len() {
        let l_fi = buf[pos + 19] as usize;
        let l_iu = u16::from_le_bytes([buf[pos + 36], buf[pos + 37]]) as usize;
        let unpadded = 38 + l_iu + l_fi;
        if pos + unpadded > buf.len() {
            break;
        }
        let block = first_block + (pos / SECTOR) as u32;
        finish_tag(&mut buf[pos..pos + unpadded], 257, block, unpadded);
        pos += unpadded.div_ceil(4) * 4;
    }
}

// ── Whole-image assembly ────────────────────────────────────────────────────

/// Volume-space block of the Volume Recognition Sequence.
const VRS_START: u32 = 16;
/// Volume-space block of the Main Volume Descriptor Sequence.
pub(super) const MAIN_VDS_START: u32 = 32;
/// Volume-space block of the Reserve Volume Descriptor Sequence.
pub(super) const RESERVE_VDS_START: u32 = 48;
/// Sectors reserved for each VDS. ECMA-167 3/10.2.1 requires an anchor to
/// record at least 16.
pub(super) const VDS_SECTORS: u32 = 16;
/// Volume-space block of the Logical Volume Integrity Sequence.
pub(super) const LVID_START: u32 = 64;
/// Sectors reserved for the integrity sequence (LVID + TD).
pub(super) const LVID_SECTORS: u32 = 2;
/// The mandatory anchor block (ECMA-167 3/10.2).
pub(super) const ANCHOR_LBA: u32 = 256;
/// First block a partition may start at. Everything above is volume space.
pub(super) const MIN_PART_START: u32 = 320;

/// Emit the six-descriptor Volume Descriptor Sequence at `start`.
fn write_vds(out: &mut MetaSectors, layout: &Layout, start: u32) {
    let vid = &layout.volume_id;
    out.insert(start, primary_volume(vid, start, 1));
    out.insert(start + 1, impl_use_volume(vid, start + 1, 2));
    out.insert(
        start + 2,
        partition(layout.part_start, layout.part_sectors, start + 2, 3),
    );
    out.insert(
        start + 3,
        logical_volume(vid, 0, LVID_START, LVID_SECTORS, start + 3, 4),
    );
    out.insert(start + 4, unallocated_space(start + 4, 5));
    out.insert(start + 5, terminating(start + 5));
}

/// Recursively emit one directory's File Entry and FID list, then its
/// children's.
fn write_dir(out: &mut MetaSectors, layout: &Layout, dir: &DirNode) -> Result<()> {
    let mut fids = dir_fids(dir);
    fix_fid_tag_locations(&mut fids, dir.data_lba);
    debug_assert_eq!(fids.len(), dir.data_bytes as usize);

    // A directory's link count is 1 (its own FID in the parent) plus one for
    // each child directory's parent FID pointing back at it.
    // The planner caps subdirectory fan-out (`layout::MAX_SUBDIRS`) so this
    // cannot overflow; saturating rather than wrapping keeps a future change to
    // that cap from silently producing a wrong count.
    let link_count = (dir.dirs.len() as u16).saturating_add(1);
    let fe = file_entry(
        true,
        fids.len() as u64,
        &[(dir.data_lba, fids.len() as u32)],
        link_count,
        dir.unique_id,
        dir.icb_lba,
    )?;
    out.insert(layout.part_start + dir.icb_lba, fe);

    for (i, chunk) in fids.chunks(SECTOR).enumerate() {
        let mut s = blank();
        s[..chunk.len()].copy_from_slice(chunk);
        out.insert(layout.part_start + dir.data_lba + i as u32, s);
    }

    for f in &dir.files {
        let extents: Vec<(u32, u32)> = f.extents.iter().map(|e| (e.lba, e.bytes)).collect();
        let fe = file_entry(false, f.size, &extents, 1, f.unique_id, f.icb_lba)?;
        out.insert(layout.part_start + f.icb_lba, fe);
    }

    for sub in &dir.dirs {
        write_dir(out, layout, sub)?;
    }
    Ok(())
}

/// Build every metadata sector of the synthesized volume.
pub(super) fn encode(layout: &Layout) -> Result<MetaSectors> {
    let mut out = MetaSectors::new();

    out.insert(VRS_START, volume_recognition(b"BEA01"));
    out.insert(VRS_START + 1, volume_recognition(b"NSR02"));
    out.insert(VRS_START + 2, volume_recognition(b"TEA01"));

    write_vds(&mut out, layout, MAIN_VDS_START);
    write_vds(&mut out, layout, RESERVE_VDS_START);

    out.insert(
        LVID_START,
        integrity(
            layout.part_sectors,
            layout.file_count,
            layout.dir_count,
            layout.next_unique_id,
            LVID_START,
        ),
    );
    out.insert(LVID_START + 1, terminating(LVID_START + 1));

    let avdp = anchor(MAIN_VDS_START, RESERVE_VDS_START, VDS_SECTORS, ANCHOR_LBA);
    out.insert(ANCHOR_LBA, avdp);
    let last = layout.total_sectors - 1;
    out.insert(
        last,
        anchor(MAIN_VDS_START, RESERVE_VDS_START, VDS_SECTORS, last),
    );

    // Partition block 0 must hold the File Set Descriptor: `read_filesystem`
    // reads exactly `metadata_start` (== partition start on a single-partition
    // volume) and rejects the volume outright if the tag there is not 256.
    out.insert(
        layout.part_start,
        file_set(&layout.volume_id, layout.root.icb_lba, 0),
    );
    out.insert(layout.part_start + 1, terminating(1));

    write_dir(&mut out, layout, &layout.root)?;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The reference check value for CRC-16/XMODEM — poly 0x1021 seeded at 0,
    /// which is what ECMA-167 7.2.4 specifies: "123456789" → 0x31C3. Seeding
    /// at 0xFFFF instead (CCITT-FALSE) yields 0x29B1, and that mutant is
    /// invisible to `udf.rs`, which never verifies a tag CRC — it would only
    /// show up as a volume no operating system will mount.
    #[test]
    fn crc16_matches_the_ecma167_check_value() {
        assert_eq!(crc16(b"123456789"), 0x31C3);
        assert_ne!(crc16(b"123456789"), 0x29B1, "not the 0xFFFF-seeded variant");
    }

    /// ECMA-167 3/7.2.3: the checksum is the sum of the tag's first 16 bytes
    /// EXCLUDING the checksum byte itself, modulo 256.
    #[test]
    fn tag_checksum_excludes_its_own_byte() {
        let mut buf = [0u8; 512];
        buf[16..24].copy_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8]);
        finish_tag(&mut buf, 261, 0x1234, 512);
        let sum: u32 = buf[0..16]
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != 4)
            .map(|(_, b)| *b as u32)
            .sum();
        assert_eq!(buf[4] as u32, sum % 256);
        // And the recorded CRC covers the body, not the tag.
        let crc = u16::from_le_bytes([buf[8], buf[9]]);
        assert_eq!(crc, crc16(&buf[16..512]));
        assert_eq!(u16::from_le_bytes([buf[10], buf[11]]), 496);
        assert_eq!(
            u32::from_le_bytes([buf[12], buf[13], buf[14], buf[15]]),
            0x1234
        );
    }

    /// ASCII takes compression ID 8; anything above takes 16 (UTF-16BE),
    /// because `parse_udf_name` decodes compression-8 bytes as UTF-8.
    #[test]
    fn cs0_picks_the_encoding_the_parser_can_decode() {
        assert_eq!(encode_cs0("AB"), vec![8, b'A', b'B']);
        let e = encode_cs0("Ä");
        assert_eq!(e[0], 16);
        assert_eq!(&e[1..], &[0x00, 0xC4]);
        assert_eq!(crate::udf::parse_udf_name(&e), "Ä");
    }

    /// A d-string records its used length in the field's LAST byte, and the
    /// production parser must read the same string back.
    #[test]
    fn dstring_round_trips_through_the_production_parser() {
        let mut field = [0u8; 32];
        put_dstring(&mut field, "FREEMKV");
        assert_eq!(field[31], 8, "compid byte + 7 characters");
        assert_eq!(crate::udf::parse_dstring_for_test(&field), "FREEMKV");
    }
}
