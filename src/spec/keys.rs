//! `KS-n`: the quotes behind AACS key, unit-grid and UDF extent handling.
//!
//! KS-1…KS-19 and KS-29 are the public AACS books (Final Revision 0.953, downloaded
//! from aacsla.com); KS-20 and KS-21 are ECMA-167 3rd edition; KS-22…KS-24 are
//! libaacs `55be92be`, corroboration only; KS-25…KS-28 and KS-31 are evidence for what
//! has no public spec (AACS 2.x / UHD, FMTS, HD DVD). KS-30 is the AACS HD DVD book, Final
//! Revision 0.952, since withdrawn from aacsla.com (KS-27). "PDF p." is the physical page.

use super::{QuoteKind, SpecQuote};

const BD: &str = "AACS Blu-ray Disc Pre-recorded Book, Final Rev 0.953";
const PV: &str = "AACS Pre-recorded Video Book, Final Rev 0.953";
const CM: &str = "AACS Introduction and Common Cryptographic Elements, Final Rev 0.953";
const UDF: &str = "ECMA-167, 3rd edition, June 1997";
const LIBAACS: &str = "libaacs (VideoLAN) @55be92be, src/libaacs/aacs.c";
const EVIDENCE: &str = "freemkv evidence (no public spec)";
const HD: &str = "AACS HD DVD and DVD Pre-recorded Book, Final Rev 0.952";

const AACS_URL: &str = "https://aacsla.com/aacs-specifications/";
const UDF_URL: &str =
    "https://www.ecma-international.org/wp-content/uploads/ECMA-167_3rd_edition_june_1997.pdf";
const LIBAACS_URL: &str =
    "https://code.videolan.org/videolan/libaacs/-/blob/55be92be/src/libaacs/aacs.c";

pub const KS_1_ENCRYPT_EVERY_UNIT: SpecQuote = SpecQuote {
    id: "KS-1",
    kind: QuoteKind::Normative,
    source: BD,
    section: "§3.10.1 Encryption Scheme",
    locator: "PDF p.58",
    url: AACS_URL,
    text: "When AACS encryption is applied to Clip AV stream files under the “\\BDMV” directory, \
           encryption is applied to every Aligned Unit in the file.",
};

pub const KS_2_ALIGNED_UNIT: SpecQuote = SpecQuote {
    id: "KS-2",
    kind: QuoteKind::Normative,
    source: BD,
    section: "§3.10.1 Encryption Scheme",
    locator: "PDF p.58",
    url: AACS_URL,
    text: "An Aligned Unit consists of 32 MPEG source packets: Each MPEG source packet consists \
           of the TP_extra_header (4 bytes) and an MPEG Transport packet (188 bytes). The total \
           size of an Aligned Unit is 6144 bytes, which is equal to the size of 3 logical sectors.",
};

pub const KS_3_CBC_PER_UNIT: SpecQuote = SpecQuote {
    id: "KS-3",
    kind: QuoteKind::Normative,
    source: BD,
    section: "§3.10.1 Encryption Scheme",
    locator: "PDF p.58",
    url: AACS_URL,
    text: "The final 6128 bytes of each Aligned Unit is encrypted using the Block Key and \
           AES-128CBCE. A new CBC cipher chain is started for each Aligned Unit (see Figure 3-7).",
};

pub const KS_4_SEED: SpecQuote = SpecQuote {
    id: "KS-4",
    kind: QuoteKind::Normative,
    source: BD,
    section: "§3.10.1 Encryption Scheme (method: Figure 3-8, a diagram; see KS-23)",
    locator: "PDF p.58",
    url: AACS_URL,
    text: "The first 16 bytes of each Aligned Unit is used as the seed for calculating the Block \
           Key.",
};

pub const KS_5_CPI: SpecQuote = SpecQuote {
    id: "KS-5",
    kind: QuoteKind::Normative,
    source: BD,
    section: "§3.10.2 Copy Permission Indicator",
    locator: "PDF p.59",
    url: AACS_URL,
    text: "Copy_permission_indicator shall be set to 11₂ if the data is encrypted, or shall be \
           set to 00₂ if the data is not encrypted. If the Licensed Player encounters the packet \
           with Copy_permission_indicator set to 10₂ or 01₂, the data shall be considered \
           encrypted.",
};

pub const KS_6_TP_EXTRA_HEADER: SpecQuote = SpecQuote {
    id: "KS-6",
    kind: QuoteKind::Normative,
    source: BD,
    section: "§3.10.2, Table 3-34 TP_extra_header",
    locator: "PDF p.58–59",
    url: AACS_URL,
    text: "TP_extra_header { Copy_permission_indicator 2 uimsbf Arrival_time_stamp 30 uimsbf }",
};

pub const KS_7_UNIT_CONTIGUOUS: SpecQuote = SpecQuote {
    id: "KS-7",
    kind: QuoteKind::Informative,
    source: BD,
    section: "Annex A. Restriction on Data Allocation (Informative)",
    locator: "PDF p.165",
    url: AACS_URL,
    text: "Each physical sector in an Aligned Unit shall be allocated contiguously on the BD-ROM \
           disc.",
};

pub const KS_8_EXTENTS_ASCENDING: SpecQuote = SpecQuote {
    id: "KS-8",
    kind: QuoteKind::Informative,
    source: BD,
    section: "Annex A. Restriction on Data Allocation (Informative)",
    locator: "PDF p.165",
    url: AACS_URL,
    text: "All the extents of each Clip AV stream file shall be allocated with ascending order in \
           physical layer.",
};

pub const KS_9_SSIF_ALIGNED: SpecQuote = SpecQuote {
    id: "KS-9",
    kind: QuoteKind::Normative,
    source: BD,
    section: "§8.1.2 Encryption Scheme (3D)",
    locator: "PDF p.161",
    url: AACS_URL,
    text: "Within the Stereoscopic Interleaved file, segments of the two Clip AV stream files are \
           recorded alternately. The boundary of these segments shall be always aligned to \
           Aligned Unit boundary",
};

pub const KS_10_TITLE_ONE_CPS_UNIT: SpecQuote = SpecQuote {
    id: "KS-10",
    kind: QuoteKind::Normative,
    source: BD,
    section: "§3.9.2 CPS Unit",
    locator: "PDF p.40",
    url: AACS_URL,
    text: "All AV stream files that are referred to by one Title are included in the same CPS \
           Unit. … If multiple Titles share one or more Clips, these Titles shall be included in \
           the same CPS Unit",
};

pub const KS_11_CPS_NUMBER_FROM_ONE: SpecQuote = SpecQuote {
    id: "KS-11",
    kind: QuoteKind::Normative,
    source: BD,
    section: "§3.9.2 CPS Unit",
    locator: "PDF p.40",
    url: AACS_URL,
    text: "CPS_Unit_number values are defined in ascending order, starting from one.",
};

pub const KS_12_UNIT_KEY_BLOCK_START: SpecQuote = SpecQuote {
    id: "KS-12",
    kind: QuoteKind::Normative,
    source: BD,
    section: "§3.9.3 CPS Unit Key File, Table 3-12",
    locator: "PDF p.43",
    url: AACS_URL,
    text: "Unit_Key_Block_start_address field (32 bits) indicates the start address of \
           Unit_Key_Block() in the relative byte number from the first byte of CPS Unit Key File.",
};

pub const KS_13_UNIT_KEY_FILE_HEADER: SpecQuote = SpecQuote {
    id: "KS-13",
    kind: QuoteKind::Normative,
    source: BD,
    section: "§3.9.3, Table 3-13 Unit_Key_File_Header()",
    locator: "PDF p.44",
    url: AACS_URL,
    text: "Application_Type (= 01₁₆) 8 … Num_of_BD_Directory (= 01₁₆) 8 … CPS_Unit_number for \
           First Playback#I 16 … CPS_Unit_number for Top Menu#I 16 … Num_of_Title#I 16 … \
           (reserved) 16 … CPS_Unit_number for Title#J in Directory #I 16",
};

// "(reserved) 112" is a bit count (112 bits), not a rendered radix 11₂.
pub const KS_14_UNIT_KEY_BLOCK: SpecQuote = SpecQuote {
    id: "KS-14",
    kind: QuoteKind::Normative,
    source: BD,
    section: "§3.9.3, Table 3-15 Unit_Key_Block() and text",
    locator: "PDF p.45",
    url: AACS_URL,
    text: "Num_of_CPS_Unit 16 … (reserved) 112 … MAC of PMSN#I 128 … MAC of Device Binding \
           Nonce#I 128 … Encrypted CPS Unit Key for CPS Unit#I 128 … Num_of_CPS_Unit field (16 \
           bits) indicates the number of CPS Units on the disc.",
};

pub const KS_15_KCU_WRAP: SpecQuote = SpecQuote {
    id: "KS-15",
    kind: QuoteKind::Normative,
    source: BD,
    section: "§3.9.3 CPS Unit Key File",
    locator: "PDF p.46",
    url: AACS_URL,
    text: "The CPS Unit Key is encrypted as follows: AES-128E( Kvu, Kcu )",
};

pub const KS_16_KVU: SpecQuote = SpecQuote {
    id: "KS-16",
    kind: QuoteKind::Normative,
    source: PV,
    section: "§3.3 Calculating the Volume Unique Keys (same equation: [BD] §3.3 Volume \
              Identifier, PDF p.30)",
    locator: "PDF p.31",
    url: AACS_URL,
    text: "Kvu = AES-G(Km, IDv)",
};

pub const KS_17_AES_G: SpecQuote = SpecQuote {
    id: "KS-17",
    kind: QuoteKind::Normative,
    source: CM,
    section: "§2.1.3 AES-based One-way Function (AES-G)",
    locator: "PDF p.21",
    url: AACS_URL,
    text: "AES-G(x1, x2) = AES-128D(x1, x2) ⊕ x2.",
};

pub const KS_18_BUS_ENCRYPTION_FLAG: SpecQuote = SpecQuote {
    id: "KS-18",
    kind: QuoteKind::Normative,
    source: BD,
    section: "§3.7 Bus Encryption Flag",
    locator: "PDF p.35",
    url: AACS_URL,
    text: "If the Bus Encryption Enabled (BEE) flag in the Content Certificate is set to 1₂, the \
           BEF shall be set to 1₂ for all the sectors that correspond to the Aligned Unit with \
           Copy_permission_indicator set to 11₂ of the Clip AV stream files under \
           “\\BDMV\\STREAM” directory.",
};

pub const KS_19_IV0: SpecQuote = SpecQuote {
    id: "KS-19",
    kind: QuoteKind::Normative,
    source: CM,
    section: "§2.1.2 CBC Mode (AES-128CBCE and AES-128CBCD)",
    locator: "PDF p.20",
    url: AACS_URL,
    text: "Unless otherwise specified, the Initialization Vector used at the beginning of a CBC \
           encryption or decryption chain is a constant, iv0, which is: \
           0BA0F8DDFEA61FB3D8DF9F566A050F78₁₆",
};

pub const KS_20_UDF_EXTENT_LENGTH: SpecQuote = SpecQuote {
    id: "KS-20",
    kind: QuoteKind::Normative,
    source: UDF,
    section: "4/14.14.1.1 Extent Length",
    locator: "PDF p.116",
    url: UDF_URL,
    text: "The 30 least significant bits of this field shall be interpreted as a 30-bit unsigned \
           binary number specifying the length of the extent in bytes. … The 2 most significant \
           bits shall be interpreted as a 2-bit unsigned binary number specifying the type of the \
           extent",
};

pub const KS_21_UDF_EXTENT_TYPE: SpecQuote = SpecQuote {
    id: "KS-21",
    kind: QuoteKind::Normative,
    source: UDF,
    section: "4/14.14.1.1, Figure 4/42 Extent interpretation",
    locator: "PDF p.116",
    url: UDF_URL,
    text: "0 Extent recorded and allocated / 1 Extent not recorded but allocated / 2 Extent not \
           recorded and not allocated / 3 The extent is the next extent of allocation descriptors",
};

pub const KS_22_LIBAACS_VERIFY_TS: SpecQuote = SpecQuote {
    id: "KS-22",
    kind: QuoteKind::Corroboration,
    source: LIBAACS,
    section: "_verify_ts",
    locator: "aacs.c:957-969 @55be92be",
    url: LIBAACS_URL,
    text: "if (BD_UNLIKELY(buf[i + 4] != 0x47)) { return 0; } … /* Clear \
           copy_permission_indicator bits */ buf[i] &= ~0xc0;",
};

pub const KS_23_LIBAACS_BLOCK_KEY: SpecQuote = SpecQuote {
    id: "KS-23",
    kind: QuoteKind::Corroboration,
    source: LIBAACS,
    section: "_decrypt_unit (Block Key = AES-128E(Kcu, seed) ⊕ seed; KS-4, Figure 3-8)",
    locator: "aacs.c:983-990 @55be92be",
    url: LIBAACS_URL,
    text: "crypto_aes128e(aacs->uk->uk[curr_uk].key, out_buf, key); … key[a] ^= out_buf[a]; /* \
           here out_buf is plain data from in_buf */",
};

pub const KS_24_LIBAACS_CPI_CLEAR_UNIT: SpecQuote = SpecQuote {
    id: "KS-24",
    kind: QuoteKind::Corroboration,
    source: LIBAACS,
    section: "aacs_decrypt_unit",
    locator: "aacs.c:1191-1194 @55be92be",
    url: LIBAACS_URL,
    text: "if (!(buf[0] & 0xc0)) { // TP_extra_header Copy_permission_indicator == 0, unit is \
           not encrypted",
};

pub const KS_25_AACS2_EVIDENCE: SpecQuote = SpecQuote {
    id: "KS-25",
    kind: QuoteKind::Evidence,
    source: EVIDENCE,
    section: "AACS 2.x (UHD): Unit_Key_RO.inf stride; FMTS model",
    locator: "qa UHD fixture; aacs-unit-grid.md §2",
    url: "",
    text: "AACS 2.x (UHD): the 64-byte Unit_Key_RO.inf stride and the FMTS \
           IndividualSegment.tbl / .fmts model have no public book.",
};

pub const KS_26_FMTS_ANCHOR_EVIDENCE: SpecQuote = SpecQuote {
    id: "KS-26",
    kind: QuoteKind::Evidence,
    source: EVIDENCE,
    section: "FMTS anchor contract (key decode server)",
    locator: "key service FMTS anchor handler",
    url: "",
    text: "the decode server returns the whole set only for an index-1 anchor, and 422 for the \
           wrong phase",
};

pub const KS_27_HDDVD_EVIDENCE: SpecQuote = SpecQuote {
    id: "KS-27",
    kind: QuoteKind::Evidence,
    source: EVIDENCE,
    section: "HD DVD: AACS HD DVD book withdrawn from aacsla.com",
    locator: "aacsla.com AACS Specifications page, “HD DVD and DVD Books”",
    url: AACS_URL,
    text: "have been removed from this AACS website due to inactivity",
};

pub const KS_28_FILE_GRID_EVIDENCE: SpecQuote = SpecQuote {
    id: "KS-28",
    kind: QuoteKind::Evidence,
    source: EVIDENCE,
    section: "File-anchored unit grid on real discs (corroborates KS-1)",
    locator: "aacs-unit-grid.md §2 table",
    url: "",
    text: "32/32 sync on the file grid vs 0–1/32 on the disc-LBA grid",
};

pub const KS_29_VID_FROM_MEDIA: SpecQuote = SpecQuote {
    id: "KS-29",
    kind: QuoteKind::Normative,
    source: CM,
    section: "§4.4 Protocol for Transferring Volume Identifier, step 3",
    locator: "PDF p.53",
    url: AACS_URL,
    text: "The Licensed Drive reads Volume ID (Volume_ID) from the media and calculates a message \
           authentication code (Dm) from the Volume ID and the Bus Key (BK) calculated in step 26 \
           in Section 4.3.",
};

pub const KS_30_HDDVD_PACK_ENCRYPTION: SpecQuote = SpecQuote {
    id: "KS-30",
    kind: QuoteKind::Normative,
    source: HD,
    section: "§4.3.2 Pack Encryption, Table 4-7",
    locator: "PDF p.88",
    url: "",
    text: "For each encrypted Pack, the first 128 bytes are called the Unencrypted Portion and the \
           remaining 1920 bytes are called the Encrypted Portion. … Kc = AES-G (Kt, Dtk || \
           CPIlsb_96), … When a Pack is encrypted, the 2-bit PES_scrambling_control shall be 01₂. \
           Otherwise, the PES_scrambling_control shall be 00₂.",
};

pub const KS_31_HDDVD_CPI_EVIDENCE: SpecQuote = SpecQuote {
    id: "KS-31",
    kind: QuoteKind::Evidence,
    source: EVIDENCE,
    section: "HD DVD: CPI location in the NV_PCK GCI packet",
    locator: "two encrypted 300 pressings, decrypted against KS-30",
    url: "",
    text: "the 16-byte CPI sits at offset 12 of the GCI data (after the 0x04 sub_stream_id); \
           Dtk at pack bytes 84..88 opens 185/185 E-AC-3 packs, 83 or 85 opens 0/185",
};

/// Every `KS-n` quote, in ID order.
pub const ALL: &[&SpecQuote] = &[
    &KS_1_ENCRYPT_EVERY_UNIT,
    &KS_2_ALIGNED_UNIT,
    &KS_3_CBC_PER_UNIT,
    &KS_4_SEED,
    &KS_5_CPI,
    &KS_6_TP_EXTRA_HEADER,
    &KS_7_UNIT_CONTIGUOUS,
    &KS_8_EXTENTS_ASCENDING,
    &KS_9_SSIF_ALIGNED,
    &KS_10_TITLE_ONE_CPS_UNIT,
    &KS_11_CPS_NUMBER_FROM_ONE,
    &KS_12_UNIT_KEY_BLOCK_START,
    &KS_13_UNIT_KEY_FILE_HEADER,
    &KS_14_UNIT_KEY_BLOCK,
    &KS_15_KCU_WRAP,
    &KS_16_KVU,
    &KS_17_AES_G,
    &KS_18_BUS_ENCRYPTION_FLAG,
    &KS_19_IV0,
    &KS_20_UDF_EXTENT_LENGTH,
    &KS_21_UDF_EXTENT_TYPE,
    &KS_22_LIBAACS_VERIFY_TS,
    &KS_23_LIBAACS_BLOCK_KEY,
    &KS_24_LIBAACS_CPI_CLEAR_UNIT,
    &KS_25_AACS2_EVIDENCE,
    &KS_26_FMTS_ANCHOR_EVIDENCE,
    &KS_27_HDDVD_EVIDENCE,
    &KS_28_FILE_GRID_EVIDENCE,
    &KS_29_VID_FROM_MEDIA,
    &KS_30_HDDVD_PACK_ENCRYPTION,
    &KS_31_HDDVD_CPI_EVIDENCE,
];
