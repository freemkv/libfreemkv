//! `SS-n`: the quotes behind the Stop design's external behaviour (SCSI, POSIX, RFC,
//! OS and language docs). IDs SS-1…SS-25 are allocated by the Stop design §5.9;
//! SS-16…SS-19, SS-24 and SS-25 are retired and never reused. Rows land with the
//! step that first cites them.

use super::{QuoteKind, SpecQuote};

const SEAGATE: &str = "Seagate SCSI Commands Reference Manual, 100293068 Rev. J (October 2016)";
const SEAGATE_URL: &str = "https://www.seagate.com/files/staticfiles/support/docs/manual/Interface%20manuals/100293068j.pdf";
const MMC6: &str =
    "T10/1836-D SCSI Multi-Media Commands - 6 (MMC-6), Revision 2g, 11 December 2009";
const MMC6_URL: &str = "https://www.13thmonkey.org/documentation/SCSI/mmc6r02g.pdf";
const LIBAACS_MMC: &str = "libaacs (VideoLAN) @55be92be, src/libaacs/mmc.c";
const LIBAACS_MMC_URL: &str =
    "https://code.videolan.org/videolan/libaacs/-/blob/55be92be/src/libaacs/mmc.c";

// SS-1, SS-2, SS-4: the Seagate manual reproduces the SPC text but is not SPC
// itself, so these are Corroboration (J-5.5-6: the reviewer re-kinds per row).

pub const SS_1_SENSE_PROGRESS: SpecQuote = SpecQuote {
    id: "SS-1",
    kind: QuoteKind::Corroboration,
    source: SEAGATE,
    section: "SPC §2.4.1.1.4.4 Progress indication sense key specific data (Table 18, Table 21); \
              §2.4.1.2 Fixed format sense data (Table 27), as reproduced in the Seagate manual",
    locator: "PDF p.51-52, p.56-57",
    url: SEAGATE_URL,
    text: "NO SENSE or NOT READY … Progress indication … If the sense key is NO SENSE or NOT \
           READY, the SENSE KEY SPECIFIC field shall be as shown in table 21. … The PROGRESS \
           INDICATION field is a percent complete indication in which the returned value is a \
           numerator that has 65 536 (10000h) as its denominator. … A sense-key specific valid \
           (SKSV) bit set to one indicates the SENSE KEY SPECIFIC field contains valid \
           information as defined in this manual.",
};

pub const SS_2_DESCRIPTOR_SENSE: SpecQuote = SpecQuote {
    id: "SS-2",
    kind: QuoteKind::Corroboration,
    source: SEAGATE,
    section: "SPC §2.4.1.1.1 Descriptor format sense data (Table 12); §2.4.1.1.4.1 Sense key \
              specific sense data descriptor (Table 17), as reproduced in the Seagate manual",
    locator: "PDF p.47, p.50",
    url: SEAGATE_URL,
    text: "The descriptor format sense data for response codes 72h (current errors) and 73h \
           (deferred errors) is defined in table 12. … The sense key specific sense data \
           descriptor (see table 17) provides additional information about the exception \
           condition.",
};

pub const SS_3_READINESS_ERRORS: SpecQuote = SpecQuote {
    id: "SS-3",
    kind: QuoteKind::Normative,
    source: MMC6,
    section: "Annex F §F.3.3 Readiness Errors, Table F.3",
    locator: "PDF p.695 (printed 646-647)",
    url: MMC6_URL,
    text: "In the event that a command requires a level of readiness that does not currently \
           exist, the Drive should be terminated with CHECK CONDITION status and sense bytes \
           SK/ASC/ASCQ should be selected from those shown in Table F.3. … 2 04 01 LOGICAL UNIT \
           IS IN PROCESS OF BECOMING READY 2 04 02 LOGICAL UNIT NOT READY, INITIALIZING CMD. \
           REQUIRED … 2 30 00 INCOMPATIBLE MEDIUM INSTALLED … 2 3A 00 MEDIUM NOT PRESENT",
};

pub const SS_4_TEST_UNIT_READY: SpecQuote = SpecQuote {
    id: "SS-4",
    kind: QuoteKind::Corroboration,
    source: SEAGATE,
    section: "SPC §3.53 TEST UNIT READY command, as reproduced in the Seagate manual",
    locator: "PDF p.230",
    url: SEAGATE_URL,
    text: "The TEST UNIT READY command (see table 202) provides a means to check if the logical \
           unit is ready. … If the logical unit is unable to become operational or is in a \
           state such that an application client action (e.g., START UNIT command) is required \
           to make the logical unit ready, the command shall be terminated with CHECK CONDITION \
           status, with the sense key set to NOT READY.",
};

pub const SS_5_PREVENT_ALLOW: SpecQuote = SpecQuote {
    id: "SS-5",
    kind: QuoteKind::Normative,
    source: MMC6,
    section: "§6.13.2 PREVENT ALLOW MEDIUM REMOVAL, The CDB and its Parameters, Table 329",
    locator: "PDF p.396 (printed 348)",
    url: MMC6_URL,
    text: "The Persistent and Prevent bits are used to independently select values for these \
           states. See Table 329. … 0 0 Prevent State shall be cleared (Unlocked) 0 1 Prevent \
           State shall be set (Locked)",
};

pub const SS_6_START_STOP_LOEJ: SpecQuote = SpecQuote {
    id: "SS-6",
    kind: QuoteKind::Normative,
    source: MMC6,
    section: "§6.42.2.6 LoEj and Start, Table 633",
    locator: "PDF p.623 (printed 575)",
    url: MMC6_URL,
    text: "When Power Conditions field is zero and FL is zero, the meanings of LoEj and Start \
           are defined in Table 633. … 0 1 Start the disc and make ready for access 1 0 Eject \
           the disc if permitted.",
};

pub const SS_7_AGID_INVALIDATE: SpecQuote = SpecQuote {
    id: "SS-7",
    kind: QuoteKind::Evidence,
    source: LIBAACS_MMC,
    section: "_mmc_report_key, _mmc_invalidate_agid, _mmc_report_agid (no public MMC key-format \
              table)",
    locator: "mmc.c:114-135, 202-230 @55be92be",
    url: LIBAACS_MMC_URL,
    text: "cmd[10] = (agid << 6) | (format & 0x3f); … return _mmc_report_key(mmc, agid, 0, 0, \
           MMC_REPORT_KEY_AACS_INVALIDATE_AGID, buf, sizeof(buf)); … *agid = (buf[7] & 0xff) \
           >> 6;",
};

const RUST_STD: &str = "The Rust Standard Library, core::sync::atomic (Rust 1.98.1)";
const ORDERING_URL: &str = "https://doc.rust-lang.org/std/sync/atomic/enum.Ordering.html";

const POSIX: &str = "The Open Group Base Specifications Issue 8 (IEEE Std 1003.1-2024), XSH";
const FSYNC_URL: &str = "https://pubs.opengroup.org/onlinepubs/9799919799/functions/fsync.html";

pub const SS_12_FSYNC_FDATASYNC: SpecQuote = SpecQuote {
    id: "SS-12",
    kind: QuoteKind::Normative,
    source: POSIX,
    section: "fsync() DESCRIPTION; fdatasync() DESCRIPTION",
    locator: "pubs.opengroup.org functions/fsync.html and functions/fdatasync.html",
    url: FSYNC_URL,
    text: "The fsync() function shall request that all data for the open file descriptor named \
           by fildes is to be transferred to the storage device associated with the file \
           described by fildes. … The fsync() function shall not return until the system has \
           completed that action or until an error is detected. … The fdatasync() function \
           shall force all currently queued I/O operations associated with the file indicated \
           by file descriptor fildes to the synchronized I/O completion state.",
};

pub const SS_13_SYNC_FILE_RANGE: SpecQuote = SpecQuote {
    id: "SS-13",
    kind: QuoteKind::Normative,
    source: "Linux man-pages, sync_file_range(2)",
    section: "DESCRIPTION, Warning and Some details",
    locator: "man7.org sync_file_range(2)",
    url: "https://man7.org/linux/man-pages/man2/sync_file_range.2.html",
    text: "None of these operations writes out the file's metadata. … This system call does \
           not flush disk write caches and thus does not provide any data integrity on systems \
           with volatile disk write caches. … SYNC_FILE_RANGE_WAIT_BEFORE | \
           SYNC_FILE_RANGE_WRITE | SYNC_FILE_RANGE_WAIT_AFTER This is a write-for-data-integrity \
           operation that will ensure that all pages in the specified range which were dirty \
           when sync_file_range() was called are committed to disk.",
};

pub const SS_14_F_FULLFSYNC: SpecQuote = SpecQuote {
    id: "SS-14",
    kind: QuoteKind::Normative,
    source: "Apple xnu, fcntl(2) manual page (bsd/man/man2/fcntl.2)",
    section: "DESCRIPTION, F_FULLFSYNC",
    locator: "xnu bsd/man/man2/fcntl.2 (apple-oss-distributions, main); man 2 fcntl on macOS",
    url: "https://github.com/apple-oss-distributions/xnu/blob/main/bsd/man/man2/fcntl.2",
    text: "Does the same thing as fsync(2) then asks the drive to flush all buffered data to \
           the permanent storage device (arg is ignored). … This is currently implemented on \
           HFS, MS-DOS (FAT), Universal Disk Format (UDF) and APFS file systems.",
};

pub const SS_15_FLUSH_FILE_BUFFERS: SpecQuote = SpecQuote {
    id: "SS-15",
    kind: QuoteKind::Normative,
    source: "Microsoft Learn, Win32 API: FlushFileBuffers function (fileapi.h)",
    section: "Remarks",
    locator: "learn.microsoft.com nf-fileapi-flushfilebuffers",
    url: "https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-flushfilebuffers",
    text: "The FlushFileBuffers function writes all the buffered information for a specified \
           file to the device or pipe.",
};

pub const SS_23_RELEASE_ACQUIRE: SpecQuote = SpecQuote {
    id: "SS-23",
    kind: QuoteKind::Normative,
    source: RUST_STD,
    section: "enum Ordering, variants Release and Acquire",
    locator: "doc.rust-lang.org std::sync::atomic::Ordering; library/core/src/sync/atomic.rs",
    url: ORDERING_URL,
    text: "In particular, all previous writes become visible to all threads that perform an \
           Acquire (or stronger) load of this value. … In particular, all subsequent loads will \
           see data written before the store.",
};

/// Every `SS-n` quote, in ID order.
pub const ALL: &[&SpecQuote] = &[
    &SS_1_SENSE_PROGRESS,
    &SS_2_DESCRIPTOR_SENSE,
    &SS_3_READINESS_ERRORS,
    &SS_4_TEST_UNIT_READY,
    &SS_5_PREVENT_ALLOW,
    &SS_6_START_STOP_LOEJ,
    &SS_7_AGID_INVALIDATE,
    &SS_12_FSYNC_FDATASYNC,
    &SS_13_SYNC_FILE_RANGE,
    &SS_14_F_FULLFSYNC,
    &SS_15_FLUSH_FILE_BUFFERS,
    &SS_23_RELEASE_ACQUIRE,
];
