//! Error types for libfreemkv.
//!
//! Every error is a code with structured data. No English text.
//! Applications map codes to localized messages.

// ── Error codes ─────────────────────────────────────────────────────────────

// Device (1xxx)
pub const E_DEVICE_NOT_FOUND: u16 = 1000;
pub const E_DEVICE_PERMISSION: u16 = 1001;
pub const E_DEVICE_NOT_READY: u16 = 1002;
pub const E_DEVICE_RESET_FAILED: u16 = 1003;
pub const E_SCSI_INTERFACE_UNAVAILABLE: u16 = 1004;
pub const E_DEVICE_LOCKED: u16 = 1005;
pub const E_IOKIT_PLUGIN_FAILED: u16 = 1006;

// Profile (2xxx)
pub const E_UNSUPPORTED_DRIVE: u16 = 2000;
// 2001: burned/retired — do not reuse.
pub const E_PROFILE_PARSE: u16 = 2002;
pub const E_UNSUPPORTED_PLATFORM: u16 = 2003;
pub const E_PLATFORM_NOT_IMPLEMENTED: u16 = 2004;

// Unlock (3xxx)
pub const E_UNLOCK_FAILED: u16 = 3000;
pub const E_SIGNATURE_MISMATCH: u16 = 3001;

// SCSI (4xxx)
pub const E_SCSI_ERROR: u16 = 4000;
pub const E_INVALID_CDB_LENGTH: u16 = 4001;

// I/O (5xxx)
pub const E_IO_ERROR: u16 = 5000;
pub const E_SOURCE_TERMINATED: u16 = 5001;

// Disc format (6xxx)
pub const E_DISC_READ: u16 = 6000;
pub const E_MPLS_PARSE: u16 = 6001;
pub const E_CLPI_PARSE: u16 = 6002;
pub const E_UDF_NOT_FOUND: u16 = 6003;
// 6004: burned/retired — do not reuse.
pub const E_DISC_TITLE_RANGE: u16 = 6005;
// 6006: burned/retired — do not reuse.
pub const E_IFO_PARSE: u16 = 6007;
pub const E_MKV_INVALID: u16 = 6008;
pub const E_NO_STREAMS: u16 = 6009;
pub const E_HALTED: u16 = 6010;
pub const E_MAPFILE_INVALID: u16 = 6011;
pub const E_SELECTION_PID_UNKNOWN: u16 = 6014;
pub const E_UDF_BUFFER_TOO_SMALL: u16 = 6012;
pub const E_UDF_NOT_FILESYSTEM: u16 = 6013;
pub const E_IMAGE_TRUNCATED: u16 = 6015;
pub const E_UDF_AD_CHAIN_TOO_LONG: u16 = 6016;
pub const E_UDF_UNRECORDED_EXTENT: u16 = 6017;
pub const E_UDF_EMBEDDED_DATA: u16 = 6018;
/// A file that EXISTS and whose allocation descriptors resolved without error,
/// yet yields not one usable extent: an empty AD list, or a list every entry of
/// which is zero-length or points at LBA 0. Reported by the HD-DVD clip
/// resolver (`disc::hddvd`).
///
/// It has no [`Error`] variant on purpose: `UdfFs::file_extents` returns `Ok(vec![])` here and
/// the CALLER detects the condition.
pub const E_UDF_NO_USABLE_EXTENT: u16 = 6019;
pub const E_IMAGE_ENDS_BEFORE_READ: u16 = 6020;
/// A whole-disc image (`iso://`, sweep) is refused: a bus-encrypted Clip AV stream
/// file's File Entry could not be read, so its sectors cannot be located to de-bus
/// (AACS BD Pre-recorded Book 0.953 §3.7). MKV and `dir://` still proceed.
pub const E_BUS_STREAM_UNMAPPED: u16 = 6021;
/// An image staged for an MKV rip (its mapfile records a scope) was offered as a
/// whole-disc image (`iso://` copy, `dir://` extract): only the chosen titles, nav
/// and UDF were ever read, so the rest of it is not disc data.
pub const E_IMAGE_SCOPED: u16 = 6022;
/// An HD-DVD `ADV_OBJ/VPLST*.XPL` playlist exceeds the parser's size cap. Logged by
/// `disc::hddvd`, which then falls back to the per-clip heuristic; no [`Error`] variant.
pub const E_XPL_TOO_LARGE: u16 = 6023;

// AACS (7xxx)
pub const E_AACS_NO_KEYS: u16 = 7000;
pub const E_AACS_CERT_SHORT: u16 = 7001;
pub const E_AACS_AGID_ALLOC: u16 = 7002;
pub const E_AACS_CERT_REJECTED: u16 = 7003;
pub const E_AACS_CERT_READ: u16 = 7004;
pub const E_AACS_CERT_VERIFY: u16 = 7005;
pub const E_AACS_KEY_READ: u16 = 7006;
pub const E_AACS_KEY_REJECTED: u16 = 7007;
pub const E_AACS_KEY_VERIFY: u16 = 7008;
pub const E_AACS_VID_READ: u16 = 7009;
pub const E_AACS_VID_MAC: u16 = 7010;
pub const E_AACS_DATA_KEY: u16 = 7011;
// 7012: burned/retired — do not reuse.
pub const E_DECRYPT_FAILED: u16 = 7013;
pub const E_CSS_AUTH_FAILED: u16 = 7014;
pub const E_AACS_HOST_CERT_REJECTED: u16 = 7015;
pub const E_AACS_RAW_READ_UNSUPPORTED: u16 = 7016;
pub const E_AACS_VID_UNAVAILABLE: u16 = 7017;
pub const E_AACS_MK_UNAVAILABLE: u16 = 7018;
pub const E_AACS_VUK_NOT_IN_KEYDB: u16 = 7019;
pub const E_DRIVE_PROFILE_MISSING: u16 = 7020;
pub const E_VID_CDB_UNAVAILABLE: u16 = 7021;
pub const E_NO_DISC_KEY: u16 = 7022;
pub const E_CSS_KEY_MISSING: u16 = 7023;
pub const E_AACS_NO_HOST_CERT: u16 = 7024;
pub const E_AACS_BUS_KEY_UNAVAILABLE: u16 = 7025;
pub const E_FMTS_KEY_MISSING: u16 = 7026;
/// The CSS disc as a WHOLE could not be decrypted — the scan saw scrambled sectors and the
/// known-plaintext crack recovered no title key at all, so every title will fail identically.
/// The CSS analogue of [`E_NO_DISC_KEY`], and deliberately NOT [`E_CSS_KEY_MISSING`], which
/// [`is_skippable_title_stub`] treats as a skippable per-title stub. [`is_disc_level_no_key`]
/// classifies this code, so a multi-title rip loop fails fast on it.
pub const E_CSS_NO_DISC_KEY: u16 = 7027;
/// A key SOURCE could not be reached, or failed on its own side — transport
/// error, DNS failure, timeout, TLS failure, an HTTP 5xx, or a reply the client
/// could not read. The source never got as far as answering the question, so
/// nothing at all is known about whether a key for this disc exists.
///
/// Deliberately NOT [`E_NO_DISC_KEY`], which asserts the OPPOSITE — every source answered and
/// none holds a key. Transient: retry later.
pub const E_KEY_SERVICE_UNAVAILABLE: u16 = 7028;
/// A key source rejected the configured credentials (HTTP 401/403 from the online
/// key service). NOT transient and NOT an absent key — the operator action is to
/// fix the token, not to wait and not to look for a VUK.
pub const E_KEY_SERVICE_UNAUTHORIZED: u16 = 7029;
/// A key source rate-limited the request (HTTP 429 from the online key service).
/// The operator action is to back off and retry more slowly; the disc's key may
/// well exist.
pub const E_KEY_SERVICE_RATE_LIMITED: u16 = 7030;
/// A live AACS disc's `Unit_Key_RO.inf` (both copies) is missing or unreadable.
/// The key file is required, so the live scan stops. NOT [`E_AACS_NO_KEYS`]:
/// no key lookup was reached, and the fix is the disc, not a key database.
pub const E_AACS_KEY_FILE_UNREADABLE: u16 = 7031;
/// A decrypted whole-disc image (disc/image -> ISO) cannot be made: a stream
/// file no title plays is encrypted with a key none of the held keys opens.
/// NOT [`E_DECRYPT_FAILED`]: the fix is an MKV rip or a raw copy, not a key.
pub const E_WHOLE_DISC_KEY_MISSING: u16 = 7032;
/// A key source offered host cert(s), but every one failed a local check
/// before any drive round-trip (keydb data problem). NOT
/// [`E_AACS_NO_HOST_CERT`] (no cert offered at all) or
/// [`E_AACS_HOST_CERT_REJECTED`] (the drive itself rejected a cert).
pub const E_AACS_NO_USABLE_HOST_CERT: u16 = 7033;
/// An image piece is Missing, no VID is in hand, and the sidecar mapfile
/// carries a `vidfp`: the key is derivable only from the disc's Volume ID.
/// "the server must tell the two apart by code" (keys-upfront-design J11).
pub const E_AACS_VID_NEEDS_DISC: u16 = 7034;
// AACS 2.1 variant media-key chain (`MediaKeyVariantError`); 7105 is a reserved gap.
pub const E_MKB_VARIANT_NOT_VARIANT: u16 = 7100;
pub const E_MKB_VARIANT_INCOMPLETE: u16 = 7101;
pub const E_MKB_VARIANT_PK_UNAVAILABLE: u16 = 7102;
pub const E_MKB_VARIANT_SOFT_CORRECTION: u16 = 7103;
pub const E_MKB_VARIANT_ONLINE_CHALLENGE: u16 = 7104;
pub const E_MKB_VARIANT_TABLE_UNAVAILABLE: u16 = 7106;
pub const E_MKB_VARIANT_VKD_RANGE: u16 = 7107;
pub const E_MKB_VARIANT_VERIFY_FAILED: u16 = 7108;
/// The disc's MKB is Class II (`0x000A1003`): its Media Key Data is record `0x0c`, not the
/// classical `0x05` the PK/DK derivation reads, so no key can be derived from it
/// (`MkbClassError`).
pub const E_MKB_CLASS_UNSUPPORTED: u16 = 7109;

// Keydb (8xxx)
pub const E_KEYDB_CONNECT: u16 = 8000;
pub const E_KEYDB_HTTP: u16 = 8001;
pub const E_KEYDB_INVALID: u16 = 8002;
pub const E_KEYDB_WRITE: u16 = 8003;
pub const E_KEYDB_PARSE: u16 = 8004;
pub const E_KEYDB_LOAD: u16 = 8005;
pub const E_KEYDB_UNSUPPORTED_SCHEME: u16 = 8006;
pub const E_KEYDB_TOO_MANY_REDIRECTS: u16 = 8007;

// Stream/mux (9xxx)
pub const E_STREAM_READ_ONLY: u16 = 9000;
pub const E_STREAM_WRITE_ONLY: u16 = 9001;
pub const E_STREAM_URL_INVALID: u16 = 9002;
pub const E_STREAM_URL_MISSING_PATH: u16 = 9003;
pub const E_STREAM_URL_MISSING_PORT: u16 = 9004;
pub const E_PES_FRAME_TOO_LARGE: u16 = 9005;
pub const E_PES_INVALID_MAGIC: u16 = 9006;
pub const E_ISO_TOO_LARGE: u16 = 9007;
pub const E_NO_METADATA: u16 = 9008;
/// `--raw` given with a `dir://` destination (raw + decrypted-tree is
/// a contradiction; raw bytes go to `iso://`).
pub const E_DIR_RAW_REJECTED: u16 = 9019;
pub const E_HEVC_PARAM_PARSE: u16 = 9010;
pub const E_MUX_TRACK_RANGE: u16 = 9011;
pub const E_FMP4_UNIMPLEMENTED: u16 = 9012;
pub const E_DEMUX_THREAD_PANICKED: u16 = 9013;
pub const E_PIPELINE_JOIN_TIMEOUT: u16 = 9014;
pub const E_PIPELINE_CONSUMER_PANICKED: u16 = 9015;
pub const E_SWEEP_CONSUMER_GONE: u16 = 9016;
pub const E_PES_TRACK_TOO_LARGE: u16 = 9017;
pub const E_PIPELINE_CONSUMER_GONE: u16 = 9018;
pub const E_DISC_CAPACITY_OVERFLOW: u16 = 9020;
/// `--multipass` given with a `dir://` destination (`dir://` is 1-shot;
/// recovery is the `iso://` path's job).
pub const E_DIR_MULTIPASS_REJECTED: u16 = 9024;
/// A non-disc (byte-stream) source was routed into `dir://`, which needs a
/// filesystem (only `disc://` / `iso://` qualify).
pub const E_DIR_SOURCE_UNSUPPORTED: u16 = 9025;
/// `dir://` target directory is non-empty and `--force` was not given.
pub const E_DIR_NOT_EMPTY: u16 = 9026;
/// `dir://` target filesystem free space is below the sum of file extents.
pub const E_DIR_INSUFFICIENT_SPACE: u16 = 9027;
/// Two distinct disc paths sanitize to the same host path (would silently
/// overwrite — surfaced as a hard error instead).
pub const E_DIR_NAME_COLLISION: u16 = 9028;
/// A `dir://` create_dir_all / file write / rename failed.
pub const E_DIR_WRITE_FAILED: u16 = 9029;
/// A `dir://` SOURCE folder carries `BDMV/STREAM/SSIF/` (Blu-ray 3D). The
/// scanner detects SSIF unconditionally and would rip it as 3D, but the
/// synthetic-image planner has no extent-aliasing support (an SSIF interleaves
/// the same sectors as the base/dependent `.m2ts`), so the output would be
/// silently wrong. Rejected up front instead.
pub const E_DIR_IMAGE_SSIF_UNSUPPORTED: u16 = 9061;
/// A `dir://` SOURCE `VIDEO_TS` folder's IFO-declared VOB offsets cannot be
/// satisfied by any placement: the required start sector of a VOB lies BELOW
/// the end of the file that must precede it. Carries the offending file's disc
/// path. Also raised for an IFO too short to resolve its placement offsets.
pub const E_DIR_IMAGE_PLACEMENT: u16 = 9062;
/// A `dir://` SOURCE folder holds no disc structure the image synthesizer
/// understands (no `BDMV/`, no `VIDEO_TS/`).
pub const E_DIR_IMAGE_UNSUPPORTED_TREE: u16 = 9064;
/// A file inside a `dir://` SOURCE folder changed (shrank / was removed)
/// between planning and reading. Zero-filling the gap would produce corrupt
/// output at exit 0, so the read fails instead. Carries the disc path.
pub const E_DIR_IMAGE_FILE_CHANGED: u16 = 9065;
/// A `dir://` SOURCE folder does not fit a 32-bit sector address space
/// (> 2^32 sectors ≈ 8 TiB), or holds more entries than a UDF tree can carry.
pub const E_DIR_IMAGE_TOO_LARGE: u16 = 9066;
/// A name in the folder is too long to record in a UDF directory entry: the
/// File Identifier Descriptor stores the encoded length in one byte.
pub const E_DIR_NAME_TOO_LONG: u16 = 9067;
/// One directory in the folder holds more subdirectories than a UDF link count
/// can express (16 bits), or more entries than the reader's directory size cap.
pub const E_DIR_IMAGE_FANOUT: u16 = 9068;
/// A title's clip marks excluded more frames than they kept.
pub const E_SEAM_PLAN_DROPPED_MOST: u16 = 9069;
/// A sink finished having written no frames at all.
pub const E_SINK_WROTE_NOTHING: u16 = 9070;
/// A write or finish reached a stream that is already finished or whose output failed.
pub const E_STREAM_CLOSED: u16 = 9071;
/// Per-track metadata was set after the stream header was already written.
pub const E_STREAM_HEADER_WRITTEN: u16 = 9072;
pub const E_M2TS_PACKET_MALFORMED: u16 = 9021;
/// A `network://` output target resolved to no connectable address (every
/// resolved IP was unspecified / multicast / broadcast / reserved).
pub const E_NETWORK_ADDR_BLOCKED: u16 = 9022;
/// A muxer's `finish()` was called after zero frames were emitted — the
/// output would be a header-only container with no media. Surfaced so a
/// zero-frame mux (undecryptable input, fully-unreadable title, every
/// frame dropped before the first keyframe) cannot report success.
pub const E_MUX_EMPTY: u16 = 9023;
/// The mux driver buffered past its pre-headers cap without every video track's
/// `codec_private` resolving. Deliberately NOT [`E_MKV_INVALID`], which
/// [`is_skippable_title_stub`] treats as a skippable empty nav/menu stub — a
/// cap-overflow is a real title and must never be silently skipped.
pub const E_MUX_HEADER_BUFFER_EXCEEDED: u16 = 9051;
/// An `mkv://` SOURCE Block declared lacing (RFC 9559 §10.3) whose header does
/// not describe its own payload, so the frame boundaries inside the Block are
/// unknowable. Deliberately NOT [`E_MKV_INVALID`], which
/// [`is_skippable_title_stub`] treats as a skippable empty nav/menu stub: a
/// laced Block belongs to a track with real media in it, and mis-reporting the
/// rejection as a stub would drop that media from a run that then exits
/// successfully — the same conflation [`E_MUX_HEADER_BUFFER_EXCEEDED`] exists to
/// avoid.
pub const E_MKV_LACING_INVALID: u16 = 9052;
/// An `mkv://` SOURCE file is malformed or truncated — the EBML/Matroska reader
/// rejected it (bad element ID or VINT size, an unknown-size element where a
/// finite one is required, a child overrunning its parent, a truncated element
/// body, a non-UTF-8 string element, an element size above the parser's
/// allocation caps, an out-of-range TimestampScale / cluster timestamp / track
/// number).
///
/// Deliberately NOT [`E_MKV_INVALID`] (a no-frames stub, skippable).
pub const E_MKV_SOURCE_INVALID: u16 = 9053;
/// The Matroska WRITER was asked to emit something EBML cannot represent: an
/// element body at or above the 56-bit VINT payload limit (which would encode
/// byte-for-byte as the reserved "unknown size" marker or not fit at all), or a
/// master-element size placeholder that no longer lies inside the buffer being
/// patched. An output-side limit, not a property of any input — so neither
/// [`E_MKV_INVALID`] (a no-frames stub) nor [`E_MKV_SOURCE_INVALID`] (a corrupt
/// source) describes it, and it must not be classified as skippable.
pub const E_MKV_UNENCODABLE: u16 = 9054;
pub const E_EXTENT_NOT_UNIT_ALIGNED: u16 = 9030;
/// `mp4://` output but the title has no (primary) video track to carry.
pub const E_MP4_NO_VIDEO_TRACK: u16 = 9048;
/// `mp4://` SOURCE file is malformed/truncated (bad box structure, sample table,
/// or offsets) — the MP4 demuxer could not parse it.
pub const E_MP4_INVALID: u16 = 9049;
/// `mp4://` video track is missing its codec-configuration record
/// (`hvcC`/`avcC`), without which the sample entry can't be written.
pub const E_MP4_MISSING_CODEC_PRIVATE: u16 = 9050;
/// `mp4://` video track has no resolved frame dimensions. ISO/IEC 14496-12
/// makes width and height mandatory in both `tkhd` (8.3.2) and
/// VisualSampleEntry (12.1.3), so unlike Matroska there is no element to omit:
/// the sink would have to write 0x0, producing a structurally complete file no
/// player can render. Refuse instead.
pub const E_MP4_UNKNOWN_RESOLUTION: u16 = 9055;
/// The bounded durable flush did not complete within its deadline. The data is
/// NOT known to be on stable storage; the kernel will still flush on close, but
/// that is a probability, not a barrier.
pub const E_SYNC_TIMEOUT: u16 = 9056;
/// The bounded durable flush's worker thread was lost before it reported. Same
/// durability consequence as [`E_SYNC_TIMEOUT`], different cause — a caller
/// retrying a timeout should not retry this.
pub const E_SYNC_WORKER_LOST: u16 = 9057;
/// READ CAPACITY returned a short or overflowing transfer.
pub const E_DISC_CAPACITY_MALFORMED: u16 = 9047;
pub const E_DRIVE_INQUIRY_SHORT: u16 = 9058;
pub const E_SHORT_IMAGE_READ: u16 = 9059;
pub const E_EMPTY_IMAGE: u16 = 9060;
/// A `StallTimer` expired: `op` names the stalled operation (e.g.
/// "artifact_lock", "verify"). "An expired `StallTimer` produces
/// `Error::TimedOut { op }`" (stop-design-v5 §2.1).
pub const E_TIMED_OUT: u16 = 9073;
/// `mpg://` output but the title has no video track a program stream can carry
/// (MPEG-1/2, H.264, HEVC or VC-1). The `mpg://` twin of [`E_MP4_NO_VIDEO_TRACK`].
pub const E_MPG_NO_VIDEO_TRACK: u16 = 9074;
/// `mpg://` reached end of input with access units no pack could take.
pub const E_MPG_UNPACKETIZED: u16 = 9075;
// 9076: burned/retired — do not reuse.
/// A finished remux output failed its read-back check. See [`RemuxVerifyKind`].
pub const E_REMUX_VERIFY_FAILED: u16 = 9077;
/// A title's mux returned without completing and no Stop was requested.
pub const E_MUX_INCOMPLETE: u16 = 9078;
/// A remux staging path collides with the target or its own `.partial`.
pub const E_REMUX_STAGING_INVALID: u16 = 9079;
/// Copying a staged output into place wrote a different byte count than the source holds.
pub const E_STAGED_COPY_SIZE_MISMATCH: u16 = 9080;
/// A remux worker thread (`op`: "copy", "verify") was lost before it reported.
pub const E_WORKER_LOST: u16 = 9081;
/// A requested audio/subtitle language tag resolves to no known language.
pub const E_STREAM_LANGUAGE_UNKNOWN: u16 = 9083;
/// A remux target already exists and replacing it was not requested.
pub const E_REMUX_TARGET_EXISTS: u16 = 9084;
/// A mux was opened with a zero read batch (`MuxOptions::default()`), which reads nothing.
pub const E_MUX_BATCH_SECTORS_ZERO: u16 = 9085;

// ── Error enum ──────────────────────────────────────────────────────────────

/// Structured error with numeric code and context data. No English text.
///
/// Marked `#[non_exhaustive]`: downstream crates must not match it
/// exhaustively, so new variants can be added without a semver break.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    // Device (1xxx)
    DeviceNotFound {
        path: String,
    },
    DevicePermission {
        path: String,
    },
    DeviceNotReady {
        path: String,
    },
    DeviceResetFailed {
        path: String,
    },
    /// Platform-specific SCSI interface couldn't be obtained from the OS
    /// (macOS: `SCSITaskDeviceInterface` unavailable). The `path` field
    /// carries the device path; no English commentary on the failure mode.
    ScsiInterfaceUnavailable {
        path: String,
    },
    /// Device is held by another process / kernel state. `kr` is the
    /// platform return code (macOS IOReturn, Linux errno-equivalent).
    DeviceLocked {
        path: String,
        kr: u32,
    },
    /// macOS IOKit plugin couldn't be created for this device. `kr` is
    /// the IOReturn code from `IOCreatePlugInInterfaceForService`.
    IoKitPluginFailed {
        path: String,
        kr: u32,
    },

    // Profile (2xxx)
    UnsupportedDrive {
        vendor_id: String,
        product_id: String,
        product_revision: String,
    },
    ProfileParse,
    /// SCSI transport was requested on an OS without a backend
    /// implementation. `target` is the `std::env::consts::OS` value.
    UnsupportedPlatform {
        target: String,
    },
    /// Drive matched a known platform that we haven't implemented yet
    /// (e.g. Renesas firmware). `platform` is a stable identifier.
    PlatformNotImplemented {
        platform: String,
    },

    // Unlock (3xxx)
    UnlockFailed,
    SignatureMismatch {
        expected: [u8; 4],
        got: [u8; 4],
    },

    // SCSI (4xxx)
    /// SCSI command failed.
    ///
    /// `opcode` is the failing CDB byte 0. `status` is the raw SCSI status byte:
    /// `0x02` = CHECK CONDITION (sense data present), `0xFF` = synthesised
    /// sentinel for "no SCSI status delivered" (transport wedge). `sense` is the
    /// drive's SPC-4 sense triple, `None` for transport-layer failures.
    ///
    /// Prefer [`Error::is_scsi_transport_failure`], [`Error::is_marginal_read`],
    /// and [`Error::scsi_sense`] over pattern-matching these fields directly.
    ScsiError {
        opcode: u8,
        status: u8,
        sense: Option<crate::scsi::ScsiSense>,
    },
    /// CDB supplied to the transport exceeded the maximum supported length.
    /// `len` is the supplied CDB length; `max` is the transport's limit.
    InvalidCdbLength {
        len: usize,
        max: usize,
    },

    // I/O (5xxx)
    IoError {
        source: std::io::Error,
    },

    // Disc format (6xxx)
    DiscRead {
        sector: u64,
        status: Option<u8>,
        sense: Option<crate::scsi::ScsiSense>,
    },
    /// Drive was halted by caller.
    Halted,
    MplsParse,
    ClpiParse,
    UdfNotFound {
        path: String,
    },
    /// The file's ICB allocation list contains an unrecorded (ECMA-167
    /// 4/14.14.1.1 type-1/type-2) extent: space allocated to the file at that
    /// location but never written, so its true content there is zeros while
    /// the media holds something else.
    ///
    /// Raised by [`crate::udf::UdfFs::file_extents`] because a
    /// `(lba, sector_count)` read plan cannot express a hole — reading it
    /// splices undefined sectors into the rip as content, and dropping it
    /// slides every later extent's byte space.
    UdfUnrecordedExtent {
        path: String,
    },
    /// The reader was addressable but the bytes are structurally NOT a UDF
    /// filesystem — a deterministic tag/format mismatch (e.g. no Anchor Volume
    /// Descriptor Pointer at sector 256, no partition descriptor, no File Set
    /// Descriptor). Distinct from [`Error::DiscRead`] (a transient I/O fault):
    /// this is a stable property of the media, not something a retry fixes. Lets
    /// callers (notably FMTS key resolution) treat "not a UDF/FMTS disc" as a
    /// clean negative while still failing loud on a real read fault.
    UdfNotFilesystem,
    /// A `SectorSource` caller passed a destination buffer smaller than one
    /// 2048-byte sector. A contract violation on the public reader API —
    /// returned instead of panicking on the slice.
    UdfBufferTooSmall,
    /// A file's allocation-descriptor continuation chain did not end within the
    /// hop budget the UDF reader allows.
    ///
    /// The budget exists so a crafted or corrupt disc cannot loop the reader
    /// forever. Hitting it is NOT the end of the chain: the extents beyond that
    /// point are unknown, so the extent list in hand describes only part of the
    /// file. Returning that list would let a caller zero-pad the remainder to
    /// the declared size and report a mostly-empty file as a complete
    /// extraction, so the read fails instead.
    UdfAdChainTooLong,
    /// A file's ICB declares its data EMBEDDED inline (ECMA-167 4/14.6.8
    /// allocation-descriptor type 3), so it has no out-of-line extents at all.
    ///
    /// Returned only when a caller asked for a read plan over such a file; decoding the inline
    /// bytes as (length, LBA) pairs would manufacture extents out of file content and point the
    /// reader at unrelated sectors. Callers that expect an embedded file (e.g. AACS `*.inf`)
    /// read it via `read_inline_data` instead.
    UdfEmbeddedData,
    DiscTitleRange {
        index: usize,
        count: usize,
    },
    /// An image-level write read fewer bytes than the sector count it asked for.
    ///
    /// Distinct from [`Error::IoError`] on purpose: the read SUCCEEDED and simply
    /// returned less than a whole sector run, which for a file-backed source means
    /// the source is shorter than its declared capacity. Zero-filling the gap
    /// would produce an image that looks complete and is not — the worst outcome
    /// for a copy someone intends to keep — so it is an error instead.
    ShortImageRead {
        lba: u32,
        expected: u32,
        got: u32,
    },
    /// An image-level write was asked for zero sectors. A zero-byte image is
    /// never the intent, and reporting it here names the problem at the point it
    /// is knowable rather than leaving an empty file behind.
    EmptyImage,
    IfoParse,
    /// A title produced NO muxable frames: the mux driver's pump ended without
    /// any video track's `codec_private` resolving, or the MKV muxer reached
    /// `finish()` with zero frames written. The canonical case is an empty
    /// nav/menu PGC stub, and [`is_skippable_title_stub`] classifies this code as
    /// skippable so an all-titles rip drops the stub and finishes the rest.
    ///
    /// EXCLUSIVE meaning: malformed `mkv://` source is [`Error::MkvSourceInvalid`];
    /// an unrepresentable write-side element is [`Error::MkvUnencodable`]. Neither
    /// is safe to skip.
    MkvInvalid,
    /// An `mkv://` SOURCE file is malformed or truncated — the EBML/Matroska
    /// reader rejected it. NOT [`Error::MkvInvalid`]: see
    /// [`E_MKV_SOURCE_INVALID`].
    MkvSourceInvalid,
    /// The Matroska WRITER cannot encode an element size in EBML (body at or
    /// above the 56-bit VINT limit, or a stale master-size placeholder). NOT
    /// [`Error::MkvInvalid`]: see [`E_MKV_UNENCODABLE`].
    MkvUnencodable,
    /// An `mkv://` source Block's lacing header does not describe its payload —
    /// the frames packed into that Block cannot be separated. NOT
    /// [`Error::MkvInvalid`]: see [`E_MKV_LACING_INVALID`].
    MkvLacingInvalid,
    NoStreams,
    /// A [`crate::StreamSelection`] listed a PID that does not exist in the
    /// title's declared streams — a caller bug (e.g. a stale scan), reported
    /// loudly rather than silently producing an MKV missing a requested track.
    SelectionPidUnknown {
        pid: u16,
    },
    /// ddrescue mapfile parse failed. `kind` is a stable, language-neutral
    /// identifier (e.g. `"status_char"`, `"hex"`); not a translatable
    /// English message.
    MapfileInvalid {
        kind: &'static str,
    },
    /// The image a mapfile describes is shorter than the mapfile's own total
    /// size, so the two no longer agree about the same disc. Resuming against
    /// it would treat the absent tail as already recovered. `have` and `want`
    /// are byte lengths; a missing file reports `have == 0`.
    ImageTruncated {
        have: u64,
        want: u64,
    },
    /// A sector read from an image file runs past its end. `lba` is the read's
    /// first sector; `have`/`want` are the file length and the read's end offset.
    ImageEndsBeforeRead {
        lba: u32,
        have: u64,
        want: u64,
    },
    /// A whole-disc image read would carry bus-encrypted stream-file sectors as if
    /// plaintext: `files` (comma-separated `path (cause)`) could not be located to de-bus.
    BusStreamUnmapped {
        files: String,
    },
    /// `path` is an image staged for an MKV rip, not a whole-disc image.
    ImageScoped {
        path: String,
    },

    // AACS (7xxx)
    AacsNoKeys,
    AacsCertShort,
    AacsAgidAlloc,
    AacsCertRejected,
    AacsCertRead,
    AacsCertVerify,
    AacsKeyRead,
    AacsKeyRejected,
    AacsKeyVerify,
    AacsVidRead,
    AacsVidMac,
    AacsDataKey,
    DecryptFailed,
    CssAuthFailed,
    /// Host certificate rejected by the drive's revocation list (HRL hit).
    /// All available host certs failed mutual auth on this drive.
    AacsHostCertRejected,
    /// Drive cannot be put into raw-read mode and standard AACS cert
    /// auth failed. No path to decryption remains.
    AacsRawReadUnsupported,
    /// Volume ID could not be retrieved from the drive (neither via cert
    /// auth nor via the alternate VID read path). Downstream of step 1
    /// of the AACS chain.
    AacsVidUnavailable,
    /// No available path produced a Media Key (no MK+VID in keydb, no
    /// PK match, no DK derivation).
    AacsMkUnavailable,
    /// Disc-hash lookup in the keydb missed and no other path is
    /// available (typically because VID is missing).
    AacsVukNotInKeydb,
    /// Drive identity did not match any bundled profile; per-drive CDB
    /// templates aren't available so the OEM VID retrieval path can't
    /// run.
    DriveProfileMissing,
    /// Drive's profile is present but doesn't carry a VID-retrieval CDB
    /// template (older profile blob, or a drive class without an OEM
    /// VID path).
    VidCdbUnavailable,
    /// The disc is AACS-encrypted and decryption was requested, but key
    /// resolution produced no usable key for it — so muxing would emit
    /// undecryptable garbage. Distinct from [`Error::KeydbLoad`] (no keydb
    /// file at all): a keydb may be present but lack an entry for this disc.
    /// `disc_hash` is the 40-hex SHA1 of `Unit_Key_RO.inf` (no `0x` prefix)
    /// so the application can name the disc; empty if the hash wasn't
    /// captured at scan.
    NoDiscKey {
        disc_hash: String,
    },
    /// The disc is CSS-encrypted and decryption was requested, but the
    /// known-plaintext crack resolved no usable title key for the chosen
    /// title (e.g. a multi-VTS DVD where the title's VTS could not be
    /// re-cracked). Muxing would emit scrambled ciphertext, so the caller
    /// fails fast instead. CSS analogue of [`Error::NoDiscKey`].
    ///
    /// PER-TITLE: [`is_skippable_title_stub`] classifies this code so an
    /// all-titles rip skips just this title. The whole-disc counterpart is
    /// [`Error::CssNoDiscKey`].
    CssKeyMissing,
    /// The disc is CSS-encrypted and decryption was requested, but the known-plaintext crack
    /// recovered NO title key for the disc at all (the scan saw scrambled sectors and stamped
    /// `Disc::css_error`). A whole-disc condition: every title would fail the same way, so a
    /// multi-title rip loop must stop instead of iterating. The CSS analogue of
    /// [`Error::NoDiscKey`], classified by [`is_disc_level_no_key`] — NOT
    /// [`is_skippable_title_stub`], which owns the per-title [`Error::CssKeyMissing`].
    CssNoDiscKey,
    /// A key source could not be reached, or failed on its own side — transport
    /// error, DNS failure, timeout, TLS failure, HTTP 5xx, or an unreadable /
    /// unparseable reply. See [`E_KEY_SERVICE_UNAVAILABLE`]: the source never
    /// answered the question, so this is emphatically NOT [`Error::NoDiscKey`]
    /// (which asserts every source DID answer and none holds a key). Transient.
    ///
    /// Carries no detail by design: the key-service URL and the resolved address
    /// are operator-confidential and must not reach a log or a bug report.
    KeyServiceUnavailable,
    /// A key source rejected the configured credentials (HTTP 401/403). See
    /// [`E_KEY_SERVICE_UNAUTHORIZED`]. Not transient: fix the token.
    KeyServiceUnauthorized,
    /// A key source rate-limited the request (HTTP 429). See
    /// [`E_KEY_SERVICE_RATE_LIMITED`]. Back off and retry more slowly.
    KeyServiceRateLimited,
    /// The live-drive AACS cert-auth handshake (the OEM/AACS baseline route)
    /// could not run because NO host certificate was available from any key
    /// source. Host certs are keysource-served, never compiled in, so the OEM
    /// route fails gracefully here rather than panicking. Resolution still
    /// proceeds with a zero Volume ID and relies on the path-1 disc-hash → VUK
    /// lookup, so the error is dropped when that lookup hits. `path` carries the
    /// sentinel `<no host cert>` (mirroring [`Error::KeydbLoad`]'s sentinel) so a
    /// CLI can render "No Host Certs Found."
    AacsNoHostCert {
        path: String,
    },
    /// A key source offered host cert(s) for the drive's handshake, but every
    /// one failed a LOCAL check (stored private key vs cert public key)
    /// before any drive round-trip — a keydb data problem, not a drive
    /// rejection. Distinct from [`Error::AacsNoHostCert`] (no cert offered at
    /// all) and [`Error::AacsHostCertRejected`] (the drive rejected a cert
    /// it actually saw).
    AacsNoUsableHostCert,
    /// A bus-encrypted disc (AACS 2.0 / UHD, Content Certificate bus-encryption bit set) was
    /// scanned on a live drive, the Volume ID was obtained, but no `read_data_key` (bus key)
    /// was produced — so the on-disc bytes are still bus-encrypted and would decrypt to
    /// garbage. The bus key is derivable ONLY from the AACS host-certificate cert-auth
    /// handshake; a VID-only OEM unlock path is insufficient for such a disc. Surfaced instead
    /// of silently producing a corrupt rip. NOT raised for AACS 1.0 BD or file-backed (ISO)
    /// scans, where no bus-key handshake runs.
    AacsBusKeyUnavailable,
    /// A live AACS disc's `Unit_Key_RO.inf` is missing or unreadable (both copies).
    /// Recorded in `Disc::aacs_error` (the scan goes on); every key is refused.
    AacsKeyFileUnreadable,
    /// A decrypted whole-disc image would keep encrypted pieces: a stream file
    /// no title plays is encrypted and no held key opens it. See
    /// [`E_WHOLE_DISC_KEY_MISSING`]. Raised before the copy where it can be.
    WholeDiscKeyMissing,
    /// An image piece is Missing, no VID is in hand (neither passed nor scanned),
    /// and the sidecar mapfile has a `vidfp`: the key can only be derived from
    /// the disc's Volume ID. See [`E_AACS_VID_NEEDS_DISC`]. Raised before any
    /// output; the image and mapfile are left untouched.
    AacsVidNeedsDisc,

    /// AACS 2.1 (FMTS) disc carries forensic variant segments, but no segment
    /// (variant) key is available to open them. Raised UPFRONT — before the mux —
    /// exactly like a missing unit key, so a 2.1 disc that would rip with holes is
    /// refused rather than silently producing a forensic-holed output. (The mux
    /// resolves the full forensic key set up front; a resolution gap fails here.)
    FmtsKeyMissing,

    // Keydb (8xxx)
    KeydbConnect {
        host: String,
    },
    KeydbHttp {
        status: u16,
    },
    KeydbInvalid,
    KeydbWrite {
        path: String,
    },
    KeydbParse,
    KeydbLoad {
        path: String,
    },
    /// A redirect (or the configured URL) targets a scheme this
    /// dependency-light HTTP client cannot fetch (e.g. `https://`).
    /// Carries the offending scheme for diagnostics.
    KeydbUnsupportedScheme {
        scheme: String,
    },
    /// The redirect chain exceeded the follow limit.
    KeydbTooManyRedirects,

    // Stream/mux (9xxx)
    StreamReadOnly,
    StreamWriteOnly,
    StreamUrlInvalid {
        url: String,
    },
    StreamUrlMissingPath {
        scheme: String,
    },
    StreamUrlMissingPort {
        addr: String,
    },
    /// A `network://` output host resolved to no connectable address —
    /// every resolved IP was unspecified / multicast / broadcast / reserved.
    /// Carries the offending `host:port`.
    NetworkAddrBlocked {
        addr: String,
    },
    /// A muxer's `finish()` was reached after zero frames were written, so
    /// the output would be a header-only container with no media. Surfaced
    /// (instead of writing a valid-but-empty file and reporting success) so
    /// a zero-frame mux — undecryptable input, a fully-unreadable title, or
    /// every frame dropped before the first keyframe — fails loudly. The
    /// `m2ts://` analogue of [`Error::MkvInvalid`]'s zero-frame guard.
    MuxEmpty,
    /// The mux driver's pre-headers frame buffer passed its cap before every
    /// video track's `codec_private` resolved: the title keeps yielding real
    /// frames but its codec init data never appears, so buffering further would
    /// swap the box to death. Carries the buffered byte count.
    ///
    /// DISTINCT from [`Error::MkvInvalid`] (an empty nav/menu stub, skippable): real frames
    /// with unresolvable headers is a main feature, not a stub. This code is not skippable.
    MuxHeaderBufferExceeded {
        bytes: u64,
    },
    /// `mp4://` target title has no primary video track to mux.
    Mp4NoVideoTrack,
    /// `mp4://` source file is malformed/truncated — the MP4 demuxer failed.
    Mp4Invalid,
    /// `mp4://` video track is missing its `hvcC`/`avcC` configuration record.
    Mp4MissingCodecPrivate,
    /// `mp4://` video track has no resolved frame dimensions. See
    /// [`E_MP4_UNKNOWN_RESOLUTION`].
    Mp4UnknownResolution,
    /// The bounded durable flush timed out. See [`E_SYNC_TIMEOUT`].
    SyncTimeout,
    /// The bounded durable flush's worker was lost. See [`E_SYNC_WORKER_LOST`].
    SyncWorkerLost,
    PesFrameTooLarge {
        size: usize,
    },
    PesInvalidMagic,
    /// PES frame track index exceeds the 1-byte on-wire field (> 255).
    /// Carries the offending index. Distinct from [`Error::PesInvalidMagic`],
    /// which signals corrupt input on the read side.
    PesTrackTooLarge {
        track: usize,
    },
    IsoTooLarge {
        path: String,
    },
    NoMetadata,
    /// A non-empty `HEVCDecoderConfigurationRecord` (hvcC) was supplied to
    /// a muxer but failed to parse into any VPS/SPS/PPS NAL — emitting the
    /// stream without parameter sets would yield an undecodable result.
    HevcParamParse,
    /// A muxer `write_frame` / `set_codec_private` was given a track index
    /// beyond the configured PID/track count.
    MuxTrackRange {
        track: usize,
        tracks: usize,
    },
    /// The fragmented-MP4 sink cannot emit media — `moof`/`mdat` framing is
    /// not implemented. Surfaced instead of silently discarding samples.
    Fmp4Unimplemented,
    /// A worker thread in the threaded mux pipeline terminated without
    /// sending its terminal sentinel — i.e. it panicked or was dropped
    /// mid-stream. Surfaced so a parser/demux panic is never silently
    /// reported to the caller as a clean end-of-stream (which would
    /// truncate output without any error).
    DemuxThreadPanicked,
    /// A pipeline `join()` exceeded its deadline while waiting for the
    /// consumer thread to drain. The consumer is intentionally leaked;
    /// the caller should fall back to a degraded path.
    PipelineJoinTimeout,
    /// The pipeline consumer thread panicked. The original panic
    /// payload is not preserved (no English text in the library); it is
    /// logged at the panic site instead.
    PipelineConsumerPanicked,
    /// A pipeline producer's `send` failed because the consumer thread
    /// has already terminated (the receiver end is gone).
    SweepConsumerGone,
    /// A producer thread tried to hand work to its pipeline consumer
    /// (sweep / patch sink) but the consumer thread had already
    /// terminated (panicked or dropped the receiver). The producer
    /// surfaces this so the outer pass can abort cleanly instead of
    /// blocking on a dead channel.
    PipelineConsumerGone,
    /// READ CAPACITY(10) reported a last-LBA of `0xFFFFFFFF` — the SPC
    /// sentinel meaning "capacity exceeds 32-bit addressing". Adding 1 to
    /// derive the sector count would overflow `u32`. Reachable from
    /// disc-reported bytes and synthetic [`crate::sector::SectorSource`]
    /// fixtures.
    DiscCapacityOverflow,
    /// An extent fed to the prefetch producer has a `sector_count`
    /// whose trailing 1-2 sectors cannot form a complete AACS aligned
    /// unit (3 sectors / 6144 bytes). Emitting that tail as a
    /// standalone batch would hand the decrypt step a sub-unit chunk
    /// it silently leaves encrypted. The producer surfaces this rather
    /// than emit still-encrypted bytes.
    ExtentNotUnitAligned,
    /// A [`crate::sector::SectorSource`] that feeds its reads from a
    /// producer thread has terminated for good — the thread exited after
    /// an error or before delivering the extents it was given — so it can
    /// never return another byte.
    ///
    /// It exists because `Ok(0)` from a dead source is indistinguishable from end-of-stream,
    /// and callers may legitimately treat a short read as a skippable hole. This cannot be
    /// retried or skipped past; every consumer must abort the pass on it.
    SourceTerminated,
    /// An MPEG-TS packet under construction violated the 188-byte fixed
    /// size (over-long adaptation field, overflowing payload, or a
    /// short/mis-assembled packet). Indicates a muxer invariant break,
    /// not untrusted input — surfaced instead of writing a corrupt
    /// transport stream.
    M2tsPacketMalformed,
    /// READ CAPACITY transferred fewer than 4 bytes, or the decoded
    /// last-LBA + 1 overflowed `u32`. Either case means the capacity
    /// response is unusable; no English commentary.
    DiscCapacityMalformed,
    /// INQUIRY returned GOOD status but transferred fewer bytes than the
    /// standard 36-byte header, so the identity fields would decode from a
    /// zero-filled buffer. A drive reporting an empty INQUIRY would otherwise
    /// present as peripheral type 0x00 and be dropped from enumeration.
    DriveInquiryShort,
    /// `--raw` was given with a `dir://` destination. An encrypted file
    /// tree is useless; raw bytes belong in `iso://`.
    DirRawRejected,
    /// `--multipass` was given with a `dir://` destination. `dir://` is
    /// 1-shot; recovery is the `iso://` multipass path's job.
    DirMultipassRejected,
    /// A non-disc (byte-stream) source was routed into `dir://`, which
    /// requires a filesystem (only `disc://` / `iso://` qualify).
    DirSourceUnsupported,
    /// The `dir://` target directory is non-empty and `--force` was not
    /// given. Mixing two discs' trees is refused by default.
    DirNotEmpty,
    /// The `dir://` target filesystem's free space is below the sum of
    /// the file extents to extract. Carries required / available bytes.
    DirInsufficientSpace {
        required: u64,
        available: u64,
    },
    /// Two distinct disc paths sanitize to the same host path. Surfaced
    /// as a hard error rather than a silent overwrite. Carries the
    /// colliding host component.
    DirNameCollision {
        host: String,
    },
    /// A `dir://` create_dir_all / file write / rename failed. Carries
    /// the underlying errno when present.
    DirWriteFailed {
        errno: Option<i32>,
    },
    /// A `dir://` SOURCE folder carries `BDMV/STREAM/SSIF/` (Blu-ray 3D),
    /// which the synthetic-image planner cannot represent. See
    /// [`E_DIR_IMAGE_SSIF_UNSUPPORTED`].
    DirImageSsifUnsupported,
    /// A `dir://` SOURCE `VIDEO_TS` placement constraint is unsatisfiable.
    /// `path` is the disc path of the file that could not be placed.
    DirImagePlacement {
        path: String,
    },
    /// A `dir://` SOURCE folder holds no recognized disc structure.
    DirImageUnsupportedTree,
    /// A file in a `dir://` SOURCE folder changed between plan and read.
    DirImageFileChanged {
        path: String,
    },
    /// A `dir://` SOURCE folder exceeds the addressable image size.
    DirImageTooLarge,
    /// A name is too long to record in a UDF directory entry.
    ///
    /// The File Identifier Descriptor stores the encoded name length in ONE
    /// byte, so a name whose OSTA CS0 encoding exceeds 254 bytes cannot be
    /// described. Truncating the length field instead would desynchronise the
    /// whole directory — every later entry in it would be read from the wrong
    /// offset — so an over-long name is refused while the tree is still being
    /// planned.
    DirNameTooLong {
        path: String,
    },
    /// One directory holds more subdirectories than a UDF link count can express,
    /// or more entries than the reader's per-directory size cap allows.
    ///
    /// A directory's File Entry records its link count in 16 bits, and that
    /// count is one per child directory plus one for its own entry in its
    /// parent. Beyond that the count silently wraps, so the tree is refused.
    DirImageFanout {
        path: String,
    },
    /// A title's PlayItem marks excluded more frames than they kept.
    ///
    /// Placing clips by their marks drops whatever falls outside them, which is
    /// correct at a join — a disc stores the join twice. Discarding the
    /// majority of a title is not a join; it means the marks do not describe
    /// the clock the frames are on. Refused rather than written, because the
    /// result otherwise looks like a complete file containing seconds of a
    /// feature.
    SeamPlanDroppedMost {
        dropped: u64,
        written: u64,
    },
    /// A sink finished having written no frames at all.
    SinkWroteNothing,
    /// See [`E_STREAM_CLOSED`].
    StreamClosed,
    /// See [`E_STREAM_HEADER_WRITTEN`].
    StreamHeaderWritten,
    /// A `StallTimer` expired with no forward progress on `op`. See
    /// [`E_TIMED_OUT`]. "An expired `StallTimer` produces `Error::TimedOut
    /// { op }`" (stop-design-v5 §2.1); existing timeout codes that already
    /// name their cause (`PipelineJoinTimeout`, `SyncTimeout`) keep them.
    TimedOut {
        op: &'static str,
    },
    /// `mpg://` target title has no video track a program stream can carry. See
    /// [`E_MPG_NO_VIDEO_TRACK`]. Declared ahead of the `mpg://` sink that raises it.
    MpgNoVideoTrack,
    /// `mpg://` end of input left access units unwritten. See [`E_MPG_UNPACKETIZED`].
    MpgUnpacketized,
    /// A finished remux output failed its read-back check. See [`E_REMUX_VERIFY_FAILED`].
    /// `path` is the file that failed.
    RemuxVerifyFailed {
        kind: RemuxVerifyKind,
        path: String,
    },
    /// A title's mux returned incomplete without a Stop. `title` is 1-based.
    MuxIncomplete {
        title: usize,
    },
    /// See [`E_REMUX_STAGING_INVALID`].
    RemuxStagingInvalid,
    /// A staged copy wrote `have` bytes where the source holds `want`.
    StagedCopySizeMismatch {
        have: u64,
        want: u64,
    },
    /// See [`E_WORKER_LOST`]. `op` is a stable identifier, like [`Error::TimedOut`]'s.
    WorkerLost {
        op: &'static str,
    },
    /// `tag` is the caller's unresolvable language tag, verbatim.
    StreamLanguageUnknown {
        tag: String,
    },
    /// See [`E_MUX_BATCH_SECTORS_ZERO`].
    MuxBatchSectorsZero,
    /// See [`E_REMUX_TARGET_EXISTS`].
    RemuxTargetExists {
        path: String,
    },
}

/// Why a finished remux output failed read-back ([`Error::RemuxVerifyFailed`]).
/// Displayed as a stable identifier ([`RemuxVerifyKind::key`]), never prose.
#[derive(Debug, Clone, Copy, PartialEq)]
#[non_exhaustive]
pub enum RemuxVerifyKind {
    /// The output file is zero bytes.
    Empty,
    /// The output carries no tracks.
    NoTracks,
    /// The title declares a runtime but the output records none.
    NoRuntime,
    /// The output's runtime is outside tolerance of the title's declared runtime.
    RuntimeMismatch { have_secs: f64, want_secs: f64 },
}

impl RemuxVerifyKind {
    /// Stable, language-neutral identifier for this kind.
    pub fn key(&self) -> &'static str {
        match self {
            RemuxVerifyKind::Empty => "empty",
            RemuxVerifyKind::NoTracks => "no-tracks",
            RemuxVerifyKind::NoRuntime => "no-runtime",
            RemuxVerifyKind::RuntimeMismatch { .. } => "runtime-mismatch",
        }
    }
}

impl Error {
    pub fn code(&self) -> u16 {
        match self {
            Error::DeviceNotFound { .. } => E_DEVICE_NOT_FOUND,
            Error::DevicePermission { .. } => E_DEVICE_PERMISSION,
            Error::DeviceNotReady { .. } => E_DEVICE_NOT_READY,
            Error::DeviceResetFailed { .. } => E_DEVICE_RESET_FAILED,
            Error::ScsiInterfaceUnavailable { .. } => E_SCSI_INTERFACE_UNAVAILABLE,
            Error::DeviceLocked { .. } => E_DEVICE_LOCKED,
            Error::IoKitPluginFailed { .. } => E_IOKIT_PLUGIN_FAILED,
            Error::UnsupportedDrive { .. } => E_UNSUPPORTED_DRIVE,
            Error::ProfileParse => E_PROFILE_PARSE,
            Error::UnsupportedPlatform { .. } => E_UNSUPPORTED_PLATFORM,
            Error::PlatformNotImplemented { .. } => E_PLATFORM_NOT_IMPLEMENTED,
            Error::UnlockFailed => E_UNLOCK_FAILED,
            Error::SignatureMismatch { .. } => E_SIGNATURE_MISMATCH,
            Error::ScsiError { .. } => E_SCSI_ERROR,
            Error::InvalidCdbLength { .. } => E_INVALID_CDB_LENGTH,
            Error::IoError { .. } => E_IO_ERROR,
            Error::SourceTerminated => E_SOURCE_TERMINATED,
            Error::DiscRead { .. } => E_DISC_READ,
            Error::Halted => E_HALTED,
            Error::MplsParse => E_MPLS_PARSE,
            Error::ClpiParse => E_CLPI_PARSE,
            Error::UdfNotFound { .. } => E_UDF_NOT_FOUND,
            Error::UdfUnrecordedExtent { .. } => E_UDF_UNRECORDED_EXTENT,
            Error::UdfNotFilesystem => E_UDF_NOT_FILESYSTEM,
            Error::UdfBufferTooSmall => E_UDF_BUFFER_TOO_SMALL,
            Error::UdfAdChainTooLong => E_UDF_AD_CHAIN_TOO_LONG,
            Error::UdfEmbeddedData => E_UDF_EMBEDDED_DATA,
            Error::DiscTitleRange { .. } => E_DISC_TITLE_RANGE,
            Error::ShortImageRead { .. } => E_SHORT_IMAGE_READ,
            Error::EmptyImage => E_EMPTY_IMAGE,
            Error::IfoParse => E_IFO_PARSE,
            Error::MkvInvalid => E_MKV_INVALID,
            Error::MkvSourceInvalid => E_MKV_SOURCE_INVALID,
            Error::MkvUnencodable => E_MKV_UNENCODABLE,
            Error::MkvLacingInvalid => E_MKV_LACING_INVALID,
            Error::NoStreams => E_NO_STREAMS,
            Error::SelectionPidUnknown { .. } => E_SELECTION_PID_UNKNOWN,
            Error::MapfileInvalid { .. } => E_MAPFILE_INVALID,
            Error::ImageTruncated { .. } => E_IMAGE_TRUNCATED,
            Error::ImageEndsBeforeRead { .. } => E_IMAGE_ENDS_BEFORE_READ,
            Error::BusStreamUnmapped { .. } => E_BUS_STREAM_UNMAPPED,
            Error::ImageScoped { .. } => E_IMAGE_SCOPED,
            Error::AacsNoKeys => E_AACS_NO_KEYS,
            Error::AacsCertShort => E_AACS_CERT_SHORT,
            Error::AacsAgidAlloc => E_AACS_AGID_ALLOC,
            Error::AacsCertRejected => E_AACS_CERT_REJECTED,
            Error::AacsCertRead => E_AACS_CERT_READ,
            Error::AacsCertVerify => E_AACS_CERT_VERIFY,
            Error::AacsKeyRead => E_AACS_KEY_READ,
            Error::AacsKeyRejected => E_AACS_KEY_REJECTED,
            Error::AacsKeyVerify => E_AACS_KEY_VERIFY,
            Error::AacsVidRead => E_AACS_VID_READ,
            Error::AacsVidMac => E_AACS_VID_MAC,
            Error::AacsDataKey => E_AACS_DATA_KEY,
            Error::DecryptFailed => E_DECRYPT_FAILED,
            Error::CssAuthFailed => E_CSS_AUTH_FAILED,
            Error::AacsHostCertRejected => E_AACS_HOST_CERT_REJECTED,
            Error::AacsRawReadUnsupported => E_AACS_RAW_READ_UNSUPPORTED,
            Error::AacsVidUnavailable => E_AACS_VID_UNAVAILABLE,
            Error::AacsMkUnavailable => E_AACS_MK_UNAVAILABLE,
            Error::AacsVukNotInKeydb => E_AACS_VUK_NOT_IN_KEYDB,
            Error::DriveProfileMissing => E_DRIVE_PROFILE_MISSING,
            Error::VidCdbUnavailable => E_VID_CDB_UNAVAILABLE,
            Error::NoDiscKey { .. } => E_NO_DISC_KEY,
            Error::CssKeyMissing => E_CSS_KEY_MISSING,
            Error::CssNoDiscKey => E_CSS_NO_DISC_KEY,
            Error::KeyServiceUnavailable => E_KEY_SERVICE_UNAVAILABLE,
            Error::KeyServiceUnauthorized => E_KEY_SERVICE_UNAUTHORIZED,
            Error::KeyServiceRateLimited => E_KEY_SERVICE_RATE_LIMITED,
            Error::AacsNoHostCert { .. } => E_AACS_NO_HOST_CERT,
            Error::AacsNoUsableHostCert => E_AACS_NO_USABLE_HOST_CERT,
            Error::AacsBusKeyUnavailable => E_AACS_BUS_KEY_UNAVAILABLE,
            Error::AacsKeyFileUnreadable => E_AACS_KEY_FILE_UNREADABLE,
            Error::WholeDiscKeyMissing => E_WHOLE_DISC_KEY_MISSING,
            Error::AacsVidNeedsDisc => E_AACS_VID_NEEDS_DISC,
            Error::FmtsKeyMissing => E_FMTS_KEY_MISSING,
            Error::KeydbConnect { .. } => E_KEYDB_CONNECT,
            Error::KeydbHttp { .. } => E_KEYDB_HTTP,
            Error::KeydbInvalid => E_KEYDB_INVALID,
            Error::KeydbWrite { .. } => E_KEYDB_WRITE,
            Error::KeydbParse => E_KEYDB_PARSE,
            Error::KeydbLoad { .. } => E_KEYDB_LOAD,
            Error::KeydbUnsupportedScheme { .. } => E_KEYDB_UNSUPPORTED_SCHEME,
            Error::KeydbTooManyRedirects => E_KEYDB_TOO_MANY_REDIRECTS,
            Error::StreamReadOnly => E_STREAM_READ_ONLY,
            Error::StreamWriteOnly => E_STREAM_WRITE_ONLY,
            Error::StreamUrlInvalid { .. } => E_STREAM_URL_INVALID,
            Error::StreamUrlMissingPath { .. } => E_STREAM_URL_MISSING_PATH,
            Error::StreamUrlMissingPort { .. } => E_STREAM_URL_MISSING_PORT,
            Error::NetworkAddrBlocked { .. } => E_NETWORK_ADDR_BLOCKED,
            Error::MuxEmpty => E_MUX_EMPTY,
            Error::MuxHeaderBufferExceeded { .. } => E_MUX_HEADER_BUFFER_EXCEEDED,
            Error::Mp4NoVideoTrack => E_MP4_NO_VIDEO_TRACK,
            Error::Mp4Invalid => E_MP4_INVALID,
            Error::Mp4MissingCodecPrivate => E_MP4_MISSING_CODEC_PRIVATE,
            Error::Mp4UnknownResolution => E_MP4_UNKNOWN_RESOLUTION,
            Error::SyncTimeout => E_SYNC_TIMEOUT,
            Error::SyncWorkerLost => E_SYNC_WORKER_LOST,
            Error::PesFrameTooLarge { .. } => E_PES_FRAME_TOO_LARGE,
            Error::PesInvalidMagic => E_PES_INVALID_MAGIC,
            Error::PesTrackTooLarge { .. } => E_PES_TRACK_TOO_LARGE,
            Error::IsoTooLarge { .. } => E_ISO_TOO_LARGE,
            Error::NoMetadata => E_NO_METADATA,
            Error::HevcParamParse => E_HEVC_PARAM_PARSE,
            Error::MuxTrackRange { .. } => E_MUX_TRACK_RANGE,
            Error::Fmp4Unimplemented => E_FMP4_UNIMPLEMENTED,
            Error::DemuxThreadPanicked => E_DEMUX_THREAD_PANICKED,
            Error::PipelineJoinTimeout => E_PIPELINE_JOIN_TIMEOUT,
            Error::PipelineConsumerPanicked => E_PIPELINE_CONSUMER_PANICKED,
            Error::SweepConsumerGone => E_SWEEP_CONSUMER_GONE,
            Error::PipelineConsumerGone => E_PIPELINE_CONSUMER_GONE,
            Error::DiscCapacityOverflow => E_DISC_CAPACITY_OVERFLOW,
            Error::ExtentNotUnitAligned => E_EXTENT_NOT_UNIT_ALIGNED,
            Error::M2tsPacketMalformed => E_M2TS_PACKET_MALFORMED,
            Error::DiscCapacityMalformed => E_DISC_CAPACITY_MALFORMED,
            Error::DriveInquiryShort => E_DRIVE_INQUIRY_SHORT,
            Error::DirRawRejected => E_DIR_RAW_REJECTED,
            Error::DirMultipassRejected => E_DIR_MULTIPASS_REJECTED,
            Error::DirSourceUnsupported => E_DIR_SOURCE_UNSUPPORTED,
            Error::DirNotEmpty => E_DIR_NOT_EMPTY,
            Error::DirInsufficientSpace { .. } => E_DIR_INSUFFICIENT_SPACE,
            Error::DirNameCollision { .. } => E_DIR_NAME_COLLISION,
            Error::DirWriteFailed { .. } => E_DIR_WRITE_FAILED,
            Error::DirImageSsifUnsupported => E_DIR_IMAGE_SSIF_UNSUPPORTED,
            Error::DirImagePlacement { .. } => E_DIR_IMAGE_PLACEMENT,
            Error::DirImageUnsupportedTree => E_DIR_IMAGE_UNSUPPORTED_TREE,
            Error::DirNameTooLong { .. } => E_DIR_NAME_TOO_LONG,
            Error::DirImageFanout { .. } => E_DIR_IMAGE_FANOUT,
            Error::SeamPlanDroppedMost { .. } => E_SEAM_PLAN_DROPPED_MOST,
            Error::SinkWroteNothing => E_SINK_WROTE_NOTHING,
            Error::StreamClosed => E_STREAM_CLOSED,
            Error::StreamHeaderWritten => E_STREAM_HEADER_WRITTEN,
            Error::TimedOut { .. } => E_TIMED_OUT,
            Error::MpgNoVideoTrack => E_MPG_NO_VIDEO_TRACK,
            Error::MpgUnpacketized => E_MPG_UNPACKETIZED,
            Error::DirImageFileChanged { .. } => E_DIR_IMAGE_FILE_CHANGED,
            Error::DirImageTooLarge => E_DIR_IMAGE_TOO_LARGE,
            Error::RemuxVerifyFailed { .. } => E_REMUX_VERIFY_FAILED,
            Error::MuxIncomplete { .. } => E_MUX_INCOMPLETE,
            Error::RemuxStagingInvalid => E_REMUX_STAGING_INVALID,
            Error::StagedCopySizeMismatch { .. } => E_STAGED_COPY_SIZE_MISMATCH,
            Error::WorkerLost { .. } => E_WORKER_LOST,
            Error::StreamLanguageUnknown { .. } => E_STREAM_LANGUAGE_UNKNOWN,
            Error::RemuxTargetExists { .. } => E_REMUX_TARGET_EXISTS,
            Error::MuxBatchSectorsZero => E_MUX_BATCH_SECTORS_ZERO,
        }
    }
}

/// Display: "E{code}" with structured data. No English words.
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::DeviceNotFound { path } => write!(f, "E{}: {}", self.code(), path),
            Error::DevicePermission { path } => write!(f, "E{}: {}", self.code(), path),
            Error::DeviceNotReady { path } => write!(f, "E{}: {}", self.code(), path),
            Error::DeviceResetFailed { path } => write!(f, "E{}: {}", self.code(), path),
            Error::ScsiInterfaceUnavailable { path } => write!(f, "E{}: {}", self.code(), path),
            Error::DeviceLocked { path, kr } => {
                write!(f, "E{}: {} 0x{:08x}", self.code(), path, kr)
            }
            Error::IoKitPluginFailed { path, kr } => {
                write!(f, "E{}: {} 0x{:08x}", self.code(), path, kr)
            }
            Error::UnsupportedPlatform { target } => {
                write!(f, "E{}: {}", self.code(), target)
            }
            Error::PlatformNotImplemented { platform } => {
                write!(f, "E{}: {}", self.code(), platform)
            }
            Error::MapfileInvalid { kind } => {
                write!(f, "E{}: {}", self.code(), kind)
            }
            Error::UnsupportedDrive {
                vendor_id,
                product_id,
                product_revision,
            } => write!(
                f,
                "E{}: {} {} {}",
                self.code(),
                vendor_id.trim(),
                product_id.trim(),
                product_revision.trim()
            ),
            Error::SignatureMismatch { expected, got } => write!(
                f,
                "E{}: {:02x}{:02x}{:02x}{:02x}!={:02x}{:02x}{:02x}{:02x}",
                self.code(),
                expected[0],
                expected[1],
                expected[2],
                expected[3],
                got[0],
                got[1],
                got[2],
                got[3]
            ),
            Error::ScsiError {
                opcode,
                status,
                sense,
            } => match sense {
                Some(s) => write!(
                    f,
                    "E{}: 0x{:02x}/0x{:02x}/0x{:02x}/0x{:02x}/0x{:02x}",
                    self.code(),
                    opcode,
                    status,
                    s.sense_key,
                    s.asc,
                    s.ascq,
                ),
                None => write!(f, "E{}: 0x{:02x}/0x{:02x}", self.code(), opcode, status,),
            },
            // Language-neutral: std::io::Error's Display is English
            // ("permission denied"); emit the raw OS errno when present,
            // else the ErrorKind debug name (an identifier, not prose).
            Error::IoError { source } => match source.raw_os_error() {
                Some(errno) => write!(f, "E{}: {}", self.code(), errno),
                None => write!(f, "E{}: {:?}", self.code(), source.kind()),
            },
            Error::DiscRead {
                sector,
                status,
                sense,
            } => match (status, sense) {
                (Some(st), Some(s)) => write!(
                    f,
                    "E{}: {} 0x{:02x}/0x{:02x}/0x{:02x}/0x{:02x}",
                    self.code(),
                    sector,
                    st,
                    s.sense_key,
                    s.asc,
                    s.ascq,
                ),
                (Some(st), None) => write!(f, "E{}: {} 0x{:02x}", self.code(), sector, st,),
                (None, Some(s)) => write!(
                    f,
                    "E{}: {} 0x{:02x}/0x{:02x}/0x{:02x}",
                    self.code(),
                    sector,
                    s.sense_key,
                    s.asc,
                    s.ascq,
                ),
                (None, None) => write!(f, "E{}: {}", self.code(), sector),
            },
            Error::Halted => write!(f, "E{}", self.code()),
            // Disc- or backup-folder-derived names: escape control characters.
            Error::UdfNotFound { path } | Error::UdfUnrecordedExtent { path } => {
                write!(f, "E{}: {}", self.code(), path.escape_debug())
            }
            Error::SeamPlanDroppedMost { dropped, written } => {
                write!(f, "E{} {dropped}/{written}", self.code())
            }
            Error::ShortImageRead { lba, expected, got } => {
                write!(f, "E{} {lba} {expected} {got}", self.code())
            }
            Error::DirImagePlacement { path }
            | Error::DirImageFileChanged { path }
            | Error::DirNameTooLong { path }
            | Error::DirImageFanout { path } => {
                write!(f, "E{}: {}", self.code(), path.escape_debug())
            }
            Error::DiscTitleRange { index, count } => {
                write!(f, "E{}: {}/{}", self.code(), index, count)
            }
            Error::DirInsufficientSpace {
                required,
                available,
            } => write!(f, "E{}: {}/{}", self.code(), required, available),
            // `host` is a raw disc name: escape control characters.
            Error::DirNameCollision { host } => {
                write!(f, "E{}: {}", self.code(), host.escape_debug())
            }
            // errno is Option: emit it after the code when present, else the
            // bare code (mirrors NoDiscKey's empty-field handling — no dangling
            // "colon space" suffix).
            Error::DirWriteFailed { errno: Some(e) } => write!(f, "E{}: {}", self.code(), e),
            Error::DirWriteFailed { errno: None } => write!(f, "E{}", self.code()),
            Error::KeydbConnect { host } => write!(f, "E{}: {}", self.code(), host),
            Error::KeydbHttp { status } => write!(f, "E{}: {}", self.code(), status),
            Error::KeydbWrite { path } => write!(f, "E{}: {}", self.code(), path),
            Error::KeydbLoad { path } => write!(f, "E{}: {}", self.code(), path),
            Error::AacsNoHostCert { path } => write!(f, "E{}: {}", self.code(), path),
            Error::KeydbUnsupportedScheme { scheme } => {
                write!(f, "E{}: {}", self.code(), scheme)
            }
            Error::StreamUrlInvalid { url } => write!(f, "E{}: {}", self.code(), url),
            Error::StreamUrlMissingPath { scheme } => write!(f, "E{}: {}", self.code(), scheme),
            Error::StreamUrlMissingPort { addr } => write!(f, "E{}: {}", self.code(), addr),
            Error::NetworkAddrBlocked { addr } => write!(f, "E{}: {}", self.code(), addr),
            Error::PesFrameTooLarge { size } => write!(f, "E{}: {}", self.code(), size),
            Error::PesTrackTooLarge { track } => write!(f, "E{}: {}", self.code(), track),
            Error::IsoTooLarge { path } => write!(f, "E{}: {}", self.code(), path),
            Error::NoDiscKey { disc_hash } => {
                if disc_hash.is_empty() {
                    write!(f, "E{}", self.code())
                } else {
                    write!(f, "E{}: {}", self.code(), disc_hash)
                }
            }
            Error::MuxTrackRange { track, tracks } => {
                write!(f, "E{}: {}/{}", self.code(), track, tracks)
            }
            Error::InvalidCdbLength { len, max } => {
                write!(f, "E{}: {}/{}", self.code(), len, max)
            }
            Error::SelectionPidUnknown { pid } => {
                write!(f, "E{}: 0x{:04x}", self.code(), pid)
            }
            Error::MuxHeaderBufferExceeded { bytes } => {
                write!(f, "E{}: {}", self.code(), bytes)
            }
            // have/want are byte lengths (an identifier-free, language-neutral
            // pair) — surface them so a truncated-resume report carries the
            // actual mismatch, not just the bare code.
            Error::ImageTruncated { have, want } => {
                write!(f, "E{}: {have}/{want}", self.code())
            }
            Error::ImageEndsBeforeRead { lba, have, want } => {
                write!(f, "E{}: {lba} {have}/{want}", self.code())
            }
            // `op` is a stable, language-neutral identifier (e.g. "verify",
            // "artifact_lock"), not translatable prose.
            Error::TimedOut { op } | Error::WorkerLost { op } => {
                write!(f, "E{}: {op}", self.code())
            }
            Error::BusStreamUnmapped { files } => {
                write!(f, "E{}: {}", self.code(), files.escape_debug())
            }
            Error::ImageScoped { path } => write!(f, "E{}: {}", self.code(), path.escape_debug()),
            Error::RemuxVerifyFailed { kind, path } => match kind {
                RemuxVerifyKind::RuntimeMismatch {
                    have_secs,
                    want_secs,
                } => write!(
                    f,
                    "E{}: {} {have_secs:.1}/{want_secs:.1} {path}",
                    self.code(),
                    kind.key()
                ),
                _ => write!(f, "E{}: {} {path}", self.code(), kind.key()),
            },
            Error::MuxIncomplete { title } => write!(f, "E{}: {title}", self.code()),
            Error::StagedCopySizeMismatch { have, want } => {
                write!(f, "E{}: {have}/{want}", self.code())
            }
            // `tag` is raw user input: escape control characters.
            Error::StreamLanguageUnknown { tag } => {
                write!(f, "E{}: {}", self.code(), tag.escape_debug())
            }
            Error::RemuxTargetExists { path } => write!(f, "E{}: {path}", self.code()),
            _ => write!(f, "E{}", self.code()),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::IoError { source } => Some(source),
            _ => None,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        // If this `io::Error` is ours, unwrap to the original typed `Error` rather
        // than re-wrapping as `IoError` (which `is_scsi_transport_failure` reads as
        // a bridge wedge) — preserves classification, SCSI status, and sense data.
        match e.downcast::<Error>() {
            Ok(typed) => typed,
            // A genuine OS/`std` error — the `IoError` wrapper is correct here.
            Err(io) => Error::IoError { source: io },
        }
    }
}

impl From<Error> for std::io::Error {
    fn from(e: Error) -> Self {
        // `Error::IoError` just wraps an `io::Error` that entered via
        // `From<io::Error> for Error` — round-trip it back unchanged so the
        // original `ErrorKind`/OS code survive instead of flattening to `Other`.
        if let Error::IoError { source } = e {
            return source;
        }
        let code = e.code();
        // Map our error categories to io::ErrorKind
        let kind = match code {
            // Device access-denied semantics map to PermissionDenied;
            // the rest of the 1xxx block is "device absent" -> NotFound.
            E_DEVICE_PERMISSION | E_DEVICE_LOCKED => std::io::ErrorKind::PermissionDenied,
            1000..=1999 => std::io::ErrorKind::NotFound,
            2000..=2999 => std::io::ErrorKind::Unsupported,
            3000..=3999 => std::io::ErrorKind::PermissionDenied,
            4000..=4999 => std::io::ErrorKind::Other,
            5000..=5999 => std::io::ErrorKind::Other,
            // A stop is an interruption, not invalid data. MUST precede the
            // 6000..=6999 arm — E_HALTED is 6010 and match arms are ordered.
            E_HALTED => std::io::ErrorKind::Interrupted,
            6000..=6999 => std::io::ErrorKind::InvalidData,
            7000..=7999 => std::io::ErrorKind::PermissionDenied,
            8000..=8999 => std::io::ErrorKind::Other,
            9000..=9001 => std::io::ErrorKind::Unsupported,
            9002..=9008 => std::io::ErrorKind::InvalidInput,
            // 9010 HevcParamParse: malformed hvcC payload.
            E_HEVC_PARAM_PARSE => std::io::ErrorKind::InvalidData,
            // 9011 MuxTrackRange: caller passed a bad track index.
            E_MUX_TRACK_RANGE => std::io::ErrorKind::InvalidInput,
            // 9012 Fmp4Unimplemented: sink can't emit media yet.
            E_FMP4_UNIMPLEMENTED => std::io::ErrorKind::Unsupported,
            // 9014 PipelineJoinTimeout: consumer drain exceeded deadline.
            E_PIPELINE_JOIN_TIMEOUT => std::io::ErrorKind::TimedOut,
            // 9017 PesTrackTooLarge: out-of-range track index on serialize.
            E_PES_TRACK_TOO_LARGE => std::io::ErrorKind::InvalidInput,
            // 9020 DiscCapacityOverflow: disc reported a capacity sentinel
            // we can't represent — treat as bad/invalid device data.
            E_DISC_CAPACITY_OVERFLOW => std::io::ErrorKind::InvalidData,
            // 9021 M2tsPacketMalformed: a muxer invariant break produced
            // a non-188-byte packet — treat as invalid data.
            E_M2TS_PACKET_MALFORMED => std::io::ErrorKind::InvalidData,
            // 9022 NetworkAddrBlocked: the output host resolved only to
            // invalid (unspecified/multicast/broadcast/reserved) addresses.
            E_NETWORK_ADDR_BLOCKED => std::io::ErrorKind::PermissionDenied,
            // 9023 MuxEmpty: finish() reached with zero frames — the output
            // would be a header-only container. Treat as invalid output.
            E_MUX_EMPTY => std::io::ErrorKind::InvalidData,
            // 9051 MuxHeaderBufferExceeded: the source kept yielding frames but
            // never its codec init data — the input is unusable as declared.
            E_MUX_HEADER_BUFFER_EXCEEDED => std::io::ErrorKind::InvalidData,
            // 9052 MkvLacingInvalid: a source Block's lacing header does not
            // describe its own payload — malformed input data.
            E_MKV_LACING_INVALID => std::io::ErrorKind::InvalidData,
            // 9053 MkvSourceInvalid: the mkv:// source file is malformed or
            // truncated — invalid input data.
            E_MKV_SOURCE_INVALID => std::io::ErrorKind::InvalidData,
            // 9054 MkvUnencodable: the writer was asked for an element size EBML
            // cannot represent; the data it was given cannot be encoded.
            E_MKV_UNENCODABLE => std::io::ErrorKind::InvalidData,
            // mp4:// demux errors: a malformed/truncated source file
            // (E_MP4_INVALID), or a source whose tracks the mux can't use — no
            // video track / missing codec-private config. All are invalid data.
            E_MP4_NO_VIDEO_TRACK
            | E_MPG_NO_VIDEO_TRACK
            | E_MPG_UNPACKETIZED
            | E_MP4_INVALID
            | E_MP4_MISSING_CODEC_PRIVATE
            | E_MP4_UNKNOWN_RESOLUTION => std::io::ErrorKind::InvalidData,
            // Durability, not data validity: the write landed, the flush did
            // not. TimedOut keeps the std kind a caller might already branch on
            // while the E-code carries the distinction.
            E_SYNC_TIMEOUT | E_SYNC_WORKER_LOST => std::io::ErrorKind::TimedOut,
            // 9030 ExtentNotUnitAligned: a malformed/non-AACS-aligned
            // extent was handed to the prefetch producer.
            E_EXTENT_NOT_UNIT_ALIGNED => std::io::ErrorKind::InvalidInput,
            // 9047 DiscCapacityMalformed: the drive returned an unusable
            // READ CAPACITY response (short transfer / overflow).
            E_DISC_CAPACITY_MALFORMED => std::io::ErrorKind::InvalidData,
            // dir:// usage / footgun gates (9019, 9024–9026, 9028): the caller
            // gave an invalid flag/source/name combination — InvalidInput.
            E_DIR_RAW_REJECTED
            | E_DIR_MULTIPASS_REJECTED
            | E_DIR_SOURCE_UNSUPPORTED
            | E_DIR_NOT_EMPTY
            | E_DIR_NAME_COLLISION => std::io::ErrorKind::InvalidInput,
            // 9027 insufficient space / 9029 write failed: a filesystem-level
            // failure, not bad input.
            E_DIR_INSUFFICIENT_SPACE | E_DIR_WRITE_FAILED => std::io::ErrorKind::Other,
            // dir:// SOURCE gates (9061-9064, 9066-9068): the folder can't become a
            // disc image (3D SSIF, bad VIDEO_TS placement, encrypted, unrecognized
            // tree, too large, name too long, fan-out overflow) — input properties.
            E_DIR_IMAGE_SSIF_UNSUPPORTED
            | E_DIR_IMAGE_PLACEMENT
            | E_DIR_IMAGE_UNSUPPORTED_TREE
            | E_DIR_IMAGE_TOO_LARGE
            | E_DIR_NAME_TOO_LONG
            | E_DIR_IMAGE_FANOUT => std::io::ErrorKind::InvalidInput,
            // 9065: the folder changed underneath a running read. Not bad input
            // at plan time — a mid-flight mutation of the source.
            E_DIR_IMAGE_FILE_CHANGED => std::io::ErrorKind::InvalidData,
            // 9073 TimedOut: a StallTimer expired with no forward progress.
            E_TIMED_OUT => std::io::ErrorKind::TimedOut,
            // Remux/engine codes keep the kinds their io::Error texts carried.
            E_REMUX_VERIFY_FAILED | E_STAGED_COPY_SIZE_MISMATCH => std::io::ErrorKind::InvalidData,
            E_REMUX_STAGING_INVALID | E_STREAM_LANGUAGE_UNKNOWN | E_MUX_BATCH_SECTORS_ZERO => {
                std::io::ErrorKind::InvalidInput
            }
            E_REMUX_TARGET_EXISTS => std::io::ErrorKind::AlreadyExists,
            _ => std::io::ErrorKind::Other,
        };
        // Carry the typed value itself as the payload, not its stringification.
        // `Display` is unchanged (same `E<code>[: …]` string), so `error_code` is
        // unaffected — but the typed error now SURVIVES and can round-trip back.
        std::io::Error::new(kind, e)
    }
}

/// Convenience alias for `Result<T, Error>`.
pub type Result<T> = std::result::Result<T, Error>;

/// The numeric error code carried by an [`io::Error`](std::io::Error) that was
/// produced from an [`Error`], or `None` if it carries none.
///
/// Public because consumers need the code itself, not just the yes/no
/// predicates built on it below — parsing the `E<code>` prefix is this
/// function's job, so callers don't reimplement it.
///
/// [`From<Error> for io::Error`] is the ONLY path from a typed [`Error`] to an `io::Error` in
/// this crate.
pub fn error_code(e: &std::io::Error) -> Option<u16> {
    // Typed payload first: exact, and immune to a foreign message that looks like a code.
    if let Some(typed) = e.get_ref().and_then(|i| i.downcast_ref::<Error>()) {
        return Some(typed.code());
    }
    // Round-tripped: `From<Error> for io::Error` renders as "E<code>[: …]".
    let s = e.to_string();
    let digits = s.strip_prefix('E')?;
    let end = digits
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(digits.len());
    digits.get(..end)?.parse::<u16>().ok()
}

/// Whether a per-title mux failure is a *skippable title stub* — a
/// copy-protected-but-uncrackable title ([`Error::CssKeyMissing`]) or a title
/// that produced no muxable frames ([`Error::MkvInvalid`]). An all-titles rip
/// skips such a title and finishes the rest; every other error stays fatal.
///
/// NOT skippable: a broken `mkv://` source ([`Error::MkvSourceInvalid`] and siblings) or a
/// whole-disc key failure ([`Error::CssNoDiscKey`], see [`is_disc_level_no_key`]).
pub fn is_skippable_title_stub(e: &std::io::Error) -> bool {
    matches!(error_code(e), Some(E_MKV_INVALID | E_CSS_KEY_MISSING))
}

/// Whether an [`io::Error`](std::io::Error) is a cooperative user stop
/// ([`Error::Halted`], code [`E_HALTED`]) — vs a structural failure. A stop is
/// resumable, not a rip failure: `mux_with_keys` maps a mid-run halt to
/// `completed = false`, and consumers preserve staging rather than quarantining.
/// Typed replacement for the consumers' `E<code>`-leading-token string match.
pub fn is_halt(e: &std::io::Error) -> bool {
    error_code(e) == Some(E_HALTED)
}

/// Whether an [`io::Error`](std::io::Error) is a **disc-level** key failure — the disc as a
/// whole cannot be decrypted, so EVERY title will fail the same way. Distinct from a per-title
/// skippable stub ([`is_skippable_title_stub`]). Covers `E_NO_DISC_KEY`, `E_KEYDB_LOAD`,
/// `E_AACS_NO_KEYS`, `E_CSS_NO_DISC_KEY`, and the key-SOURCE failures
/// (`E_KEY_SERVICE_UNAVAILABLE`/`UNAUTHORIZED`/ `RATE_LIMITED`) — a down/throttled/unauthorized
/// source fails every title too. A multi-title rip loop should fail fast rather than iterate.
pub fn is_disc_level_no_key(e: &std::io::Error) -> bool {
    matches!(
        error_code(e),
        Some(
            E_NO_DISC_KEY
                | E_KEYDB_LOAD
                | E_AACS_NO_KEYS
                | E_CSS_NO_DISC_KEY
                | E_KEY_SERVICE_UNAVAILABLE
                | E_KEY_SERVICE_UNAUTHORIZED
                | E_KEY_SERVICE_RATE_LIMITED
        )
    )
}

impl Error {
    /// Borrow the drive-returned SPC-4 sense triple if this error is a
    /// [`Error::ScsiError`] carrying sense data. `None` for any other
    /// variant **and** for `ScsiError`s that represent a transport-layer
    /// failure (where the device never delivered a SCSI status reply, so
    /// no sense data exists).
    pub fn scsi_sense(&self) -> Option<&crate::scsi::ScsiSense> {
        match self {
            Error::ScsiError { sense: Some(s), .. } => Some(s),
            Error::DiscRead { sense: Some(s), .. } => Some(s),
            _ => None,
        }
    }

    /// True if this is a [`Error::ScsiError`] representing a transport-layer
    /// failure — kernel timeout, USB bridge wedge, IOKit service error.
    /// The device never delivered a SCSI status reply, so there is no
    /// sense data to inspect; retrying typically requires physical
    /// intervention (replug).
    pub fn is_scsi_transport_failure(&self) -> bool {
        matches!(
            self,
            Error::ScsiError {
                status: crate::scsi::SCSI_STATUS_TRANSPORT_FAILURE,
                ..
            }
        ) || matches!(
            self,
            Error::DiscRead {
                status: Some(crate::scsi::SCSI_STATUS_TRANSPORT_FAILURE),
                ..
            }
        ) || matches!(
            // A failed ioctl (IoError, e.g. ENODEV/EIO) or vanished device
            // (DeviceNotFound) is a dead-bus fault too, not a bad sector — treat
            // as transport failure so the pass aborts instead of zero-filling.
            self,
            Error::IoError { .. } | Error::DeviceNotFound { .. }
        )
    }

    /// True if the read SOURCE itself is gone, as opposed to one range of
    /// media being unreadable. Kept separate from
    /// [`is_scsi_transport_failure`](Self::is_scsi_transport_failure) because a
    /// terminated producer thread is neither a wedged bridge nor a bad sector.
    ///
    /// What it shares with a transport failure: retrying smaller or skipping
    /// ahead cannot recover anything, so the pass must abort rather than
    /// fabricate zeros for the rest of the title.
    pub fn is_source_terminated(&self) -> bool {
        matches!(self, Error::SourceTerminated)
    }

    /// True if this error indicates bridge degradation — the SCSI status
    /// is neither GOOD (0x00), CHECK CONDITION (0x02), nor transport failure
    /// (0xFF). Observed on the Initio INIC-1618L USB bridge preceding a full
    /// crash: the bridge firmware returns non-standard status bytes (e.g.
    /// 0x04, 0x05) with empty sense data. The caller should cool down
    /// (10 s pause) and retry rather than hammering the bridge.
    pub fn is_bridge_degradation(&self) -> bool {
        let status = match self {
            Error::ScsiError { status, .. } => *status,
            Error::DiscRead { status, .. } => status.unwrap_or(0),
            _ => return false,
        };
        status != crate::scsi::SCSI_STATUS_GOOD
            && status != crate::scsi::SCSI_STATUS_CHECK_CONDITION
            && status != crate::scsi::SCSI_STATUS_TRANSPORT_FAILURE
    }

    /// True if the underlying SCSI failure is a *marginal read* — the drive
    /// returned an error category where smaller-granularity retries can
    /// sometimes recover the data: MEDIUM ERROR (3), ABORTED COMMAND (B),
    /// NOT READY (2), RECOVERED ERROR (1), or NO SENSE (0).
    ///
    /// `false` for transport failures, HARDWARE ERROR, DATA PROTECT, UNIT ATTENTION, ILLEGAL
    /// REQUEST, BLANK CHECK, `IoError`, and non-SCSI variants.
    pub fn is_marginal_read(&self) -> bool {
        self.scsi_sense()
            .map(crate::scsi::ScsiSense::is_marginal)
            .unwrap_or(false)
    }
}

#[cfg(test)]
#[path = "error_tests.rs"]
mod tests;
