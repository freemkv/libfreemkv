//! Smoke tests for the error code → variant mapping. Each new variant
//! added in 0.13.0 (English-elimination work) gets a code() check + a
//! Display sanity-check (no English words) + an io::ErrorKind mapping
//! check. Without these, future drift between the const codes and the
//! match arms in `code()` / the From impl could silently miscategorize.
use super::*;

#[test]
fn is_skippable_title_stub_excludes_malformed_mkv_input() {
    // The two skippable per-title codes, round-tripped through io::Error as
    // `mux_with_keys` returns them. `MkvInvalid` now means ONLY the no-muxable-
    // frames stub, matching this predicate's documented meaning.
    let mkv: std::io::Error = Error::MkvInvalid.into();
    let css: std::io::Error = Error::CssKeyMissing.into();
    assert!(is_skippable_title_stub(&mkv));
    assert!(is_skippable_title_stub(&css));

    // A malformed/truncated mkv:// SOURCE is a FAILURE, not a stub. It used to
    // be raised as `MkvInvalid`, so a corrupt source landed in the skippable
    // set and an all-titles rip silently passed over it, exiting successfully.
    let corrupt: std::io::Error = Error::MkvSourceInvalid.into();
    assert!(
        !is_skippable_title_stub(&corrupt),
        "a malformed mkv:// source must never classify as a skippable stub, got {corrupt}"
    );
    // Same for the write side: an element size EBML cannot represent is an
    // output-side limit, not an empty nav/menu stub.
    let unencodable: std::io::Error = Error::MkvUnencodable.into();
    assert!(!is_skippable_title_stub(&unencodable));
    // And for the lacing rejection carved out in the same spirit.
    let lacing: std::io::Error = Error::MkvLacingInvalid.into();
    assert!(!is_skippable_title_stub(&lacing));

    // A different coded error is NOT skippable (kills a "match anything with
    // an E-code" mutant).
    let nostreams: std::io::Error = Error::NoStreams.into();
    assert!(!is_skippable_title_stub(&nostreams));

    // A plain io::Error with no E-code prefix is not skippable.
    let plain = std::io::Error::from(std::io::ErrorKind::BrokenPipe);
    assert!(!is_skippable_title_stub(&plain));

    // A header-buffer cap overflow is a REAL title whose codec init data never
    // resolved, not an empty stub. Reported as `MkvInvalid` it would land in
    // the skippable set, so an all-titles rip would drop a main feature silently.
    let cap: std::io::Error = Error::MuxHeaderBufferExceeded {
        bytes: 512 * 1024 * 1024 + 1,
    }
    .into();
    assert!(!is_skippable_title_stub(&cap));
}

// The two CSS no-key conditions land on opposite sides of the per-title / whole-disc split
// (mirroring the AACS pair).
#[test]
fn css_no_key_codes_split_disc_level_from_skippable() {
    let wide: std::io::Error = Error::CssNoDiscKey.into();
    assert!(
        is_disc_level_no_key(&wide),
        "the disc-wide CSS no-key code must be disc-level: {wide}"
    );
    assert!(
        !is_skippable_title_stub(&wide),
        "the disc-wide CSS no-key code must not be skippable: {wide}"
    );

    let per_title: std::io::Error = Error::CssKeyMissing.into();
    assert!(
        is_skippable_title_stub(&per_title),
        "the per-title CSS no-key code must stay skippable: {per_title}"
    );
    assert!(
        !is_disc_level_no_key(&per_title),
        "the per-title CSS no-key code must not stop the whole rip: {per_title}"
    );

    // The AACS side of the same split, unchanged.
    let aacs: std::io::Error = Error::NoDiscKey {
        disc_hash: String::new(),
    }
    .into();
    assert!(is_disc_level_no_key(&aacs));
    assert!(!is_skippable_title_stub(&aacs));
}

// A stop maps to Interrupted, not the 6xxx InvalidData arm it sits inside.
#[test]
fn halted_maps_to_interrupted() {
    let e: std::io::Error = Error::Halted.into();
    assert_eq!(e.kind(), std::io::ErrorKind::Interrupted);
    assert!(is_halt(&e));
}

// Every whole-disc key code, and only those, is disc-level.
#[test]
fn disc_level_no_key_covers_every_documented_code() {
    for e in [
        Error::KeydbLoad { path: "p".into() },
        Error::AacsNoKeys,
        Error::KeyServiceUnavailable,
        Error::KeyServiceUnauthorized,
        Error::KeyServiceRateLimited,
    ] {
        let io: std::io::Error = e.into();
        assert!(is_disc_level_no_key(&io), "{io}");
    }
    let per_title: std::io::Error = Error::MkvInvalid.into();
    assert!(!is_disc_level_no_key(&per_title));
}

// Pin, not a guard: typed and string paths agree for every `Error` (Display leads
// with its code). The fallback stays for consumers' string-coded io::Errors.
#[test]
fn error_code_reads_the_typed_payload() {
    let typed: std::io::Error = Error::Halted.into();
    assert_eq!(error_code(&typed), Some(E_HALTED));
    assert_eq!(
        error_code(&std::io::Error::other("E6010: x")),
        Some(E_HALTED)
    );
    assert_eq!(error_code(&std::io::Error::other("plain")), None);
}

// A raw disc name with control bytes cannot inject terminal escapes via Display.
#[test]
fn dir_name_collision_display_escapes_control_characters() {
    let e = Error::DirNameCollision {
        host: "A\x1b[2JB\n".into(),
    };
    let shown = e.to_string();
    assert!(!shown.chars().any(char::is_control), "{shown:?}");
    assert!(shown.contains("A\\u{1b}[2JB\\n"), "{shown}");
}

#[test]
fn disc_derived_names_cannot_inject_terminal_escapes_via_display() {
    let n = || "A\x1b[2JB".to_string();
    let errors = [
        Error::UdfNotFound { path: n() },
        Error::UdfUnrecordedExtent { path: n() },
        Error::DirImagePlacement { path: n() },
        Error::DirImageFileChanged { path: n() },
        Error::DirNameTooLong { path: n() },
        Error::DirImageFanout { path: n() },
        Error::BusStreamUnmapped { files: n() },
        Error::ImageScoped { path: n() },
    ];
    for e in errors {
        let shown = e.to_string();
        assert!(!shown.chars().any(char::is_control), "{shown:?}");
        assert_eq!(shown, format!("E{}: A\\u{{1b}}[2JB", e.code()));
    }
}

#[test]
fn image_ends_before_read_display_is_lba_then_have_slash_want() {
    let e = Error::ImageEndsBeforeRead {
        lba: 111,
        have: 222,
        want: 333,
    };
    assert_eq!(e.to_string(), format!("E{}: 111 222/333", e.code()));
}

#[test]
fn new_variants_have_distinct_codes() {
    let codes = [
        Error::ScsiInterfaceUnavailable { path: "p".into() }.code(),
        Error::DeviceLocked {
            path: "p".into(),
            kr: 0,
        }
        .code(),
        Error::IoKitPluginFailed {
            path: "p".into(),
            kr: 0,
        }
        .code(),
        Error::UnsupportedPlatform { target: "x".into() }.code(),
        Error::PlatformNotImplemented {
            platform: "renesas".into(),
        }
        .code(),
        Error::MapfileInvalid { kind: "hex" }.code(),
        Error::ExtentNotUnitAligned.code(),
        Error::M2tsPacketMalformed.code(),
        Error::DiscCapacityMalformed.code(),
        Error::DirRawRejected.code(),
        Error::DirMultipassRejected.code(),
        Error::DirSourceUnsupported.code(),
        Error::DirNotEmpty.code(),
        Error::DirInsufficientSpace {
            required: 1,
            available: 0,
        }
        .code(),
        Error::DirNameCollision { host: "x".into() }.code(),
        Error::DirWriteFailed { errno: Some(28) }.code(),
        Error::DirImageSsifUnsupported.code(),
        Error::DirImagePlacement { path: "x".into() }.code(),
        Error::DirImageUnsupportedTree.code(),
        Error::DirImageFileChanged { path: "x".into() }.code(),
        Error::DirImageTooLarge.code(),
        Error::DirNameTooLong { path: "x".into() }.code(),
        Error::DirImageFanout { path: "x".into() }.code(),
        Error::SeamPlanDroppedMost {
            dropped: 1,
            written: 0,
        }
        .code(),
        Error::SinkWroteNothing.code(),
        Error::StreamClosed.code(),
        Error::StreamHeaderWritten.code(),
        Error::AacsKeyFileUnreadable.code(),
        Error::AacsNoUsableHostCert.code(),
        Error::WholeDiscKeyMissing.code(),
        Error::AacsVidNeedsDisc.code(),
        Error::TimedOut { op: "verify" }.code(),
        Error::MpgNoVideoTrack.code(),
        Error::MpgUnpacketized.code(),
        Error::ShortImageRead {
            lba: 0,
            expected: 1,
            got: 0,
        }
        .code(),
        Error::EmptyImage.code(),
        Error::ImageTruncated { have: 0, want: 1 }.code(),
        Error::ImageEndsBeforeRead {
            lba: 0,
            have: 0,
            want: 1,
        }
        .code(),
        Error::BusStreamUnmapped { files: "x".into() }.code(),
        Error::ImageScoped { path: "x".into() }.code(),
        Error::RemuxVerifyFailed {
            kind: RemuxVerifyKind::Empty,
            path: "/m/a.mkv".into(),
        }
        .code(),
        Error::MuxIncomplete { title: 1 }.code(),
        Error::RemuxStagingInvalid.code(),
        Error::StagedCopySizeMismatch { have: 0, want: 1 }.code(),
        Error::WorkerLost { op: "copy" }.code(),
        Error::StreamLanguageUnknown { tag: "x".into() }.code(),
        Error::RemuxTargetExists { path: "x".into() }.code(),
    ];
    let mut sorted = codes.to_vec();
    sorted.sort();
    sorted.dedup();
    assert_eq!(
        sorted.len(),
        codes.len(),
        "two new variants share a code — check error.rs constants"
    );
}

#[test]
fn display_emits_no_english_words() {
    // Every variant's Display must be `E{code}: {data}` — no English.
    // Sample a few of the new variants and a few existing ones to
    // catch accidental string-stuffing in future edits.
    let cases: &[(Error, u16)] = &[
        (
            Error::ScsiInterfaceUnavailable {
                path: "/dev/sg4".into(),
            },
            E_SCSI_INTERFACE_UNAVAILABLE,
        ),
        (
            Error::DeviceLocked {
                path: "/dev/sg4".into(),
                kr: 0xE00002C5,
            },
            E_DEVICE_LOCKED,
        ),
        (
            Error::UnsupportedPlatform {
                target: "freebsd".into(),
            },
            E_UNSUPPORTED_PLATFORM,
        ),
        (
            Error::PlatformNotImplemented {
                platform: "renesas".into(),
            },
            E_PLATFORM_NOT_IMPLEMENTED,
        ),
        (Error::MapfileInvalid { kind: "hex" }, E_MAPFILE_INVALID),
        (
            Error::ImageTruncated {
                have: 0,
                want: 1024,
            },
            E_IMAGE_TRUNCATED,
        ),
        (
            Error::ImageEndsBeforeRead {
                lba: 7,
                have: 0,
                want: 1024,
            },
            E_IMAGE_ENDS_BEFORE_READ,
        ),
        (Error::ExtentNotUnitAligned, E_EXTENT_NOT_UNIT_ALIGNED),
        // Both CSS no-key verdicts: numeric-only Display, no English.
        (Error::CssKeyMissing, E_CSS_KEY_MISSING),
        (Error::CssNoDiscKey, E_CSS_NO_DISC_KEY),
        // Key-SOURCE failures: bare numeric Display. They must carry NO
        // detail — the service URL and its resolved address are
        // operator-confidential and must never reach a pasted bug report.
        (Error::KeyServiceUnavailable, E_KEY_SERVICE_UNAVAILABLE),
        (Error::KeyServiceUnauthorized, E_KEY_SERVICE_UNAUTHORIZED),
        (Error::KeyServiceRateLimited, E_KEY_SERVICE_RATE_LIMITED),
        (Error::AacsKeyFileUnreadable, E_AACS_KEY_FILE_UNREADABLE),
        (Error::AacsNoUsableHostCert, E_AACS_NO_USABLE_HOST_CERT),
        (Error::WholeDiscKeyMissing, E_WHOLE_DISC_KEY_MISSING),
        (Error::AacsVidNeedsDisc, E_AACS_VID_NEEDS_DISC),
        (Error::TimedOut { op: "verify" }, E_TIMED_OUT),
        (Error::MpgNoVideoTrack, E_MPG_NO_VIDEO_TRACK),
        (Error::MpgUnpacketized, E_MPG_UNPACKETIZED),
        (
            Error::BusStreamUnmapped {
                files: "/BDMV/STREAM/00002.m2ts".into(),
            },
            E_BUS_STREAM_UNMAPPED,
        ),
        (
            Error::ImageScoped {
                path: "/rips/DISC.iso".into(),
            },
            E_IMAGE_SCOPED,
        ),
    ];
    for (e, want_code) in cases {
        let s = e.to_string();
        assert!(
            s.starts_with(&format!("E{}", want_code)),
            "{:?} display does not lead with code: {}",
            e,
            s
        );
        // Crude English filter — `Display` should never emit ASCII words longer
        // than 8 chars (identifiers like `/dev/sg4`, `renesas` pass;
        // "exclusive access denied" would not).
        for word in s.split(|c: char| !c.is_ascii_alphabetic()) {
            assert!(
                word.len() <= 8,
                "Display contains suspicious English-looking word `{word}` in `{s}`"
            );
        }
    }
}

/// The three `Dir*` variants whose fields previously fell through to the
/// bare-code `_` arm must now SURFACE their fields: `E9027` carries
/// `required/available`, `E9028` the colliding host, `E9029` the errno.
/// Red-before-green: before the arms were added each rendered only `E{code}`
/// with no field values, so these substring assertions failed.
#[test]
fn dir_space_collision_write_variants_surface_fields() {
    let space = Error::DirInsufficientSpace {
        required: 4096,
        available: 512,
    };
    assert_eq!(
        space.to_string(),
        format!("E{}: 4096/512", E_DIR_INSUFFICIENT_SPACE)
    );

    let collision = Error::DirNameCollision {
        host: "TITLE".into(),
    };
    assert_eq!(
        collision.to_string(),
        format!("E{}: TITLE", E_DIR_NAME_COLLISION)
    );

    let write_failed = Error::DirWriteFailed { errno: Some(28) };
    assert_eq!(
        write_failed.to_string(),
        format!("E{}: 28", E_DIR_WRITE_FAILED)
    );
    // A None errno renders as the bare code — no dangling "colon space".
    let write_none = Error::DirWriteFailed { errno: None };
    assert_eq!(write_none.to_string(), format!("E{}", E_DIR_WRITE_FAILED));
}

/// `ImageTruncated` Display must carry BOTH the `have` and `want` byte
/// lengths as substrings, so a truncated-resume report shows the actual
/// mismatch rather than just the bare code.
#[test]
fn image_truncated_display_has_have_and_want() {
    let e = Error::ImageTruncated {
        have: 12345,
        want: 67890,
    };
    let s = e.to_string();
    assert!(
        s.contains("12345"),
        "have value missing from ImageTruncated display `{s}`"
    );
    assert!(
        s.contains("67890"),
        "want value missing from ImageTruncated display `{s}`"
    );
    // The documented `E<code>: <args>` shape the app layer splits on.
    assert_eq!(s, format!("E{}: 12345/67890", E_IMAGE_TRUNCATED));
}

#[test]
fn iokind_mapping_for_new_variants() {
    use std::io::ErrorKind;
    let mapped = |e: Error| -> ErrorKind {
        let io: std::io::Error = e.into();
        io.kind()
    };
    // 1xxx "device absent" → NotFound
    assert_eq!(
        mapped(Error::ScsiInterfaceUnavailable { path: "p".into() }),
        ErrorKind::NotFound
    );
    assert_eq!(
        mapped(Error::DeviceNotFound { path: "p".into() }),
        ErrorKind::NotFound
    );
    // 1xxx access-denied semantics → PermissionDenied (not NotFound)
    assert_eq!(
        mapped(Error::DevicePermission { path: "p".into() }),
        ErrorKind::PermissionDenied
    );
    assert_eq!(
        mapped(Error::DeviceLocked {
            path: "p".into(),
            kr: 0
        }),
        ErrorKind::PermissionDenied
    );
    // 2xxx range → Unsupported
    assert_eq!(
        mapped(Error::UnsupportedPlatform { target: "x".into() }),
        ErrorKind::Unsupported
    );
    assert_eq!(
        mapped(Error::PlatformNotImplemented {
            platform: "x".into()
        }),
        ErrorKind::Unsupported
    );
    // 6xxx range → InvalidData
    assert_eq!(
        mapped(Error::MapfileInvalid { kind: "hex" }),
        ErrorKind::InvalidData
    );
    // 9021 special-cased to InvalidData
    assert_eq!(mapped(Error::M2tsPacketMalformed), ErrorKind::InvalidData);
    // 9047 DiscCapacityMalformed → InvalidData
    assert_eq!(mapped(Error::DiscCapacityMalformed), ErrorKind::InvalidData);
    // 9053/9054: the mkv:// read-path and write-path rejections split off
    // `MkvInvalid`. Both are InvalidData, and both must render as a bare code
    // with no English (this crate has none).
    assert_eq!(mapped(Error::MkvSourceInvalid), ErrorKind::InvalidData);
    assert_eq!(mapped(Error::MkvUnencodable), ErrorKind::InvalidData);
    assert_eq!(
        Error::MkvSourceInvalid.to_string(),
        format!("E{}", E_MKV_SOURCE_INVALID)
    );
    assert_eq!(
        Error::MkvUnencodable.to_string(),
        format!("E{}", E_MKV_UNENCODABLE)
    );
}

// `Error::IoError` must round-trip to the *original* `io::Error` —
// preserving `ErrorKind` and raw OS error — not flatten to `Other` with a
// stringified message.
#[test]
fn ioerror_roundtrips_preserving_kind_and_oscode() {
    use std::io::ErrorKind;
    let original = std::io::Error::from_raw_os_error(13); // EACCES
    let original_kind = original.kind();
    let wrapped: Error = original.into(); // From<io::Error> for Error
    let back: std::io::Error = wrapped.into(); // From<Error> for io::Error
    assert_eq!(back.kind(), original_kind);
    assert_eq!(back.raw_os_error(), Some(13));

    // A synthesized kind (no OS code) must also survive.
    let timeout: Error = std::io::Error::from(ErrorKind::TimedOut).into();
    let back2: std::io::Error = timeout.into();
    assert_eq!(back2.kind(), ErrorKind::TimedOut);
}

/// `DiscRead` Display must include the ASCQ byte (the 5th field) so
/// NOT_READY substates (0x04/0x3E vs 0x04/0x01) are distinguishable
/// in logs and bug reports.
#[test]
fn discread_display_includes_ascq() {
    let e = Error::DiscRead {
        sector: 42,
        status: Some(0x02),
        sense: Some(crate::scsi::ScsiSense {
            sense_key: 0x02,
            asc: 0x04,
            ascq: 0x3e,
        }),
    };
    let s = e.to_string();
    // sense_key/asc/ascq triple all present.
    assert!(s.contains("0x02/0x04/0x3e"), "ascq missing from `{s}`");
}

/// `NoDiscKey` with an empty hash must not emit a dangling
/// "colon space" suffix.
#[test]
fn nodisckey_empty_hash_has_no_trailing_colon() {
    let e = Error::NoDiscKey {
        disc_hash: String::new(),
    };
    assert_eq!(e.to_string(), format!("E{}", E_NO_DISC_KEY));
    let e2 = Error::NoDiscKey {
        disc_hash: "abc".into(),
    };
    assert_eq!(e2.to_string(), format!("E{}: abc", E_NO_DISC_KEY));
}

// ── New comprehensive tests ────────────────────────────────────────────────

// Every `pub const E_*`, parsed from source (not hand-listed) so a new constant can't
// escape the uniqueness check below.
fn declared_error_codes() -> Vec<(&'static str, u16)> {
    const SRC: &str = include_str!("error.rs");
    SRC.lines()
        .filter_map(|line| {
            // Only real declarations at column 0. A retired code is
            // recorded as `// 2001: burned/retired`, which carries no `pub
            // const` and is therefore correctly invisible here.
            let decl = line.strip_prefix("pub const ")?;
            let (name, value) = decl.split_once(": u16 = ")?;
            if !name.starts_with("E_") {
                return None;
            }
            let value = value.strip_suffix(';')?;
            Some((
                name,
                value
                    .parse::<u16>()
                    .unwrap_or_else(|_| panic!("non-literal error code for `{name}`")),
            ))
        })
        .collect()
}

// Guards against `declared_error_codes` silently returning empty (which
// would make the uniqueness test below pass vacuously). Mutation: a
// `strip_prefix` typo or off-by-one in the name slice fails here.
#[test]
fn declared_error_codes_parses_the_declarations_it_claims_to() {
    let declared = declared_error_codes();
    // Independent count of the declarations, computed a different way
    // from the parser under test.
    let expected = include_str!("error.rs")
        .lines()
        .filter(|l| l.starts_with("pub const E_"))
        .count();
    assert_eq!(
        declared.len(),
        expected,
        "the parser must see every `pub const E_*` line"
    );
    assert!(
        expected >= 120,
        "sanity floor: this file declares well over a hundred codes, got {expected}"
    );
    // Names and values must match the compiled constants, so a parser that
    // mis-slices the name or mis-reads the digits cannot pass.
    for (name, value) in [
        ("E_DEVICE_NOT_FOUND", E_DEVICE_NOT_FOUND),
        ("E_UDF_UNRECORDED_EXTENT", E_UDF_UNRECORDED_EXTENT),
        ("E_KEYDB_PARSE", E_KEYDB_PARSE),
    ] {
        assert!(
            declared.contains(&(name, value)),
            "`{name}` = {value} not parsed out of the source; got {:?}",
            declared
                .iter()
                .filter(|(n, _)| *n == name)
                .collect::<Vec<_>>()
        );
    }
    // A comment-only retired code must NOT be picked up as a constant.
    assert!(
        !declared.iter().any(|(n, _)| n.is_empty()),
        "no empty names"
    );
}

// Every published error code constant must be unique. Set is derived from
// `declared_error_codes` (not hand-kept) so new constants are covered
// automatically. Mutation: duplicating any code's value fails here.
#[test]
fn all_error_code_constants_are_unique() {
    let mut by_code: std::collections::BTreeMap<u16, Vec<&str>> = std::collections::BTreeMap::new();
    for (name, value) in declared_error_codes() {
        by_code.entry(value).or_default().push(name);
    }
    let dupes: Vec<_> = by_code
        .iter()
        .filter(|(_, names)| names.len() > 1)
        .collect();
    assert!(
        dupes.is_empty(),
        "duplicate error code constants detected — check error.rs: {dupes:?}"
    );
}

// Error code ranges match their documented category buckets (e.g. device
// codes 1000-1999, AACS codes 7000-7999). Mutation: shifting a constant
// out of range breaks CLI range-based dispatch and logging.
#[test]
fn error_code_range_buckets_are_correct() {
    // Device (1xxx)
    assert!((1000..2000).contains(&E_DEVICE_NOT_FOUND));
    assert!((1000..2000).contains(&E_DEVICE_PERMISSION));
    assert!((1000..2000).contains(&E_SCSI_INTERFACE_UNAVAILABLE));
    // Profile (2xxx)
    assert!((2000..3000).contains(&E_UNSUPPORTED_DRIVE));
    assert!((2000..3000).contains(&E_PROFILE_PARSE));
    // Unlock (3xxx)
    assert!((3000..4000).contains(&E_UNLOCK_FAILED));
    assert!((3000..4000).contains(&E_SIGNATURE_MISMATCH));
    // SCSI (4xxx)
    assert!((4000..5000).contains(&E_SCSI_ERROR));
    // I/O (5xxx)
    assert!((5000..6000).contains(&E_IO_ERROR));
    // Disc format (6xxx)
    assert!((6000..7000).contains(&E_DISC_READ));
    assert!((6000..7000).contains(&E_HALTED));
    assert!((6000..7000).contains(&E_MAPFILE_INVALID));
    assert!((6000..7000).contains(&E_IMAGE_TRUNCATED));
    assert!((6000..7000).contains(&E_IMAGE_ENDS_BEFORE_READ));
    assert!((6000..7000).contains(&E_BUS_STREAM_UNMAPPED));
    assert!((6000..7000).contains(&E_IMAGE_SCOPED));
    assert!((6000..7000).contains(&E_XPL_TOO_LARGE));
    // AACS (7xxx)
    assert!((7000..8000).contains(&E_AACS_NO_KEYS));
    assert!((7000..8000).contains(&E_NO_DISC_KEY));
    assert!((7000..8000).contains(&E_AACS_KEY_FILE_UNREADABLE));
    assert!((7000..8000).contains(&E_WHOLE_DISC_KEY_MISSING));
    // Keydb (8xxx)
    assert!((8000..9000).contains(&E_KEYDB_CONNECT));
    assert!((8000..9000).contains(&E_KEYDB_TOO_MANY_REDIRECTS));
    // Stream/mux (9xxx)
    assert!((9000..10000).contains(&E_STREAM_READ_ONLY));
    assert!((9000..10000).contains(&E_DISC_CAPACITY_MALFORMED));
}

/// Error.code() matches its associated constant for every new 9xxx variant.
/// Mutation: swapping two adjacent code() arms (e.g. SweepConsumerGone ↔
///           PipelineConsumerGone) makes the wrong code appear in logs.
#[test]
fn error_code_matches_constant_for_stream_variants() {
    use std::io::ErrorKind;
    let cases: &[(Error, u16)] = &[
        (Error::StreamReadOnly, E_STREAM_READ_ONLY),
        (Error::StreamWriteOnly, E_STREAM_WRITE_ONLY),
        (Error::PesInvalidMagic, E_PES_INVALID_MAGIC),
        (Error::NoMetadata, E_NO_METADATA),
        (Error::HevcParamParse, E_HEVC_PARAM_PARSE),
        (Error::Fmp4Unimplemented, E_FMP4_UNIMPLEMENTED),
        (Error::DemuxThreadPanicked, E_DEMUX_THREAD_PANICKED),
        (
            Error::PipelineConsumerPanicked,
            E_PIPELINE_CONSUMER_PANICKED,
        ),
        (Error::SweepConsumerGone, E_SWEEP_CONSUMER_GONE),
        (Error::PipelineConsumerGone, E_PIPELINE_CONSUMER_GONE),
        (Error::DiscCapacityOverflow, E_DISC_CAPACITY_OVERFLOW),
        (Error::MuxEmpty, E_MUX_EMPTY),
        (
            Error::MuxHeaderBufferExceeded { bytes: 0 },
            E_MUX_HEADER_BUFFER_EXCEEDED,
        ),
        (Error::MkvLacingInvalid, E_MKV_LACING_INVALID),
        (Error::MkvSourceInvalid, E_MKV_SOURCE_INVALID),
        (Error::MkvUnencodable, E_MKV_UNENCODABLE),
        (Error::StreamClosed, E_STREAM_CLOSED),
        (Error::StreamHeaderWritten, E_STREAM_HEADER_WRITTEN),
        (Error::Mp4NoVideoTrack, E_MP4_NO_VIDEO_TRACK),
        (Error::MpgNoVideoTrack, E_MPG_NO_VIDEO_TRACK),
        (Error::MpgUnpacketized, E_MPG_UNPACKETIZED),
        (Error::Mp4Invalid, E_MP4_INVALID),
        (Error::Mp4MissingCodecPrivate, E_MP4_MISSING_CODEC_PRIVATE),
        (Error::Mp4UnknownResolution, E_MP4_UNKNOWN_RESOLUTION),
        (Error::SyncTimeout, E_SYNC_TIMEOUT),
        (Error::SyncWorkerLost, E_SYNC_WORKER_LOST),
        (Error::M2tsPacketMalformed, E_M2TS_PACKET_MALFORMED),
        (Error::ExtentNotUnitAligned, E_EXTENT_NOT_UNIT_ALIGNED),
        (Error::DiscCapacityMalformed, E_DISC_CAPACITY_MALFORMED),
        (
            Error::NetworkAddrBlocked {
                addr: String::new(),
            },
            E_NETWORK_ADDR_BLOCKED,
        ),
    ];
    for (e, expected_code) in cases {
        assert_eq!(
            e.code(),
            *expected_code,
            "{:?}.code() must equal {} (const)",
            e,
            expected_code
        );
    }
    // io::ErrorKind mapping spot-check for 9xxx variants.
    let to_kind = |e: Error| -> ErrorKind {
        let io: std::io::Error = e.into();
        io.kind()
    };
    assert_eq!(to_kind(Error::StreamReadOnly), ErrorKind::Unsupported);
    assert_eq!(to_kind(Error::HevcParamParse), ErrorKind::InvalidData);
    assert_eq!(to_kind(Error::Fmp4Unimplemented), ErrorKind::Unsupported);
    assert_eq!(to_kind(Error::PipelineJoinTimeout), ErrorKind::TimedOut);
    assert_eq!(
        to_kind(Error::ExtentNotUnitAligned),
        ErrorKind::InvalidInput
    );
}

/// Error.code() for AACS variants matches their constants.
/// Mutation: swapping E_AACS_CERT_READ and E_AACS_CERT_VERIFY codes
///           makes the wrong diagnostic appear in the UI.
#[test]
fn error_code_matches_constant_for_aacs_variants() {
    let aacs_cases: &[(Error, u16)] = &[
        (Error::AacsNoKeys, E_AACS_NO_KEYS),
        (Error::AacsCertShort, E_AACS_CERT_SHORT),
        (Error::AacsAgidAlloc, E_AACS_AGID_ALLOC),
        (Error::AacsCertRejected, E_AACS_CERT_REJECTED),
        (Error::AacsCertRead, E_AACS_CERT_READ),
        (Error::AacsCertVerify, E_AACS_CERT_VERIFY),
        (Error::AacsKeyRead, E_AACS_KEY_READ),
        (Error::AacsKeyRejected, E_AACS_KEY_REJECTED),
        (Error::AacsKeyVerify, E_AACS_KEY_VERIFY),
        (Error::AacsVidRead, E_AACS_VID_READ),
        (Error::AacsVidMac, E_AACS_VID_MAC),
        (Error::AacsDataKey, E_AACS_DATA_KEY),
        (Error::DecryptFailed, E_DECRYPT_FAILED),
        (Error::CssAuthFailed, E_CSS_AUTH_FAILED),
        (Error::AacsHostCertRejected, E_AACS_HOST_CERT_REJECTED),
        (Error::AacsNoUsableHostCert, E_AACS_NO_USABLE_HOST_CERT),
        (Error::AacsVidNeedsDisc, E_AACS_VID_NEEDS_DISC),
        (Error::AacsRawReadUnsupported, E_AACS_RAW_READ_UNSUPPORTED),
        (Error::AacsVidUnavailable, E_AACS_VID_UNAVAILABLE),
        (Error::AacsMkUnavailable, E_AACS_MK_UNAVAILABLE),
        (Error::AacsVukNotInKeydb, E_AACS_VUK_NOT_IN_KEYDB),
        (Error::DriveProfileMissing, E_DRIVE_PROFILE_MISSING),
        (Error::VidCdbUnavailable, E_VID_CDB_UNAVAILABLE),
    ];
    for (e, expected_code) in aacs_cases {
        assert_eq!(
            e.code(),
            *expected_code,
            "{:?}.code() must be {}",
            e,
            expected_code
        );
    }
}

/// LT8 (CC-L0): E9073's code, Display and `ErrorKind::TimedOut` round trip.
/// "declare `E_TIMED_OUT = 9073` ... with their `Error` variants, `code()`,
/// `Display`, `ErrorKind` and table rows" (stop-design-v5 §6 R7.2).
#[test]
fn timed_out_code_display_kind() {
    use std::io::ErrorKind;
    let e = Error::TimedOut {
        op: "artifact_lock",
    };
    assert_eq!(e.code(), E_TIMED_OUT);
    assert_eq!(e.to_string(), format!("E{E_TIMED_OUT}: artifact_lock"));
    let io: std::io::Error = e.into();
    assert_eq!(io.kind(), ErrorKind::TimedOut);
    // The declared-constants table (parsed from source) carries the code.
    assert!(declared_error_codes().contains(&("E_TIMED_OUT", E_TIMED_OUT)));
}

/// MPG-L0: E9074's code, Display and `ErrorKind` (the E9048 bucket). "No carriable
/// video → `MpgNoVideoTrack` (E9074), as `mp4://` does with E9048" (mpg design v5 §0).
/// Declared only; `mpg://` (L2) raises it. Per spec; do not change without a design citation.
#[test]
fn mpg_no_video_track_code_display_kind() {
    use std::io::ErrorKind;
    let e = Error::MpgNoVideoTrack;
    assert_eq!(e.code(), 9074);
    assert_eq!(e.code(), E_MPG_NO_VIDEO_TRACK);
    assert_eq!(e.to_string(), "E9074");
    let io: std::io::Error = e.into();
    assert_eq!(io.kind(), ErrorKind::InvalidData);
    assert_eq!(
        io.kind(),
        std::io::Error::from(Error::Mp4NoVideoTrack).kind()
    );
    assert!(declared_error_codes().contains(&("E_MPG_NO_VIDEO_TRACK", E_MPG_NO_VIDEO_TRACK)));
}

/// LT8 companion (KU v3.4 J11): E7034's code, Display and its `ErrorKind`
/// (7xxx maps to `PermissionDenied`, same bucket as the sibling AACS codes).
#[test]
fn aacs_vid_needs_disc_code_display_kind() {
    use std::io::ErrorKind;
    let e = Error::AacsVidNeedsDisc;
    assert_eq!(e.code(), E_AACS_VID_NEEDS_DISC);
    assert_eq!(e.to_string(), format!("E{E_AACS_VID_NEEDS_DISC}"));
    let io: std::io::Error = e.into();
    assert_eq!(io.kind(), ErrorKind::PermissionDenied);
    assert!(declared_error_codes().contains(&("E_AACS_VID_NEEDS_DISC", E_AACS_VID_NEEDS_DISC)));
}

/// Engine remux/core codes E9077-E9084: code, exact Display and ErrorKind for
/// each, plus a typed io::Error round trip that keeps the fields.
#[test]
fn engine_remux_codes_display_and_kind() {
    use std::io::ErrorKind;
    let cases: Vec<(Error, u16, &str, ErrorKind)> = vec![
        (
            Error::RemuxVerifyFailed {
                kind: RemuxVerifyKind::Empty,
                path: "/m/a.mkv".into(),
            },
            9077,
            "E9077: empty /m/a.mkv",
            ErrorKind::InvalidData,
        ),
        (
            Error::RemuxVerifyFailed {
                kind: RemuxVerifyKind::NoTracks,
                path: "/m/a.mkv".into(),
            },
            9077,
            "E9077: no-tracks /m/a.mkv",
            ErrorKind::InvalidData,
        ),
        (
            Error::RemuxVerifyFailed {
                kind: RemuxVerifyKind::NoRuntime,
                path: "/m/a.mkv".into(),
            },
            9077,
            "E9077: no-runtime /m/a.mkv",
            ErrorKind::InvalidData,
        ),
        (
            Error::RemuxVerifyFailed {
                kind: RemuxVerifyKind::RuntimeMismatch {
                    have_secs: 5400.04,
                    want_secs: 7200.0,
                },
                path: "/m/a.mkv".into(),
            },
            9077,
            "E9077: runtime-mismatch 5400.0/7200.0 /m/a.mkv",
            ErrorKind::InvalidData,
        ),
        (
            Error::MuxIncomplete { title: 3 },
            9078,
            "E9078: 3",
            ErrorKind::Other,
        ),
        (
            Error::RemuxStagingInvalid,
            9079,
            "E9079",
            ErrorKind::InvalidInput,
        ),
        (
            Error::StagedCopySizeMismatch { have: 10, want: 12 },
            9080,
            "E9080: 10/12",
            ErrorKind::InvalidData,
        ),
        (
            Error::WorkerLost { op: "verify" },
            9081,
            "E9081: verify",
            ErrorKind::Other,
        ),
        (
            Error::StreamLanguageUnknown {
                tag: "xx\nyy".into(),
            },
            9083,
            "E9083: xx\\nyy",
            ErrorKind::InvalidInput,
        ),
        (
            Error::RemuxTargetExists {
                path: "/m/a.mkv".into(),
            },
            9084,
            "E9084: /m/a.mkv",
            ErrorKind::AlreadyExists,
        ),
        (
            Error::MuxBatchSectorsZero,
            9085,
            "E9085",
            ErrorKind::InvalidInput,
        ),
    ];
    for (e, code, shown, kind) in cases {
        assert_eq!(e.code(), code, "{e:?}");
        assert_eq!(e.to_string(), shown, "{e:?}");
        let io: std::io::Error = e.into();
        assert_eq!(io.kind(), kind, "E{code}");
        assert_eq!(error_code(&io), Some(code));
    }
    let back: Error =
        std::io::Error::from(Error::StagedCopySizeMismatch { have: 1, want: 2 }).into();
    assert!(matches!(
        back,
        Error::StagedCopySizeMismatch { have: 1, want: 2 }
    ));
}

// is_scsi_transport_failure is true for the 0xFF sentinel and non-SCSI
// dead-bus faults (IoError, DeviceNotFound), never for CHECK CONDITION.
// Mutation: dropping the IoError/DeviceNotFound arm lets a dead bus zero-fill.
#[test]
fn is_scsi_transport_failure_only_for_0xff() {
    use crate::scsi::SCSI_STATUS_TRANSPORT_FAILURE;
    // True: transport failure sentinel.
    let tf = Error::ScsiError {
        opcode: 0x28,
        status: SCSI_STATUS_TRANSPORT_FAILURE,
        sense: None,
    };
    assert!(tf.is_scsi_transport_failure());

    // False: CHECK CONDITION is a real SCSI reply, not a transport failure.
    let cc = Error::ScsiError {
        opcode: 0x28,
        status: crate::scsi::SCSI_STATUS_CHECK_CONDITION,
        sense: Some(crate::scsi::ScsiSense {
            sense_key: 0x03,
            asc: 0x11,
            ascq: 0x00,
        }),
    };
    assert!(!cc.is_scsi_transport_failure());

    // True: non-SCSI dead-bus faults — a failed ioctl(SG_IO) and a vanished
    // device are transport-layer failures, not recoverable bad sectors.
    assert!(
        Error::IoError {
            source: std::io::Error::from(std::io::ErrorKind::NotConnected)
        }
        .is_scsi_transport_failure()
    );
    assert!(
        Error::DeviceNotFound {
            path: "/dev/sg9".into()
        }
        .is_scsi_transport_failure()
    );

    // False for unrelated errors.
    assert!(!Error::Halted.is_scsi_transport_failure());
}

// is_marginal_read is true for sense keys MEDIUM ERROR(3), NOT READY(2),
// ABORTED COMMAND(B), RECOVERED ERROR(1), NO SENSE(0). Mutation: dropping
// NOT_READY treats BU40N "bad sector" responses as fatal instead of retriable.
#[test]
fn is_marginal_read_sense_key_coverage() {
    use crate::scsi::ScsiSense;
    let marginal_keys = [
        0x00, // NO SENSE
        0x01, // RECOVERED ERROR
        0x02, // NOT READY — dominant BU40N bad-sector sense key
        0x03, // MEDIUM ERROR — canonical bad sector
        0x0B, // ABORTED COMMAND
    ];
    for sk in marginal_keys {
        let e = Error::ScsiError {
            opcode: 0x28,
            status: crate::scsi::SCSI_STATUS_CHECK_CONDITION,
            sense: Some(ScsiSense {
                sense_key: sk,
                asc: 0x11,
                ascq: 0x00,
            }),
        };
        assert!(
            e.is_marginal_read(),
            "sense_key=0x{:02x} must be marginal",
            sk
        );
    }
    // Non-marginal keys: HARDWARE ERROR (4), ILLEGAL REQUEST (5),
    // UNIT ATTENTION (6), DATA PROTECT (7), BLANK CHECK (8).
    let non_marginal_keys = [0x04, 0x05, 0x06, 0x07, 0x08];
    for sk in non_marginal_keys {
        let e = Error::ScsiError {
            opcode: 0x28,
            status: crate::scsi::SCSI_STATUS_CHECK_CONDITION,
            sense: Some(ScsiSense {
                sense_key: sk,
                asc: 0x00,
                ascq: 0x00,
            }),
        };
        assert!(
            !e.is_marginal_read(),
            "sense_key=0x{:02x} must NOT be marginal",
            sk
        );
    }
}

// is_bridge_degradation is true for any status byte that isn't GOOD, CHECK
// CONDITION, or TRANSPORT_FAILURE (bridge firmware returns non-standard
// bytes like 0x04/0x05). Mutation: checking only 0x04 misses 0x05 etc.
#[test]
fn is_bridge_degradation_detects_non_standard_status() {
    use crate::scsi::{
        SCSI_STATUS_CHECK_CONDITION, SCSI_STATUS_GOOD, SCSI_STATUS_TRANSPORT_FAILURE,
    };
    // 0x04 and 0x05 are non-standard bridge degradation codes.
    for bad_status in [0x04u8, 0x05, 0x08, 0x10] {
        let e = Error::ScsiError {
            opcode: 0x28,
            status: bad_status,
            sense: None,
        };
        assert!(
            e.is_bridge_degradation(),
            "status=0x{:02x} must be bridge degradation",
            bad_status
        );
    }
    // Standard codes must NOT be classified as bridge degradation.
    assert!(
        !Error::ScsiError {
            opcode: 0x28,
            status: SCSI_STATUS_GOOD,
            sense: None
        }
        .is_bridge_degradation()
    );
    assert!(
        !Error::ScsiError {
            opcode: 0x28,
            status: SCSI_STATUS_CHECK_CONDITION,
            sense: None
        }
        .is_bridge_degradation()
    );
    assert!(
        !Error::ScsiError {
            opcode: 0x28,
            status: SCSI_STATUS_TRANSPORT_FAILURE,
            sense: None
        }
        .is_bridge_degradation()
    );
}

/// scsi_sense returns Some for ScsiError with sense and DiscRead with sense.
/// Mutation: only checking ScsiError misses DiscRead sense data.
#[test]
fn scsi_sense_from_disc_read() {
    use crate::scsi::ScsiSense;
    let sense = ScsiSense {
        sense_key: 0x02,
        asc: 0x04,
        ascq: 0x3e,
    };
    let disc_read = Error::DiscRead {
        sector: 12345,
        status: Some(0x02),
        sense: Some(sense),
    };
    let got = disc_read.scsi_sense().unwrap();
    assert_eq!(got.sense_key, 0x02);
    assert_eq!(got.asc, 0x04);
    assert_eq!(got.ascq, 0x3e);

    // Non-SCSI errors return None.
    assert!(Error::Halted.scsi_sense().is_none());
    assert!(Error::NoMetadata.scsi_sense().is_none());
}

/// Display for SignatureMismatch includes both expected and got bytes in hex.
/// Mutation: printing only `expected` without `got` makes the mismatch undiscoverable.
#[test]
fn signature_mismatch_display_includes_both_sides() {
    let e = Error::SignatureMismatch {
        expected: [0xAA, 0xBB, 0xCC, 0xDD],
        got: [0x11, 0x22, 0x33, 0x44],
    };
    let s = e.to_string();
    // Must include the E-code prefix.
    assert!(
        s.starts_with(&format!("E{}", E_SIGNATURE_MISMATCH)),
        "must start with code: {s}"
    );
    // Must include the expected bytes.
    assert!(s.contains("aabbccdd"), "must contain expected bytes: {s}");
    // Must include the got bytes.
    assert!(s.contains("11223344"), "must contain got bytes: {s}");
    // Must use '!=' as the separator between expected and got.
    assert!(s.contains("!="), "must use '!=' separator: {s}");
}

/// DiscTitleRange display format is "E6005: index/count".
/// Mutation: swapping index and count in the format string makes logs misleading.
#[test]
fn disc_title_range_display_is_index_slash_count() {
    let e = Error::DiscTitleRange {
        index: 3,
        count: 10,
    };
    let expected = format!("E{}: 3/10", E_DISC_TITLE_RANGE);
    assert_eq!(e.to_string(), expected);
}

/// Keydb error variants display correctly with their structured data.
/// Mutation: using a generic "E{code}" fallback drops the host/path data from logs.
#[test]
fn keydb_errors_include_structured_data_in_display() {
    let e_connect = Error::KeydbConnect {
        host: "mirror.example".into(),
    };
    assert!(
        e_connect.to_string().contains("mirror.example"),
        "KeydbConnect display must include host"
    );

    let e_http = Error::KeydbHttp { status: 403 };
    assert!(
        e_http.to_string().contains("403"),
        "KeydbHttp display must include status code"
    );

    let e_write = Error::KeydbWrite {
        path: "/root/.config/freemkv/keydb.cfg".into(),
    };
    assert!(
        e_write.to_string().contains("/root"),
        "KeydbWrite display must include path"
    );

    let e_load = Error::KeydbLoad {
        path: "<no keydb in search paths>".into(),
    };
    assert!(
        e_load.to_string().contains("<no keydb in search paths>"),
        "KeydbLoad display must include the sentinel path"
    );

    let e_no_cert = Error::AacsNoHostCert {
        path: "<no host cert>".into(),
    };
    assert!(
        e_no_cert.to_string().contains("<no host cert>"),
        "AacsNoHostCert display must include the sentinel path"
    );

    let e_scheme = Error::KeydbUnsupportedScheme {
        scheme: "ftp".into(),
    };
    assert!(
        e_scheme.to_string().contains("ftp"),
        "KeydbUnsupportedScheme display must include scheme"
    );
}

/// MuxTrackRange display format is "E9011: track/tracks".
/// Mutation: formatting as "track/count" or "tracks/track" is wrong.
#[test]
fn mux_track_range_display_is_track_slash_tracks() {
    let e = Error::MuxTrackRange {
        track: 5,
        tracks: 3,
    };
    let expected = format!("E{}: 5/3", E_MUX_TRACK_RANGE);
    assert_eq!(e.to_string(), expected);
}
