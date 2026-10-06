use super::*;

const AASTORE: u8 = 0x53;

// Reader::slice takes an attacker-supplied length (JVMS u4/u2 field);
// pos + len must not overflow/wrap past the bounds check and panic —
// untrusted disc input must yield an EOF error, not a panic.
#[test]
fn slice_rejects_a_length_that_would_wrap_pos() {
    let data = [0u8; 16];
    let mut r = Reader::new(&data);
    r.u64("advance pos").expect("8 bytes available");
    // pos is now 8; usize::MAX would wrap the end offset to 7.
    match r.slice(usize::MAX, "wrapping length") {
        Err(Error::UnexpectedEof { .. }) => {}
        Err(other) => panic!("expected UnexpectedEof, got {other:?}"),
        Ok(s) => panic!("expected UnexpectedEof, got a {}-byte slice", s.len()),
    }
    // The reader must not have consumed anything.
    match r.slice(8, "remaining bytes") {
        Ok(s) => assert_eq!(s.len(), 8, "pos moved on the rejected slice"),
        Err(e) => panic!("the remaining 8 bytes must still be readable: {e:?}"),
    }
}

/// The ordinary out-of-range case (no wrap) must keep returning EOF, and
/// an exactly-fitting length must still succeed — the check is `>`, not
/// `>=`.
#[test]
fn slice_boundary_is_inclusive_of_the_final_byte() {
    let data = [0u8; 16];
    let mut r = Reader::new(&data);
    assert_eq!(r.slice(16, "whole buffer").expect("exact fit").len(), 16);
    let mut r = Reader::new(&data);
    assert!(matches!(
        r.slice(17, "one past"),
        Err(Error::UnexpectedEof { .. })
    ));
}

#[test]
fn rejects_non_class_bytes() {
    match ClassFile::parse(b"\x00\x01\x02\x03DEAD") {
        Err(Error::BadMagic(_)) | Err(Error::UnexpectedEof { .. }) => {}
        Err(other) => panic!("expected magic/eof error, got {:?}", other),
        Ok(_) => panic!("expected error, got Ok"),
    }
}

#[test]
fn modified_utf8_basic_ascii() {
    let s = decode_modified_utf8(b"English").unwrap();
    assert_eq!(s, "English");
}

#[test]
fn modified_utf8_null_encoding() {
    // 0xC0 0x80 in modified UTF-8 encodes U+0000.
    let s = decode_modified_utf8(&[0xC0, 0x80]).unwrap();
    assert_eq!(s, "\u{0000}");
}

#[test]
fn modified_utf8_rejects_raw_zero() {
    assert!(decode_modified_utf8(&[0x00]).is_err());
}

#[test]
fn modified_utf8_combines_surrogate_pair() {
    // U+1F600 = D83D DE00, each half as a 3-byte sequence.
    let s = decode_modified_utf8(&[0xED, 0xA0, 0xBD, 0xED, 0xB8, 0x80]).unwrap();
    assert_eq!(s, "\u{1F600}");
    // A lone high surrogate still degrades to U+FFFD.
    assert_eq!(
        decode_modified_utf8(&[0xED, 0xA0, 0xBD]).unwrap(),
        "\u{FFFD}"
    );
}

#[test]
fn modified_utf8_unpaired_surrogates_degrade_without_panicking() {
    // high + high: each is lone.
    assert_eq!(
        decode_modified_utf8(&[0xED, 0xA0, 0xBD, 0xED, 0xA0, 0xBD]).unwrap(),
        "\u{FFFD}\u{FFFD}"
    );
    // high + ASCII, and a lone low surrogate.
    assert_eq!(
        decode_modified_utf8(&[0xED, 0xA0, 0xBD, b'a']).unwrap(),
        "\u{FFFD}a"
    );
    assert_eq!(
        decode_modified_utf8(&[0xED, 0xB8, 0x80]).unwrap(),
        "\u{FFFD}"
    );
}

#[test]
fn modified_utf8_two_byte() {
    // U+00E9 'é' in the standard 2-byte modified-UTF-8 encoding
    // (0xC3 0xA9), exercising the decoder's 2-byte branch.
    let s = decode_modified_utf8(&[0xC3, 0xA9]).unwrap();
    assert_eq!(s, "é");
}

#[test]
fn instruction_size_fixed_opcodes() {
    // bipush is 2 bytes, sipush is 3, getstatic is 3.
    assert_eq!(instruction_size(&[BIPUSH, 0x05], 0), Some(2));
    assert_eq!(instruction_size(&[SIPUSH, 0x00, 0x05], 0), Some(3));
    assert_eq!(instruction_size(&[GETSTATIC, 0x00, 0x01], 0), Some(3));
    assert_eq!(instruction_size(&[INVOKEINTERFACE, 0, 1, 2, 0], 0), Some(5));
    assert_eq!(instruction_size(&[NEW, 0, 1], 0), Some(3));
}

#[test]
fn instruction_size_tableswitch_padding() {
    // tableswitch at pc=0: pad to 4-byte boundary from pc+1, so 3 pad bytes.
    // default(4) + low(4) + high(4) + 1 entry (low=0 high=0, so high-low+1=1)
    // total = 1 (opcode) + 3 (pad) + 12 + 4 = 20
    let mut code = vec![TABLESWITCH];
    code.extend_from_slice(&[0, 0, 0]); // padding
    code.extend_from_slice(&[0, 0, 0, 0]); // default offset
    code.extend_from_slice(&[0, 0, 0, 0]); // low = 0
    code.extend_from_slice(&[0, 0, 0, 0]); // high = 0
    code.extend_from_slice(&[0, 0, 0, 0]); // 1 jump entry
    assert_eq!(instruction_size(&code, 0), Some(20));
}

#[test]
fn instruction_size_lookupswitch() {
    // pc=0: pad 3, default(4), npairs(4)=2, 2 pairs (8 bytes each) = 16
    // total = 1 + 3 + 8 + 16 = 28
    let mut code = vec![LOOKUPSWITCH];
    code.extend_from_slice(&[0, 0, 0]); // padding
    code.extend_from_slice(&[0, 0, 0, 0]); // default
    code.extend_from_slice(&[0, 0, 0, 2]); // npairs = 2
    code.extend_from_slice(&[0; 16]); // 2 pairs
    assert_eq!(instruction_size(&code, 0), Some(28));
}

#[test]
fn instruction_size_tableswitch_overflow_does_not_panic() {
    // Adversarial low/high spanning the full i32 range overflows `high - low + 1`
    // in i32; the widened i64 count then saturates the byte products. Must
    // return a value (possibly None on a 32-bit usize) without panicking.
    for (low, high) in [
        (i32::MIN, 0i32),
        (0i32, i32::MAX),
        (i32::MIN, i32::MAX),
        (-1i32, i32::MAX),
    ] {
        let mut code = vec![TABLESWITCH];
        code.extend_from_slice(&[0, 0, 0]); // padding
        code.extend_from_slice(&[0, 0, 0, 0]); // default offset
        code.extend_from_slice(&low.to_be_bytes());
        code.extend_from_slice(&high.to_be_bytes());
        // No need to supply the (enormous) jump table; size computation
        // must not read it.
        let _ = instruction_size(&code, 0);
    }
}

#[test]
fn instruction_size_lookupswitch_overflow_does_not_panic() {
    // Maximal npairs; `npairs * 8` must saturate rather than overflow.
    let mut code = vec![LOOKUPSWITCH];
    code.extend_from_slice(&[0, 0, 0]); // padding
    code.extend_from_slice(&[0, 0, 0, 0]); // default
    code.extend_from_slice(&i32::MAX.to_be_bytes()); // npairs = i32::MAX
    let _ = instruction_size(&code, 0);
}

#[test]
fn instruction_size_wide() {
    // wide iload: 4 bytes. wide iinc: 6 bytes.
    assert_eq!(instruction_size(&[WIDE, ILOAD, 0, 1], 0), Some(4));
    assert_eq!(instruction_size(&[WIDE, IINC, 0, 1, 0, 5], 0), Some(6));
}

#[test]
fn instructions_iter_walks_simple_code() {
    // ldc #1; aastore; return
    let code = vec![LDC, 0x01, AASTORE, 0xB1];
    let attr = CodeAttribute {
        max_stack: 1,
        max_locals: 0,
        code: &code,
    };
    let names: Vec<_> = attr.instructions().map(|i| i.name()).collect();
    assert_eq!(names, vec!["ldc", "aastore", "return"]);
}

#[test]
fn instructions_iter_stops_on_truncated() {
    // ldc claims 2 bytes but only 1 byte present after — iterator stops.
    let code = vec![LDC];
    let attr = CodeAttribute {
        max_stack: 1,
        max_locals: 0,
        code: &code,
    };
    let count = attr.instructions().count();
    assert_eq!(count, 0);
}

/// A `tableswitch` whose declared jump table spans the full i32 index range
/// has a size far past the buffer: the walk stops, yielding nothing, never
/// panicking. (The checked `pc + size` in `next()` is defence in depth: no
/// target reaches a wrapping size, so this test cannot pin that add.)
#[test]
fn instructions_iter_rejects_oversized_switch_without_panicking() {
    let mut code = vec![TABLESWITCH, 0, 0, 0]; // opcode + 3 pad bytes
    code.extend_from_slice(&[0, 0, 0, 0]); // default offset
    code.extend_from_slice(&i32::MIN.to_be_bytes()); // low
    code.extend_from_slice(&i32::MAX.to_be_bytes()); // high (2^32 entries)
    let attr = CodeAttribute {
        max_stack: 0,
        max_locals: 0,
        code: &code,
    };
    // The size overruns the buffer, so the switch instruction is never
    // yielded and iteration ends immediately — without panicking.
    assert!(attr.instructions().next().is_none());
    assert_eq!(attr.instructions().count(), 0);
}

#[test]
fn cp_index_extraction() {
    let i = Instruction {
        pc: 0,
        opcode: LDC,
        operands: &[0x42],
    };
    assert_eq!(i.cp_index(), Some(0x42));

    let i = Instruction {
        pc: 0,
        opcode: LDC_W,
        operands: &[0x01, 0x23],
    };
    assert_eq!(i.cp_index(), Some(0x0123));

    let i = Instruction {
        pc: 0,
        opcode: NEW,
        operands: &[0x00, 0x10],
    };
    assert_eq!(i.cp_index(), Some(0x0010));

    let i = Instruction {
        pc: 0,
        opcode: AASTORE,
        operands: &[],
    };
    assert_eq!(i.cp_index(), None);
}

// ── Robustness smoke tests ── ClassFile::parse must NEVER panic on
// adversarial input, only return Err. Lightweight alternative to a full
// cargo-fuzz target; stays useful as deterministic regression cases.

/// Tiny pseudo-random byte generator — deterministic + reproducible
/// without needing a `rand` dep. xorshift64*; good enough for
/// generating adversarial byte payloads.
fn xorshift(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x
}

#[test]
fn parse_rejects_empty_input() {
    assert!(ClassFile::parse(&[]).is_err());
}

#[test]
fn parse_rejects_short_magic() {
    for n in 0..4 {
        let buf = vec![0u8; n];
        assert!(ClassFile::parse(&buf).is_err());
    }
}

#[test]
fn parse_rejects_wrong_magic() {
    let buf = vec![0xDE, 0xAD, 0xBE, 0xEF, 0, 0, 0, 0];
    match ClassFile::parse(&buf) {
        Err(Error::BadMagic(0xDEADBEEF)) => {}
        Err(other) => panic!("expected BadMagic, got {:?}", other),
        Ok(_) => panic!("expected BadMagic error, got Ok"),
    }
}

#[test]
fn parse_rejects_truncated_after_magic() {
    // CAFEBABE + 1 byte = not enough for minor_version (u16).
    let buf = vec![0xCA, 0xFE, 0xBA, 0xBE, 0x00];
    assert!(ClassFile::parse(&buf).is_err());
}

#[test]
fn parse_rejects_bad_cp_tag() {
    // CAFEBABE + minor/major(0,0,0,52) + cp_count=2 + tag=99 (unknown).
    let buf = vec![
        0xCA, 0xFE, 0xBA, 0xBE, // magic
        0x00, 0x00, // minor
        0x00, 0x34, // major
        0x00, 0x02, // cp_count = 2 (one entry)
        99,   // unknown tag
    ];
    match ClassFile::parse(&buf) {
        Err(_) => {} // BadCpTag, BadMagic, or any other malformed-input err
        Ok(_) => panic!("expected error on unknown CP tag"),
    }
}

#[test]
fn parse_rejects_truncated_utf8() {
    // CAFEBABE + minor/major + cp_count=2 + tag=1 (Utf8) + length=10 + 3 bytes (< 10).
    let buf = vec![
        0xCA, 0xFE, 0xBA, 0xBE, 0x00, 0x00, 0x00, 0x34, 0x00, 0x02, // cp_count=2
        1,    // Utf8 tag
        0x00, 10, // length=10
        b'h', b'i', b'!', // only 3 bytes (truncated)
    ];
    assert!(ClassFile::parse(&buf).is_err());
}

#[test]
fn parse_does_not_panic_on_random_bytes() {
    // 200 deterministic pseudo-random buffers of varying lengths.
    // Contract: never panic, only return Err (or Ok in the vanishingly
    // unlikely coincidentally-valid case — we don't assert which).
    let mut state: u64 = 0xDEADBEEF_DEADBEEF;
    for _ in 0..200 {
        let len = (xorshift(&mut state) % 256) as usize;
        let mut buf = Vec::with_capacity(len);
        for _ in 0..len {
            buf.push((xorshift(&mut state) & 0xFF) as u8);
        }
        // No panic. Result doesn't matter — Err is expected for
        // 99%+ of inputs.
        let _ = ClassFile::parse(&buf);
    }
}

#[test]
fn parse_does_not_panic_on_valid_magic_random_tail() {
    // 100 buffers with valid magic + plausible minor/major but garbage
    // afterwards — the most adversarial, passing the magic check then
    // exercising every other parser path.
    let mut state: u64 = 0xCAFEBABE_DEADBEEF;
    for _ in 0..100 {
        let mut buf = vec![0xCA, 0xFE, 0xBA, 0xBE, 0x00, 0x00, 0x00, 0x34];
        let tail_len = (xorshift(&mut state) % 512) as usize;
        for _ in 0..tail_len {
            buf.push((xorshift(&mut state) & 0xFF) as u8);
        }
        let _ = ClassFile::parse(&buf);
    }
}

#[test]
fn instructions_never_panic_on_random_code() {
    // Bytecode iterator must not panic on any byte sequence.
    let mut state: u64 = 0x12345678_87654321;
    for _ in 0..200 {
        let len = (xorshift(&mut state) % 256) as usize;
        let mut code = Vec::with_capacity(len);
        for _ in 0..len {
            code.push((xorshift(&mut state) & 0xFF) as u8);
        }
        let attr = CodeAttribute {
            max_stack: 0,
            max_locals: 0,
            code: &code,
        };
        // Bounded — iterator stops on truncated/unknown opcodes.
        let _: Vec<_> = attr.instructions().collect();
    }
}

#[test]
fn instruction_size_never_panics() {
    // Cover every opcode byte 0..=255 with various code-buffer
    // shapes. instruction_size returns Option but must not panic.
    for op in 0u8..=255 {
        for tail_len in [0usize, 1, 2, 3, 7, 16, 32] {
            let mut buf = vec![op];
            for i in 0..tail_len {
                buf.push((i as u8).wrapping_mul(31));
            }
            let _ = instruction_size(&buf, 0);
        }
    }
}

#[test]
fn modified_utf8_never_panics_on_random_bytes() {
    let mut state: u64 = 0xABCDEF12_34567890;
    for _ in 0..500 {
        let len = (xorshift(&mut state) % 64) as usize;
        let mut buf = Vec::with_capacity(len);
        for _ in 0..len {
            buf.push((xorshift(&mut state) & 0xFF) as u8);
        }
        // Either Ok or Err; never a panic.
        let _ = decode_modified_utf8(&buf);
    }
}

// ConstantPool / ClassFile accessor correctness: exercise data accessors
// on an already-parsed pool (test-only `from_entries` ctor), checking each
// variant maps to the right Option value.

fn sample_pool() -> ConstantPool {
    // index: 0=Empty (reserved), 1=Utf8("Hello"), 2=Integer(42),
    // 3=String{string_index:1}, 4=Class{name_index:1}, 5=Float(1.5),
    // 6=Long(9), 7=Empty (2-slot tail), 8=Double(2.5), 9=Empty (tail).
    ConstantPool::from_entries(vec![
        CpInfo::Empty,
        CpInfo::Utf8("Hello".to_string()),
        CpInfo::Integer(42),
        CpInfo::String { string_index: 1 },
        CpInfo::Class { name_index: 1 },
        CpInfo::Float(1.5),
        CpInfo::Long(9),
        CpInfo::Empty,
        CpInfo::Double(2.5),
        CpInfo::Empty,
    ])
}

#[test]
fn constant_pool_string_resolves_through_string_index() {
    let pool = sample_pool();
    // index 3 is CpInfo::String{string_index: 1} -> utf8(1) = "Hello".
    assert_eq!(pool.string(3), Some("Hello"));
    // Wrong variant (Integer at index 2) must not resolve as a string.
    assert_eq!(pool.string(2), None);
    // Out of range index.
    assert_eq!(pool.string(999), None);
}

#[test]
fn constant_pool_integer_resolves_only_integer_entries() {
    let pool = sample_pool();
    assert_eq!(pool.integer(2), Some(42));
    // Wrong variant (Utf8 at index 1) must not resolve as an integer.
    assert_eq!(pool.integer(1), None);
    assert_eq!(pool.integer(999), None);
}

#[test]
fn constant_pool_load_constant_display_covers_ldc_operand_kinds() {
    let pool = sample_pool();
    assert_eq!(
        pool.load_constant_display(1),
        Some("utf8:\"Hello\"".to_string())
    );
    assert_eq!(pool.load_constant_display(2), Some("int:42".to_string()));
    assert_eq!(
        pool.load_constant_display(3),
        Some("str:\"Hello\"".to_string())
    );
    assert_eq!(
        pool.load_constant_display(4),
        Some("class:\"Hello\"".to_string())
    );
    assert_eq!(pool.load_constant_display(5), Some("float:1.5".to_string()));
    assert_eq!(pool.load_constant_display(6), Some("long:9".to_string()));
    assert_eq!(
        pool.load_constant_display(8),
        Some("double:2.5".to_string())
    );
    // A variant with no display arm (e.g. reserved Empty slot) -> None.
    assert_eq!(pool.load_constant_display(0), None);
    assert_eq!(pool.load_constant_display(999), None);
}

#[test]
fn constant_pool_len_and_is_empty() {
    let pool = sample_pool();
    assert_eq!(pool.len(), 10);
    assert!(!pool.is_empty());

    let empty = ConstantPool::from_entries(vec![]);
    assert_eq!(empty.len(), 0);
    assert!(empty.is_empty());
}

#[test]
fn constant_pool_iter_yields_index_and_entry_pairs() {
    let pool = ConstantPool::from_entries(vec![
        CpInfo::Empty,
        CpInfo::Utf8("A".to_string()),
        CpInfo::Integer(7),
    ]);
    let indices: Vec<u16> = pool.iter().map(|(i, _)| i).collect();
    assert_eq!(indices, vec![0, 1, 2]);
    // Confirm the entries themselves come through, not an empty iterator.
    let utf8_at_1 = pool.iter().find(|(i, _)| *i == 1).map(|(_, e)| match e {
        CpInfo::Utf8(s) => s.as_str(),
        _ => "?",
    });
    assert_eq!(utf8_at_1, Some("A"));
}

fn class_file_with(this_class: u16, super_class: u16, pool: ConstantPool) -> ClassFile {
    ClassFile {
        minor_version: 0,
        major_version: 0,
        constant_pool: pool,
        access_flags: 0,
        this_class,
        super_class,
        interfaces: Vec::new(),
        fields: Vec::new(),
        methods: Vec::new(),
        attributes: Vec::new(),
    }
}

#[test]
fn this_class_name_and_super_class_name_resolve_distinct_indices() {
    let pool = ConstantPool::from_entries(vec![
        CpInfo::Empty,
        CpInfo::Utf8("com/example/Foo".to_string()),
        CpInfo::Utf8("com/example/Bar".to_string()),
        CpInfo::Class { name_index: 1 },
        CpInfo::Class { name_index: 2 },
    ]);
    let cf = class_file_with(3, 4, pool);
    assert_eq!(cf.this_class_name(), Some("com/example/Foo"));
    assert_eq!(cf.super_class_name(), Some("com/example/Bar"));

    // this_class index pointing at a non-Class entry must not resolve.
    let pool2 = ConstantPool::from_entries(vec![
        CpInfo::Empty,
        CpInfo::Utf8("not a class ref".to_string()),
    ]);
    let cf2 = class_file_with(1, 1, pool2);
    assert_eq!(cf2.this_class_name(), None);
    assert_eq!(cf2.super_class_name(), None);
}

#[test]
fn member_descriptor_resolves_the_descriptor_not_the_name() {
    let pool = ConstantPool::from_entries(vec![
        CpInfo::Empty,
        CpInfo::Utf8("doStuff".to_string()), // index 1: name
        CpInfo::Utf8("()V".to_string()),     // index 2: descriptor
    ]);
    let cf = class_file_with(0, 0, pool);
    let m = Member {
        access_flags: 0,
        name_index: 1,
        descriptor_index: 2,
        attributes: Vec::new(),
    };
    assert_eq!(cf.member_descriptor(&m), Some("()V"));
    assert_ne!(cf.member_descriptor(&m), Some("doStuff"));
}

// Reader::u16/u32/u64 boundary + value correctness: an exact-fit read
// succeeds, one byte short fails, plus positive-value tests so a
// scrambled byte assembly (not just OOB) is caught.

#[test]
fn u16_boundary_is_inclusive_of_the_final_byte() {
    let data = [0xAB, 0xCD];
    let mut r = Reader::new(&data);
    assert_eq!(r.u16("exact fit").expect("2 bytes available"), 0xABCD);

    let data = [0xAB];
    let mut r = Reader::new(&data);
    assert!(matches!(
        r.u16("one byte short"),
        Err(Error::UnexpectedEof { .. })
    ));
}

#[test]
fn u16_decodes_big_endian_value() {
    let data = [0x01, 0x02];
    let mut r = Reader::new(&data);
    assert_eq!(r.u16("value").unwrap(), 0x0102);
}

#[test]
fn u32_boundary_is_inclusive_of_the_final_byte() {
    let data = [0x00, 0x00, 0x00, 0x2A];
    let mut r = Reader::new(&data);
    assert_eq!(r.u32("exact fit").expect("4 bytes available"), 42);

    let data = [0x00, 0x00, 0x00];
    let mut r = Reader::new(&data);
    assert!(matches!(
        r.u32("one byte short"),
        Err(Error::UnexpectedEof { .. })
    ));
}

#[test]
fn u32_decodes_big_endian_value() {
    let data = [0x00, 0x00, 0x05, 0x39]; // 1337
    let mut r = Reader::new(&data);
    assert_eq!(r.u32("value").unwrap(), 1337);
}

#[test]
fn u64_boundary_is_inclusive_of_the_final_byte() {
    // pos == 0, buffer exactly 8 bytes: must succeed.
    let data = [0, 0, 0, 0, 0, 0, 0, 0x7B]; // 123
    let mut r = Reader::new(&data);
    assert_eq!(r.u64("exact fit").expect("8 bytes available"), 123);

    // pos == 0, buffer one byte short of 8: must fail cleanly, not
    // panic on the internal self.data[self.pos + 7] index.
    let data = [0u8; 7];
    let mut r = Reader::new(&data);
    assert!(matches!(
        r.u64("one byte short"),
        Err(Error::UnexpectedEof { .. })
    ));
}

#[test]
fn u64_decodes_big_endian_value() {
    let data = [0, 0, 0, 0, 0, 0, 0x05, 0x39]; // 1337
    let mut r = Reader::new(&data);
    assert_eq!(r.u64("value").unwrap(), 1337);
}

// -----------------------------------------------------------------
// decode_modified_utf8: 3-byte (BMP) decode path
// -----------------------------------------------------------------

#[test]
fn modified_utf8_three_byte_cjk() {
    // U+3042 (hiragana あ) in modified UTF-8: 1110xxxx 10xxxxxx 10xxxxxx
    // = 0xE3 0x81 0x82.
    let s = decode_modified_utf8(&[0xE3, 0x81, 0x82]).unwrap();
    assert_eq!(s, "\u{3042}");
}

#[test]
fn modified_utf8_three_byte_rejects_bad_first_continuation() {
    // Valid lead byte (0xE3), but the first continuation byte is not
    // 10xxxxxx (0x01 instead) — must be rejected, proving the first
    // `& 0xC0 != 0x80` check is live.
    assert!(decode_modified_utf8(&[0xE3, 0x01, 0x82]).is_err());
}

#[test]
fn modified_utf8_three_byte_rejects_bad_second_continuation() {
    // Valid lead + first continuation, but the second continuation
    // byte is not 10xxxxxx — proves the second check is independently
    // live (not short-circuited by the first).
    assert!(decode_modified_utf8(&[0xE3, 0x81, 0x01]).is_err());
}

// -----------------------------------------------------------------
// read_constant_pool: Long/Double two-slot skip, real byte parsing
// -----------------------------------------------------------------

#[test]
fn constant_pool_long_entry_occupies_two_slots_via_real_parse() {
    // Real class-file bytes (not the from_entries ctor): magic, minor/major,
    // cp_count=4, tag=5 (Long, payload at index 1, reserved slot 2),
    // tag=1 (Utf8 at index 3), then empty header tail sections.
    let mut buf = vec![
        0xCA, 0xFE, 0xBA, 0xBE, // magic
        0x00, 0x00, // minor
        0x00, 0x34, // major
        0x00, 0x04, // cp_count = 4 (0=Empty,1=Long,2=Empty tail,3=Utf8)
        5,    // Long tag
    ];
    buf.extend_from_slice(&0x1122_3344_5566_7788u64.to_be_bytes()); // 8-byte payload
    buf.push(1); // Utf8 tag
    let name = b"marker";
    buf.extend_from_slice(&(name.len() as u16).to_be_bytes());
    buf.extend_from_slice(name);
    // access_flags, this_class, super_class, interfaces_count
    buf.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 0]);
    // fields_count, methods_count, attributes_count
    buf.extend_from_slice(&[0, 0, 0, 0, 0, 0]);

    let cf = ClassFile::parse(&buf).expect("well-formed synthetic class file");
    assert_eq!(cf.constant_pool.len(), 4);
    // The Long occupies indices 1 AND 2 (its reserved tail slot).
    // The Utf8 must resolve at index 3 = long_index(1) + 2, NOT +1.
    assert_eq!(cf.constant_pool.utf8(3), Some("marker"));
    // Index 2 is the reserved tail slot: not a Utf8, must not
    // resolve as one (guards against the Utf8 landing one slot early).
    assert_eq!(cf.constant_pool.utf8(2), None);
    match cf.constant_pool.get(1) {
        Some(CpInfo::Long(v)) => assert_eq!(*v, 0x1122_3344_5566_7788u64 as i64),
        other => panic!("expected Long at index 1, got {:?}", other),
    }
}

#[test]
fn constant_pool_double_entry_occupies_two_slots_via_real_parse() {
    // Same shape as the Long test with tag 6: the Utf8 must land at index 3.
    let mut buf = vec![
        0xCA, 0xFE, 0xBA, 0xBE, // magic
        0x00, 0x00, 0x00, 0x34, // minor / major
        0x00, 0x04, // cp_count = 4 (0=Empty,1=Double,2=Empty tail,3=Utf8)
        6,    // Double tag
    ];
    buf.extend_from_slice(&2.5f64.to_bits().to_be_bytes());
    buf.push(1); // Utf8 tag
    buf.extend_from_slice(&6u16.to_be_bytes());
    buf.extend_from_slice(b"marker");
    buf.extend_from_slice(&[0; 8]); // access, this, super, interfaces_count
    buf.extend_from_slice(&[0; 6]); // fields, methods, attributes counts

    let cf = ClassFile::parse(&buf).expect("well-formed synthetic class file");
    assert_eq!(cf.constant_pool.len(), 4);
    assert_eq!(cf.constant_pool.utf8(3), Some("marker"));
    assert_eq!(cf.constant_pool.utf8(2), None);
    assert!(matches!(cf.constant_pool.get(1), Some(CpInfo::Double(v)) if *v == 2.5));
}

#[test]
fn instruction_size_switch_padding_depends_on_pc() {
    // Padding is (pc+1+3)&!3 - pc - 1: 3,2,1,0 bytes for pc%4 = 0,1,2,3.
    // pc=0 alone cannot tell `& !3` or `pc + 4` from the real alignment.
    for pc in 0usize..8 {
        let start = (pc + 1 + 3) & !3;
        let mut code = vec![0u8; pc];
        code.push(TABLESWITCH);
        code.resize(start, 0);
        code.extend_from_slice(&[0; 4]); // default
        code.extend_from_slice(&0i32.to_be_bytes()); // low
        code.extend_from_slice(&1i32.to_be_bytes()); // high: 2 entries
        code.extend_from_slice(&[0; 8]);
        assert_eq!(
            instruction_size(&code, pc),
            Some(start - pc + 12 + 8),
            "tableswitch pc={pc}"
        );

        let mut code = vec![0u8; pc];
        code.push(LOOKUPSWITCH);
        code.resize(start, 0);
        code.extend_from_slice(&[0; 4]); // default
        code.extend_from_slice(&1i32.to_be_bytes()); // npairs
        code.extend_from_slice(&[0; 8]);
        assert_eq!(
            instruction_size(&code, pc),
            Some(start - pc + 8 + 8),
            "lookupswitch pc={pc}"
        );
    }
}

// instruction_size: tableswitch/lookupswitch with non-degenerate
// low/high/npairs — existing tests only cover the all-zero case, which
// can't distinguish `-` from `+` in the entry-count arithmetic.

#[test]
fn instruction_size_tableswitch_non_degenerate_range() {
    // low=1, high=4 -> 4 entries (high-low+1 = 4). A `-`->`+` mutation
    // on that arithmetic would instead compute high+low+1 = 6.
    let mut code = vec![TABLESWITCH];
    code.extend_from_slice(&[0, 0, 0]); // padding
    code.extend_from_slice(&[0, 0, 0, 0]); // default offset
    code.extend_from_slice(&1i32.to_be_bytes()); // low = 1
    code.extend_from_slice(&4i32.to_be_bytes()); // high = 4
    code.extend_from_slice(&[0; 16]); // 4 jump entries * 4 bytes
    // total = 1 (opcode) + 3 (pad) + 12 (default/low/high) + 16 (entries) = 32
    assert_eq!(instruction_size(&code, 0), Some(32));
}

#[test]
fn instruction_size_lookupswitch_non_degenerate_npairs() {
    // npairs = 3 -> 3 * 8 = 24 bytes of pairs.
    let mut code = vec![LOOKUPSWITCH];
    code.extend_from_slice(&[0, 0, 0]); // padding
    code.extend_from_slice(&[0, 0, 0, 0]); // default
    code.extend_from_slice(&3i32.to_be_bytes()); // npairs = 3
    code.extend_from_slice(&[0; 24]); // 3 pairs
    // total = 1 + 3 + 8 (default/npairs) + 24 = 36
    assert_eq!(instruction_size(&code, 0), Some(36));
}

// ── Malformed-input hardening: every count/length/index is attacker- ──────
// controlled, so an oversized or lying field must yield Err/None, never a
// panic or an unbounded allocation.

/// A `constant_pool_count` far larger than the data behind it must fail
/// cleanly (running out of bytes), not pre-allocate gigabytes or panic. The
/// count is a u16, so `Vec::with_capacity` is bounded at 65535 regardless.
#[test]
fn parse_rejects_oversized_constant_pool_count() {
    let bytes = [
        0xCA, 0xFE, 0xBA, 0xBE, // magic
        0x00, 0x00, 0x00, 0x00, // minor / major
        0xFF, 0xFF, // constant_pool_count = 65535, but no entries follow
    ];
    assert!(matches!(
        ClassFile::parse(&bytes),
        Err(Error::UnexpectedEof { .. })
    ));
}

/// An oversized `interfaces_count` / `fields_count` with no data behind it
/// must fail on the first missing element, not panic.
#[test]
fn parse_rejects_oversized_interface_count() {
    let bytes = [
        0xCA, 0xFE, 0xBA, 0xBE, // magic
        0x00, 0x00, 0x00, 0x00, // minor / major
        0x00, 0x01, // constant_pool_count = 1 (no entries)
        0x00, 0x00, // access_flags
        0x00, 0x00, // this_class
        0x00, 0x00, // super_class
        0xFF, 0xFF, // interfaces_count = 65535, none follow
    ];
    assert!(matches!(
        ClassFile::parse(&bytes),
        Err(Error::UnexpectedEof { .. })
    ));
}

/// Out-of-range constant-pool indices resolve to `None` from every
/// accessor a label parser drives — no slice panic.
#[test]
fn constant_pool_out_of_range_index_is_none_everywhere() {
    let pool = sample_pool();
    let oob = 9999u16;
    assert!(pool.get(oob).is_none());
    assert!(pool.utf8(oob).is_none());
    assert!(pool.class_name(oob).is_none());
    assert!(pool.string(oob).is_none());
    assert!(pool.integer(oob).is_none());
    assert!(pool.member_ref(oob).is_none());
    assert!(pool.load_constant_display(oob).is_none());
}

/// A `Code` attribute whose declared `code_length` runs past the attribute
/// body must be rejected, not read out of bounds.
#[test]
fn code_attribute_rejects_length_past_its_body() {
    // max_stack(2) + max_locals(2) + code_length(4) then a code_length that
    // exceeds the remaining bytes.
    let mut info = Vec::new();
    info.extend_from_slice(&0u16.to_be_bytes()); // max_stack
    info.extend_from_slice(&0u16.to_be_bytes()); // max_locals
    info.extend_from_slice(&0xFFFF_FFFFu32.to_be_bytes()); // code_length: absurd
    info.extend_from_slice(&[0x00, 0x01]); // only 2 code bytes present
    assert!(matches!(
        parse_code_attribute(&info),
        Err(Error::UnexpectedEof { .. })
    ));
}
