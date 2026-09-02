//! Hand-rolled JVM `.class` file reader, tailored to the subset we need
//! for BD-J label extraction (Deluxe / dbp / similar frameworks).
//!
//! Spec: JVMS §4 (class file format) and §6 (bytecode). Exposes: constant
//! pool, methods, the `Code` attribute, and a non-allocating bytecode
//! iterator.
//!
//! No external deps beyond `std`. No `unsafe`. No panics on malformed
//! input — every parse fault is a typed [`Error`]. See docs/class-reader.md.

// Foundation module — public API is staged for `labels::deluxe` (bytecode
// walker) and `labels::dbp`'s refactor onto the constant-pool iterator.
// The dead-code allow comes off once those callers land.
#![allow(dead_code)]

const CLASS_MAGIC: u32 = 0xCAFEBABE;

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum Error {
    UnexpectedEof { needed: &'static str },
    BadMagic(u32),
    BadCpTag { index: u16, tag: u8 },
    BadUtf8 { index: u16 },
    BadCodeAttribute,
    BadInstruction { pc: usize, opcode: u8 },
}

// No Display/std::error::Error impl: crate-internal typed error for `match`/`?`
// within label parsers. Per the zero-English rule there is no user-facing
// text; variant fields carry the structured detail.

pub type Result<T> = std::result::Result<T, Error>;

// ---------------------------------------------------------------------------
// Constant pool
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub enum CpInfo {
    /// Index 0 is unused per spec; the slot after Long/Double is also unused.
    Empty,
    Utf8(String),
    Integer(i32),
    Float(f32),
    Long(i64),
    Double(f64),
    Class {
        name_index: u16,
    },
    String {
        string_index: u16,
    },
    Fieldref {
        class_index: u16,
        name_and_type_index: u16,
    },
    Methodref {
        class_index: u16,
        name_and_type_index: u16,
    },
    InterfaceMethodref {
        class_index: u16,
        name_and_type_index: u16,
    },
    NameAndType {
        name_index: u16,
        descriptor_index: u16,
    },
    MethodHandle {
        reference_kind: u8,
        reference_index: u16,
    },
    MethodType {
        descriptor_index: u16,
    },
    Dynamic {
        bootstrap_method_attr_index: u16,
        name_and_type_index: u16,
    },
    InvokeDynamic {
        bootstrap_method_attr_index: u16,
        name_and_type_index: u16,
    },
    Module {
        name_index: u16,
    },
    Package {
        name_index: u16,
    },
}

pub struct ConstantPool {
    entries: Vec<CpInfo>,
}

impl ConstantPool {
    // Test-only: build a pool directly from entries (real code goes through
    // ClassFile::parse). Caller must prepend CpInfo::Empty at index 0 and
    // after each Long/Double entry (2-slot quirk). See docs/class-reader.md.
    #[cfg(test)]
    pub(crate) fn from_entries(entries: Vec<CpInfo>) -> Self {
        ConstantPool { entries }
    }

    #[inline]
    pub fn get(&self, index: u16) -> Option<&CpInfo> {
        self.entries.get(index as usize)
    }

    /// Resolve `index` to its UTF-8 string content. Returns None unless
    /// the entry is `CpInfo::Utf8`.
    pub fn utf8(&self, index: u16) -> Option<&str> {
        match self.get(index)? {
            CpInfo::Utf8(s) => Some(s.as_str()),
            _ => None,
        }
    }

    /// Resolve a `CONSTANT_Class` entry to the class's binary name.
    pub fn class_name(&self, index: u16) -> Option<&str> {
        match self.get(index)? {
            CpInfo::Class { name_index } => self.utf8(*name_index),
            _ => None,
        }
    }

    /// Resolve a `CONSTANT_String` entry to its underlying UTF-8.
    pub fn string(&self, index: u16) -> Option<&str> {
        match self.get(index)? {
            CpInfo::String { string_index } => self.utf8(*string_index),
            _ => None,
        }
    }

    pub fn integer(&self, index: u16) -> Option<i32> {
        match self.get(index)? {
            CpInfo::Integer(v) => Some(*v),
            _ => None,
        }
    }

    /// For `ldc` / `ldc_w` operands: resolve a constant-pool index to a
    /// best-effort string. Supports Utf8, String, Integer, Float, Long,
    /// Double, and Class.
    pub fn load_constant_display(&self, index: u16) -> Option<String> {
        Some(match self.get(index)? {
            CpInfo::Utf8(s) => format!("utf8:{:?}", s),
            CpInfo::String { string_index } => {
                format!("str:{:?}", self.utf8(*string_index).unwrap_or("<?>"))
            }
            CpInfo::Integer(i) => format!("int:{}", i),
            CpInfo::Float(v) => format!("float:{}", v),
            CpInfo::Long(v) => format!("long:{}", v),
            CpInfo::Double(v) => format!("double:{}", v),
            CpInfo::Class { name_index } => {
                format!("class:{:?}", self.utf8(*name_index).unwrap_or("<?>"))
            }
            _ => return None,
        })
    }

    /// Resolve a `CONSTANT_Fieldref` / `Methodref` / `InterfaceMethodref`
    /// to (owning_class_name, member_name, descriptor).
    pub fn member_ref(&self, index: u16) -> Option<MemberRef<'_>> {
        let (class_index, nt_index) = match self.get(index)? {
            CpInfo::Fieldref {
                class_index,
                name_and_type_index,
            }
            | CpInfo::Methodref {
                class_index,
                name_and_type_index,
            }
            | CpInfo::InterfaceMethodref {
                class_index,
                name_and_type_index,
            } => (*class_index, *name_and_type_index),
            _ => return None,
        };
        let class_name = self.class_name(class_index)?;
        let (name, descriptor) = match self.get(nt_index)? {
            CpInfo::NameAndType {
                name_index,
                descriptor_index,
            } => (self.utf8(*name_index)?, self.utf8(*descriptor_index)?),
            _ => return None,
        };
        Some(MemberRef {
            class_name,
            name,
            descriptor,
        })
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (u16, &CpInfo)> {
        self.entries.iter().enumerate().map(|(i, e)| (i as u16, e))
    }
}

#[derive(Debug, Clone, Copy)]
pub struct MemberRef<'a> {
    pub class_name: &'a str,
    pub name: &'a str,
    pub descriptor: &'a str,
}

// ---------------------------------------------------------------------------
// ClassFile + Member + Attribute
// ---------------------------------------------------------------------------

pub struct ClassFile {
    pub minor_version: u16,
    pub major_version: u16,
    pub constant_pool: ConstantPool,
    pub access_flags: u16,
    pub this_class: u16,
    pub super_class: u16,
    pub interfaces: Vec<u16>,
    pub fields: Vec<Member>,
    pub methods: Vec<Member>,
    pub attributes: Vec<Attribute>,
}

pub struct Member {
    pub access_flags: u16,
    pub name_index: u16,
    pub descriptor_index: u16,
    pub attributes: Vec<Attribute>,
}

pub struct Attribute {
    pub name_index: u16,
    pub info: Vec<u8>,
}

impl ClassFile {
    pub fn parse(bytes: &[u8]) -> Result<Self> {
        let mut r = Reader::new(bytes);
        let magic = r.u32("magic")?;
        if magic != CLASS_MAGIC {
            return Err(Error::BadMagic(magic));
        }
        let minor_version = r.u16("minor_version")?;
        let major_version = r.u16("major_version")?;
        let constant_pool = read_constant_pool(&mut r)?;
        let access_flags = r.u16("access_flags")?;
        let this_class = r.u16("this_class")?;
        let super_class = r.u16("super_class")?;
        let interfaces_count = r.u16("interfaces_count")? as usize;
        let mut interfaces = Vec::with_capacity(interfaces_count);
        for _ in 0..interfaces_count {
            interfaces.push(r.u16("interface")?);
        }
        let fields = read_members(&mut r)?;
        let methods = read_members(&mut r)?;
        let attributes = read_attributes(&mut r)?;
        Ok(ClassFile {
            minor_version,
            major_version,
            constant_pool,
            access_flags,
            this_class,
            super_class,
            interfaces,
            fields,
            methods,
            attributes,
        })
    }

    pub fn this_class_name(&self) -> Option<&str> {
        self.constant_pool.class_name(self.this_class)
    }

    pub fn super_class_name(&self) -> Option<&str> {
        self.constant_pool.class_name(self.super_class)
    }

    /// Convenience: name of a `Member` belonging to this class.
    pub fn member_name<'a>(&'a self, m: &Member) -> Option<&'a str> {
        self.constant_pool.utf8(m.name_index)
    }

    pub fn member_descriptor<'a>(&'a self, m: &Member) -> Option<&'a str> {
        self.constant_pool.utf8(m.descriptor_index)
    }
}

impl Member {
    /// Locate the `Code` attribute on this member (only methods have one).
    /// Returns the parsed [`CodeAttribute`] for direct bytecode iteration.
    pub fn code<'a>(&'a self, pool: &'a ConstantPool) -> Option<CodeAttribute<'a>> {
        for attr in &self.attributes {
            if pool.utf8(attr.name_index) == Some("Code") {
                return parse_code_attribute(&attr.info).ok();
            }
        }
        None
    }
}

pub struct CodeAttribute<'a> {
    pub max_stack: u16,
    pub max_locals: u16,
    pub code: &'a [u8],
}

impl<'a> CodeAttribute<'a> {
    /// Iterate instructions in this method's bytecode. The iterator
    /// stops at the first malformed instruction, which is the safe
    /// behavior for label extraction (we read straight-line `<clinit>`).
    pub fn instructions(&self) -> Instructions<'a> {
        Instructions {
            code: self.code,
            pos: 0,
        }
    }
}

fn parse_code_attribute(info: &[u8]) -> Result<CodeAttribute<'_>> {
    if info.len() < 8 {
        return Err(Error::BadCodeAttribute);
    }
    let mut r = Reader::new(info);
    let max_stack = r.u16("max_stack")?;
    let max_locals = r.u16("max_locals")?;
    let code_length = r.u32("code_length")? as usize;
    let code = r.slice(code_length, "code bytes")?;
    Ok(CodeAttribute {
        max_stack,
        max_locals,
        code,
    })
}

// ---------------------------------------------------------------------------
// Constant pool reader
// ---------------------------------------------------------------------------

fn read_constant_pool(r: &mut Reader<'_>) -> Result<ConstantPool> {
    let count = r.u16("constant_pool_count")? as usize;
    let mut entries: Vec<CpInfo> = Vec::with_capacity(count);
    entries.push(CpInfo::Empty); // index 0 unused per spec
    let mut i = 1usize;
    while i < count {
        let tag = r.u8("cp tag")?;
        let entry = match tag {
            1 => {
                // CONSTANT_Utf8
                let length = r.u16("utf8 length")? as usize;
                let bytes = r.slice(length, "utf8 bytes")?;
                let s =
                    decode_modified_utf8(bytes).map_err(|_| Error::BadUtf8 { index: i as u16 })?;
                CpInfo::Utf8(s)
            }
            3 => CpInfo::Integer(r.i32("integer")?),
            4 => CpInfo::Float(f32::from_bits(r.u32("float")?)),
            5 => CpInfo::Long(r.i64("long")?),
            6 => CpInfo::Double(f64::from_bits(r.u64("double")?)),
            7 => CpInfo::Class {
                name_index: r.u16("class name_index")?,
            },
            8 => CpInfo::String {
                string_index: r.u16("string_index")?,
            },
            9 => CpInfo::Fieldref {
                class_index: r.u16("fieldref class")?,
                name_and_type_index: r.u16("fieldref nat")?,
            },
            10 => CpInfo::Methodref {
                class_index: r.u16("methodref class")?,
                name_and_type_index: r.u16("methodref nat")?,
            },
            11 => CpInfo::InterfaceMethodref {
                class_index: r.u16("imethodref class")?,
                name_and_type_index: r.u16("imethodref nat")?,
            },
            12 => CpInfo::NameAndType {
                name_index: r.u16("nat name")?,
                descriptor_index: r.u16("nat descriptor")?,
            },
            15 => CpInfo::MethodHandle {
                reference_kind: r.u8("mh kind")?,
                reference_index: r.u16("mh index")?,
            },
            16 => CpInfo::MethodType {
                descriptor_index: r.u16("mt descriptor")?,
            },
            17 => CpInfo::Dynamic {
                bootstrap_method_attr_index: r.u16("dynamic bootstrap")?,
                name_and_type_index: r.u16("dynamic nat")?,
            },
            18 => CpInfo::InvokeDynamic {
                bootstrap_method_attr_index: r.u16("invokedynamic bootstrap")?,
                name_and_type_index: r.u16("invokedynamic nat")?,
            },
            19 => CpInfo::Module {
                name_index: r.u16("module name")?,
            },
            20 => CpInfo::Package {
                name_index: r.u16("package name")?,
            },
            other => {
                return Err(Error::BadCpTag {
                    index: i as u16,
                    tag: other,
                });
            }
        };
        let is_long_or_double = matches!(entry, CpInfo::Long(_) | CpInfo::Double(_));
        entries.push(entry);
        i += 1;
        if is_long_or_double {
            // JVMS §4.4.5: Long and Double occupy TWO slots; the slot
            // immediately following must be skipped.
            entries.push(CpInfo::Empty);
            i += 1;
        }
    }
    Ok(ConstantPool { entries })
}

// Decode JVM "modified UTF-8" (JVMS §4.4.7): like UTF-8 but U+0000 is 0xC0
// 0x80, and supplementary chars use a 3-byte-surrogate-pair encoding we
// don't bother handling — no label string needs it. See docs/class-reader.md.
fn decode_modified_utf8(bytes: &[u8]) -> std::result::Result<String, ()> {
    let mut out = String::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let b0 = bytes[i];
        if b0 == 0 {
            // Spec disallows raw 0x00 in modified UTF-8; reject.
            return Err(());
        }
        if b0 < 0x80 {
            out.push(b0 as char);
            i += 1;
        } else if (b0 & 0xE0) == 0xC0 {
            // 2-byte sequence
            if i + 1 >= bytes.len() {
                return Err(());
            }
            let b1 = bytes[i + 1];
            if (b1 & 0xC0) != 0x80 {
                return Err(());
            }
            let cp = (((b0 & 0x1F) as u32) << 6) | ((b1 & 0x3F) as u32);
            // Modified UTF-8 special: 0xC0 0x80 → U+0000.
            if let Some(c) = char::from_u32(cp) {
                out.push(c);
            } else {
                return Err(());
            }
            i += 2;
        } else if (b0 & 0xF0) == 0xE0 {
            // 3-byte sequence (BMP only in modified UTF-8)
            if i + 2 >= bytes.len() {
                return Err(());
            }
            let b1 = bytes[i + 1];
            let b2 = bytes[i + 2];
            if (b1 & 0xC0) != 0x80 || (b2 & 0xC0) != 0x80 {
                return Err(());
            }
            let cp =
                (((b0 & 0x0F) as u32) << 12) | (((b1 & 0x3F) as u32) << 6) | ((b2 & 0x3F) as u32);
            // Lone surrogates are valid in modified UTF-8 but invalid
            // chars in Rust. For label data we'd never see one; treat
            // as replacement char rather than error to stay robust.
            match char::from_u32(cp) {
                Some(c) => out.push(c),
                None => out.push('\u{FFFD}'),
            }
            i += 3;
        } else {
            // 4-byte or higher: not valid in modified UTF-8.
            return Err(());
        }
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Fields, methods, attributes
// ---------------------------------------------------------------------------

fn read_members(r: &mut Reader<'_>) -> Result<Vec<Member>> {
    let count = r.u16("members_count")? as usize;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        let access_flags = r.u16("member access")?;
        let name_index = r.u16("member name")?;
        let descriptor_index = r.u16("member descriptor")?;
        let attributes = read_attributes(r)?;
        out.push(Member {
            access_flags,
            name_index,
            descriptor_index,
            attributes,
        });
    }
    Ok(out)
}

fn read_attributes(r: &mut Reader<'_>) -> Result<Vec<Attribute>> {
    let count = r.u16("attributes_count")? as usize;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        let name_index = r.u16("attribute name")?;
        let length = r.u32("attribute length")? as usize;
        let info = r.slice(length, "attribute info")?.to_vec();
        out.push(Attribute { name_index, info });
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Bytecode iterator
// ---------------------------------------------------------------------------

pub struct Instructions<'a> {
    code: &'a [u8],
    pos: usize,
}

#[derive(Debug, Clone, Copy)]
pub struct Instruction<'a> {
    pub pc: usize,
    pub opcode: u8,
    pub operands: &'a [u8],
}

impl Instruction<'_> {
    /// Mnemonic for this opcode (e.g. "ldc", "invokespecial").
    pub fn name(&self) -> &'static str {
        opcode_name(self.opcode)
    }

    /// Operand as a single u8 (e.g. ldc cp index, bipush value).
    pub fn operand_u8(&self) -> Option<u8> {
        self.operands.first().copied()
    }

    /// Operand as a big-endian u16 (e.g. ldc_w/new/getstatic cp index,
    /// branch offset for if*/goto).
    pub fn operand_u16(&self) -> Option<u16> {
        if self.operands.len() >= 2 {
            Some(u16::from_be_bytes([self.operands[0], self.operands[1]]))
        } else {
            None
        }
    }

    /// For instructions whose operand is a constant-pool index — ldc,
    /// ldc_w, ldc2_w, new, getstatic, putstatic, getfield, putfield,
    /// invokevirtual, invokespecial, invokestatic, invokeinterface,
    /// invokedynamic, checkcast, instanceof, anewarray, multianewarray,
    /// ldc with cp index in `operands[0]` — return the index. Returns
    /// None for opcodes whose operand is not a CP index.
    pub fn cp_index(&self) -> Option<u16> {
        match self.opcode {
            // ldc: 1-byte cp index, zero-extended
            LDC => self.operand_u8().map(u16::from),
            LDC_W | LDC2_W | NEW | GETSTATIC | PUTSTATIC | GETFIELD | PUTFIELD | INVOKEVIRTUAL
            | INVOKESPECIAL | INVOKESTATIC | INVOKEINTERFACE | INVOKEDYNAMIC | CHECKCAST
            | INSTANCEOF | ANEWARRAY | MULTIANEWARRAY => self.operand_u16(),
            _ => None,
        }
    }
}

impl<'a> Iterator for Instructions<'a> {
    type Item = Instruction<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.code.len() {
            return None;
        }
        let pc = self.pos;
        let opcode = self.code[pc];
        let size = instruction_size(self.code, pc)?;
        if pc + size > self.code.len() {
            return None;
        }
        let operands = &self.code[pc + 1..pc + size];
        self.pos = pc + size;
        Some(Instruction {
            pc,
            opcode,
            operands,
        })
    }
}

/// Total size of the instruction at `pc` (opcode + operands). Returns
/// None on malformed input. Handles all JVMS §6 opcodes including
/// the variable-length `tableswitch`, `lookupswitch`, and `wide`.
fn instruction_size(code: &[u8], pc: usize) -> Option<usize> {
    let op = *code.get(pc)?;
    // Fixed-size opcodes use a precomputed table; the few variable-size
    // ones get special cases below.
    if let Some(sz) = FIXED_SIZE[op as usize] {
        return Some(sz as usize);
    }
    match op {
        TABLESWITCH => {
            // 1 opcode byte + 0..3 padding bytes (align to 4-byte boundary
            // from start of method) + 4 default + 4 low + 4 high + 4*(high-low+1)
            let padded_start = (pc + 1 + 3) & !3;
            if padded_start + 12 > code.len() {
                return None;
            }
            let default_offset_pos = padded_start;
            let low = i32::from_be_bytes(
                code[default_offset_pos + 4..default_offset_pos + 8]
                    .try_into()
                    .ok()?,
            );
            let high = i32::from_be_bytes(
                code[default_offset_pos + 8..default_offset_pos + 12]
                    .try_into()
                    .ok()?,
            );
            if high < low {
                return None;
            }
            // `high - low + 1` can overflow i32 for adversarial bytecode, so widen
            // to i64 before adding; the product/sum below saturate so they can't
            // overflow usize on a 32-bit target either.
            let entries = (high as i64 - low as i64 + 1) as u64;
            let table_bytes = entries.saturating_mul(4);
            let base = (padded_start - pc + 12) as u64;
            usize::try_from(base.saturating_add(table_bytes)).ok()
        }
        LOOKUPSWITCH => {
            let padded_start = (pc + 1 + 3) & !3;
            if padded_start + 8 > code.len() {
                return None;
            }
            let npairs =
                i32::from_be_bytes(code[padded_start + 4..padded_start + 8].try_into().ok()?);
            if npairs < 0 {
                return None;
            }
            // Saturating product/sum so an attacker-supplied npairs cannot
            // overflow usize on a 32-bit target.
            let pair_bytes = (npairs as u64).saturating_mul(8);
            let base = (padded_start - pc + 8) as u64;
            usize::try_from(base.saturating_add(pair_bytes)).ok()
        }
        WIDE => {
            // `wide` prefixes one of: iload/lload/fload/dload/aload/
            // istore/lstore/fstore/dstore/astore/ret  → 4 total bytes
            // or `iinc`                                → 6 total bytes
            let next = *code.get(pc + 1)?;
            match next {
                IINC => Some(6),
                ILOAD | LLOAD | FLOAD | DLOAD | ALOAD | ISTORE | LSTORE | FSTORE | DSTORE
                | ASTORE | RET => Some(4),
                _ => None,
            }
        }
        _ => None, // unknown opcode → halt iteration
    }
}

// Opcode table: named opcode constants for the ones we walk in the parser.
#[allow(dead_code)]
pub const NOP: u8 = 0x00;
pub const ACONST_NULL: u8 = 0x01;
pub const ICONST_M1: u8 = 0x02;
pub const ICONST_0: u8 = 0x03;
pub const ICONST_1: u8 = 0x04;
pub const ICONST_2: u8 = 0x05;
pub const ICONST_3: u8 = 0x06;
pub const ICONST_4: u8 = 0x07;
pub const ICONST_5: u8 = 0x08;
pub const BIPUSH: u8 = 0x10;
pub const SIPUSH: u8 = 0x11;
pub const LDC: u8 = 0x12;
pub const LDC_W: u8 = 0x13;
pub const LDC2_W: u8 = 0x14;
pub const ILOAD: u8 = 0x15;
pub const LLOAD: u8 = 0x16;
pub const FLOAD: u8 = 0x17;
pub const DLOAD: u8 = 0x18;
pub const ALOAD: u8 = 0x19;
pub const ISTORE: u8 = 0x36;
pub const LSTORE: u8 = 0x37;
pub const FSTORE: u8 = 0x38;
pub const DSTORE: u8 = 0x39;
pub const ASTORE: u8 = 0x3A;
pub const AASTORE: u8 = 0x53;
pub const IINC: u8 = 0x84;
pub const RET: u8 = 0xA9;
pub const TABLESWITCH: u8 = 0xAA;
pub const LOOKUPSWITCH: u8 = 0xAB;
pub const GETSTATIC: u8 = 0xB2;
pub const PUTSTATIC: u8 = 0xB3;
pub const GETFIELD: u8 = 0xB4;
pub const PUTFIELD: u8 = 0xB5;
pub const INVOKEVIRTUAL: u8 = 0xB6;
pub const INVOKESPECIAL: u8 = 0xB7;
pub const INVOKESTATIC: u8 = 0xB8;
pub const INVOKEINTERFACE: u8 = 0xB9;
pub const INVOKEDYNAMIC: u8 = 0xBA;
pub const NEW: u8 = 0xBB;
pub const NEWARRAY: u8 = 0xBC;
pub const ANEWARRAY: u8 = 0xBD;
pub const CHECKCAST: u8 = 0xC0;
pub const INSTANCEOF: u8 = 0xC1;
pub const WIDE: u8 = 0xC4;
pub const MULTIANEWARRAY: u8 = 0xC5;

/// Fixed instruction sizes (opcode + operand bytes). `None` means the
/// opcode is either variable-length (see `instruction_size`) or
/// unallocated / reserved.
const FIXED_SIZE: [Option<u8>; 256] = {
    let mut t = [None; 256];
    // 0x00..0x0F: constant stack ops
    let one = Some(1u8);
    let mut i = 0x00u16;
    while i <= 0x0F {
        t[i as usize] = one;
        i += 1;
    }
    t[0x10] = Some(2); // bipush
    t[0x11] = Some(3); // sipush
    t[0x12] = Some(2); // ldc
    t[0x13] = Some(3); // ldc_w
    t[0x14] = Some(3); // ldc2_w
    // 0x15..0x19: iload/lload/fload/dload/aload  (each 2 bytes)
    let two = Some(2u8);
    let mut i = 0x15u16;
    while i <= 0x19 {
        t[i as usize] = two;
        i += 1;
    }
    // 0x1A..0x35: iload_0..aload_3 + iaload..saload (1 byte each)
    let mut i = 0x1Au16;
    while i <= 0x35 {
        t[i as usize] = one;
        i += 1;
    }
    // 0x36..0x3A: istore/lstore/fstore/dstore/astore (2 bytes)
    let mut i = 0x36u16;
    while i <= 0x3A {
        t[i as usize] = two;
        i += 1;
    }
    // 0x3B..0x83: istore_0..astore_3 + array stores + stack + math (1 byte)
    let mut i = 0x3Bu16;
    while i <= 0x83 {
        t[i as usize] = one;
        i += 1;
    }
    t[0x84] = Some(3); // iinc
    // 0x85..0x98: type conversions + comparisons (1 byte)
    let mut i = 0x85u16;
    while i <= 0x98 {
        t[i as usize] = one;
        i += 1;
    }
    // 0x99..0xA6: ifeq..if_acmpne (3 bytes — 2-byte branch offset)
    let three = Some(3u8);
    let mut i = 0x99u16;
    while i <= 0xA6 {
        t[i as usize] = three;
        i += 1;
    }
    t[0xA7] = Some(3); // goto
    t[0xA8] = Some(3); // jsr
    t[0xA9] = Some(2); // ret
    // 0xAA, 0xAB: tableswitch / lookupswitch — variable, handled separately
    // 0xAC..0xB1: returns (1 byte)
    let mut i = 0xACu16;
    while i <= 0xB1 {
        t[i as usize] = one;
        i += 1;
    }
    // 0xB2..0xB8: getstatic/putstatic/getfield/putfield + invokes (3 bytes)
    let mut i = 0xB2u16;
    while i <= 0xB8 {
        t[i as usize] = three;
        i += 1;
    }
    t[0xB9] = Some(5); // invokeinterface: 2-byte cp + 1-byte count + 1-byte 0
    t[0xBA] = Some(5); // invokedynamic: 2-byte cp + 2 bytes 0
    t[0xBB] = Some(3); // new
    t[0xBC] = Some(2); // newarray
    t[0xBD] = Some(3); // anewarray
    t[0xBE] = Some(1); // arraylength
    t[0xBF] = Some(1); // athrow
    t[0xC0] = Some(3); // checkcast
    t[0xC1] = Some(3); // instanceof
    t[0xC2] = Some(1); // monitorenter
    t[0xC3] = Some(1); // monitorexit
    // 0xC4: wide — variable, handled separately
    t[0xC5] = Some(4); // multianewarray: 2-byte cp + 1 byte dimensions
    t[0xC6] = Some(3); // ifnull
    t[0xC7] = Some(3); // ifnonnull
    t[0xC8] = Some(5); // goto_w
    t[0xC9] = Some(5); // jsr_w
    // 0xCA: breakpoint (reserved, 1 byte)
    t[0xCA] = Some(1);
    // 0xFE / 0xFF: impdep1 / impdep2 (reserved, 1 byte)
    t[0xFE] = Some(1);
    t[0xFF] = Some(1);
    t
};

fn opcode_name(op: u8) -> &'static str {
    match op {
        0x00 => "nop",
        0x01 => "aconst_null",
        0x02 => "iconst_m1",
        0x03 => "iconst_0",
        0x04 => "iconst_1",
        0x05 => "iconst_2",
        0x06 => "iconst_3",
        0x07 => "iconst_4",
        0x08 => "iconst_5",
        0x09 => "lconst_0",
        0x0A => "lconst_1",
        0x0B => "fconst_0",
        0x0C => "fconst_1",
        0x0D => "fconst_2",
        0x0E => "dconst_0",
        0x0F => "dconst_1",
        0x10 => "bipush",
        0x11 => "sipush",
        0x12 => "ldc",
        0x13 => "ldc_w",
        0x14 => "ldc2_w",
        0x15 => "iload",
        0x16 => "lload",
        0x17 => "fload",
        0x18 => "dload",
        0x19 => "aload",
        0x36 => "istore",
        0x37 => "lstore",
        0x38 => "fstore",
        0x39 => "dstore",
        0x3A => "astore",
        0x53 => "aastore",
        0x57 => "pop",
        0x58 => "pop2",
        0x59 => "dup",
        0x84 => "iinc",
        0xA7 => "goto",
        0xAA => "tableswitch",
        0xAB => "lookupswitch",
        0xAC => "ireturn",
        0xAD => "lreturn",
        0xAE => "freturn",
        0xAF => "dreturn",
        0xB0 => "areturn",
        0xB1 => "return",
        0xB2 => "getstatic",
        0xB3 => "putstatic",
        0xB4 => "getfield",
        0xB5 => "putfield",
        0xB6 => "invokevirtual",
        0xB7 => "invokespecial",
        0xB8 => "invokestatic",
        0xB9 => "invokeinterface",
        0xBA => "invokedynamic",
        0xBB => "new",
        0xBC => "newarray",
        0xBD => "anewarray",
        0xBE => "arraylength",
        0xBF => "athrow",
        0xC0 => "checkcast",
        0xC1 => "instanceof",
        0xC4 => "wide",
        0xC5 => "multianewarray",
        _ => "?",
    }
}

// ---------------------------------------------------------------------------
// Internal cursor reader
// ---------------------------------------------------------------------------

struct Reader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Reader { data, pos: 0 }
    }

    fn u8(&mut self, needed: &'static str) -> Result<u8> {
        let b = *self
            .data
            .get(self.pos)
            .ok_or(Error::UnexpectedEof { needed })?;
        self.pos += 1;
        Ok(b)
    }

    fn u16(&mut self, needed: &'static str) -> Result<u16> {
        if self.pos + 2 > self.data.len() {
            return Err(Error::UnexpectedEof { needed });
        }
        let v = u16::from_be_bytes([self.data[self.pos], self.data[self.pos + 1]]);
        self.pos += 2;
        Ok(v)
    }

    fn u32(&mut self, needed: &'static str) -> Result<u32> {
        if self.pos + 4 > self.data.len() {
            return Err(Error::UnexpectedEof { needed });
        }
        let v = u32::from_be_bytes([
            self.data[self.pos],
            self.data[self.pos + 1],
            self.data[self.pos + 2],
            self.data[self.pos + 3],
        ]);
        self.pos += 4;
        Ok(v)
    }

    fn i32(&mut self, needed: &'static str) -> Result<i32> {
        self.u32(needed).map(|v| v as i32)
    }

    fn u64(&mut self, needed: &'static str) -> Result<u64> {
        if self.pos + 8 > self.data.len() {
            return Err(Error::UnexpectedEof { needed });
        }
        let v = u64::from_be_bytes([
            self.data[self.pos],
            self.data[self.pos + 1],
            self.data[self.pos + 2],
            self.data[self.pos + 3],
            self.data[self.pos + 4],
            self.data[self.pos + 5],
            self.data[self.pos + 6],
            self.data[self.pos + 7],
        ]);
        self.pos += 8;
        Ok(v)
    }

    fn i64(&mut self, needed: &'static str) -> Result<i64> {
        self.u64(needed).map(|v| v as i64)
    }

    fn slice(&mut self, n: usize, needed: &'static str) -> Result<&'a [u8]> {
        // `n` is attacker-supplied (a JVMS u4/u2 length), so unlike the fixed-width
        // readers above, `self.pos + n` can wrap on a 32-bit target and pass a naive
        // bounds check. checked_add turns an out-of-range length into an EOF error.
        let Some(end) = self.pos.checked_add(n) else {
            return Err(Error::UnexpectedEof { needed });
        };
        if end > self.data.len() {
            return Err(Error::UnexpectedEof { needed });
        }
        let s = &self.data[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

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
}
