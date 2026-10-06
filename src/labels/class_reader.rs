//! Hand-rolled JVM `.class` file reader, tailored to the subset we need
//! for BD-J label extraction (Deluxe / dbp / similar frameworks).
//!
//! Spec: JVMS §4 (class file format) and §6 (bytecode). Exposes: constant
//! pool, methods, the `Code` attribute, and a non-allocating bytecode
//! iterator.
//!
//! No external deps beyond `std`. No `unsafe`. No panics on malformed input — every parse fault
//! is a typed [`Error`].

const CLASS_MAGIC: u32 = 0xCAFEBABE;

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

#[derive(Debug)]
#[expect(
    dead_code,
    reason = "payloads are fault detail for Debug; callers match the variant"
)]
pub enum Error {
    UnexpectedEof { needed: &'static str },
    BadMagic(u32),
    BadCpTag { index: u16, tag: u8 },
    BadUtf8 { index: u16 },
    BadCodeAttribute,
    // NOTE: no `BadInstruction` variant. The bytecode iterator STOPS at the
    // first malformed instruction (returns `None`) rather than surfacing a
    // typed error, so nothing ever constructed one — it was dead and removed.
}

// No Display/std::error::Error impl: crate-internal typed error for `match`/`?`
// within label parsers. Per the zero-English rule there is no user-facing
// text; variant fields carry the structured detail.

pub type Result<T> = std::result::Result<T, Error>;

// ---------------------------------------------------------------------------
// Constant pool
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
#[expect(
    dead_code,
    reason = "every JVMS 4.4 tag is decoded; label parsers read only some payloads"
)]
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
    // Test-only: build a pool directly from entries (real code goes through ClassFile::parse).
    // Caller must prepend CpInfo::Empty at index 0 and after each Long/Double entry (2-slot
    // quirk).
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

    #[cfg(test)]
    /// Resolve a `CONSTANT_String` entry to its underlying UTF-8.
    pub fn string(&self, index: u16) -> Option<&str> {
        match self.get(index)? {
            CpInfo::String { string_index } => self.utf8(*string_index),
            _ => None,
        }
    }

    #[cfg(test)]
    pub fn integer(&self, index: u16) -> Option<i32> {
        match self.get(index)? {
            CpInfo::Integer(v) => Some(*v),
            _ => None,
        }
    }

    #[cfg(test)]
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

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[cfg(test)]
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

#[expect(
    dead_code,
    reason = "parsed in full per JVMS 4.1; label parsers read the pool and methods"
)]
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

#[expect(dead_code, reason = "parsed in full per JVMS 4.5/4.6")]
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

    #[cfg(test)]
    pub fn super_class_name(&self) -> Option<&str> {
        self.constant_pool.class_name(self.super_class)
    }

    /// Convenience: name of a `Member` belonging to this class.
    pub fn member_name<'a>(&'a self, m: &Member) -> Option<&'a str> {
        self.constant_pool.utf8(m.name_index)
    }

    #[cfg(test)]
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
    #[expect(
        dead_code,
        reason = "parsed per JVMS 4.7.3; the decoder tracks only the stack"
    )]
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

// Decode JVM "modified UTF-8" (JVMS §4.4.7): like UTF-8 but U+0000 is 0xC0 0x80, and
// supplementary chars are two 3-byte surrogate halves, combined here into one char.
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
            i += 3;
            // A high surrogate followed by a low one is one supplementary char.
            if (0xD800..0xDC00).contains(&cp)
                && let [0xED, b1 @ 0xB0..=0xBF, b2 @ 0x80..=0xBF, ..] = &bytes[i..]
            {
                let low = 0xD000 | (((b1 & 0x3F) as u32) << 6) | ((b2 & 0x3F) as u32);
                if let Some(c) = char::from_u32(0x10000 + ((cp - 0xD800) << 10) + (low - 0xDC00)) {
                    out.push(c);
                    i += 3;
                    continue;
                }
            }
            // A lone surrogate is invalid in Rust: replacement char, not an error.
            out.push(char::from_u32(cp).unwrap_or('\u{FFFD}'));
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
    #[cfg(test)]
    /// Mnemonic for the opcodes this reader models (e.g. "ldc"); "?" for others.
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
        // Defence in depth: a wrapped `end` would pass the bounds check below.
        let end = pc.checked_add(size)?;
        if end > self.code.len() {
            return None;
        }
        let operands = &self.code[pc + 1..end];
        self.pos = end;
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
pub const IASTORE: u8 = 0x4F;
pub const SASTORE: u8 = 0x56;
pub const DCONST_1: u8 = 0x0F;
pub const POP: u8 = 0x57;
pub const POP2: u8 = 0x58;
pub const DUP: u8 = 0x59;
pub const GOTO: u8 = 0xA7;
pub const RETURN: u8 = 0xB1;
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
pub const LCONST_0: u8 = 0x09;
pub const LCONST_1: u8 = 0x0A;
pub const DCONST_0: u8 = 0x0E;
pub const LLOAD_0: u8 = 0x1E;
pub const LLOAD_3: u8 = 0x21;
pub const DLOAD_0: u8 = 0x26;
pub const DLOAD_3: u8 = 0x29;
pub const ALOAD_3: u8 = 0x2D;
pub const IALOAD: u8 = 0x2E;
pub const LALOAD: u8 = 0x2F;
pub const DALOAD: u8 = 0x31;
pub const SALOAD: u8 = 0x35;
pub const ASTORE_3: u8 = 0x4E;
pub const DUP_X1: u8 = 0x5A;
pub const DUP_X2: u8 = 0x5B;
pub const DUP2: u8 = 0x5C;
pub const DUP2_X1: u8 = 0x5D;
pub const DUP2_X2: u8 = 0x5E;
pub const SWAP: u8 = 0x5F;
pub const IADD: u8 = 0x60;
pub const DREM: u8 = 0x73;
pub const INEG: u8 = 0x74;
pub const DNEG: u8 = 0x77;
pub const ISHL: u8 = 0x78;
pub const LXOR: u8 = 0x83;
pub const I2L: u8 = 0x85;
pub const I2D: u8 = 0x87;
pub const L2D: u8 = 0x8A;
pub const F2L: u8 = 0x8C;
pub const F2D: u8 = 0x8D;
pub const D2L: u8 = 0x8F;
pub const I2S: u8 = 0x93;
pub const LCMP: u8 = 0x94;
pub const DCMPG: u8 = 0x98;
pub const IFEQ: u8 = 0x99;
pub const IFLE: u8 = 0x9E;
pub const IF_ICMPEQ: u8 = 0x9F;
pub const IF_ACMPNE: u8 = 0xA6;
pub const IRETURN: u8 = 0xAC;
pub const ARRAYLENGTH: u8 = 0xBE;
pub const ATHROW: u8 = 0xBF;
pub const MONITORENTER: u8 = 0xC2;
pub const MONITOREXIT: u8 = 0xC3;
pub const IFNULL: u8 = 0xC6;
pub const IFNONNULL: u8 = 0xC7;
pub const GOTO_W: u8 = 0xC8;

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

#[cfg(test)]
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
#[path = "class_reader_tests.rs"]
mod tests;
