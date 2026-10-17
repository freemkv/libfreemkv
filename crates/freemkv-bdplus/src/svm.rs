//! Lossless on-disc BD+ SVM framing, independent of online lookup protocols.
//!
//! Sections start every 2 MiB. Each contains a 24-byte `BDSVM_CC` header,
//! its declared body, a 40-byte opaque trailer, and alignment bytes. The final
//! section may end after its trailer without filling the complete 2 MiB slot.
//! Trailer boundaries and section addressing are corroborated by captured SVMs.
//! The on-disc `BDSVM` directory, numbered `.svm` files, content-code blocks and
//! signatures are independently described by the [BDSVM content-code patent]
//! and [security-check content-code patent]. This is NOT a VM executor, signature
//! verifier, instruction decoder, or FUT parser.
//!
//! [BDSVM content-code patent]: https://patentimages.storage.googleapis.com/aa/30/8e/7e223065e263a8/US8625967.pdf
//! [security-check content-code patent]: https://patentimages.storage.googleapis.com/09/a3/1a/b8884b8530f95b/US8424101.pdf
//!
//! Preserve unknown fields and alignment bytes verbatim. In particular, the
//! `0xaa` fill used by an online ID algorithm is NOT SVM file padding and does
//! not belong in this parser. No movie title has been identified in the header.

use crate::{BdPlusError, BdPlusResult};
use std::sync::Arc;

/// On-disc stride between content-code sections.
pub const SECTION_SIZE: usize = 0x20_0000;
/// Bytes before a section's body.
pub const HEADER_SIZE: usize = 24;
/// Opaque bytes immediately after a section's declared body.
pub const TRAILER_SIZE: usize = 40;

/// Parsed SVM file. Clones share the original bytes; accessors borrow slices.
/// No content is rewritten, discarded, or implicitly transmitted.
#[derive(Clone)]
pub struct Svm {
    bytes: Arc<[u8]>,
}

impl Svm {
    /// Validate every section, retaining the complete original file.
    ///
    /// Passing `Arc<[u8]>` avoids copying the input. Unknown header values and
    /// nonzero alignment bytes are preserved, not rejected or mislabelled.
    /// The online fingerprint's minimum body size is not a format constraint.
    pub fn parse(bytes: impl Into<Arc<[u8]>>) -> BdPlusResult<Self> {
        let bytes = bytes.into();
        if bytes.is_empty() {
            return Err(BdPlusError::SvmFormat { offset: 0 });
        }
        for (index, section) in bytes.chunks(SECTION_SIZE).enumerate() {
            let offset = index * SECTION_SIZE;
            let error = || BdPlusError::SvmFormat { offset };
            let header = section.get(..HEADER_SIZE).ok_or_else(error)?;
            if &header[..8] != b"BDSVM_CC" {
                return Err(error());
            }
            let length = u32::from_be_bytes(header[20..24].try_into().unwrap()) as usize;
            // Bound before addition, including on 32-bit platforms.
            if length > SECTION_SIZE - HEADER_SIZE - TRAILER_SIZE
                || section.len() < HEADER_SIZE + length + TRAILER_SIZE
            {
                return Err(error());
            }
        }
        Ok(Self { bytes })
    }

    /// Entire original file, including every section and alignment byte.
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Share the original capture without copying its data.
    pub fn shared_bytes(&self) -> Arc<[u8]> {
        Arc::clone(&self.bytes)
    }

    /// Number of validated on-disc sections.
    pub fn section_count(&self) -> usize {
        self.bytes.len().div_ceil(SECTION_SIZE)
    }

    /// First section, used by the current online identity algorithm.
    pub fn primary(&self) -> SvmSection<'_> {
        self.section(0).expect("validated nonempty SVM")
    }

    /// A section by its zero-based on-disc index.
    pub fn section(&self, index: usize) -> Option<SvmSection<'_>> {
        if index >= self.section_count() {
            return None;
        }
        let offset = index * SECTION_SIZE;
        let end = offset + (self.bytes.len() - offset).min(SECTION_SIZE);
        Some(SvmSection {
            offset,
            bytes: &self.bytes[offset..end],
        })
    }

    /// All sections in file order, without allocating or copying payloads.
    pub fn sections(&self) -> impl ExactSizeIterator<Item = SvmSection<'_>> {
        (0..self.section_count()).map(|i| self.section(i).unwrap())
    }
}

impl std::fmt::Debug for Svm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Svm")
            .field("bytes", &self.bytes.len())
            .field("sections", &self.section_count())
            .finish()
    }
}

/// Borrowed, structurally validated content-code section.
#[derive(Clone, Copy)]
pub struct SvmSection<'a> {
    offset: usize,
    bytes: &'a [u8],
}

impl<'a> SvmSection<'a> {
    /// Absolute file offset of this section's header.
    pub fn offset(self) -> usize {
        self.offset
    }
    /// Exact on-disc section, including alignment bytes.
    pub fn as_bytes(self) -> &'a [u8] {
        self.bytes
    }
    /// Header with verified fields and raw access to the unresolved fields.
    pub fn header(self) -> SvmHeader<'a> {
        SvmHeader {
            bytes: self.bytes[..HEADER_SIZE].try_into().unwrap(),
        }
    }
    /// Entire declared body. Its contents may be code or program data;
    /// this accessor does not interpret or execute them.
    pub fn body(self) -> SvmBody<'a> {
        SvmBody {
            bytes: &self.bytes[HEADER_SIZE..HEADER_SIZE + self.header().body_length() as usize],
        }
    }
    /// Exact 40-byte trailer. Its signature/cryptographic semantics are not
    /// established by this parser; no verification is implied.
    pub fn trailer(self) -> &'a [u8; TRAILER_SIZE] {
        let start = HEADER_SIZE + self.header().body_length() as usize;
        self.bytes[start..start + TRAILER_SIZE].try_into().unwrap()
    }
    /// Bytes between the trailer and the next section boundary (or EOF).
    /// Not guaranteed zero, and never synthesized or discarded.
    pub fn padding(self) -> &'a [u8] {
        &self.bytes[HEADER_SIZE + self.header().body_length() as usize + TRAILER_SIZE..]
    }
}

impl std::fmt::Debug for SvmSection<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SvmSection")
            .field("offset", &self.offset)
            .field("body_length", &self.header().body_length())
            .field("padding_length", &self.padding().len())
            .finish()
    }
}

/// Borrowed section body, split at the primary VM's 4 KiB entry boundary.
/// For section zero the prefix is cleared at startup and used for event/control
/// state; execution starts at the payload. Other sections need not have the same
/// runtime role: these accessors only describe byte ranges, not their meaning.
#[derive(Clone, Copy)]
pub struct SvmBody<'a> {
    bytes: &'a [u8],
}

impl<'a> SvmBody<'a> {
    /// Complete original body, including the prefix.
    pub fn as_bytes(self) -> &'a [u8] {
        self.bytes
    }
    /// First 4 KiB, or the entire body if shorter. Opaque on-disc bytes;
    /// parsing never clears them or interprets them as live VM event values.
    pub fn prefix(self) -> &'a [u8] {
        &self.bytes[..self.bytes.len().min(0x1000)]
    }
    /// All bytes after the prefix, not just the online fingerprint sample.
    /// May contain instructions, constants, tables, or other data.
    pub fn payload(self) -> &'a [u8] {
        &self.bytes[self.bytes.len().min(0x1000)..]
    }
}

impl std::fmt::Debug for SvmBody<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SvmBody")
            .field("prefix_length", &self.prefix().len())
            .field("payload_length", &self.payload().len())
            .finish()
    }
}

/// Original section header. Do not reconstruct it when hashing an identity:
/// even fields with unknown meaning participate in that calculation.
#[derive(Clone, Copy, Debug)]
pub struct SvmHeader<'a> {
    bytes: &'a [u8; HEADER_SIZE],
}

/// Content-code creation date, not the movie's release date.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SvmDate {
    pub year: u16,
    pub month: u8,
    pub day: u8,
}

impl<'a> SvmHeader<'a> {
    /// Original 24 bytes, including magic and unresolved fields.
    pub fn as_bytes(self) -> &'a [u8; HEADER_SIZE] {
        self.bytes
    }
    /// Validated `BDSVM_CC` magic, at offsets 0..8.
    pub fn magic(self) -> &'a [u8; 8] {
        self.bytes[..8].try_into().unwrap()
    }
    /// Offsets 8..13. Cast Away has `01 00 00 00 01`; version/flag
    /// interpretations are unverified, so these remain explicitly opaque.
    pub fn unknown_08_0c(self) -> &'a [u8; 5] {
        self.bytes[8..13].try_into().unwrap()
    }
    /// Date at offsets 13..17: big-endian year, month, day. Raw values are
    /// preserved even for an unrecognized or invalid calendar date.
    pub fn code_date(self) -> SvmDate {
        SvmDate {
            year: u16::from_be_bytes([self.bytes[13], self.bytes[14]]),
            month: self.bytes[15],
            day: self.bytes[16],
        }
    }
    /// Offsets 17..20. Zero in Cast Away; reserved meaning is not assumed.
    pub fn unknown_11_13(self) -> &'a [u8; 3] {
        self.bytes[17..20].try_into().unwrap()
    }
    /// Big-endian body length at offsets 20..24, excluding header/trailer.
    pub fn body_length(self) -> u32 {
        u32::from_be_bytes(self.bytes[20..24].try_into().unwrap())
    }
}

#[cfg(test)]
#[path = "svm_tests.rs"]
mod tests;
