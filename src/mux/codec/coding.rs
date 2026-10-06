//! Codec-agnostic per-picture coding carrier.
//!
//! [`PictureInfo`] is the single per-frame carrier of coding signals that the
//! muxer (and any downstream index/diagnostic) reads WITHOUT branching on the
//! codec. Each codec's parser decodes its own bitstream once and folds the raw
//! signals into a [`CodingDetail`](crate::mux::codec::coding::CodingDetail) variant; consumers call ONLY the
//! codec-agnostic accessors ([`coding_type`](PictureInfo::coding_type),
//! [`field_order`](PictureInfo::field_order), [`nb_fields`](PictureInfo::nb_fields),
//! [`progressive`](PictureInfo::progressive)); adding a codec adds a `CodingDetail` arm.

/// Coding/prediction type of a coded picture, mapped to the three families the
/// muxer cares about (cue/keyframe marking, B-frame ordering). Each codec maps
/// its own picture/slice type onto this:
/// - MPEG-2 `picture_coding_type` (ISO/IEC 13818-2 §6.3.8): 1→I, 2→P, 3→B.
/// - H.264/HEVC: slice type / IDR detection → I for intra-coded keyframes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CodingType {
    /// Intra-coded (I / IDR) — independently decodable, a cue/keyframe point.
    I,
    /// Predicted (P) — references earlier pictures.
    P,
    /// Bi-predicted (B) — references earlier and later pictures.
    B,
}

/// Field display order of an interlaced coded picture, mapped onto the Matroska
/// `FieldOrder` element (RFC 9559 §5.1.4.1.28, element 0x9D). `Progressive`
/// means the picture is not interlaced (the element is omitted by the muxer);
/// `None` from [`PictureInfo::field_order`] means the codec could not determine
/// it (signal absent / not yet decoded).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FieldOrder {
    /// Top field is displayed first (MPEG-2 `top_field_first == 1`).
    Tff,
    /// Bottom field is displayed first (MPEG-2 `top_field_first == 0`).
    Bff,
    /// Progressive frame — no field order applies.
    Progressive,
}

/// MPEG-2 picture coding extension signals, decoded once at the parse site.
///
/// These flags are read from ISO/IEC 13818-2 §6.3.10 (picture coding
/// extension) and §6.3.5 (sequence extension `progressive_sequence`); this
/// struct is the raw record the agnostic accessors derive from. Consumers do
/// NOT read these fields directly — they go through [`PictureInfo`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Mpeg2Coding {
    /// `top_field_first` (picture coding extension) for a frame picture; for a
    /// field picture (§6.3.10 forces it 0) the pair's field order, set by the parser.
    pub top_field_first: bool,
    /// `repeat_first_field` (picture coding extension) — the 2:3 pulldown bit.
    pub repeat_first_field: bool,
    /// `progressive_frame` (picture coding extension).
    pub progressive_frame: bool,
    /// `progressive_sequence` (sequence extension) in force for this picture.
    pub progressive_sequence: bool,
    /// True when this access unit codes a whole frame (`picture_structure == 11`);
    /// false for a single field picture (occupies one field period).
    pub frame_picture: bool,
}

/// Per-codec raw coding detail. One arm per codec carrying that codec's own
/// signals; the agnostic accessors on [`PictureInfo`] match on this. Codecs
/// that have not yet had their field/pulldown signals wired carry `None` for
/// `field_order` via the accessor (the arm exists, the bits do not).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CodingDetail {
    /// MPEG-2 Video (ISO/IEC 13818-2) picture coding extension signals.
    Mpeg2(Mpeg2Coding),
    /// A codec that reports coding type but no field/pulldown detail yet
    /// (H.264 / HEVC / VC-1). Field order is reported as unknown.
    CodingTypeOnly,
}

/// HDR10 static metadata measured from a video bitstream (HEVC SEI). Carried on
/// [`PictureInfo`] as the per-stream colour-volume signalling: it only ever
/// reaches the muxer when the mastering-display SEI was present in the stream,
/// so an SDR / no-SEI track leaves it `None` and the muxer omits the elements
/// (never fabricated). Content light is optional: some UHD discs omit that SEI. Values are stored in raw SEI integer units, not yet
/// scaled to the Matroska float domain.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Hdr10Metadata {
    /// `display_primaries_x[c]` / `display_primaries_y[c]` for c = 0,1,2.
    /// Per H.265 D.3.28 the SEI order is c=0 → Green, c=1 → Blue, c=2 → Red.
    /// Stored here in that SAME SEI order; the muxer maps to Matroska's R/G/B
    /// element layout. Units of 0.00002 (chromaticity).
    pub display_primaries_x: [u16; 3],
    pub display_primaries_y: [u16; 3],
    /// `white_point_x` / `white_point_y` in units of 0.00002 (chromaticity).
    pub white_point_x: u16,
    pub white_point_y: u16,
    /// `max_display_mastering_luminance` in units of 0.0001 cd/m².
    pub max_display_mastering_luminance: u32,
    /// `min_display_mastering_luminance` in units of 0.0001 cd/m².
    pub min_display_mastering_luminance: u32,
    /// `max_content_light_level` (MaxCLL) in cd/m², or `None` when the stream
    /// carried no content-light SEI.
    pub max_content_light_level: Option<u16>,
    /// `max_pic_average_light_level` (MaxFALL) in cd/m², or `None` when the
    /// stream carried no content-light SEI.
    pub max_pic_average_light_level: Option<u16>,
}

/// Codec-agnostic per-picture coding carrier — the single per-frame record the
/// muxer reads through the accessors below. Raw codec signals live in
/// [`CodingDetail`]; consumers MUST use the accessors, never the inner fields.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PictureInfo {
    /// Agnostic coding type (I/P/B). Set by every video parser that fills
    /// `coding`, derived from the codec's own picture/slice type.
    coding_type: CodingType,
    /// Raw per-codec coding detail. Holds the bits the field/pulldown
    /// accessors derive from.
    detail: CodingDetail,
    /// HDR10 static metadata measured from the bitstream (HEVC SEI), or `None`
    /// when the stream carried no HDR10 SEI (SDR / not signalled). Per-stream,
    /// but rides the per-picture carrier so it flows the same deferred-muxer
    /// path the measured field order does. Never fabricated.
    hdr10: Option<Hdr10Metadata>,
}

impl PictureInfo {
    /// Build a `PictureInfo` for MPEG-2 from its decoded coding type and the
    /// picture-coding-extension signals.
    pub fn mpeg2(coding_type: CodingType, m: Mpeg2Coding) -> Self {
        Self {
            coding_type,
            detail: CodingDetail::Mpeg2(m),
            hdr10: None,
        }
    }

    /// Build a `PictureInfo` for a codec that only reports its coding type
    /// (no field/pulldown detail decoded yet): H.264, HEVC, VC-1.
    pub fn coding_type_only(coding_type: CodingType) -> Self {
        Self {
            coding_type,
            detail: CodingDetail::CodingTypeOnly,
            hdr10: None,
        }
    }

    /// Attach measured HDR10 static metadata (HEVC SEI) to this picture,
    /// consuming and returning `self` for builder-style use. Only ever called
    /// with `Some(..)` once the mastering-display SEI has been seen, so an SDR
    /// track never carries fabricated colour-volume data.
    pub fn with_hdr10(mut self, hdr10: Option<Hdr10Metadata>) -> Self {
        self.hdr10 = hdr10;
        self
    }

    /// Measured HDR10 static metadata for this picture's stream, or `None` when
    /// the bitstream signalled no HDR10 SEI. Read at mux time to emit the
    /// Matroska MasteringMetadata / MaxCLL / MaxFALL — omitted entirely when
    /// `None`.
    pub fn hdr10(&self) -> Option<Hdr10Metadata> {
        self.hdr10
    }

    /// Agnostic coding type (I/P/B). The single signal for cue/keyframe marking
    /// and B-frame display ordering.
    pub fn coding_type(&self) -> CodingType {
        self.coding_type
    }

    /// Field display order for this picture, or `None` when the codec could not
    /// determine it (signal absent / not yet wired). MPEG-2: derived from
    /// `top_field_first` (the pair's order for field pictures) and the progressive flags (ISO/IEC 13818-2 §6.3.10) —
    /// a progressive frame/sequence reports [`FieldOrder::Progressive`].
    pub fn field_order(&self) -> Option<FieldOrder> {
        match self.detail {
            CodingDetail::Mpeg2(m) => {
                if !m.frame_picture {
                    // §6.3.10 forces top_field_first to 0 in a field picture, so the
                    // parser stores `picture_structure == top field` there instead.
                    Some(if m.top_field_first {
                        FieldOrder::Tff
                    } else {
                        FieldOrder::Bff
                    })
                } else if m.progressive_sequence || m.progressive_frame {
                    Some(FieldOrder::Progressive)
                } else if m.top_field_first {
                    Some(FieldOrder::Tff)
                } else {
                    Some(FieldOrder::Bff)
                }
            }
            CodingDetail::CodingTypeOnly => None,
        }
    }

    /// Number of field-display periods this picture occupies — the basis for
    /// soft-telecine (2:3 pulldown) timing. MPEG-2 (ISO/IEC 13818-2 §6.3.10):
    /// a field picture occupies 1 field,
    /// a normal frame 2, a `repeat_first_field` progressive-frame 3 (or 4/6 in a
    /// progressive sequence); an rff bit on a non-progressive interlaced frame is
    /// spec-forbidden (§6.3.10) and is treated as 2. Codecs without pulldown
    /// signalling report the normal 2 fields.
    pub fn nb_fields(&self) -> u8 {
        match self.detail {
            CodingDetail::Mpeg2(m) => {
                if !m.frame_picture {
                    return 1;
                }
                if !m.repeat_first_field {
                    return 2;
                }
                if m.progressive_sequence {
                    if m.top_field_first { 6 } else { 4 }
                } else if m.progressive_frame {
                    3
                } else {
                    2
                }
            }
            CodingDetail::CodingTypeOnly => 2,
        }
    }

    /// Whether this picture is progressive, or `None` when the codec did not
    /// signal it. MPEG-2: `progressive_sequence || progressive_frame`.
    pub fn progressive(&self) -> Option<bool> {
        match self.detail {
            CodingDetail::Mpeg2(m) => Some(m.progressive_sequence || m.progressive_frame),
            CodingDetail::CodingTypeOnly => None,
        }
    }

    /// Whether a later picture may predict from this one. An MPEG-2 B-picture never is a
    /// reference; other codecs' B-pictures may be (H.264/HEVC), or are not decoded that far.
    pub(crate) fn may_be_referenced(&self) -> bool {
        !(self.coding_type == CodingType::B && matches!(self.detail, CodingDetail::Mpeg2(_)))
    }

    /// I-picture ⇒ cue/keyframe point. Convenience over `coding_type()`.
    pub fn keyframe(&self) -> bool {
        self.coding_type == CodingType::I
    }
}

#[cfg(test)]
#[path = "coding_tests.rs"]
mod tests;
