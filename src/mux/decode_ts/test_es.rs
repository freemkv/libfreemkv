//! Test-only elementary-stream builders for the DTS deriver and its sinks: an MSB-first
//! bit writer with ue(v)/se(v), emulation-prevention insertion, and minimal MPEG-2,
//! H.264 and HEVC headers carrying exactly the fields the deriver reads.

/// MSB-first bit writer.
#[derive(Default)]
pub(crate) struct BitWriter {
    bytes: Vec<u8>,
    bits: u32,
}

impl BitWriter {
    pub(crate) fn u(&mut self, n: u32, v: u64) -> &mut Self {
        for i in (0..n).rev() {
            if self.bits.is_multiple_of(8) {
                self.bytes.push(0);
            }
            let bit = ((v >> i) & 1) as u8;
            let last = self.bytes.last_mut().expect("byte pushed above");
            *last |= bit << (7 - self.bits % 8);
            self.bits += 1;
        }
        self
    }
    pub(crate) fn flag(&mut self, b: bool) -> &mut Self {
        self.u(1, b as u64)
    }
    /// ue(v): `code_num + 1` in binary, preceded by (bit length − 1) zeros.
    pub(crate) fn ue(&mut self, v: u32) -> &mut Self {
        let x = v as u64 + 1;
        let len = 64 - x.leading_zeros();
        self.u(len - 1, 0).u(len, x)
    }
    /// se(v): k > 0 → 2k − 1, k ≤ 0 → −2k.
    pub(crate) fn se(&mut self, v: i32) -> &mut Self {
        let code = if v > 0 { 2 * v - 1 } else { -2 * v };
        self.ue(code as u32)
    }
    /// rbsp_trailing_bits(): a stop bit then zero alignment.
    pub(crate) fn trailing(&mut self) -> Vec<u8> {
        self.u(1, 1);
        std::mem::take(&mut self.bytes)
    }
}

/// Insert emulation-prevention bytes: `00 00` followed by a byte ≤ 3 gets a `03`.
pub(crate) fn add_epb(rbsp: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(rbsp.len() + 8);
    let mut zeros = 0;
    for &b in rbsp {
        if zeros >= 2 && b <= 3 {
            out.push(3);
            zeros = 0;
        }
        zeros = if b == 0 { zeros + 1 } else { 0 };
        out.push(b);
    }
    out
}

/// Length-prefix (4 octets) and concatenate NAL units: the IR form of H.264/HEVC.
pub(crate) fn length_prefixed(nals: &[Vec<u8>]) -> Vec<u8> {
    let mut out = Vec::new();
    for n in nals {
        out.extend_from_slice(&(n.len() as u32).to_be_bytes());
        out.extend_from_slice(n);
    }
    out
}

/// H.264 SPS options (defaults: High 4.0 1920×1080 progressive, POC type 0, no VUI).
#[derive(Clone)]
pub(crate) struct H264Sps {
    pub profile_idc: u8,
    pub constraint_flags: u8,
    pub level_idc: u8,
    pub scaling_lists: bool,
    pub poc_type: u32,
    pub width_mbs: u32,
    pub height_map_units: u32,
    pub frame_mbs_only: bool,
    pub log2_max_frame_num: u32,
    pub vui: Option<H264Vui>,
}

#[derive(Clone, Default)]
pub(crate) struct H264Vui {
    /// (num_units_in_tick, time_scale)
    pub timing: Option<(u32, u32)>,
    pub nal_hrd: bool,
    pub vcl_hrd: bool,
    /// bitstream_restriction: (max_num_reorder_frames, max_dec_frame_buffering)
    pub restriction: Option<(u32, u32)>,
}

impl Default for H264Sps {
    fn default() -> Self {
        Self {
            profile_idc: 100,
            constraint_flags: 0,
            level_idc: 40,
            scaling_lists: false,
            poc_type: 0,
            width_mbs: 120,
            height_map_units: 68,
            frame_mbs_only: true,
            log2_max_frame_num: 4,
            vui: None,
        }
    }
}

fn hrd(w: &mut BitWriter) {
    w.ue(0).u(4, 3).u(4, 5); // cpb_cnt_minus1, bit_rate_scale, cpb_size_scale
    w.ue(2999).ue(9999).flag(false); // bit_rate_value_minus1, cpb_size_value_minus1, cbr
    w.u(5, 23).u(5, 23).u(5, 23).u(5, 24);
}

impl H264Sps {
    /// The SPS NAL (header `0x67`), emulation-prevented.
    pub(crate) fn nal(&self) -> Vec<u8> {
        let mut w = BitWriter::default();
        w.u(8, self.profile_idc as u64)
            .u(8, self.constraint_flags as u64)
            .u(8, self.level_idc as u64)
            .ue(0);
        if [100, 110, 122, 244, 44, 83, 86, 118, 128, 138, 139, 134, 135]
            .contains(&self.profile_idc)
        {
            w.ue(1).ue(0).ue(0).flag(false).flag(self.scaling_lists);
            if self.scaling_lists {
                // List 0 present with non-flat deltas (se(v) incl. negatives), rest absent.
                w.flag(true);
                for d in [-3, 5, -8, 0, 7, -1, 2, -2, 1, -4, 3, 0, 0, -6, 6, 1] {
                    w.se(d);
                }
                for _ in 1..8 {
                    w.flag(false);
                }
            }
        }
        w.ue(self.log2_max_frame_num - 4).ue(self.poc_type);
        match self.poc_type {
            0 => {
                w.ue(2);
            }
            1 => {
                w.flag(false).se(-2).se(3).ue(3).se(1).se(-5).se(9);
            }
            _ => {}
        }
        w.ue(4).flag(false); // max_num_ref_frames, gaps
        w.ue(self.width_mbs - 1).ue(self.height_map_units - 1);
        w.flag(self.frame_mbs_only);
        if !self.frame_mbs_only {
            w.flag(false);
        }
        w.flag(true).flag(false); // direct_8x8_inference, frame_cropping
        w.flag(self.vui.is_some());
        if let Some(v) = &self.vui {
            w.flag(true).u(8, 1); // aspect_ratio_idc 1
            w.flag(false); // overscan
            w.flag(true)
                .u(3, 5)
                .flag(false)
                .flag(true)
                .u(24, 0x01_01_01);
            w.flag(false); // chroma_loc
            w.flag(v.timing.is_some());
            if let Some((n, t)) = v.timing {
                w.u(32, n as u64).u(32, t as u64).flag(true);
            }
            w.flag(v.nal_hrd);
            if v.nal_hrd {
                hrd(&mut w);
            }
            w.flag(v.vcl_hrd);
            if v.vcl_hrd {
                hrd(&mut w);
            }
            if v.nal_hrd || v.vcl_hrd {
                w.flag(false);
            }
            w.flag(false); // pic_struct_present
            w.flag(v.restriction.is_some());
            if let Some((reorder, dpb)) = v.restriction {
                w.flag(true).ue(0).ue(0).ue(16).ue(16).ue(reorder).ue(dpb);
            }
        }
        let mut nal = vec![0x67];
        nal.extend(add_epb(&w.trailing()));
        nal
    }
}

/// An H.264 coded slice NAL: IDR (type 5) or non-IDR (type 1), with `field` =
/// `Some(bottom_field_flag)` for a field picture of an SPS without frame_mbs_only.
pub(crate) fn h264_slice(
    sps: &H264Sps,
    idr: bool,
    first_mb: u32,
    frame_num: u32,
    field: Option<bool>,
) -> Vec<u8> {
    let mut w = BitWriter::default();
    w.ue(first_mb).ue(if idr { 7 } else { 5 }).ue(0);
    w.u(sps.log2_max_frame_num, frame_num as u64);
    if !sps.frame_mbs_only {
        w.flag(field.is_some());
        if let Some(bottom) = field {
            w.flag(bottom);
        }
    }
    w.u(24, 0); // slice data stand-in: zeros exercise emulation prevention
    let mut nal = vec![if idr { 0x65 } else { 0x41 }];
    nal.extend(add_epb(&w.trailing()));
    nal
}

/// An HEVC SPS NAL (type 33). `ordering`: the (max_dec_pic_buffering_minus1,
/// max_num_reorder_pics, max_latency_increase_plus1) per coded sub-layer;
/// `ordering.len() == 1` codes `sps_sub_layer_ordering_info_present_flag = 0`.
pub(crate) fn hevc_sps(max_sub_layers_minus1: u32, ordering: &[(u32, u32, u32)]) -> Vec<u8> {
    let mut w = BitWriter::default();
    w.u(4, 0).u(3, max_sub_layers_minus1 as u64).flag(true);
    w.u(8, 0x01)
        .u(32, 0x6000_0000)
        .u(48, 0x9000_0000_0000)
        .u(8, 120); // general PTL
    if max_sub_layers_minus1 > 0 {
        for _ in 0..max_sub_layers_minus1 {
            w.flag(false).flag(true); // sub-layer profile absent, level present
        }
        for _ in max_sub_layers_minus1..8 {
            w.u(2, 0);
        }
        for _ in 0..max_sub_layers_minus1 {
            w.u(8, 90);
        }
    }
    w.ue(0).ue(1).ue(1920).ue(1080).flag(false).ue(2).ue(2); // 4:2:0 10-bit
    w.ue(4); // log2_max_pic_order_cnt_lsb_minus4
    let present = ordering.len() > 1;
    w.flag(present);
    for &(dpb, reorder, latency) in ordering {
        w.ue(dpb).ue(reorder).ue(latency);
    }
    w.u(16, 0);
    let mut nal = vec![33 << 1, 0x01];
    nal.extend(add_epb(&w.trailing()));
    nal
}

/// An HEVC SPS (R = `reorder`) coded through to `vui_timing_info` (H.265 §7.3.2.2, E.2.1):
/// optional scaling_list_data, PCM, two short-term RPS (the second inter-predicted), one
/// long-term picture, then VUI timing `(num_units_in_tick, time_scale)`.
pub(crate) fn hevc_sps_vui(reorder: u32, timing: Option<(u32, u32)>, scaling: bool) -> Vec<u8> {
    let mut w = BitWriter::default();
    w.u(4, 0).u(3, 0).flag(true);
    w.u(8, 0x01)
        .u(32, 0x6000_0000)
        .u(48, 0x9000_0000_0000)
        .u(8, 120);
    w.ue(0).ue(1).ue(1920).ue(1080).flag(false).ue(2).ue(2);
    w.ue(4).flag(false).ue(4).ue(reorder).ue(0); // log2_max_poc_lsb = 8
    w.ue(0).ue(3).ue(0).ue(3).ue(2).ue(2); // block/transform sizes and depths
    w.flag(scaling);
    if scaling {
        w.flag(true); // sps_scaling_list_data_present_flag
        for size_id in 0..4u32 {
            for matrix in (0..6).step_by(if size_id == 3 { 3 } else { 1 }) {
                let explicit = matrix == 0;
                w.flag(explicit);
                if !explicit {
                    w.ue(0);
                    continue;
                }
                if size_id > 1 {
                    w.se(-3);
                }
                for c in 0..64.min(1u32 << (4 + (size_id << 1))) {
                    w.se(if c % 2 == 0 { 1 } else { -1 });
                }
            }
        }
    }
    w.flag(true).flag(true); // amp, sample_adaptive_offset
    w.flag(true).u(4, 7).u(4, 7).ue(0).ue(1).flag(false); // PCM
    w.ue(2); // num_short_term_ref_pic_sets
    w.ue(2).ue(1); // set 0: 2 negative, 1 positive
    for _ in 0..3 {
        w.ue(0).flag(true);
    }
    // set 1, inter-predicted from set 0 (NumDeltaPocs 3 → 4 entries).
    w.flag(true).flag(false).ue(0);
    w.flag(true)
        .flag(false)
        .flag(true)
        .flag(false)
        .flag(false)
        .flag(true);
    w.flag(true).ue(1).u(8, 5).flag(true); // one long-term picture (8-bit lsb)
    w.flag(true).flag(true); // temporal MVP, strong intra smoothing
    w.flag(true); // vui_parameters_present_flag
    w.flag(true).u(8, 255).u(16, 1).u(16, 1); // Extended_SAR
    w.flag(false)
        .flag(true)
        .u(3, 5)
        .flag(false)
        .flag(true)
        .u(24, 0x01_01_01);
    w.flag(true).ue(0).ue(0); // chroma loc
    w.u(3, 0).flag(true).ue(0).ue(0).ue(0).ue(0); // flags, default display window
    w.flag(timing.is_some());
    if let Some((n, t)) = timing {
        w.u(32, n as u64).u(32, t as u64).flag(false).flag(false);
    }
    w.flag(false).flag(false); // bitstream_restriction, sps_extension
    let mut nal = vec![33 << 1, 0x01];
    nal.extend(add_epb(&w.trailing()));
    nal
}

/// An HEVC coded slice NAL of `nal_type` (the deriver reads only the type).
pub(crate) fn hevc_slice(nal_type: u8) -> Vec<u8> {
    vec![nal_type << 1, 0x01, 0xAF, 0x12, 0x34]
}

/// MPEG-2 sequence header + sequence_extension (`low_delay`), frame_rate_code `frc`.
pub(crate) fn mpeg2_seq(frc: u8, low_delay: bool) -> Vec<u8> {
    let mut v = vec![
        0,
        0,
        1,
        0xB3,
        0x78,
        0x04,
        0x38,
        0x30 | frc,
        0xFF,
        0xFF,
        0xE0,
        0x18,
    ];
    v.extend_from_slice(&[
        0,
        0,
        1,
        0xB5,
        0x14,
        0x8A,
        0x00,
        0x01,
        0x00,
        (low_delay as u8) << 7,
    ]);
    v
}

/// The sequence header alone (MPEG-1 style: no sequence_extension).
pub(crate) fn mpeg1_seq(frc: u8) -> Vec<u8> {
    vec![
        0,
        0,
        1,
        0xB3,
        0x16,
        0x00,
        0xF0,
        0x10 | frc,
        0xFF,
        0xFF,
        0xE0,
        0x18,
    ]
}

/// MPEG-2 picture header (coding type 1 I, 2 P, 3 B) + picture_coding_extension with
/// `structure` 1 top field, 2 bottom field, 3 frame; then one slice.
pub(crate) fn mpeg2_pic(coding: u8, structure: u8) -> Vec<u8> {
    let mut v = vec![0, 0, 1, 0x00, 0x00, coding << 3, 0xFF, 0xF8];
    v.extend_from_slice(&[0, 0, 1, 0xB5, 0x8F, 0xFF, 0xF0 | structure, 0x80, 0x80]);
    v.extend_from_slice(&[0, 0, 1, 0x01, 0x12, 0x34, 0x56]);
    v
}

/// An avcC record (4-octet NAL prefixes) holding `sps` and a stand-in PPS.
pub(crate) fn avcc(sps: &[u8]) -> Vec<u8> {
    let pps = [0x68u8, 0xCE, 0x3C, 0x80];
    let mut v = vec![1, sps[1], sps[2], sps[3], 0xFF, 0xE1];
    v.extend_from_slice(&(sps.len() as u16).to_be_bytes());
    v.extend_from_slice(sps);
    v.push(1);
    v.extend_from_slice(&(pps.len() as u16).to_be_bytes());
    v.extend_from_slice(&pps);
    v
}

/// An H.264 SPS whose VUI states `max_num_reorder_frames = reorder`.
pub(crate) fn sps_with_reorder(reorder: u32) -> H264Sps {
    H264Sps {
        vui: Some(H264Vui {
            restriction: Some((reorder, reorder + 1)),
            ..Default::default()
        }),
        ..Default::default()
    }
}
