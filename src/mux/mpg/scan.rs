//! The `mpg://` source scan (mpg-output-design v5 §4 step 3): the program stream map when
//! present (track order, stream types, ISO 639, the 13818-3 hierarchy, the FMKV tag-0xFA
//! table and palette), and in-band probes in every case.

use super::pack;
use crate::disc::{
    AudioChannels, AudioStream, Codec, ColorSpace, FrameRate, HdrFormat, LabelPurpose,
    LabelQualifier, Resolution, SampleRate, Stream, SubtitleStream, VideoStream,
};
use crate::mux::ps::{PsDemuxer, PsPacket};
use std::collections::BTreeMap;

/// Bytes of each stream's first packets kept for the probes.
const PROBE_BYTES: usize = 64 * 1024;

type Key = (u8, Option<u8>);

/// What one program stream map says.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Map {
    /// `(stream_type, elementary_stream_id, descriptors)` in map order.
    pub entries: Vec<(u8, u8, Vec<u8>)>,
    pub subs: Vec<pack::SubStreamInfo>,
    pub palette: Option<[[u8; 3]; 16]>,
}

/// Parse a program stream map (MS-8), `None` unless its CRC_32 checks (MS-9).
pub(crate) fn parse_map(m: &[u8]) -> Option<Map> {
    // MS-8 syntax (lengths bound every walk below); MS-9 the CRC_32 residue must be zero.
    if m.len() < 16 || m[..4] != [0, 0, 1, pack::PSM_ID] || pack::crc32(m) != 0 {
        return None;
    }
    let u16_at = |i: usize| {
        m.get(i..i + 2)
            .map(|b| usize::from(u16::from_be_bytes([b[0], b[1]])))
    };
    let info_len = u16_at(8)?;
    let info = m.get(10..10 + info_len)?;
    let mut map = Map::default();
    let mut i = 0;
    while i + 2 <= info.len() {
        let (tag, len) = (info[i], usize::from(info[i + 1]));
        let body = info.get(i + 2..i + 2 + len)?;
        // Design §2.2: tag 0xFA, magic FMKV, version, type (1 table, 2 palette).
        if tag == pack::FMKV_TAG && body.len() >= 6 && &body[..4] == pack::FMKV_MAGIC {
            match body[5] {
                pack::FMKV_SUBSTREAMS => {
                    for r in body[6..].as_chunks::<5>().0 {
                        map.subs.push(pack::SubStreamInfo {
                            sub_id: r[0],
                            lang: [r[1], r[2], r[3]],
                            forced: r[4] & 1 != 0,
                        });
                    }
                }
                pack::FMKV_PALETTE if body.len() == 6 + 48 => {
                    let mut p = [[0u8; 3]; 16];
                    for (k, c) in body[6..].as_chunks::<3>().0.iter().enumerate() {
                        p[k] = [c[0], c[1], c[2]];
                    }
                    map.palette = Some(p);
                }
                _ => {}
            }
        }
        i += 2 + len;
    }
    let es_len = u16_at(10 + info_len)?;
    let es = m.get(12 + info_len..12 + info_len + es_len)?;
    let mut j = 0;
    while j + 4 <= es.len() {
        let n = usize::from(u16::from_be_bytes([es[j + 2], es[j + 3]]));
        map.entries
            .push((es[j], es[j + 1], es.get(j + 4..j + 4 + n)?.to_vec()));
        j += 4 + n;
    }
    Some(map)
}

// Walk descriptors: `(tag, body)`.
fn descriptors(d: &[u8]) -> impl Iterator<Item = (u8, &[u8])> {
    let mut i = 0;
    std::iter::from_fn(move || {
        let (tag, len) = (*d.get(i)?, usize::from(*d.get(i + 1)?));
        let body = d.get(i + 2..i + 2 + len)?;
        i += 2 + len;
        Some((tag, body))
    })
}

fn lang_of(bytes: [u8; 3]) -> String {
    match &bytes {
        b"und" => String::new(),
        b if b.iter().all(u8::is_ascii_alphabetic) => {
            String::from_utf8_lossy(b).to_ascii_lowercase()
        }
        _ => String::new(),
    }
}

// Codec, resolution, frame rate and display aspect from a sequence header.
type Probed = (Codec, Resolution, FrameRate, Option<(u32, u32)>);

// MPEG-2 (or MPEG-1) video probed from its sequence header.
fn probe_video(es: &[u8], map_type: Option<u8>) -> Option<Probed> {
    let seq = es.windows(4).position(|w| w == [0, 0, 1, 0xB3]);
    let ext = es
        .windows(5)
        .position(|w| w[..4] == [0, 0, 1, 0xB5] && w[4] >> 4 == 1);
    let codec = match map_type {
        Some(0x01) => Codec::Mpeg1,
        Some(0x02) => Codec::Mpeg2,
        // J24: other video stream types await the 2013+ text (F8).
        Some(_) => return None,
        None if seq.is_some() && ext.is_some() => Codec::Mpeg2,
        None if seq.is_some() => Codec::Mpeg1,
        None => return None,
    };
    let (mut res, mut rate) = (Resolution::Unknown, FrameRate::Unknown);
    let mut dar = None;
    if let Some(h) = seq.and_then(|s| es.get(s + 4..s + 8)) {
        let height = (u32::from(h[1] & 0x0F) << 8) | u32::from(h[2]);
        let progressive = ext
            .and_then(|e| es.get(e + 5))
            .is_none_or(|b| b & 0x08 != 0);
        res = match (height, progressive) {
            (480, false) => Resolution::R480i,
            (576, false) => Resolution::R576i,
            (1080, false) => Resolution::R1080i,
            (h, _) => Resolution::from_height(h),
        };
        rate = match h[3] & 0x0F {
            1 => FrameRate::F23_976,
            2 => FrameRate::F24,
            3 => FrameRate::F25,
            4 => FrameRate::F29_97,
            5 => FrameRate::F30,
            6 => FrameRate::F50,
            7 => FrameRate::F59_94,
            8 => FrameRate::F60,
            _ => FrameRate::Unknown,
        };
        // aspect_ratio_information: 1 = square pixels (coded size). 2/3/4 are 4:3, 16:9, 2.21:1
        // only in 13818-2; in 11172-2 they are pel ratios, so MPEG-1 leaves them unknown.
        let width = (u32::from(h[0]) << 4) | u32::from(h[1] >> 4);
        dar = match (h[3] >> 4, codec) {
            (1, _) if width > 0 && height > 0 => Some((width, height)),
            (2, Codec::Mpeg2) => Some((4, 3)),
            (3, Codec::Mpeg2) => Some((16, 9)),
            (4, Codec::Mpeg2) => Some((221, 100)),
            _ => None,
        };
    }
    Some((codec, res, rate, dar))
}

// Design §4 step 3 "MPEG audio | header + mc_header": Layer II frames from `at` go through
// the M1 channel tracker (a CRC-verified 13818-3 mc_header run), else `None`.
fn mc_channels(es: &[u8], at: usize) -> Option<AudioChannels> {
    use crate::mux::codec::mp2_channels::{ChannelTracker, Header};
    let mut t = ChannelTracker::default();
    let mut pos = at;
    while let Some(n) = es
        .get(pos..)
        .and_then(Header::parse)
        .and_then(|h| h.frame_bytes())
        .filter(|&n| n > 4 && pos + n <= es.len())
    {
        if let Some(c) = t.observe(&es[pos..pos + n]) {
            return Some(AudioChannels::from_count(c));
        }
        pos += n;
    }
    t.finish().map(AudioChannels::from_count)
}

// 11172-3 / 13818-3 frame header: codec, channels, sample rate.
fn probe_mpeg_audio(es: &[u8]) -> (Codec, AudioChannels, SampleRate) {
    let Some(at) = es
        .windows(2)
        .position(|w| w[0] == 0xFF && w[1] & 0xE0 == 0xE0)
    else {
        return (Codec::Mp2, AudioChannels::Unknown, SampleRate::Unknown);
    };
    let h = &es[at..(at + 4).min(es.len())];
    let codec = if h[1] >> 1 & 3 == 1 {
        Codec::Mp3
    } else {
        Codec::Mp2
    };
    let (channels, hz) = match h {
        [_, b1, b2, b3] => {
            let rates: [u32; 3] = if b1 & 0x08 != 0 {
                [44_100, 48_000, 32_000]
            } else {
                [22_050, 24_000, 16_000]
            };
            let hz = rates.get(usize::from(b2 >> 2 & 3)).copied().unwrap_or(0);
            let ch = if b3 >> 6 == 3 {
                AudioChannels::Mono
            } else {
                AudioChannels::Stereo
            };
            (ch, hz)
        }
        _ => (AudioChannels::Unknown, 0),
    };
    let channels = match codec {
        Codec::Mp2 => mc_channels(es, at).unwrap_or(channels),
        _ => channels,
    };
    (codec, channels, SampleRate::from_hz(hz))
}

// The 3-byte DVD LPCM header (after the private header the demuxer strips).
fn probe_lpcm(es: &[u8]) -> (AudioChannels, SampleRate) {
    match es {
        [_, b, ..] => {
            let hz = [48_000, 96_000, 44_100, 32_000][usize::from(b >> 4 & 3)];
            (
                AudioChannels::from_count((b & 7) + 1),
                SampleRate::from_hz(hz),
            )
        }
        _ => (AudioChannels::Unknown, SampleRate::Unknown),
    }
}

// The VobSub `.idx` text the DvdSub parser takes (as `format_palette` writes it), from RGB.
fn idx_text(palette: &[[u8; 3]; 16], res: Resolution) -> Vec<u8> {
    let mut out = String::new();
    if let Some((w, h)) = res.pixels() {
        out.push_str(&format!("size: {w}x{h}\n"));
    }
    let parts: Vec<String> = palette
        .iter()
        .map(|[r, g, b]| format!("{r:02x}{g:02x}{b:02x}"))
        .collect();
    out.push_str(&format!("palette: {}\n", parts.join(", ")));
    out.into_bytes()
}

fn audio(
    pid: u16,
    codec: Codec,
    channels: AudioChannels,
    rate: SampleRate,
    language: String,
    label: String,
) -> Stream {
    Stream::Audio(AudioStream {
        pid,
        codec,
        channels,
        language,
        sample_rate: rate,
        secondary: false,
        purpose: LabelPurpose::Normal,
        label,
    })
}

/// [`scan`]'s streams alone.
#[cfg(test)]
pub(crate) fn scan_streams(head: &[u8]) -> Option<Vec<Stream>> {
    scan(head).map(|s| s.streams)
}

/// What the scan found: the streams, and the one video `stream_id` carried.
pub(crate) struct Scan {
    pub streams: Vec<Stream>,
    /// Only this video `stream_id` is routed; others are left out (one video track).
    pub video_id: Option<u8>,
}

// `(hierarchy_type, hierarchy_layer_index, hierarchy_embedded_layer_index)` (MS-23).
fn hierarchy(desc: &[u8]) -> Option<(u8, u8, u8)> {
    descriptors(desc)
        .find(|(t, b)| *t == 4 && b.len() >= 4)
        .map(|(_, b)| (b[0] & 0x0F, b[1] & 0x3F, b[2] & 0x3F))
}

/// Streams of the program stream in `head`, in map order (else id order), with the
/// `pid`s the DVD demux routes (`PsPacket::dvd_pid`), and the chosen video `stream_id`.
/// `None` when `head` holds no pack.
pub(crate) fn scan(head: &[u8]) -> Option<Scan> {
    if !head.windows(4).any(|w| w == [0, 0, 1, 0xBA]) {
        return None;
    }
    let mut d = PsDemuxer::new();
    let mut seen: BTreeMap<Key, Vec<u8>> = BTreeMap::new();
    let packets: Vec<PsPacket> = d.feed(head).into_iter().chain(d.flush()).collect();
    for p in packets.iter().filter(|p| p.dvd_pid().is_some()) {
        let e = seen.entry((p.stream_id, p.sub_stream_id)).or_default();
        if e.len() < PROBE_BYTES {
            e.extend_from_slice(&p.data);
        }
    }
    let map = d.psm().and_then(parse_map);
    if d.psm().is_some() && map.is_none() {
        tracing::warn!(target: "mux", "mpg: program stream map fails its CRC_32; using in-band probes only");
    }
    // Track order: the map's entries (0xBD expanded by the FMKV table), then anything seen
    // that it does not name, in id order (design §4 step 3).
    let mut order: Vec<Key> = Vec::new();
    let mut map_type: BTreeMap<u8, (u8, Vec<u8>)> = BTreeMap::new();
    if let Some(m) = &map {
        for (ty, id, desc) in &m.entries {
            map_type.insert(*id, (*ty, desc.clone()));
            if *id == pack::PRIVATE_STREAM_1 {
                order.extend(
                    m.subs
                        .iter()
                        .map(|s| (pack::PRIVATE_STREAM_1, Some(s.sub_id))),
                );
            } else {
                order.push((*id, None));
            }
        }
    }
    let mut rest: Vec<Key> = seen
        .keys()
        .filter(|k| !order.contains(k))
        .copied()
        .collect();
    // Without a map: video, MPEG audio, then private sub-streams (audio before subpictures).
    rest.sort_by_key(|&(id, sub)| match (id, sub) {
        (0xE0..=0xEF, _) => (0, id, 0),
        (0xC0..=0xDF, _) => (1, id, 0),
        (_, Some(s @ 0x20..=0x3F)) => (3, id, s),
        (_, s) => (2, id, s.unwrap_or(0)),
    });
    order.extend(rest);

    let fmkv = |sub: u8| {
        map.as_ref()
            .and_then(|m| m.subs.iter().find(|s| s.sub_id == sub).copied())
    };
    let empty = Vec::new();
    let mut streams: Vec<Stream> = Vec::new();
    let mut video_res = Resolution::Unknown;
    let mut video_id = None;
    // Extensions resolved against their base once every base is known: `(index, base id)`.
    let mut exts: Vec<(usize, u8)> = Vec::new();
    for key in order {
        let es = seen.get(&key).unwrap_or(&empty);
        let (ty, desc) = map_type
            .get(&key.0)
            .map_or((None, &empty), |(t, d)| (Some(*t), d));
        let lang = descriptors(desc)
            .find(|(t, b)| *t == 10 && b.len() >= 3)
            .map(|(_, b)| lang_of([b[0], b[1], b[2]]))
            .unwrap_or_default();
        match key {
            (0xE0..=0xEF, _) => {
                if video_id.is_some() {
                    tracing::warn!(target: "mux", stream_id = key.0, "mpg: a second video stream; left out");
                    continue;
                }
                let Some((codec, res, rate, dar)) = probe_video(es, ty) else {
                    tracing::warn!(target: "mux", stream_id = key.0, "mpg: video stream with no MPEG-1/2 sequence header in the head; left out");
                    continue;
                };
                video_res = res;
                video_id = Some(key.0);
                streams.push(Stream::Video(VideoStream {
                    pid: crate::mux::ps::DVD_VIDEO_PID,
                    codec,
                    resolution: res,
                    frame_rate: rate,
                    hdr: HdrFormat::Sdr,
                    color_space: if res.pixels().is_some_and(|(_, h)| h == 576) {
                        ColorSpace::Bt470bg
                    } else {
                        ColorSpace::Bt709
                    },
                    display_aspect: dar,
                    secondary: false,
                    label: String::new(),
                    measured_cicp: None,
                }));
            }
            (id @ 0xD0..=0xD7, None) => {
                // With a map, the hierarchy descriptor decides (type 5, MS-23) and names the
                // base by hierarchy_embedded_layer_index; without one, the first sync word:
                // 13818-3 ext_syncword 0x7FF is an extension (base 0xC0|n), 0xFFF audio.
                let base = match &map {
                    Some(m) => hierarchy(desc)
                        .filter(|h| h.0 == pack::HIERARCHY_EXTENSION)
                        .map(|(_, _, embedded)| {
                            m.entries
                                .iter()
                                .find(|(_, _, d)| hierarchy(d).is_some_and(|h| h.1 == embedded))
                                .map(|e| e.1)
                        }),
                    None => es
                        .windows(2)
                        .find(|w| (w[0] == 0x7F || w[0] == 0xFF) && w[1] & 0xF0 == 0xF0)
                        .filter(|w| w[0] == 0x7F)
                        .map(|_| Some(0xC0 | (id & 7))),
                };
                if let Some(base) = base {
                    // Paired in a second pass, when every base is known.
                    exts.push((streams.len(), base.unwrap_or(0)));
                    streams.push(audio(
                        u16::from(id),
                        Codec::Mp2,
                        AudioChannels::Unknown,
                        SampleRate::Unknown,
                        String::new(),
                        crate::disc::MP2_EXTENSION_LABEL.into(),
                    ));
                } else {
                    let (codec, ch, rate) = probe_mpeg_audio(es);
                    streams.push(audio(u16::from(id), codec, ch, rate, lang, String::new()));
                }
            }
            (id @ 0xC0..=0xC7, None) => {
                let (codec, ch, rate) = probe_mpeg_audio(es);
                streams.push(audio(u16::from(id), codec, ch, rate, lang, String::new()));
            }
            (pack::PRIVATE_STREAM_1, Some(sub)) => {
                let info = fmkv(sub);
                let lang = info.map(|i| lang_of(i.lang)).unwrap_or_default();
                let pid = 0xBD00 | u16::from(sub);
                let s = match sub {
                    0x80..=0x87 => audio(
                        pid,
                        Codec::Ac3,
                        AudioChannels::Unknown,
                        SampleRate::S48,
                        lang,
                        String::new(),
                    ),
                    0x88..=0x8F => audio(
                        pid,
                        Codec::Dts,
                        AudioChannels::Unknown,
                        SampleRate::S48,
                        lang,
                        String::new(),
                    ),
                    0xA0..=0xA7 => {
                        let (ch, rate) = probe_lpcm(es);
                        audio(pid, Codec::Lpcm, ch, rate, lang, String::new())
                    }
                    0xC0..=0xCF => audio(
                        pid,
                        Codec::Ac3Plus,
                        AudioChannels::Unknown,
                        SampleRate::S48,
                        lang,
                        String::new(),
                    ),
                    0x20..=0x3F => Stream::Subtitle(SubtitleStream {
                        pid: u16::from(sub),
                        codec: Codec::DvdSub,
                        language: lang,
                        forced: info.is_some_and(|i| i.forced),
                        qualifier: LabelQualifier::None,
                        codec_data: map
                            .as_ref()
                            .and_then(|m| m.palette.as_ref())
                            .map(|p| idx_text(p, video_res)),
                    }),
                    _ => continue,
                };
                streams.push(s);
            }
            _ => {}
        }
    }
    // The IR pairs an extension 0xD0|n with base 0xC0|n (M1); a map pairing anything else,
    // or a base not carried, leaves the extension out.
    let mut drop = Vec::new();
    for (i, base) in exts {
        let Stream::Audio(ext) = &streams[i] else {
            continue;
        };
        let id = ext.pid as u8;
        let b = streams.iter().find_map(|s| match s {
            Stream::Audio(a) if a.pid == u16::from(base) && !a.is_mp2_extension() => Some(a),
            _ => None,
        });
        match b {
            Some(b) if base == 0xC0 | (id & 7) => {
                let (channels, rate, lang) = (b.channels, b.sample_rate, b.language.clone());
                if let Stream::Audio(e) = &mut streams[i] {
                    (e.channels, e.sample_rate, e.language) = (channels, rate, lang);
                }
            }
            _ => {
                tracing::warn!(target: "mux", stream_id = id, "mpg: 13818-3 extension with no base stream it can pair with; left out");
                drop.push(i);
            }
        }
    }
    for i in drop.into_iter().rev() {
        streams.remove(i);
    }
    (!streams.is_empty()).then_some(Scan { streams, video_id })
}

#[cfg(test)]
mod dar_tests {
    use super::*;

    #[test]
    fn probe_video_reports_the_sequence_header_aspect() {
        // 720x576, aspect code 3 (16:9), frame-rate code 3 (25 fps).
        let es = [0, 0, 1, 0xB3, 0x2D, 0x02, 0x40, 0x33, 0, 0];
        let (_, res, _, dar) = probe_video(&es, Some(0x02)).expect("video");
        assert_eq!(res, Resolution::R576p);
        assert_eq!(dar, Some((16, 9)));
    }

    #[test]
    fn square_pixel_code_uses_the_coded_size() {
        // 320x240, aspect code 1 (square pixels), frame-rate code 4.
        let es = [0, 0, 1, 0xB3, 0x14, 0x00, 0xF0, 0x14, 0, 0];
        let (_, _, _, dar) = probe_video(&es, Some(0x02)).expect("video");
        assert_eq!(dar, Some((320, 240)));
    }

    #[test]
    fn mpeg1_pel_aspect_code_is_not_a_display_ratio() {
        // MPEG-1 352x288, code 3 is a pel aspect ratio (11172-2), not 16:9.
        let es = [0, 0, 1, 0xB3, 0x16, 0x01, 0x20, 0x33, 0, 0];
        let (_, _, _, dar) = probe_video(&es, Some(0x01)).expect("video");
        assert_eq!(dar, None);
    }
}
