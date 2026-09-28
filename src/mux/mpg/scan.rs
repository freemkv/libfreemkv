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

// MPEG-2 (or MPEG-1) video from its sequence header: codec, resolution, frame rate.
fn probe_video(es: &[u8], map_type: Option<u8>) -> Option<(Codec, Resolution, FrameRate)> {
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
    }
    Some((codec, res, rate))
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

/// Streams of the program stream in `head`, in map order (else id order), with the
/// `pid`s the DVD demux routes (`PsPacket::dvd_pid`). `None` when `head` holds no pack.
pub(crate) fn scan_streams(head: &[u8]) -> Option<Vec<Stream>> {
    if !head.windows(4).any(|w| w == [0, 0, 1, 0xBA]) {
        return None;
    }
    let mut d = PsDemuxer::new();
    let mut seen: BTreeMap<Key, Vec<u8>> = BTreeMap::new();
    let packets: Vec<PsPacket> = d.feed(head).into_iter().chain(d.flush()).collect();
    for p in packets.iter().filter(|p| p.dvd_pid().is_some()) {
        let key = (
            if (0xE0..=0xEF).contains(&p.stream_id) {
                0xE0
            } else {
                p.stream_id
            },
            p.sub_stream_id,
        );
        let e = seen.entry(key).or_default();
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
                if streams.iter().any(|s| matches!(s, Stream::Video(_))) {
                    continue;
                }
                let Some((codec, res, rate)) = probe_video(es, ty) else {
                    tracing::warn!(target: "mux", stream_id = key.0, "mpg: video stream with no MPEG-1/2 sequence header in the head; left out");
                    continue;
                };
                video_res = res;
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
                    display_aspect: None,
                    secondary: false,
                    label: String::new(),
                    measured_cicp: None,
                }));
            }
            (id @ 0xD0..=0xD7, None) => {
                // With a map, the hierarchy descriptor decides (type 5, MS-23); without one,
                // the sync word: 13818-3 ext_syncword 0x7FF is an extension, 0xFFF audio.
                let is_ext = match &map {
                    Some(_) => descriptors(desc).any(|(t, b)| {
                        t == 4
                            && b.first()
                                .is_some_and(|x| x & 0x0F == pack::HIERARCHY_EXTENSION)
                    }),
                    None => es.len() >= 2 && es[0] == 0x7F && es[1] & 0xF0 == 0xF0,
                };
                if is_ext {
                    let base = u16::from(0xC0 | (id & 7));
                    let Some(Stream::Audio(b)) = streams
                        .iter()
                        .find(|s| matches!(s, Stream::Audio(a) if a.pid == base))
                        .cloned()
                    else {
                        tracing::warn!(target: "mux", stream_id = id, "mpg: 13818-3 extension with no base stream; left out");
                        continue;
                    };
                    streams.push(audio(
                        u16::from(id),
                        Codec::Mp2,
                        b.channels,
                        b.sample_rate,
                        b.language.clone(),
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
                    0xC0..=0xC7 => audio(
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
    (!streams.is_empty()).then_some(streams)
}
