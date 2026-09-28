//! What an `mpg://` program stream carries, and on which stream id (mpg-output-design v5
//! §2.1-§2.2 as narrowed by J24 to the DVD core the 2000 edition of H.222.0 covers).

use crate::disc::{Codec, DiscTitle, Stream};
use crate::mux::fit::{FitReport, SkipReason};

/// How one carried track is packetized.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Carriage {
    /// `0xE0`; `mpeg1` selects stream_type 0x01 over 0x02 (MS-22).
    Video { mpeg1: bool },
    /// `0xC0|n` (MS-27 "110x xxxx … audio stream number"), n in 0-7 only (design §2.2).
    MpegAudio { stream_id: u8 },
    /// `0xD0|n`, the 13818-3 extension of the base `0xC0|n` (design §3).
    Mp2Extension { stream_id: u8, base: usize },
    /// private_stream_1 sub-stream (MS-14: "user definable"; DVD convention, MS-29).
    Private { sub_id: u8, kind: PrivateKind },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PrivateKind {
    Ac3,
    Dts,
    Lpcm { channels: usize, rate: u32 },
    Subpicture,
}

impl PrivateKind {
    /// The DVD sub-id range (design §2.2 table).
    fn range(self) -> std::ops::RangeInclusive<u8> {
        match self {
            PrivateKind::Ac3 => 0x80..=0x87,
            PrivateKind::Dts => 0x88..=0x8F,
            PrivateKind::Lpcm { .. } => 0xA0..=0xA7,
            PrivateKind::Subpicture => 0x20..=0x3F,
        }
    }
}

/// The fit plan and the carriage of every included track.
#[derive(Debug, Clone)]
pub(crate) struct Plan {
    pub report: FitReport,
    /// Per `title.streams` index; `None` for a skipped (or declared-only extension) track.
    pub carriage: Vec<Option<Carriage>>,
}

enum Want {
    Video { mpeg1: bool },
    MpegAudio,
    Extension,
    Private(PrivateKind),
}

fn want(s: &Stream, have_video: bool) -> Result<Want, SkipReason> {
    // MS-22 stream types 0x01-0x04 and 0x06; MS-27 stream ids; J24 the DVD core only.
    match s {
        Stream::Video(v) if v.is_mvc_dependent() || have_video => Err(SkipReason::SecondaryVideo),
        // J24: H.264, HEVC and VC-1 await the 2013+ text (F8); AV1 has no PS carriage.
        Stream::Video(v) => match v.codec {
            Codec::Mpeg2 => Ok(Want::Video { mpeg1: false }),
            Codec::Mpeg1 => Ok(Want::Video { mpeg1: true }),
            _ => Err(SkipReason::UnmappableVideo),
        },
        Stream::Audio(a) if a.is_mp2_extension() => Ok(Want::Extension),
        Stream::Audio(a) => match a.codec {
            Codec::Mp2 | Codec::Mp3 => Ok(Want::MpegAudio),
            Codec::Ac3 => Ok(Want::Private(PrivateKind::Ac3)),
            Codec::Dts => Ok(Want::Private(PrivateKind::Dts)),
            Codec::Lpcm => {
                let channels = usize::from(a.channels.count());
                let rate = a.sample_rate.hz() as u32;
                crate::mux::codec::lpcm::dvd_header(channels, rate, 16)
                    .map(|_| Want::Private(PrivateKind::Lpcm { channels, rate }))
                    .ok_or(SkipReason::UnmappableAudio)
            }
            // J24/F8: E-AC-3; F1: TrueHD, DTS-HD, AAC, FLAC, Opus.
            _ => Err(SkipReason::UnmappableAudio),
        },
        Stream::Subtitle(s) => match s.codec {
            Codec::DvdSub => Ok(Want::Private(PrivateKind::Subpicture)),
            Codec::Pgs => Err(SkipReason::BitmapSubtitle),
            _ => Err(SkipReason::UnmappableSubtitle),
        },
    }
}

fn pid(s: &Stream) -> u16 {
    match s {
        Stream::Video(v) => v.pid,
        Stream::Audio(a) => a.pid,
        Stream::Subtitle(s) => s.pid,
    }
}

/// Plan `title` for `mpg://`. DVD sources keep their source ids (the IR PID is the id);
/// other tracks take the lowest free id of their range, in track order; a full range is
/// [`SkipReason::NoStreamId`]. An extension track follows its base's `n` when the base is
/// carried, and is otherwise in neither half of the report (J23: reported only once seen).
pub(crate) fn plan(title: &DiscTitle) -> Plan {
    let n = title.streams.len();
    let mut wants: Vec<Result<Want, SkipReason>> = Vec::with_capacity(n);
    let mut have_video = false;
    for s in &title.streams {
        let w = want(s, have_video);
        have_video |= matches!(w, Ok(Want::Video { .. }));
        wants.push(w);
    }
    let mut carriage: Vec<Option<Carriage>> = vec![None; n];
    let mut skipped: Vec<(usize, SkipReason)> = Vec::new();
    let mut used = [false; 256];
    let mut used_private = [false; 256];

    // Pass 1: DVD source ids, kept where the IR PID is already the id.
    for (i, w) in wants.iter().enumerate() {
        let p = pid(&title.streams[i]);
        match w {
            Ok(Want::MpegAudio) if (0xC0..=0xC7).contains(&p) && !used[p as usize] => {
                used[p as usize] = true;
                carriage[i] = Some(Carriage::MpegAudio { stream_id: p as u8 });
            }
            Ok(Want::Private(k)) => {
                let sub = if p >> 8 == 0xBD { p & 0xFF } else { p };
                if sub <= 0xFF && k.range().contains(&(sub as u8)) && !used_private[sub as usize] {
                    used_private[sub as usize] = true;
                    carriage[i] = Some(Carriage::Private {
                        sub_id: sub as u8,
                        kind: *k,
                    });
                }
            }
            _ => {}
        }
    }
    // Pass 2: everything else, lowest free id in track order.
    for (i, w) in wants.iter().enumerate() {
        if carriage[i].is_some() {
            continue;
        }
        match w {
            Ok(Want::Video { mpeg1 }) => carriage[i] = Some(Carriage::Video { mpeg1: *mpeg1 }),
            Ok(Want::MpegAudio) => match (0xC0..=0xC7u8).find(|&id| !used[id as usize]) {
                Some(id) => {
                    used[id as usize] = true;
                    carriage[i] = Some(Carriage::MpegAudio { stream_id: id });
                }
                None => skipped.push((i, SkipReason::NoStreamId)),
            },
            Ok(Want::Private(k)) => match k.range().find(|&id| !used_private[id as usize]) {
                Some(id) => {
                    used_private[id as usize] = true;
                    carriage[i] = Some(Carriage::Private {
                        sub_id: id,
                        kind: *k,
                    });
                }
                None => skipped.push((i, SkipReason::NoStreamId)),
            },
            Ok(Want::Extension) => {}
            Err(r) => skipped.push((i, *r)),
        }
    }
    // Pass 3: extensions bind to their carried base `0xC0|n` (same n, design §3).
    for (i, w) in wants.iter().enumerate() {
        if !matches!(w, Ok(Want::Extension)) {
            continue;
        }
        let base_pid = 0x00C0 | (pid(&title.streams[i]) & 0x07);
        let base = title.streams.iter().position(
            |s| matches!(s, Stream::Audio(a) if !a.is_mp2_extension() && a.pid == base_pid),
        );
        if let Some((b, Some(Carriage::MpegAudio { stream_id }))) = base.map(|b| (b, carriage[b])) {
            carriage[i] = Some(Carriage::Mp2Extension {
                stream_id: 0xD0 | (stream_id & 0x07),
                base: b,
            });
        }
    }
    skipped.sort_unstable_by_key(|&(i, _)| i);
    let included = (0..n).filter(|&i| carriage[i].is_some()).collect();
    Plan {
        report: FitReport { included, skipped },
        carriage,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disc::{
        AudioChannels, AudioStream, ColorSpace, FrameRate, HdrFormat, LabelPurpose, LabelQualifier,
        Resolution, SampleRate, SubtitleStream, VideoStream,
    };

    fn video(codec: Codec) -> Stream {
        Stream::Video(VideoStream {
            pid: 0xE0,
            codec,
            resolution: Resolution::R480i,
            frame_rate: FrameRate::F29_97,
            hdr: HdrFormat::Sdr,
            color_space: ColorSpace::Bt709,
            display_aspect: None,
            secondary: false,
            label: String::new(),
            measured_cicp: None,
        })
    }
    fn audio(pid: u16, codec: Codec, rate: SampleRate, label: &str) -> Stream {
        Stream::Audio(AudioStream {
            pid,
            codec,
            channels: AudioChannels::Stereo,
            language: "eng".into(),
            sample_rate: rate,
            secondary: false,
            purpose: LabelPurpose::Normal,
            label: label.into(),
        })
    }
    fn sub(pid: u16, codec: Codec) -> Stream {
        Stream::Subtitle(SubtitleStream {
            pid,
            codec,
            language: "eng".into(),
            forced: false,
            qualifier: LabelQualifier::None,
            codec_data: None,
        })
    }
    fn title(streams: Vec<Stream>) -> DiscTitle {
        DiscTitle {
            streams,
            ..DiscTitle::empty()
        }
    }

    #[test]
    fn a_dvd_title_keeps_its_source_ids() {
        let t = title(vec![
            video(Codec::Mpeg2),
            audio(0x00C1, Codec::Mp2, SampleRate::S48, ""),
            audio(
                0x00D1,
                Codec::Mp2,
                SampleRate::S48,
                crate::disc::MP2_EXTENSION_LABEL,
            ),
            audio(0xBD81, Codec::Ac3, SampleRate::S48, ""),
            audio(0xBD89, Codec::Dts, SampleRate::S48, ""),
            audio(0xBDA2, Codec::Lpcm, SampleRate::S48, ""),
            sub(0x0025, Codec::DvdSub),
        ]);
        let p = plan(&t);
        assert_eq!(p.report.included, vec![0, 1, 2, 3, 4, 5, 6]);
        assert!(p.report.skipped.is_empty());
        assert_eq!(p.carriage[1], Some(Carriage::MpegAudio { stream_id: 0xC1 }));
        assert_eq!(
            p.carriage[2],
            Some(Carriage::Mp2Extension {
                stream_id: 0xD1,
                base: 1
            })
        );
        assert!(matches!(
            p.carriage[3],
            Some(Carriage::Private {
                sub_id: 0x81,
                kind: PrivateKind::Ac3
            })
        ));
        assert!(matches!(
            p.carriage[4],
            Some(Carriage::Private {
                sub_id: 0x89,
                kind: PrivateKind::Dts
            })
        ));
        assert!(matches!(
            p.carriage[5],
            Some(Carriage::Private {
                sub_id: 0xA2,
                kind: PrivateKind::Lpcm {
                    channels: 2,
                    rate: 48_000
                }
            })
        ));
        assert!(matches!(
            p.carriage[6],
            Some(Carriage::Private {
                sub_id: 0x25,
                kind: PrivateKind::Subpicture
            })
        ));
    }

    // J24 (coordinator JUDGEMENT): H.264/HEVC/VC-1 go through the excluded-track note until
    // the 2013+ H.222.0 text is sourced (F8). Per design; do not change without a citation.
    #[test]
    fn j24_newer_video_and_e_ac3_are_excluded_with_a_reason() {
        for codec in [Codec::H264, Codec::Hevc, Codec::Vc1, Codec::Av1] {
            let p = plan(&title(vec![video(codec)]));
            assert_eq!(
                p.report.skipped,
                vec![(0, SkipReason::UnmappableVideo)],
                "{codec:?}"
            );
        }
        let t = title(vec![
            video(Codec::Mpeg2),
            audio(0x1100, Codec::Ac3Plus, SampleRate::S48, ""),
            audio(0x1101, Codec::TrueHd, SampleRate::S48, ""),
            audio(0x1102, Codec::Lpcm, SampleRate::S192, ""),
            sub(0x1200, Codec::Pgs),
            sub(0x1201, Codec::Srt),
            video(Codec::Mpeg2),
        ]);
        assert_eq!(
            plan(&t).report.skipped,
            vec![
                (1, SkipReason::UnmappableAudio),
                (2, SkipReason::UnmappableAudio),
                (3, SkipReason::UnmappableAudio),
                (4, SkipReason::BitmapSubtitle),
                (5, SkipReason::UnmappableSubtitle),
                (6, SkipReason::SecondaryVideo),
            ]
        );
    }

    // Design §2.2: "Overflow → NoStreamId"; MPEG audio "0xC0–0xC7 … 8", AC-3 "8 each".
    #[test]
    fn a_full_range_is_no_stream_id() {
        let mut s = vec![video(Codec::Mpeg2)];
        s.extend((0..9).map(|i| audio(0x1100 + i, Codec::Ac3, SampleRate::S48, "")));
        s.extend((0..9).map(|i| audio(0x1200 + i, Codec::Mp2, SampleRate::S48, "")));
        let p = plan(&title(s));
        assert_eq!(
            p.report.skipped,
            vec![(9, SkipReason::NoStreamId), (18, SkipReason::NoStreamId)]
        );
        assert_eq!(
            p.carriage[1],
            Some(Carriage::Private {
                sub_id: 0x80,
                kind: PrivateKind::Ac3
            })
        );
        assert_eq!(
            p.carriage[10],
            Some(Carriage::MpegAudio { stream_id: 0xC0 })
        );
    }

    // J23: a declared extension whose base is not carried is in neither half (seen-only).
    #[test]
    fn an_extension_without_a_carried_base_is_not_planned_out() {
        let t = title(vec![
            video(Codec::Mpeg2),
            audio(
                0x00D3,
                Codec::Mp2,
                SampleRate::S48,
                crate::disc::MP2_EXTENSION_LABEL,
            ),
        ]);
        let p = plan(&t);
        assert_eq!(p.report.included, vec![0]);
        assert!(p.report.skipped.is_empty());
    }

    #[test]
    fn allocated_ids_avoid_kept_dvd_ids() {
        let t = title(vec![
            video(Codec::Mpeg2),
            audio(0x1100, Codec::Mp2, SampleRate::S48, ""),
            audio(0x00C0, Codec::Mp2, SampleRate::S48, ""),
        ]);
        let p = plan(&t);
        assert_eq!(p.carriage[1], Some(Carriage::MpegAudio { stream_id: 0xC1 }));
        assert_eq!(p.carriage[2], Some(Carriage::MpegAudio { stream_id: 0xC0 }));
    }
}
