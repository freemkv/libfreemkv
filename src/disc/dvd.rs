//! DVD title scanning — IFO parsing, stream mapping, VOB extent building.

use super::*;
use crate::ifo;
use crate::sector::SectorReader;
use crate::udf;

impl Disc {
    /// Scan DVD titles from IFO files (VIDEO_TS.IFO + VTS_XX_0.IFO).
    pub(super) fn scan_dvd_titles(reader: &mut dyn SectorReader, udf_fs: &udf::UdfFs) -> Vec<DiscTitle> {
        let dvd_info = match ifo::parse_vmg(reader, udf_fs) {
            Ok(info) => info,
            Err(_) => return Vec::new(),
        };

        let mut titles = Vec::new();
        let mut title_number: u16 = 0;

        for ts in &dvd_info.title_sets {
            // Map DvdVideoAttr to Stream::Video
            let video_codec = match ts.video.codec.as_str() {
                "mpeg2" => Codec::Mpeg2,
                "mpeg1" => Codec::Mpeg2, // treat MPEG-1 as MPEG-2 for container purposes
                _ => Codec::Mpeg2,
            };

            let video_stream = Stream::Video(VideoStream {
                pid: 0xE0, // DVD video PID (standard MPEG PS video stream)
                codec: video_codec,
                resolution: ts.video.resolution.clone(),
                frame_rate: match ts.video.standard.as_str() {
                    "PAL" => "25".to_string(),
                    _ => "29.97".to_string(),
                },
                hdr: HdrFormat::Sdr,
                color_space: ColorSpace::Bt709,
                secondary: false,
                label: String::new(),
            });

            // Map DvdAudioAttr to Stream::Audio
            let audio_streams: Vec<Stream> = ts
                .audio_streams
                .iter()
                .enumerate()
                .map(|(i, a)| {
                    let codec = match a.codec.as_str() {
                        "ac3" => Codec::Ac3,
                        "dts" => Codec::Dts,
                        "lpcm" => Codec::Lpcm,
                        "mpeg1" | "mpeg2" => Codec::Mpeg2,
                        _ => Codec::Unknown(0),
                    };
                    let channels = match a.channels {
                        1 => "mono".to_string(),
                        2 => "stereo".to_string(),
                        6 => "5.1".to_string(),
                        8 => "7.1".to_string(),
                        n => format!("{}ch", n),
                    };
                    let sample_rate = match a.sample_rate {
                        48000 => "48kHz".to_string(),
                        96000 => "96kHz".to_string(),
                        sr => format!("{}kHz", sr / 1000),
                    };
                    Stream::Audio(AudioStream {
                        pid: 0xBD00 + i as u16, // DVD private stream 1 sub-IDs
                        codec,
                        channels,
                        language: a.language.clone(),
                        sample_rate,
                        secondary: false,
                        label: String::new(),
                    })
                })
                .collect();

            for dvd_title in &ts.titles {
                title_number += 1;

                // Build extents from cell sector ranges (absolute = vob_start + cell offset)
                let extents: Vec<Extent> = dvd_title
                    .cells
                    .iter()
                    .map(|cell| {
                        let start = ts.vob_start_sector + cell.first_sector;
                        let count = cell.last_sector.saturating_sub(cell.first_sector) + 1;
                        Extent {
                            start_lba: start,
                            sector_count: count,
                        }
                    })
                    .collect();

                let size_bytes: u64 = extents
                    .iter()
                    .map(|e| e.sector_count as u64 * 2048)
                    .sum();

                let mut streams = vec![video_stream.clone()];
                streams.extend(audio_streams.iter().cloned());

                titles.push(DiscTitle {
                    playlist: format!("VTS_{:02}_{}.VOB", ts.vts_number, title_number),
                    playlist_id: title_number,
                    duration_secs: dvd_title.duration_secs,
                    size_bytes,
                    clips: Vec::new(),
                    streams,
                    extents,
                    content_format: ContentFormat::MpegPs,
                });
            }
        }

        titles
    }
}
