//! DVD title scanning — IFO parsing, stream mapping, VOB extent building.

use super::*;
use crate::ifo;
use crate::sector::SectorSource;
use crate::udf;

impl Disc {
    /// Scan DVD titles from IFO files (VIDEO_TS.IFO + VTS_XX_0.IFO).
    pub(super) fn scan_dvd_titles(
        reader: &mut dyn SectorSource,
        udf_fs: &udf::UdfFs,
    ) -> Vec<DiscTitle> {
        let dvd_info = match ifo::parse_vmg(reader, udf_fs) {
            Ok(info) => info,
            Err(_) => return Vec::new(),
        };

        let mut titles = Vec::new();
        let mut title_number: u16 = 0;

        for ts in &dvd_info.title_sets {
            let video_stream = Stream::Video(VideoStream {
                pid: 0xE0, // DVD video PID (standard MPEG PS video stream)
                codec: ts.video.codec,
                resolution: ts.video.resolution,
                frame_rate: match ts.video.standard.as_str() {
                    "PAL" => FrameRate::F25,
                    _ => FrameRate::F29_97,
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
                    let codec = a.codec;
                    Stream::Audio(AudioStream {
                        pid: 0xBD00 + i as u16, // DVD private stream 1 sub-IDs
                        codec,
                        channels: AudioChannels::from_count(a.channels),
                        language: a.language.clone(),
                        sample_rate: SampleRate::from_hz(a.sample_rate),
                        secondary: false,
                        purpose: crate::disc::LabelPurpose::Normal,
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
                        let start = ts.vob_start_sector.saturating_add(cell.first_sector);
                        let count = cell
                            .last_sector
                            .saturating_sub(cell.first_sector)
                            .saturating_add(1);
                        Extent {
                            start_lba: start,
                            sector_count: count,
                        }
                    })
                    .collect();

                let size_bytes: u64 = extents.iter().map(|e| e.sector_count as u64 * 2048).sum();

                // Build pre-formatted palette codec_data for VobSub subtitle streams
                let codec_data = dvd_title
                    .palette
                    .as_ref()
                    .map(|pal| crate::mux::codec::dvdsub::format_palette(pal));

                // Map DvdSubtitleAttr to Stream::Subtitle
                let subtitle_streams: Vec<Stream> = ts
                    .subtitle_streams
                    .iter()
                    .enumerate()
                    .map(|(i, s)| {
                        Stream::Subtitle(SubtitleStream {
                            pid: 0x20 + i as u16, // DVD sub-stream IDs 0x20-0x3F
                            codec: Codec::DvdSub,
                            language: s.language.clone(),
                            forced: false,
                            qualifier: crate::disc::LabelQualifier::None,
                            codec_data: codec_data.clone(),
                        })
                    })
                    .collect();

                let mut streams = vec![video_stream.clone()];
                streams.extend(audio_streams.iter().cloned());
                streams.extend(subtitle_streams);

                let chapters: Vec<Chapter> = dvd_title
                    .chapter_times
                    .iter()
                    .enumerate()
                    .map(|(i, &t)| Chapter {
                        time_secs: t,
                        name: format!("Chapter {}", i + 1),
                    })
                    .collect();

                titles.push(DiscTitle {
                    playlist: format!("VTS_{:02}_{}.VOB", ts.vts_number, title_number),
                    playlist_id: title_number,
                    duration_secs: dvd_title.duration_secs,
                    size_bytes,
                    clips: Vec::new(),
                    streams,
                    chapters,
                    extents,
                    content_format: ContentFormat::MpegPs,
                    codec_privates: Vec::new(),
                });
            }
        }

        titles
    }
}
