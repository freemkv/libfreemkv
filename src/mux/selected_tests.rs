use super::*;
use crate::disc::{AudioChannels, AudioStream, Codec, LabelPurpose, SampleRate, Stream};
use crate::mux::select::{PidFilter, StreamSelection};

struct Frames(DiscTitle, std::collections::VecDeque<PesFrame>);

impl PesSource for Frames {
    fn read(&mut self) -> std::io::Result<Option<PesFrame>> {
        Ok(self.1.pop_front())
    }
    fn info(&self) -> &DiscTitle {
        &self.0
    }
    fn codec_private(&self, track: usize) -> Option<Vec<u8>> {
        Some(vec![track as u8])
    }
}

fn audio(pid: u16) -> Stream {
    Stream::Audio(AudioStream {
        pid,
        codec: Codec::Ac3,
        channels: AudioChannels::Stereo,
        language: "eng".into(),
        sample_rate: SampleRate::S48,
        secondary: false,
        purpose: LabelPurpose::Normal,
        label: String::new(),
    })
}

fn frame(track: usize) -> PesFrame {
    PesFrame {
        discard_padding_ns: 0,
        track,
        pts: 0,
        keyframe: true,
        data: vec![track as u8],
        duration_ns: None,
        source: None,
        coding: None,
    }
}

// A container source keeps only the selected tracks, renumbered in order.
#[test]
fn a_container_keeps_the_selected_tracks_renumbered() {
    let mut title = DiscTitle::empty();
    title.streams = vec![audio(1), audio(2), audio(3)];
    let src = Frames(title, (0..3).map(frame).collect());
    let sel = StreamSelection {
        audio: PidFilter::Only(vec![3]),
        subtitle: PidFilter::All,
    };
    let mut s = SelectedSource::wrap(Box::new(src), &sel).unwrap();
    assert_eq!(s.info().streams.len(), 1);
    assert_eq!(s.codec_private(0), Some(vec![2]));
    let f = s.read().unwrap().expect("the kept track's frame");
    assert_eq!((f.track, f.data.as_slice()), (0, &[2u8][..]));
    assert!(s.read().unwrap().is_none());
}

struct Rich(Frames);

impl PesSource for Rich {
    fn read(&mut self) -> std::io::Result<Option<PesFrame>> {
        self.0.read()
    }
    fn info(&self) -> &DiscTitle {
        self.0.info()
    }
    fn track_timing(&self, track: usize) -> TrackTiming {
        TrackTiming {
            codec_delay_ns: track as u64 * 10,
            seek_preroll_ns: 0,
        }
    }
    fn headers_ready(&self) -> bool {
        false
    }
    fn config_changes(&self) -> Vec<(usize, u64)> {
        vec![(1, 4), (2, 7)]
    }
    fn errors(&self) -> u64 {
        5
    }
    fn lost_bytes(&self) -> u64 {
        9
    }
}

// Per-track answers are asked of the inner source by its own index and come back under the
// kept numbering; the source-wide counters and readiness pass straight through.
#[test]
fn inner_answers_are_translated_and_counters_pass_through() {
    let mut title = DiscTitle::empty();
    title.streams = vec![audio(1), audio(2), audio(3)];
    let src = Rich(Frames(title, Default::default()));
    let sel = StreamSelection {
        audio: PidFilter::Only(vec![3]),
        subtitle: PidFilter::All,
    };
    let s = SelectedSource::wrap(Box::new(src), &sel).unwrap();
    assert_eq!(
        s.track_timing(0).codec_delay_ns,
        20,
        "inner track 2's timing"
    );
    assert_eq!(s.track_timing(1), TrackTiming::default());
    assert_eq!(
        s.config_changes(),
        vec![(0, 7)],
        "renumbered, unkept dropped"
    );
    assert_eq!((s.errors(), s.lost_bytes()), (5, 9));
    assert!(!s.headers_ready());
}

// An unknown PID refuses at open, as on a disc title.
#[test]
fn an_unknown_pid_refuses() {
    let mut title = DiscTitle::empty();
    title.streams = vec![audio(1)];
    let sel = StreamSelection {
        audio: PidFilter::Only(vec![9]),
        subtitle: PidFilter::All,
    };
    let err = SelectedSource::wrap(Box::new(Frames(title, Default::default())), &sel)
        .err()
        .expect("unknown pid");
    assert!(matches!(
        err,
        crate::error::Error::SelectionPidUnknown { pid: 9 }
    ));
}
