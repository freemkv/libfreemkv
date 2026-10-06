//! Track selection over an already-demuxed source (pipeline design §2.4, DM5): a container
//! input (`mkv://`, `mp4://`, `m2ts://`, `network://`, `stdio://`) arrives as framed tracks,
//! so the selection keeps a subset of its tracks and renumbers them, exactly as a disc
//! title pruned before its demux is built. Video is always kept.

use crate::disc::DiscTitle;
use crate::pes::{PesFrame, PesSource, TrackTiming};

pub(crate) struct SelectedSource {
    inner: Box<dyn PesSource>,
    // Inner track index of each kept track, in order.
    kept: Vec<usize>,
    // Inner track index → kept index.
    map: Vec<Option<usize>>,
    info: DiscTitle,
}

impl SelectedSource {
    // `inner` restricted to what `selection` keeps; an All selection returns `inner`.
    pub(crate) fn wrap(
        inner: Box<dyn PesSource>,
        selection: &crate::StreamSelection,
    ) -> crate::error::Result<Box<dyn PesSource>> {
        if selection.is_all() {
            return Ok(inner);
        }
        let keep = selection.keeps_by_index(inner.info())?;
        let mut info = inner.info().clone();
        selection.apply(&mut info)?;
        let kept: Vec<usize> = (0..keep.len()).filter(|&i| keep[i]).collect();
        let mut map = vec![None; keep.len()];
        for (k, &i) in kept.iter().enumerate() {
            map[i] = Some(k);
        }
        Ok(Box::new(SelectedSource {
            inner,
            kept,
            map,
            info,
        }))
    }
}

impl PesSource for SelectedSource {
    fn read(&mut self) -> std::io::Result<Option<PesFrame>> {
        loop {
            let Some(mut f) = self.inner.read()? else {
                return Ok(None);
            };
            if let Some(Some(k)) = self.map.get(f.track) {
                f.track = *k;
                return Ok(Some(f));
            }
        }
    }

    fn info(&self) -> &DiscTitle {
        &self.info
    }

    fn track_timing(&self, track: usize) -> TrackTiming {
        self.kept
            .get(track)
            .map(|&i| self.inner.track_timing(i))
            .unwrap_or_default()
    }

    fn codec_private(&self, track: usize) -> Option<Vec<u8>> {
        self.kept
            .get(track)
            .and_then(|&i| self.inner.codec_private(i))
    }

    fn headers_ready(&self) -> bool {
        self.inner.headers_ready()
    }

    fn config_changes(&self) -> Vec<(usize, u64)> {
        self.inner
            .config_changes()
            .into_iter()
            .filter_map(|(i, n)| self.map.get(i).copied().flatten().map(|k| (k, n)))
            .collect()
    }

    fn errors(&self) -> u64 {
        self.inner.errors()
    }

    fn lost_bytes(&self) -> u64 {
        self.inner.lost_bytes()
    }
}

#[cfg(test)]
#[path = "selected_tests.rs"]
mod tests;
