//! [`ResolvedKeySet::resolve`] (KU §2.3). RED STUB: today's keying rules, replaced next.

use super::{Inner, KeyResolution, KeyScope, ResolveKeysOptions, ResolvedKeySet};
use crate::aacs::trace::ResolutionTrace;
use crate::decrypt::{AacsKeyMap, DecryptKeys};
use crate::disc::Disc;
use crate::error::Result;
use crate::halt::Halt;
use crate::keysource::{DiscInputsCtx, MIN_SAMPLE_UNITS};
use crate::sector::SectorSource;
use crate::session::KeySourceFactory;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Time for the J13 retry: injected so tests use a fake clock and never sleep.
pub(crate) trait Clock {
    fn now(&self) -> Duration;
    /// Wait `d`, returning `Err(Halted)` as soon as `halt` is cancelled.
    fn sleep(&self, d: Duration, halt: Option<&Halt>) -> Result<()>;
}

/// The wall clock.
pub(crate) struct RealClock(Instant);

impl RealClock {
    pub(crate) fn new() -> Self {
        RealClock(Instant::now())
    }
}

impl Clock for RealClock {
    fn now(&self) -> Duration {
        self.0.elapsed()
    }
    fn sleep(&self, d: Duration, halt: Option<&Halt>) -> Result<()> {
        match halt {
            Some(h) => h.wait(d),
            None => {
                std::thread::sleep(d);
                Ok(())
            }
        }
    }
}

// Today's rules: every source asked once with main-title samples (first non-empty wins),
// then the per-title map with its held-count single-key rule and extent inheritance.
pub(crate) fn resolve(
    disc: &Disc,
    reader: &mut dyn SectorSource,
    scope: KeyScope,
    sources: &KeySourceFactory,
    opts: ResolveKeysOptions,
    _clock: &dyn Clock,
) -> Result<KeyResolution> {
    if let Some(h) = opts.halt {
        h.check()?;
    }
    let (Some(aacs), false, Some(mut inputs)) =
        (disc.aacs.as_ref(), scope == KeyScope::None, disc.inputs())
    else {
        return Ok(KeyResolution {
            keys: ResolvedKeySet::none(),
            trace: ResolutionTrace::new(),
        });
    };
    inputs.samples = disc.content_samples(reader, MIN_SAMPLE_UNITS);
    let built = sources();
    let keys = crate::keysource::fetch_unit_keys(&built, &DiscInputsCtx::new(&inputs));
    let requests = built.len() as u32;
    drop(built);
    let mut dk = DecryptKeys::Aacs {
        unit_keys: keys
            .iter()
            .enumerate()
            .map(|(i, k)| (i as u32 + 1, k.key))
            .collect(),
        format: disc.content_format,
    };
    let sel: Vec<usize> = match &scope {
        KeyScope::Titles(v) => v.clone(),
        _ => (0..disc.titles.len()).collect(),
    };
    let mut ranges = Vec::new();
    for &t in &sel {
        let map = crate::mux::resolve_mux_key_map(
            reader,
            &disc.titles[t],
            &mut dk,
            None,
            disc.content_format,
            opts.halt,
        )?;
        ranges.extend_from_slice(map.ranges());
    }
    ranges.sort_unstable_by_key(|r| r.0);
    ranges.dedup_by_key(|r| r.0);
    let mut inner = Inner::empty();
    inner.aacs = true;
    inner.disc_hash = aacs.disc_hash.clone();
    inner.capacity = disc.capacity_sectors;
    inner.format = disc.format;
    inner.content_format = disc.content_format;
    inner.vid = Some(aacs.volume_id).filter(|v| *v != [0u8; 16]);
    inner.n_decl = disc.declared_cps_units();
    inner.scope = scope;
    if let DecryptKeys::Aacs { unit_keys, .. } = &dk {
        inner.pool = unit_keys.iter().map(|&(_, k)| k).collect();
    }
    inner.keyed = sel.len();
    inner.proven = (0..inner.pool.len()).collect();
    inner.spans = ranges
        .iter()
        .map(|&(s, e, _, _)| (s, e - s, s as u64))
        .collect();
    inner.map = Arc::new(AacsKeyMap::from_ranges_phased(ranges));
    inner.requests = requests;
    Ok(KeyResolution {
        keys: ResolvedKeySet(Arc::new(inner)),
        trace: ResolutionTrace::new(),
    })
}
