use super::*;
use crate::aacs::content::ALIGNED_UNIT_LEN;
use crate::consts::BD_SOURCE_PACKET_BYTES;

const KEY: [u8; 16] = [0x3C; 16];

// `units` clear BD-TS units flagged encrypted (CPI 11), varied by index.
fn plain_units(units: usize) -> Vec<u8> {
    let mut v: Vec<u8> = (0..units * ALIGNED_UNIT_LEN)
        .map(|i| (i % 251) as u8 | 1)
        .collect();
    for p in v.chunks_mut(BD_SOURCE_PACKET_BYTES) {
        p[0] |= 0xC0;
        p[4] = 0x47;
    }
    v
}

fn encrypted(plain: &[u8]) -> Vec<u8> {
    let mut v = plain.to_vec();
    for u in v.chunks_mut(ALIGNED_UNIT_LEN) {
        assert!(crate::aacs::content::encrypt_unit(u, &KEY));
    }
    v
}

// What a decrypt leaves: the plaintext with every packet's CPI cleared (KS-5).
fn cleared(plain: &[u8]) -> Vec<u8> {
    let mut v = plain.to_vec();
    for p in v.chunks_mut(BD_SOURCE_PACKET_BYTES) {
        p[0] &= 0x3F;
    }
    v
}

fn job(lba: u32, mapped: (u32, u32), tally: &Tally) -> Job {
    Job {
        lba,
        keys: DecryptKeys::Aacs {
            unit_keys: vec![(0, KEY)],
            format: crate::disc::ContentFormat::BdTs,
        },
        map: Arc::new(AacsKeyMap::from_ranges(vec![(mapped.0, mapped.1, 0)])),
        content: None,
        tally: tally.clone(),
    }
}

fn tally() -> Tally {
    Tally {
        blanked: Arc::default(),
        ctx: None,
    }
}

#[test]
fn an_unarmed_thread_never_defers() {
    let buf = vec![0u8; 4096];
    assert!(!try_defer(&buf, || unreachable!("not armed")));
}

#[test]
fn only_reads_inside_the_armed_buffer_defer() {
    let mut pool = vec![0u8; 4 * ALIGNED_UNIT_LEN];
    let armed = arm(&mut pool, 4 * ALIGNED_UNIT_LEN);
    let t = tally();
    assert!(try_defer(
        &pool[ALIGNED_UNIT_LEN..3 * ALIGNED_UNIT_LEN],
        || job(3, (0, 12), &t)
    ));
    let scratch = vec![0u8; ALIGNED_UNIT_LEN];
    assert!(!try_defer(&scratch, || unreachable!("outside the pool")));
    assert!(!try_defer(&pool[..0], || unreachable!("an empty read")));
    let jobs = armed.finish(&pool).expect("finish");
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].range, ALIGNED_UNIT_LEN..3 * ALIGNED_UNIT_LEN);
    assert!(!try_defer(&pool, || unreachable!("finish disarms")));
}

#[test]
fn arm_reserves_room_for_the_largest_read() {
    let mut pool = Vec::new();
    let armed = arm(&mut pool, 3 * ALIGNED_UNIT_LEN);
    assert!(pool.capacity() >= 3 * ALIGNED_UNIT_LEN);
    let base = pool.as_ptr();
    pool.resize(3 * ALIGNED_UNIT_LEN, 0);
    assert_eq!(
        pool.as_ptr(),
        base,
        "a read within the reserve never moves it"
    );
    drop(armed);
}

#[test]
fn dropping_the_arm_disarms_and_discards_jobs() {
    let mut pool = vec![0u8; ALIGNED_UNIT_LEN];
    let armed = arm(&mut pool, ALIGNED_UNIT_LEN);
    let t = tally();
    assert!(try_defer(&pool, || job(0, (0, 3), &t)));
    drop(armed);
    assert!(!try_defer(&pool, || unreachable!("disarmed")));
}

// A buffer that moved, or a job past the batch's end, is refused rather than run elsewhere.
#[test]
fn finish_refuses_a_moved_buffer_or_a_job_past_the_end() {
    let mut pool = vec![0u8; 2 * ALIGNED_UNIT_LEN];
    let armed = arm(&mut pool, 2 * ALIGNED_UNIT_LEN);
    let t = tally();
    assert!(try_defer(&pool, || job(0, (0, 6), &t)));
    let other = vec![0u8; 2 * ALIGNED_UNIT_LEN];
    assert!(matches!(armed.finish(&other), Err(Error::DecryptFailed)));

    let armed = arm(&mut pool, 2 * ALIGNED_UNIT_LEN);
    assert!(try_defer(&pool, || job(0, (0, 6), &t)));
    pool.truncate(ALIGNED_UNIT_LEN);
    assert!(matches!(armed.finish(&pool), Err(Error::DecryptFailed)));

    // No job: nothing to place, so a moved buffer is fine.
    let armed = arm(&mut pool, ALIGNED_UNIT_LEN);
    assert!(armed.finish(&other).expect("no jobs").is_empty());
}

// The deferred decrypt is byte for byte the in-place one, run later on the batch.
#[test]
fn run_decrypts_exactly_as_the_read_would_have() {
    let plain = plain_units(4);
    let mut pool = encrypted(&plain);
    let armed = arm(&mut pool, plain.len());
    let t = tally();
    assert!(try_defer(&pool[ALIGNED_UNIT_LEN..], || job(3, (0, 12), &t)));
    assert!(try_defer(&pool[..ALIGNED_UNIT_LEN], || job(0, (0, 12), &t)));
    let jobs = armed.finish(&pool).expect("finish");
    assert_ne!(pool, cleared(&plain), "nothing decrypted before the stage");
    run(&mut pool, jobs).expect("run");
    assert_eq!(pool, cleared(&plain));
    assert_eq!(t.blanked.load(Ordering::Relaxed), 0);
}

// An encrypted unit no map range keys fails the batch, as the in-place decrypt does.
#[test]
fn run_fails_on_the_first_job_that_fails() {
    let plain = plain_units(2);
    let mut pool = encrypted(&plain);
    let armed = arm(&mut pool, plain.len());
    let t = tally();
    assert!(try_defer(&pool, || job(0, (300, 303), &t)));
    let jobs = armed.finish(&pool).expect("finish");
    assert!(matches!(run(&mut pool, jobs), Err(Error::DecryptFailed)));
}

// `settle` runs the jobs inside the slice it is given now and leaves the others queued.
#[test]
fn settle_runs_only_the_jobs_inside_its_slice() {
    let plain = plain_units(2);
    let mut pool = encrypted(&plain);
    let armed = arm(&mut pool, plain.len());
    let t = tally();
    assert!(try_defer(&pool[..ALIGNED_UNIT_LEN], || job(0, (0, 6), &t)));
    assert!(try_defer(&pool[ALIGNED_UNIT_LEN..], || job(3, (0, 6), &t)));
    settle(&mut pool[..ALIGNED_UNIT_LEN]).expect("settle");
    let want = cleared(&plain);
    assert_eq!(pool[..ALIGNED_UNIT_LEN], want[..ALIGNED_UNIT_LEN]);
    let jobs = armed.finish(&pool).expect("finish");
    assert_eq!(jobs.len(), 1, "the second unit is still deferred");
    run(&mut pool, jobs).expect("run");
    assert_eq!(pool, want);
    settle(&mut pool).expect("settle while unarmed is a no-op");
}

#[test]
fn settle_reports_a_failing_job() {
    let plain = plain_units(1);
    let mut pool = encrypted(&plain);
    let _armed = arm(&mut pool, plain.len());
    let t = tally();
    assert!(try_defer(&pool, || job(0, (300, 303), &t)));
    assert!(matches!(settle(&mut pool), Err(Error::DecryptFailed)));
}

// A blank count reaches the reader's tally and, when observed, the run's stats and events.
#[test]
fn blanked_units_count_into_the_tally_and_the_run() {
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let rec = seen.clone();
    let ctx =
        crate::ctx::Ctx::default().with_events(Arc::new(move |e: &crate::event::Event<'_>| {
            if let crate::event::Event::UnitBlanked { lba, units } = *e {
                rec.lock().unwrap().push((lba, units));
            }
        }));
    let t = Tally {
        blanked: Arc::default(),
        ctx: Some(ctx.clone()),
    };
    t.add(30, 0);
    t.add(30, 2);
    assert_eq!(t.blanked.load(Ordering::Relaxed), 2);
    assert_eq!(ctx.stats.snapshot().units_blanked, 2);
    assert_eq!(
        *seen.lock().unwrap(),
        vec![(30, 2)],
        "no event for zero units"
    );
}

// A decrypting reader over another one sees plaintext: the inner reader's deferred decrypt is
// settled before the outer one judges the bytes, so nothing is left for the stage.
#[test]
fn a_reader_over_a_deferring_reader_settles_it_first() {
    use crate::sector::{DecryptingSectorSource, Keying, SectorSource};
    let plain = plain_units(3);
    let image = encrypted(&plain);
    let keys = DecryptKeys::Aacs {
        unit_keys: vec![(0, KEY)],
        format: crate::disc::ContentFormat::BdTs,
    };
    let map = Arc::new(AacsKeyMap::from_ranges(vec![(0, 9, 0)]));
    let mut inner = DecryptingSectorSource::new(
        crate::test_util::MemSource::new(image),
        Keying::ring(keys, map, None),
    );
    inner.set_unit_base(0);
    let mut outer = DecryptingSectorSource::new(inner, DecryptKeys::None);
    let mut pool = vec![0u8; plain.len()];
    let armed = arm(&mut pool, plain.len());
    assert_eq!(
        outer.read_sectors(0, 9, &mut pool, false).unwrap(),
        plain.len()
    );
    assert_eq!(
        pool,
        cleared(&plain),
        "decrypted before the outer reader returned"
    );
    assert!(armed.finish(&pool).unwrap().is_empty());
}

// The same keyed read deferred while armed, then run by the stage, is the in-place read.
#[test]
fn an_armed_keyed_read_defers_and_runs_to_the_same_bytes() {
    use crate::sector::{DecryptingSectorSource, Keying, SectorSource};
    let plain = plain_units(3);
    let keys = DecryptKeys::Aacs {
        unit_keys: vec![(0, KEY)],
        format: crate::disc::ContentFormat::BdTs,
    };
    let map = Arc::new(AacsKeyMap::from_ranges(vec![(0, 9, 0)]));
    let mut r = DecryptingSectorSource::new(
        crate::test_util::MemSource::new(encrypted(&plain)),
        Keying::ring(keys, map, None),
    );
    r.set_unit_base(0);
    let mut pool = vec![0u8; plain.len()];
    let armed = arm(&mut pool, plain.len());
    r.read_sectors(0, 9, &mut pool, false).unwrap();
    assert_eq!(
        pool,
        encrypted(&plain),
        "still ciphertext: the decrypt waits"
    );
    let jobs = armed.finish(&pool).unwrap();
    assert_eq!(jobs.len(), 1);
    run(&mut pool, jobs).unwrap();
    assert_eq!(pool, cleared(&plain));
}
