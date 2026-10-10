//! `Disc::extract_tree` — decrypted file-tree extraction (`dir://`).
//!
//! The filesystem enumeration and decryption are entirely reused:
//! [`udf::read_filesystem`] yields the recursive [`UdfFs`] tree (BD and DVD
//! alike), and [`DecryptingSectorSource`]
//! applies AACS / CSS in-place. This module is the focused per-file producer:
//! tree walk, host-path mapping + sanitization, per-VTS CSS key grouping,
//! decrypt-and-stream-to-disk, sparse-gap handling, and truncate + rename.

use super::Disc;
use crate::decrypt::DecryptKeys;
use crate::error::{Error, Result};
use crate::io::tree_sink::TreeSink;
#[cfg(test)]
use crate::io::tree_sink::{
    available_space, dir_is_case_insensitive, probe_case_insensitive, sanitize_component,
    unique_probe_token,
};
use crate::sector::{DecryptingSectorSource, SectorSource};
use crate::udf::{self, DirEntry, UdfFs};
use std::path::{Path, PathBuf};

use crate::consts::{SECTOR_BYTES, SECTOR_BYTES_U64};
/// Content reads are issued in multiples of the AACS aligned unit (3 sectors) so the
/// decrypt step always sees whole units.
const AACS_UNIT_SECTORS: u32 = crate::aacs::content::ALIGNED_UNIT_SECTORS;
/// Read batch in sectors for content streaming (a throughput knob, not a
/// correctness one). A multiple of 3 so AACS units stay whole.
const READ_BATCH_SECTORS: u32 = 1536; // 3 MiB, multiple of 3
/// Bounded per-extent retries on a read that fails before a recorded hole.
const READ_RETRIES: u32 = 3;
/// Failed unit reads (one attempt each) per batch after which the rest is zero-filled unread.
const MAX_FAILED_UNIT_READS: u32 = 4;
/// Drive error granularity: a BD ECC cluster (32 sectors), also a whole number of DVD
/// ECC blocks (16). After a bad unit, reading resumes past this boundary.
const ECC_BLOCK_SECTORS: u64 = 32;
/// Sectors per read while cracking a VTS title key.
const CRACK_BATCH_SECTORS: u16 = 64;

/// Options for [`Disc::extract_tree`].
#[derive(Default)]
pub struct ExtractOptions<'a> {
    /// Overwrite into a non-empty destination directory. Without it a
    /// non-empty target is refused (mixing two discs' trees).
    pub force: bool,
    /// The rip's up-front key set (KU §3.1), scope `WholeDisc`. For an AACS disc every file
    /// is read through the set's reader: proven files by its map, the rest proven on
    /// arrival, and a readable unit no held key opens stops the run (E7032). `None`
    /// decrypts no AACS: the caller's `check_decryptable` gate refuses an AACS disc first.
    pub keys: Option<&'a crate::keys::KeyRing>,
}

/// Per-file extraction outcome.
#[derive(Debug, Clone)]
pub struct FileResult {
    /// Host-relative path (the mirrored disc path, sanitized).
    pub path: PathBuf,
    /// Bytes written that decrypted cleanly.
    pub bytes_good: u64,
    /// Bytes lost — unreadable sectors AND undecryptable units both land here
    /// (extract fails a bad decrypt loud, so it is zero-filled like a bad sector).
    pub bytes_unreadable: u64,
    /// True when the file was fully written (renamed from `.partial`).
    pub complete: bool,
}

/// Aggregate result of an [`extract_tree`](Disc::extract_tree) run.
#[derive(Debug, Clone, Default)]
pub struct ExtractResult {
    /// Per-file results, in extraction order.
    pub files: Vec<FileResult>,
    /// Aggregate good bytes across all files.
    pub bytes_good: u64,
    /// Aggregate lost bytes — bad sectors AND undecryptable units (one bucket).
    pub bytes_unreadable: u64,
    /// True when every file completed and no loss was recorded.
    pub complete: bool,
    /// True when the run stopped early on an interrupt / progress halt.
    pub halted: bool,
}

impl ExtractResult {
    /// Total bytes lost. A non-zero value means the extraction is holed; the CLI
    /// exits non-zero so a script can re-run through the `iso://` multipass path.
    pub fn bytes_lost(&self) -> u64 {
        self.bytes_unreadable
    }
}

/// One file scheduled for extraction, resolved against the raw reader in the
/// structure phase (before the reader is moved into the decrypting decorator).
struct PlannedFile {
    /// Host-relative path (sanitized, collision-checked).
    host_rel: PathBuf,
    /// Disc path components (for VTS grouping / diagnostics).
    disc_name: String,
    /// Declared file size in bytes (trim target).
    size: u64,
    /// Inline (ICB-embedded) data, if any. When `Some`, `extents` is empty.
    inline: Option<Vec<u8>>,
    /// Absolute disc extents, each carrying whether it was ever RECORDED (an
    /// ECMA-167 4/14.14.1.1 type-1 extent is allocated but not recorded: it
    /// occupies the file's byte space and its contents are zeros).
    extents: Vec<crate::udf::AbsExtent>,
    /// A file whose data cannot be located (an unreadable File Entry, or a bus-encrypted
    /// stream file the reader's bus map could not map): never read or written, only counted lost.
    unmapped: bool,
}

impl Disc {
    /// Extract this disc's **decrypted file tree** to `dest`. 1-shot,
    /// decrypt-only, no recovery loop.
    ///
    /// `reader` is consumed for content reads. `dest` receives the tree
    /// STRAIGHT IN (no auto-named subfolder). The caller must have run the
    /// pre-flight decrypt gate ([`check_decryptable`](crate::keys::check_decryptable)).
    ///
    /// Bad sectors become zero-filled holes; files are written `<name>.partial` and renamed
    /// on success. Progress is `PassKind::Extract` events, loss goes to `ctx.stats`; a Stop
    /// ends the run at a file or batch boundary, the in-flight file left `.partial`.
    pub fn extract_tree(
        &self,
        reader: &mut dyn SectorSource,
        dest: &Path,
        opts: &ExtractOptions,
        ctx: &crate::ctx::Ctx,
    ) -> Result<ExtractResult> {
        // Output dir policy (pre-flight, before any read).
        let mut sink = TreeSink::create(dest, opts.force)?;
        self.extract_into(reader, &mut sink, opts.keys, ctx)
    }

    /// The `dir://` chain: this disc's file tree read off `reader` (bad sectors zero-filled
    /// and counted per file), decrypted through `keys` (AACS) or per VTS (CSS), into
    /// `sink` ([`crate::io::open_tree_sink`]). The caller must have run the pre-flight
    /// decrypt gate ([`check_decryptable`](crate::keys::check_decryptable)).
    pub fn extract_into(
        &self,
        reader: &mut dyn SectorSource,
        sink: &mut TreeSink,
        keys: Option<&crate::keys::KeyRing>,
        ctx: &crate::ctx::Ctx,
    ) -> Result<ExtractResult> {
        // ── Phase 1: read the FS structure + all file extents (raw) ──────
        let fs = udf::read_filesystem(reader)?;
        let unmapped: Vec<u32> = reader
            .unmapped_stream_files()
            .iter()
            .map(|u| u.icb)
            .collect();
        let mut planned: Vec<PlannedFile> = Vec::new();
        let mut dirs: Vec<PathBuf> = Vec::new();
        // The collision fold depends on the REAL target volume: fold case only
        // where the host would (APFS/NTFS), never on a case-sensitive volume.
        sink.probe_case();
        plan_tree(
            reader,
            &fs,
            &fs.root,
            Path::new(""),
            "",
            true,
            sink,
            &unmapped,
            &mut planned,
            &mut dirs,
        )?;

        // Free-space pre-check: refuse up front if the tree won't fit, before
        // writing a single file (best-effort; only enforced where the platform
        // exposes free space).
        let required: u64 = planned
            .iter()
            .map(|p| p.size)
            .fold(0u64, |a, b| a.saturating_add(b));
        sink.reserve(required)?;

        // Create directories up-front so a leaf write never races a missing
        // parent. The root itself already exists.
        sink.make_dirs(&dirs)?;

        // Per-VTS CSS key map (DVD only): "VTS_xx" -> DecryptKeys. Built lazily
        // when a scrambled VOB group needs it. An AACS disc reads through the rip's set.
        let mut base_keys = self.decrypt_keys();
        let keyed = keys.filter(|s| s.is_aacs());
        if let Some(set) = keyed {
            crate::keys::check_decryptable(
                self,
                false,
                Some(set),
                &crate::keys::KeyScope::WholeDisc,
            )?;
            base_keys = set.decrypt_keys();
        }

        // Phase 2: stream each file through the decrypting decorator, which owns a
        // borrowing wrapper so the caller keeps `reader`. Keys swap per CSS VTS
        // group via `set_keys`; AACS reads through the set's reader.
        let mut dec = match keyed {
            Some(set) => {
                set.decrypting(Borrowed(reader), None, crate::keys::StopKind::Image, false)?
            }
            None => DecryptingSectorSource::new(Borrowed(reader), base_keys.clone()),
        };
        dec.observe(ctx);

        let mut result = ExtractResult::default();
        let total_bytes = required;
        let mut done_bytes: u64 = 0;
        // Cumulative zero-filled-unreadable bytes, so the live progress channel
        // can report the good/unreadable split instead of pinning unreadable at 0.
        let mut done_unreadable: u64 = 0;

        // CSS per-VTS key cache, consulted for every DVD-Video layout: a scrambled VTS is
        // found from its content and cracked, a clear one passes. A live drive scan never
        // records a disc-wide CSS key (BUG-1), so the scan's verdict cannot gate this.
        let is_css = matches!(base_keys, DecryptKeys::Css { .. })
            || (self.format == crate::disc::DiscFormat::Dvd && self.aacs.is_none());
        let mut vts_keys: std::collections::HashMap<String, DecryptKeys> =
            std::collections::HashMap::new();

        for pf in &planned {
            if ctx.halt.is_cancelled() {
                result.halted = true;
                break;
            }
            // §3.7 Note: "PC Host shall decrypt bus-encrypted Clip AV stream file"; this one
            // cannot be located to de-bus, so it is lost whole rather than written still encrypted.
            if pf.unmapped {
                let (fr, halted) =
                    unmapped_file(pf, total_bytes, &mut done_bytes, &mut done_unreadable, ctx);
                result.bytes_unreadable =
                    result.bytes_unreadable.saturating_add(fr.bytes_unreadable);
                result.files.push(fr);
                if halted {
                    result.halted = true;
                    break;
                }
                continue;
            }
            // Resolve the key for this file. CSS title VOBs need a per-VTS key;
            // clear nav (.IFO/.BUP/menu VOB) descrambles as a no-op with any
            // key, so the disc-wide key is fine for them too.
            if is_css {
                if let Some(vts) = vts_group_of(&pf.disc_name) {
                    let key = match vts_keys.get(&vts) {
                        Some(k) => k.clone(),
                        None => {
                            let halt = Some(&ctx.halt);
                            let k = match self
                                .resolve_vts_key(&vts, &planned, &mut dec, &base_keys, halt)
                            {
                                Err(Error::Halted) => {
                                    result.halted = true;
                                    break;
                                }
                                r => r?,
                            };
                            vts_keys.insert(vts.clone(), k.clone());
                            k
                        }
                    };
                    dec.set_keys(key);
                } else {
                    dec.set_keys(base_keys.clone());
                }
            }

            // A unit that fails to decrypt fails the read loud; extract_one_file
            // zero-fills it into bytes_unreadable, the same bucket as media damage.
            let (fr, halted) = extract_one_file(
                &mut dec,
                sink,
                pf,
                total_bytes,
                &mut done_bytes,
                &mut done_unreadable,
                ctx,
            )?;

            result.bytes_good = result.bytes_good.saturating_add(fr.bytes_good);
            result.bytes_unreadable = result.bytes_unreadable.saturating_add(fr.bytes_unreadable);
            result.files.push(fr);
            if halted {
                result.halted = true;
                break;
            }
        }

        result.complete = !result.halted
            && result.bytes_unreadable == 0
            && result.files.iter().all(|f| f.complete);
        Ok(result)
    }

    // Resolves the CSS title key for a VTS group from its scrambled title-VOB sectors. MUST
    // surface a failed crack as a hard error, never silently fall back to the disc-wide key.
    fn resolve_vts_key<S: SectorSource>(
        &self,
        vts: &str,
        planned: &[PlannedFile],
        dec: &mut DecryptingSectorSource<S>,
        base_keys: &DecryptKeys,
        halt: Option<&crate::halt::Halt>,
    ) -> Result<DecryptKeys> {
        // Gather this VTS's title VOBs (VTS_xx_1..9.VOB, _0.VOB menu excluded) BY
        // NAME, ascending. Directory order is authoring order not playback order,
        // but DVD-Video numbers VOBs in playback order, so name-sort is correct.
        let mut files: Vec<&PlannedFile> = planned
            .iter()
            .filter(|pf| {
                vts_group_of(&pf.disc_name).as_deref() == Some(vts) && is_title_vob(&pf.disc_name)
            })
            .collect();
        // Case-INSENSITIVE, matching the selection above. A byte-wise sort could
        // start the crack at the wrong part on a case-sensitive volume, exhausting
        // the budget in a clear run — the exact failure this ordering prevents.
        files.sort_by_key(|f| f.disc_name.to_ascii_uppercase());

        let mut extents: Vec<crate::disc::Extent> = Vec::new();
        for pf in files {
            // Unrecorded extents hold no bytes the VOB ever wrote and carry no
            // scrambled sector, so feeding them in wastes the shared crack budget.
            for ext in pf.extents.iter().filter(|e| e.recorded) {
                extents.push(crate::disc::Extent {
                    start_lba: ext.lba,
                    sector_count: (ext.len as u64).div_ceil(SECTOR_BYTES_U64) as u32,
                });
            }
        }
        if extents.is_empty() {
            return Ok(base_keys.clone());
        }
        // PLAYBACK ORDER — do NOT sort (the 1.5.1 bug: see `decrypt_keys_for_title`).
        // Crack the raw inner reader, NOT the decrypting view. A cancelled crack
        // (token or drive) is `Halted`, never a cached verdict.
        match crate::css::crack_key_outcome(dec.inner_mut(), &extents, CRACK_BATCH_SECTORS, halt) {
            crate::css::CrackOutcome::Cracked(state) => Ok(DecryptKeys::Css {
                title_key: state.title_key,
            }),
            // No scrambled sector anywhere in this VTS: the content is clear,
            // and any key descrambles it as a no-op. The disc-wide key is the
            // right answer, and this is the ONLY case that ever was.
            crate::css::CrackOutcome::Unencrypted => Ok(base_keys.clone()),
            // Scrambled sectors WERE seen and no key came out. `CssKeyMissing`
            // (unlike MUX, which skips on it) is `?`-propagated here, aborting
            // the whole extract rather than reusing the wrong key silently.
            crate::css::CrackOutcome::ScrambledUncracked => Err(Error::CssKeyMissing),
            crate::css::CrackOutcome::Halted => Err(Error::Halted),
            crate::css::CrackOutcome::Unreadable(e) => Err(e),
        }
    }
}

// A borrowing `SectorSource` wrapper: lets the decrypting decorator "own" an inner source for
// its lifetime while the caller keeps the underlying `&mut dyn SectorSource`
struct Borrowed<'a>(&'a mut dyn SectorSource);

impl SectorSource for Borrowed<'_> {
    fn capacity_sectors(&self) -> u32 {
        self.0.capacity_sectors()
    }
    fn read_sectors(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        recovery: bool,
    ) -> Result<usize> {
        self.0.read_sectors(lba, count, buf, recovery)
    }
    fn set_speed(&mut self, kbs: u16) {
        self.0.set_speed(kbs)
    }
    fn set_unit_base(&mut self, lba: u32) {
        self.0.set_unit_base(lba)
    }
    fn unmapped_stream_files(&self) -> &[crate::sector::bus_removal::UnmappedStreamFile] {
        self.0.unmapped_stream_files()
    }
    fn random_access(&self) -> bool {
        self.0.random_access()
    }
}

/// Recursively plan the host tree: collect directories to create and files to
/// extract, sanitizing each component and detecting host-path collisions.
/// Skips the top-level `AACS/`, `CERTIFICATE/`, and HD DVD's discovered `X!`
/// AACS directories (§7).
#[allow(clippy::too_many_arguments)]
fn plan_tree(
    reader: &mut dyn SectorSource,
    fs: &UdfFs,
    dir: &DirEntry,
    host_rel: &Path,
    disc_path: &str,
    is_root: bool,
    sink: &mut TreeSink,
    unmapped: &[u32],
    files: &mut Vec<PlannedFile>,
    dirs: &mut Vec<PathBuf>,
) -> Result<()> {
    // Same discovery the AACS capture reads use, so the two can't drift.
    let hddvd_aacs_dir = is_root
        .then(|| crate::aacs::find_hddvd_aacs_dir(fs))
        .flatten();
    for entry in &dir.entries {
        if entry.name.is_empty() {
            // The "parent" FID (".") has an empty name — skip.
            continue;
        }
        // The sink strips AACS / CERTIFICATE at the top level only (a deeper dir of
        // the same name is content).
        let aacs_dir = hddvd_aacs_dir.is_some_and(|d| std::ptr::eq(d, entry));
        if is_root && !TreeSink::keeps_top_level(&entry.name, aacs_dir) {
            continue;
        }
        let child_disc = format!("{disc_path}/{}", entry.name);
        let child_rel = sink.claim(host_rel, &entry.name, &child_disc, entry.is_dir)?;
        if entry.is_dir {
            dirs.push(child_rel.clone());
            plan_tree(
                reader,
                fs,
                entry,
                &child_rel,
                &child_disc,
                false,
                sink,
                unmapped,
                files,
                dirs,
            )?;
        } else if unmapped.contains(&entry.meta_lba) {
            files.push(PlannedFile {
                host_rel: child_rel,
                disc_name: entry.name.clone(),
                size: entry.size,
                inline: None,
                extents: Vec::new(),
                unmapped: true,
            });
        } else {
            let located = fs
                .inline_data_at(reader, entry.meta_lba)
                .and_then(|inline| {
                    let extents = if inline.is_some() {
                        Vec::new()
                    } else {
                        fs.extents_abs_at(reader, entry.meta_lba)?
                    };
                    Ok((inline, extents))
                });
            match located {
                Ok((inline, extents)) => files.push(PlannedFile {
                    host_rel: child_rel,
                    disc_name: entry.name.clone(),
                    size: entry.size,
                    inline,
                    extents,
                    unmapped: false,
                }),
                // A Stop, a dead bus or a gone source ends the run; it is not a damaged entry.
                Err(e)
                    if matches!(e, Error::Halted)
                        || e.is_scsi_transport_failure()
                        || e.is_source_terminated() =>
                {
                    return Err(e);
                }
                // A damaged File Entry loses that one file, not the tree around it.
                Err(e) => {
                    tracing::warn!(
                        target: "freemkv::extract",
                        file = %child_rel.display(),
                        code = e.code(),
                        "file entry unreadable; file lost whole"
                    );
                    files.push(PlannedFile {
                        host_rel: child_rel,
                        disc_name: entry.name.clone(),
                        size: entry.size,
                        inline: None,
                        extents: Vec::new(),
                        unmapped: true,
                    });
                }
            }
        }
    }
    Ok(())
}

// Accounts a file with no locatable data (an unreadable File Entry, or a bus-encrypted stream
// file that cannot be mapped to de-bus) as lost whole: nothing is written for it on the host.
fn unmapped_file(
    pf: &PlannedFile,
    total_bytes: u64,
    done_bytes: &mut u64,
    done_unreadable: &mut u64,
    ctx: &crate::ctx::Ctx,
) -> (FileResult, bool) {
    tracing::warn!(
        target: "freemkv::extract",
        file = %pf.host_rel.display(),
        "file cannot be located on the disc; not extracted"
    );
    *done_bytes = done_bytes.saturating_add(pf.size);
    *done_unreadable = done_unreadable.saturating_add(pf.size);
    report(ctx, *done_bytes, *done_unreadable, total_bytes);
    let fr = FileResult {
        path: pf.host_rel.clone(),
        bytes_good: 0,
        bytes_unreadable: pf.size,
        complete: false,
    };
    (fr, ctx.halt.is_cancelled())
}

// Extracts one file via `<host>.partial` (bad sectors -> zero holes), then renames.
// Returns `(FileResult, halted)`; `halted` stops the run (an unfinished file stays
// `.partial`; a finished inline file is already renamed).
fn extract_one_file<S: SectorSource>(
    dec: &mut DecryptingSectorSource<S>,
    sink: &TreeSink,
    pf: &PlannedFile,
    total_bytes: u64,
    done_bytes: &mut u64,
    done_unreadable: &mut u64,
    ctx: &crate::ctx::Ctx,
) -> Result<(FileResult, bool)> {
    let mut file = sink.begin(&pf.host_rel, pf.size)?;

    let mut fr = FileResult {
        path: pf.host_rel.clone(),
        bytes_good: 0,
        bytes_unreadable: 0,
        complete: false,
    };

    // Inline (ICB-embedded) file: data already in hand, no decrypt path (nav
    // files are clear). Write verbatim, trimmed to size.
    if let Some(bytes) = &pf.inline {
        let n = (pf.size as usize).min(bytes.len());
        file.write(&bytes[..n])?;
        fr.bytes_good = n as u64;
        // Data shorter than the declared size: the padded tail is lost, not good.
        let gap = pf.size - n as u64;
        fr.bytes_unreadable = gap;
        *done_unreadable = done_unreadable.saturating_add(gap);
        file.finish()?;
        fr.complete = true;
        *done_bytes = done_bytes.saturating_add(pf.size);
        report(ctx, *done_bytes, *done_unreadable, total_bytes);
        return Ok((fr, ctx.halt.is_cancelled()));
    }

    let mut written: u64 = 0;
    let mut buf = vec![0u8; READ_BATCH_SECTORS as usize * SECTOR_BYTES];
    'extents: for &crate::udf::AbsExtent {
        lba: abs_lba,
        len: byte_len,
        recorded,
    } in &pf.extents
    {
        if written >= pf.size {
            break;
        }
        // ECMA-167 4/14.14.1.1 type 1: allocated but NOT recorded. Write zeros
        // WITHOUT reading the media (mirrors `UdfFs::read_file_limited`) — skipping
        // the extent entirely would slide every later extent's bytes down.
        if !recorded {
            let sectors = (byte_len as u64).div_ceil(SECTOR_BYTES_U64);
            let hole_bytes = (sectors * SECTOR_BYTES_U64).min(pf.size.saturating_sub(written));
            let mut left = hole_bytes;
            for b in buf.iter_mut() {
                *b = 0;
            }
            while left > 0 {
                let n = left.min(buf.len() as u64) as usize;
                file.write(&buf[..n])?;
                written = written.saturating_add(n as u64);
                *done_bytes = done_bytes.saturating_add(n as u64);
                left -= n as u64;
            }
            fr.bytes_good = fr.bytes_good.saturating_add(hole_bytes);
            report(ctx, *done_bytes, *done_unreadable, total_bytes);
            if ctx.halt.is_cancelled() {
                return Ok((fr, true));
            }
            if written >= pf.size {
                break 'extents;
            }
            continue;
        }
        // Anchor AACS unit alignment at THIS extent's start, not LBA 0 or the
        // file's first extent: a fragmented extent's start LBA is arbitrary, so
        // the gate must re-anchor per extent or a later read false-holes. No-op for CSS/None.
        dec.set_unit_base(abs_lba);
        let sectors = (byte_len as u64).div_ceil(SECTOR_BYTES_U64) as u32;
        let mut sector_off: u32 = 0;
        while sector_off < sectors {
            // AACS: read whole units (see `whole_unit_batch`). A 1-2 sector tail
            // partial is handled by `decrypt_sectors`'s trailing-partial contract:
            // clear stays clear, scrambled fails loud as DecryptFailed.
            let batch = whole_unit_batch(sectors - sector_off);
            let want = batch as usize * SECTOR_BYTES;
            let start = abs_lba.checked_add(sector_off);
            let blanked_before = dec.blanked_units();
            // Bytes of this batch that could not be read (zero-filled below).
            let lost: u64;
            match start.filter(|l| l.checked_add(batch - 1).is_some()) {
                // Crafted extent past u32::MAX: no such sector — a hole, not a wrapped read.
                None => lost = want as u64,
                Some(lba) => match read_batch_narrowed(dec, lba, batch, &mut buf[..want]) {
                    Ok(l) => lost = l,
                    // Drive-level Stop: leave the `.partial`, same as an opts halt.
                    Err(Error::Halted) => return Ok((fr, true)),
                    Err(e) => return Err(e),
                },
            }
            let chunk_bytes = want as u64;
            // Clip the chunk to the remaining file size on the final extent.
            let remaining = pf.size.saturating_sub(written);
            let usable = chunk_bytes.min(remaining) as usize;
            {
                // Unreadable ranges were zero-filled by `read_batch_narrowed`; record the
                // holes and keep going (no abort, no sweep-skip).
                let lost = lost.min(usable as u64);
                if lost > 0 {
                    ctx.stats.add_skip(lost);
                }
                file.write(&buf[..usable])?;
                // Damaged AACS units the reader blanked are unreadable, not good bytes.
                let unit = crate::aacs::content::ALIGNED_UNIT_LEN as u64;
                let blanked = (dec.blanked_units() - blanked_before) * unit;
                let blanked = blanked.min(usable as u64 - lost);
                let bad = lost + blanked;
                fr.bytes_good = fr.bytes_good.saturating_add(usable as u64 - bad);
                fr.bytes_unreadable = fr.bytes_unreadable.saturating_add(bad);
                *done_unreadable = done_unreadable.saturating_add(bad);
            }
            written = written.saturating_add(usable as u64);
            *done_bytes = done_bytes.saturating_add(usable as u64);
            report(ctx, *done_bytes, *done_unreadable, total_bytes);
            sector_off += batch;
            if ctx.halt.is_cancelled() {
                // Leave the `.partial`; do NOT rename — this file stays incomplete.
                return Ok((fr, true));
            }
            if written >= pf.size {
                break 'extents;
            }
        }
    }

    // Extents that under-cover the declared size leave a zero-padded tail: count it lost
    // so the file is not reported clean.
    if written < pf.size {
        let gap = pf.size - written;
        fr.bytes_unreadable = fr.bytes_unreadable.saturating_add(gap);
        *done_unreadable = done_unreadable.saturating_add(gap);
        *done_bytes = done_bytes.saturating_add(gap);
    }
    file.finish()?;
    fr.complete = true;
    Ok((fr, false))
}

// Sizes the next FILE-ANCHORED content read in whole AACS units, capped at READ_BATCH_SECTORS,
// rounded DOWN to 3-sector units unless it's the extent's final tail.
fn whole_unit_batch(remaining: u32) -> u32 {
    let mut batch = remaining.min(READ_BATCH_SECTORS);
    if batch >= AACS_UNIT_SECTORS && batch < remaining {
        batch -= batch % AACS_UNIT_SECTORS;
    }
    batch
}

// Reads a batch (one retry); if it fails, re-reads it one AACS unit at a time, once each. A
// media-bad unit zero-fills, unread, up to the first unit at/after its ECC block's end; past
// MAX_FAILED_UNIT_READS the rest is zero-filled; an undecryptable unit loses only itself. Returns zero-filled bytes; Err = stop/key set.
fn read_batch_narrowed<S: SectorSource>(
    dec: &mut DecryptingSectorSource<S>,
    lba: u32,
    count: u32,
    buf: &mut [u8],
) -> Result<u64> {
    if count <= AACS_UNIT_SECTORS {
        if read_batch(dec, lba, count, buf)? {
            return Ok(0);
        }
        buf.fill(0);
        return Ok(buf.len() as u64);
    }
    if read_tries(dec, lba, count, buf, 1)? == Tried::Good {
        return Ok(0);
    }
    let mut lost = 0u64;
    let mut off = 0u32;
    let mut failed = 0u32;
    while off < count {
        let n = AACS_UNIT_SECTORS.min(count - off);
        let unit = &mut buf[off as usize * SECTOR_BYTES..(off + n) as usize * SECTOR_BYTES];
        let tried = read_tries(dec, lba + off, n, unit, 0)?;
        if tried == Tried::Good {
            off += n;
            continue;
        }
        failed += 1;
        if tried == Tried::Undecryptable {
            unit.fill(0);
            lost += unit.len() as u64;
            off += n;
            if failed >= MAX_FAILED_UNIT_READS {
                buf[off as usize * SECTOR_BYTES..count as usize * SECTOR_BYTES].fill(0);
                lost += u64::from(count - off) * SECTOR_BYTES as u64;
                off = count;
            }
            continue;
        }
        // End of the ECC block holding the unit's last sector (a straddled boundary skips
        // the later block), rounded up to a unit start; the rest when over budget.
        let end = u64::from(lba) + u64::from(off + n);
        let gap = (end.div_ceil(ECC_BLOCK_SECTORS) * ECC_BLOCK_SECTORS - end) as u32;
        let next = if failed >= MAX_FAILED_UNIT_READS {
            count
        } else {
            (off + n + gap.div_ceil(AACS_UNIT_SECTORS) * AACS_UNIT_SECTORS).min(count)
        };
        let hole = &mut buf[off as usize * SECTOR_BYTES..next as usize * SECTOR_BYTES];
        hole.fill(0);
        lost += hole.len() as u64;
        off = next;
    }
    Ok(lost)
}

// One batch with bounded retries; Ok(false) = hole. Short reads retry; DecryptFailed
// holes at once (never succeeds). The only Err is `Halted` (a user Stop).
fn read_batch<S: SectorSource>(
    dec: &mut DecryptingSectorSource<S>,
    lba: u32,
    count: u32,
    buf: &mut [u8],
) -> Result<bool> {
    Ok(read_tries(dec, lba, count, buf, READ_RETRIES)? == Tried::Good)
}

#[derive(PartialEq)]
enum Tried {
    Good,
    // Media/read failure: the drive lost the whole ECC block.
    Media,
    // Read fine but the unit cannot be decrypted; the next unit may still be fine.
    Undecryptable,
}

fn read_tries<S: SectorSource>(
    dec: &mut DecryptingSectorSource<S>,
    lba: u32,
    count: u32,
    buf: &mut [u8],
    retries: u32,
) -> Result<Tried> {
    let mut attempt = 0;
    loop {
        let last = attempt >= retries;
        match dec.read_sectors(lba, count as u16, buf, true) {
            Ok(n) if n >= buf.len() => return Ok(Tried::Good),
            Ok(_) if last => return Ok(Tried::Media),
            Ok(_) => {}
            Err(Error::Halted) => return Err(Error::Halted),
            // The key set's loud stop (KU §2.4): never a hole, the run stops `.partial`.
            Err(e @ (Error::WholeDiscKeyMissing | Error::NoDiscKey { .. })) => return Err(e),
            // A dead bus or gone source is not a hole: zero-filling the rest of the tree
            // would finalize every remaining file as complete (the Read stage's rule too).
            Err(e) if e.is_scsi_transport_failure() || e.is_source_terminated() => return Err(e),
            Err(Error::DecryptFailed) => return Ok(Tried::Undecryptable),
            Err(_) if last => return Ok(Tried::Media),
            Err(_) => {}
        }
        attempt += 1;
    }
}

/// Emit the run's progress as an [`Event::Pass`](crate::Event::Pass).
fn report(ctx: &crate::ctx::Ctx, done: u64, unreadable: u64, total: u64) {
    let pp = crate::progress::PassProgress {
        kind: crate::progress::PassKind::Extract,
        work_done: done,
        work_total: total,
        // `done` counts good AND unreadable bytes together; split them so a
        // live-progress-only consumer sees a holed extraction as holed,
        // not a clean climb to 100% (this used to pin unreadable at 0).
        bytes_good_total: done.saturating_sub(unreadable),
        bytes_unreadable_total: unreadable,
        bytes_pending_total: 0,
        bytes_retryable_total: 0,
        bytes_total_disc: total,
        disc_duration_secs: None,
        bytes_bad_in_main_title: 0,
        main_title_duration_secs: None,
        main_title_size_bytes: None,
        located: Default::default(),
    };
    ctx.emit(crate::event::Event::Pass(&pp));
}

/// DVD VTS group key for a `VTS_xx_*` file name, else `None`. e.g.
/// `VTS_01_1.VOB` → `Some("VTS_01")`. Case-insensitive on the prefix.
fn vts_group_of(name: &str) -> Option<String> {
    let up = name.to_ascii_uppercase();
    let rest = up.strip_prefix("VTS_")?;
    // rest like "01_1.VOB" — take the 2-digit group number.
    let group = rest.split('_').next()?;
    if group.len() == 2 && group.bytes().all(|b| b.is_ascii_digit()) {
        Some(format!("VTS_{group}"))
    } else {
        None
    }
}

/// True for a CSS-scrambled title VOB (`VTS_xx_1.VOB`..`_9.VOB`). The menu
/// VOB `VTS_xx_0.VOB` and the `.IFO`/`.BUP` are clear, so they are excluded
/// from the per-VTS key crack.
fn is_title_vob(name: &str) -> bool {
    let up = name.to_ascii_uppercase();
    if !up.ends_with(".VOB") {
        return false;
    }
    // VTS_xx_y.VOB → y is the part number; 0 = menu (clear), 1..9 = title.
    let stem = up.trim_end_matches(".VOB");
    match stem.rsplit_once('_') {
        Some((_, part)) => part.len() == 1 && matches!(part.as_bytes()[0], b'1'..=b'9'),
        None => false,
    }
}

#[cfg(test)]
#[path = "extract_tests.rs"]
mod tests;
