//! `Disc::extract_tree` — decrypted file-tree extraction (`dir://`).
//!
//! The filesystem enumeration and decryption are entirely reused:
//! [`udf::read_filesystem`] yields the recursive [`UdfFs`] tree (BD and DVD
//! alike), and [`DecryptingSectorSource`](crate::sector::DecryptingSectorSource)
//! applies AACS / CSS in-place. This module is the focused per-file producer:
//! tree walk, host-path mapping, sparse-gap handling and file publication.
//! Decryption, alignment and read recovery belong to the prepared reader and Read stage.

use super::Disc;
#[cfg(test)]
use crate::decrypt::DecryptKeys;
use crate::error::{Error, Result};
use crate::io::tree_sink::TreeSink;
#[cfg(all(test, unix))]
use crate::io::tree_sink::available_space;
#[cfg(test)]
use crate::io::tree_sink::{
    dir_is_case_insensitive, probe_case_insensitive, sanitize_component, unique_probe_token,
};
use crate::sector::SectorSource;
use crate::udf::{self, DirEntry, UdfFs};
use std::path::{Path, PathBuf};

use crate::consts::{SECTOR_BYTES, SECTOR_BYTES_U64};
const READ_BATCH_SECTORS: u32 = crate::sector::read_stage::READ_BATCH_SECTORS;
#[cfg(test)]
use crate::css::disc::{is_title_vob, vts_group_of};
#[cfg(test)]
use crate::sector::DecryptingSectorSource;
#[cfg(test)]
use crate::sector::read_stage::{
    MAX_FAILED_UNIT_READS, READ_RETRIES, read_batch, read_batch_narrowed, whole_unit_batch,
};
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

/// One file scheduled for extraction, resolved against the reader in the
/// structure phase, before any file data is read.
struct PlannedFile {
    /// Host-relative path (sanitized, collision-checked).
    host_rel: PathBuf,
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
        let empty = crate::keys::KeyRing::none();
        let keys = opts.keys.unwrap_or(&empty);
        let mut prepared = match keys.whole_disc_reader(self, reader, Some(&ctx.halt)) {
            Ok(reader) => reader,
            Err(Error::Halted) => {
                return Ok(ExtractResult {
                    halted: true,
                    ..Default::default()
                });
            }
            Err(e) => return Err(e),
        };
        self.extract_into(&mut prepared, &mut sink, ctx)
    }

    /// Extract files from an already-prepared reader. This adapter never acquires keys,
    /// configures decryption or implements a read/recovery policy.
    pub fn extract_into<S: SectorSource>(
        &self,
        reader: &mut crate::whole_disc::PreparedReader<S>,
        sink: &mut TreeSink,
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

        reader.observe(ctx);

        let mut result = ExtractResult::default();
        let total_bytes = required;
        let mut done_bytes: u64 = 0;
        // Cumulative zero-filled-unreadable bytes, so the live progress channel
        // can report the good/unreadable split instead of pinning unreadable at 0.
        let mut done_unreadable: u64 = 0;

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
            // A unit that fails to decrypt fails the read loud; extract_one_file
            // zero-fills it into bytes_unreadable, the same bucket as media damage.
            let (fr, halted) = extract_one_file(
                reader,
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
}

/// Recursively plan the host tree: collect directories to create and files to
/// extract, sanitizing each component and detecting host-path collisions.
/// Preserves every directory, including protection and BD-J identity metadata.
#[allow(clippy::too_many_arguments)]
fn plan_tree(
    reader: &mut dyn SectorSource,
    fs: &UdfFs,
    dir: &DirEntry,
    host_rel: &Path,
    disc_path: &str,
    sink: &mut TreeSink,
    unmapped: &[u32],
    files: &mut Vec<PlannedFile>,
    dirs: &mut Vec<PathBuf>,
) -> Result<()> {
    for entry in &dir.entries {
        if entry.name.is_empty() {
            // The "parent" FID (".") has an empty name — skip.
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
                sink,
                unmapped,
                files,
                dirs,
            )?;
        } else if unmapped.contains(&entry.meta_lba) {
            files.push(PlannedFile {
                host_rel: child_rel,

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
    dec: &mut crate::whole_disc::PreparedReader<S>,
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

    // Inline (ICB-embedded) file: data already in hand, never read through the
    // extent walk (nav files are clear). Write verbatim, trimmed to size.
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
        let sectors = (byte_len as u64).div_ceil(SECTOR_BYTES_U64) as u32;
        let mut walk = dec.walk(
            vec![crate::disc::Extent {
                start_lba: abs_lba,
                sector_count: sectors,
            }],
            ctx,
        )?;
        let loss = walk.loss();
        let mut accounted_loss = 0;
        loop {
            let blanked_before = dec.blanked_units();
            match walk.next(dec, &mut buf) {
                Ok(false) => break,
                Ok(true) => {}
                Err(Error::Halted) => return Ok((fr, true)),
                Err(e) => return Err(e),
            }
            let want = buf.len();
            let lost = loss.bytes().saturating_sub(accounted_loss);
            accounted_loss = loss.bytes();
            let chunk_bytes = want as u64;
            // Clip the chunk to the remaining file size on the final extent.
            let remaining = pf.size.saturating_sub(written);
            let usable = chunk_bytes.min(remaining) as usize;
            {
                // Unreadable ranges were zero-filled by the extent walk; record the
                // holes and keep going (no abort).
                let lost = lost.min(usable as u64);
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

#[cfg(test)]
#[path = "extract_tests.rs"]
mod tests;
