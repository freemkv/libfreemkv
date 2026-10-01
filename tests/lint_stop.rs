//! Stop lints (stop design §5.8, ST-X2): grep tests over the production source.
//!
//! "These replace v4's Sim-motivated `disallowed-methods` list": clippy's list is
//! crate-wide, and these rules are scoped by directory and exempt tests. The walk
//! follows `mod` declarations from `src/lib.rs`, drops every item or file gated on a
//! test-only `cfg` (`test`, `feature = "test-util"`), and masks comments and literals,
//! so only code that ships is checked. Every allow-list entry names its site and must
//! still match, so an entry cannot outlive its reason.

use std::collections::BTreeMap;
use std::path::Path;

struct Src {
    path: String,
    raw: String,
    code: String,
    fns: Vec<(usize, usize, String)>,
}

impl Src {
    fn line(&self, at: usize) -> usize {
        self.code[..at].matches('\n').count() + 1
    }

    // The innermost `fn` whose body contains `at`.
    fn enclosing_fn(&self, at: usize) -> &str {
        self.fns
            .iter()
            .filter(|(s, e, _)| *s <= at && at < *e)
            .min_by_key(|(s, e, _)| e - s)
            .map_or("", |(_, _, n)| n.as_str())
    }

    fn hits(&self, pat: &str) -> Vec<usize> {
        self.code.match_indices(pat).map(|(i, _)| i).collect()
    }
}

fn is_ident(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

fn blank(out: &mut [u8], from: usize, to: usize) {
    for b in &mut out[from..to] {
        if *b != b'\n' {
            *b = b' ';
        }
    }
}

// Comments and string/char literal bodies become spaces; offsets and newlines are kept.
fn mask(src: &str) -> Vec<u8> {
    let s = src.as_bytes();
    let mut out = s.to_vec();
    let mut i = 0;
    while i < s.len() {
        let prev_ident = i > 0 && is_ident(s[i - 1]);
        if s[i..].starts_with(b"//") {
            let end = s[i..]
                .iter()
                .position(|&b| b == b'\n')
                .map_or(s.len(), |p| i + p);
            blank(&mut out, i, end);
            i = end;
        } else if s[i..].starts_with(b"/*") {
            let (mut depth, mut j) = (0, i);
            while j < s.len() {
                if s[j..].starts_with(b"/*") {
                    depth += 1;
                    j += 2;
                } else if s[j..].starts_with(b"*/") {
                    depth -= 1;
                    j += 2;
                    if depth == 0 {
                        break;
                    }
                } else {
                    j += 1;
                }
            }
            blank(&mut out, i, j);
            i = j;
        } else if s[i] == b'r' && !prev_ident || s[i..].starts_with(b"br") && !prev_ident {
            let start = if s[i] == b'b' { i + 2 } else { i + 1 };
            let hashes = s[start..].iter().take_while(|&&b| b == b'#').count();
            if s.get(start + hashes) != Some(&b'"') {
                i += 1;
                continue;
            }
            let body = start + hashes + 1;
            let close: Vec<u8> = std::iter::once(b'"')
                .chain(std::iter::repeat_n(b'#', hashes))
                .collect();
            let end = body
                + s[body..]
                    .windows(close.len())
                    .position(|w| w == close.as_slice())
                    .expect("unterminated raw string");
            blank(&mut out, body, end);
            i = end + close.len();
        } else if s[i] == b'"' {
            let mut j = i + 1;
            while s[j] != b'"' {
                j += if s[j] == b'\\' { 2 } else { 1 };
            }
            blank(&mut out, i + 1, j);
            i = j + 1;
        } else if s[i] == b'\'' {
            let end = if s.get(i + 1) == Some(&b'\\') {
                Some(i + 2 + s[i + 2..].iter().position(|&b| b == b'\'').unwrap())
            } else {
                let c = src[i + 1..].chars().next().map_or(1, char::len_utf8);
                (s.get(i + 1 + c) == Some(&b'\'')).then_some(i + 1 + c)
            };
            match end {
                Some(e) => {
                    blank(&mut out, i + 1, e);
                    i = e + 1;
                }
                None => i += 1, // a lifetime or label
            }
        } else {
            i += 1;
        }
    }
    out
}

// The index just past the `]` or `}` that closes the `[` or `{` at `open`.
fn close_of(code: &[u8], open: usize) -> usize {
    let (o, c) = match code[open] {
        b'[' => (b'[', b']'),
        b'{' => (b'{', b'}'),
        b => panic!("close_of on `{}`", b as char),
    };
    let mut depth = 0i32;
    for (k, &b) in code[open..].iter().enumerate() {
        if b == o {
            depth += 1;
        } else if b == c {
            depth -= 1;
            if depth == 0 {
                return open + k + 1;
            }
        }
    }
    panic!("unbalanced `{}`", o as char);
}

// The end of the item starting at `from`: its `{…}` body, or the first top-level `;` or
// `,` (a field, variant or arm); an enclosing block's `}` also ends it.
fn item_end(code: &[u8], from: usize) -> usize {
    let (mut depth, mut angle) = (0i32, 0i32);
    for k in from..code.len() {
        match code[k] {
            b'(' | b'[' => depth += 1,
            b')' | b']' => depth -= 1,
            b'<' => angle += 1,
            b'>' if k > 0 && !matches!(code[k - 1], b'-' | b'=') => angle -= 1,
            b'}' if depth == 0 => return k,
            b';' if depth == 0 => return k + 1,
            b',' if depth == 0 && angle <= 0 => return k + 1,
            b'{' if depth == 0 => {
                let end = close_of(code, k);
                return end + usize::from(code.get(end) == Some(&b';'));
            }
            _ => {}
        }
    }
    code.len()
}

// Whether a `cfg` predicate holds only in test builds (`test`, the `test-util` fixtures).
fn test_only(pred: &str) -> bool {
    let pred = pred.trim();
    if pred == "test" || pred.replace(' ', "") == "feature=\"test-util\"" {
        return true;
    }
    let Some((head, rest)) = pred.split_once('(') else {
        return false;
    };
    let inner = rest.strip_suffix(')').unwrap_or(rest);
    let mut args = Vec::new();
    let (mut depth, mut start) = (0, 0);
    for (k, c) in inner.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => depth -= 1,
            ',' if depth == 0 => {
                args.push(&inner[start..k]);
                start = k + 1;
            }
            _ => {}
        }
    }
    args.push(&inner[start..]);
    args.retain(|a| !a.trim().is_empty());
    match head.trim() {
        "all" => args.iter().any(|a| test_only(a)),
        "any" => !args.is_empty() && args.iter().all(|a| test_only(a)),
        _ => false,
    }
}

// The attributes `#[..]` / `#![..]` in `code`: (start, end, raw text inside the brackets).
fn attrs<'a>(code: &[u8], raw: &'a str) -> Vec<(usize, usize, &'a str)> {
    let mut out = Vec::new();
    for (i, _) in raw.match_indices('#') {
        if code[i] != b'#' {
            continue; // inside a masked comment or literal
        }
        let open = match (code.get(i + 1), code.get(i + 2)) {
            (Some(b'['), _) => i + 1,
            (Some(b'!'), Some(b'[')) => i + 2,
            _ => continue,
        };
        let end = close_of(code, open);
        out.push((i, end, &raw[open + 1..end - 1]));
    }
    out
}

fn cfg_pred(attr: &str) -> Option<&str> {
    let a = attr
        .trim()
        .strip_prefix("cfg")?
        .trim_start()
        .strip_prefix('(')?;
    a.strip_suffix(')')
}

// Blank every test-only item; `None` when the whole file is test-only (`#![cfg(test)]`).
fn drop_test_items(raw: &str, code: &mut [u8]) -> Option<()> {
    for (start, end, attr) in attrs(code, raw) {
        if code[start] != b'#' || !cfg_pred(attr).is_some_and(test_only) {
            continue;
        }
        if code[start + 1] == b'!' {
            return None;
        }
        let stop = item_end(code, end);
        blank(code, start, stop);
    }
    Some(())
}

fn fn_spans(code: &[u8]) -> Vec<(usize, usize, String)> {
    let text = std::str::from_utf8(code).unwrap();
    let mut out = Vec::new();
    for (i, _) in text.match_indices("fn ") {
        if i > 0 && is_ident(code[i - 1]) {
            continue;
        }
        let name: String = text[i + 3..]
            .trim_start()
            .chars()
            .take_while(|&c| c.is_alphanumeric() || c == '_')
            .collect();
        if name.is_empty() {
            continue;
        }
        let mut depth = 0i32;
        for k in i..code.len() {
            match code[k] {
                b'(' | b'[' => depth += 1,
                b')' | b']' => depth -= 1,
                b';' if depth == 0 => break,
                b'{' if depth == 0 => {
                    out.push((i, close_of(code, k), name));
                    break;
                }
                _ => {}
            }
        }
    }
    out
}

// Every production source file, reached from `src/lib.rs` through non-test `mod` items.
fn production() -> Vec<Src> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut out = Vec::new();
    let mut todo = vec![root.join("src/lib.rs")];
    while let Some(file) = todo.pop() {
        let raw = std::fs::read_to_string(&file).unwrap_or_else(|e| panic!("{file:?}: {e}"));
        let mut code = mask(&raw);
        if drop_test_items(&raw, &mut code).is_none() {
            continue;
        }
        let text = std::str::from_utf8(&code).unwrap();
        let name = file.file_name().unwrap().to_str().unwrap();
        let dir = if matches!(name, "lib.rs" | "mod.rs") {
            file.parent().unwrap().to_path_buf()
        } else {
            file.with_extension("")
        };
        let attrs = attrs(&code, &raw);
        for (i, _) in text.match_indices("mod ") {
            if i > 0 && is_ident(code[i - 1]) {
                continue;
            }
            let rest = &text[i + 4..];
            let m: String = rest.chars().take_while(|&c| is_ident(c as u8)).collect();
            let after = rest[m.len()..].trim_start();
            if m.is_empty() || !after.starts_with(';') {
                continue;
            }
            // `#[path]` in the attribute run just above this declaration.
            let mut head = text[..i].trim_end();
            head = head.strip_suffix("pub(crate)").unwrap_or(head);
            head = head.strip_suffix("pub").unwrap_or(head).trim_end();
            let mut path = None;
            while let Some((s, _, a)) = attrs.iter().find(|(_, e, _)| *e == head.len()) {
                if let Some(p) = a.trim().strip_prefix("path") {
                    path = p.split('"').nth(1);
                }
                head = text[..*s].trim_end();
            }
            let next = match path {
                Some(p) => file.parent().unwrap().join(p),
                None if dir.join(format!("{m}.rs")).exists() => dir.join(format!("{m}.rs")),
                None => dir.join(&m).join("mod.rs"),
            };
            todo.push(next);
        }
        let rel = file
            .strip_prefix(root)
            .unwrap()
            .to_string_lossy()
            .replace('\\', "/");
        let code = String::from_utf8(code).unwrap();
        let fns = fn_spans(code.as_bytes());
        out.push(Src {
            path: rel,
            raw,
            code,
            fns,
        });
    }
    out.sort_by(|a, b| a.path.cmp(&b.path));
    out
}

// An allow-listed site: (file, enclosing fn, why).
type Allow = (&'static str, &'static str, &'static str);

// Hits not on `allow`, plus allow entries that matched nothing (stale).
fn check(rule: &str, hits: Vec<(&Src, usize)>, allow: &[Allow]) {
    let mut used = vec![false; allow.len()];
    let mut bad = Vec::new();
    for (src, at) in hits {
        let f = src.enclosing_fn(at);
        match allow.iter().position(|(p, n, _)| *p == src.path && *n == f) {
            Some(k) => used[k] = true,
            None => bad.push(format!("{}:{} (fn {f})", src.path, src.line(at))),
        }
    }
    for (k, (p, n, _)) in allow.iter().enumerate() {
        if !used[k] {
            bad.push(format!("stale allow-list entry {p} fn {n}"));
        }
    }
    assert!(bad.is_empty(), "{rule}:\n  {}", bad.join("\n  "));
}

fn in_dirs(src: &Src, dirs: &[&str]) -> bool {
    dirs.iter().any(|d| src.path.starts_with(d))
}

fn not_halt(src: &Src) -> bool {
    src.path != "src/halt.rs" && !src.path.starts_with("src/halt/")
}

/// §5.8 "Ban `std::thread::sleep` on op paths: libfreemkv `src/{drive,disc,sector,io,mux}`
/// … Use `Halt::wait` / `pause` instead." Also catches `use std::thread::{sleep, ..}`.
#[test]
fn no_thread_sleep_on_op_paths() {
    const OP_DIRS: &[&str] = &[
        "src/drive/",
        "src/disc/",
        "src/sector/",
        "src/io/",
        "src/mux/",
    ];
    const ALLOW: &[Allow] = &[
        (
            "src/io/tree_sink.rs",
            "probe_case_insensitive",
            "20 ms filesystem remove retry; no drive wait, no token in reach",
        ),
        (
            "src/mux/network.rs",
            "accept_staged",
            "halt-checked accept poll; open item: convert to Halt::wait",
        ),
    ];
    let srcs = production();
    let mut hits = Vec::new();
    for s in srcs.iter().filter(|s| in_dirs(s, OP_DIRS)) {
        hits.extend(s.hits("thread::sleep").into_iter().map(|i| (s, i)));
        hits.extend(sleep_imports(&s.code).into_iter().map(|i| (s, i)));
    }
    check("thread::sleep on an op path", hits, ALLOW);
}

// Offsets of `thread::{...}` import groups that bring `sleep` into scope.
fn sleep_imports(code: &str) -> Vec<usize> {
    code.match_indices("thread::{")
        .map(|(i, _)| i)
        .filter(|&i| {
            code[i..close_of(code.as_bytes(), i + 8)]
                .split(|c: char| !is_ident(c as u8))
                .any(|w| w == "sleep")
        })
        .collect()
}

#[test]
fn sleep_imports_finds_a_grouped_sleep_only() {
    let code = "use std::thread::{self, sleep};\nuse std::thread::{self, spawn};\nuse a::thread::{sleeper};";
    assert_eq!(sleep_imports(code), vec![code.find("thread::{").unwrap()]);
}

/// §2.2: "**Every** raw `self.scsi.as_mut().execute` in `drive/mod.rs` and `identity.rs`
/// goes through [`Drive::exec`]"; §5.8 widens the grep to all of `drive/`.
#[test]
fn no_raw_execute_in_drive_or_identity() {
    const ALLOW: &[Allow] = &[
        (
            "src/drive/mod.rs",
            "dispatch",
            "the one transport call behind exec / exec_cleanup / exec_uncancellable",
        ),
        (
            "src/identity.rs",
            "from_drive",
            "public API over a bare transport; Drive::open uses identify(exec)",
        ),
    ];
    let srcs = production();
    let mut hits = Vec::new();
    let mut scanned = 0;
    for s in srcs
        .iter()
        .filter(|s| s.path.starts_with("src/drive/") || s.path == "src/identity.rs")
    {
        scanned += 1;
        for pat in [".execute(", ".execute_cleanup("] {
            hits.extend(s.hits(pat).into_iter().map(|i| (s, i)));
        }
    }
    assert!(scanned >= 5, "the walk lost drive/ files: {scanned}");
    check("raw transport execute outside Drive::exec", hits, ALLOW);
}

/// The lowercased receiver token before the atomic op at `i`. Trailing whitespace is
/// trimmed first, so a rustfmt-wrapped `self.cancel\n    .load(..)` still names `cancel`.
fn atomic_receiver(code: &str, i: usize) -> String {
    code[..i]
        .trim_end()
        .rsplit(|c: char| c.is_whitespace() || "(&*!,{;=".contains(c))
        .next()
        .unwrap_or("")
        .to_ascii_lowercase()
}

#[test]
fn atomic_receiver_sees_through_wrapped_chains() {
    for code in [
        "if self.cancel.load(x)",
        "if self.cancel\n        .load(x)",
        "if self.Cancel  \n\t.load(x)",
    ] {
        let recv = atomic_receiver(code, code.find(".load(").unwrap());
        assert_eq!(recv, "self.cancel", "{code:?}");
    }
}

/// §5.8 "A raw-halt-load grep test. Its allow-list: the `from_arc` bridges and the patch
/// latch." A `Halt`'s flag is read via `check` / `is_cancelled`, never as a raw atomic.
/// The patch latch is the engine's (§4.2 `EngineHalt`); libfreemkv has none.
#[test]
fn no_raw_halt_loads_in_production() {
    const ALLOW: &[Allow] = &[
        (
            "src/drive/mod.rs",
            "halt_flag",
            "from_arc bridge: the token as the pub Arc<AtomicBool> view",
        ),
        (
            "src/scsi/macos.rs",
            "cancel_byte",
            "from_arc bridge: the token as the macOS shim's cancel byte",
        ),
    ];
    const ATOMIC_OPS: &[&str] = &[
        ".load(",
        ".store(",
        ".swap(",
        ".fetch_or(",
        ".fetch_and(",
        ".fetch_xor(",
        ".fetch_nand(",
        ".compare_exchange(",
        ".compare_exchange_weak(",
        ".get_mut(",
        ".into_inner(",
        ".as_ptr(",
    ];
    let srcs = production();
    let mut hits = Vec::new();
    for s in srcs.iter().filter(|s| not_halt(s)) {
        for pat in [".as_arc()", "from_arc(", ".halt_flag()"] {
            for i in s.hits(pat) {
                let line = s.code[..i].rfind('\n').map_or(0, |n| n + 1);
                let eol = s.code[i..].find('\n').map_or(s.code.len(), |n| i + n);
                // `Arc::ptr_eq` compares the two tokens' identity; it reads no flag.
                if !s.code[line..eol].contains("Arc::ptr_eq(") {
                    hits.push((s, i));
                }
            }
        }
        for pat in ATOMIC_OPS {
            for i in s.hits(pat) {
                let recv = atomic_receiver(&s.code, i);
                if recv.contains("halt") || recv.contains("cancel") {
                    hits.push((s, i));
                }
            }
        }
    }
    check("raw halt flag access outside halt.rs", hits, ALLOW);
}

/// §5.8 "A `JoinHandle::is_finished` spin ban (use `join_within`)."
#[test]
fn no_is_finished_spins_outside_halt() {
    let srcs = production();
    let mut hits = Vec::new();
    for s in srcs.iter().filter(|s| not_halt(s)) {
        hits.extend(s.hits("is_finished").into_iter().map(|i| (s, i)));
    }
    check("is_finished (use halt::join_within)", hits, &[]);
}

// The `#[allow]` / `#[expect]` sites present when ST-X2 landed, per (file, attribute):
// each is a §5.8 violation awaiting a per-site fix. The list only shrinks.
const ALLOW_BASELINE: &[(&str, &str, usize)] = &[
    (
        "src/disc/extract.rs",
        r#"allow(clippy::too_many_arguments)"#,
        1,
    ),
    ("src/drive/linux.rs", r#"allow(dead_code)"#, 1),
    ("src/drive/mod.rs", r#"allow(dead_code)"#, 1),
    (
        "src/drive/mod.rs",
        r#"cfg_attr(not(target_os="linux"),allow(unused_variables))"#,
        1,
    ),
    (
        "src/io/tree_sink.rs",
        r#"allow(clippy::unnecessary_cast,clippy::useless_conversion)"#,
        1,
    ),
    (
        "src/keys/arrival.rs",
        r#"allow(clippy::too_many_arguments)"#,
        1,
    ),
    (
        "src/labels/class_reader.rs",
        r#"expect(dead_code,reason="everyJVMS4.4tagisdecoded;labelparsersreadonlysomepayloads")"#,
        1,
    ),
    (
        "src/labels/class_reader.rs",
        r#"expect(dead_code,reason="parsedinfullperJVMS4.1;labelparsersreadthepoolandmethods")"#,
        1,
    ),
    (
        "src/labels/class_reader.rs",
        r#"expect(dead_code,reason="parsedinfullperJVMS4.5/4.6")"#,
        1,
    ),
    (
        "src/labels/class_reader.rs",
        r#"expect(dead_code,reason="parsedperJVMS4.7.3;thedecodertracksonlythestack")"#,
        1,
    ),
    (
        "src/labels/class_reader.rs",
        r#"expect(dead_code,reason="payloadsarefaultdetailforDebug;callersmatchthevariant")"#,
        1,
    ),
    (
        "src/labels/ctrm.rs",
        r#"allow(clippy::items_after_test_module)"#,
        1,
    ),
    ("src/mpls.rs", r#"allow(dead_code)"#, 2),
    ("src/mux/demux_sink.rs", r#"allow(dead_code)"#, 2),
    ("src/mux/demux_thread.rs", r#"allow(dead_code)"#, 1),
    ("src/mux/mkv.rs", r#"allow(clippy::too_many_arguments)"#, 3),
    ("src/mux/mod.rs", r#"allow(dead_code)"#, 3),
    ("src/mux/pipelined_stream.rs", r#"allow(dead_code)"#, 1),
    (
        "src/mux/resolve.rs",
        r#"allow(clippy::too_many_arguments)"#,
        1,
    ),
    (
        "src/platform/fs_type/linux.rs",
        r#"allow(clippy::unnecessary_cast)"#,
        2,
    ),
    ("src/scsi/linux.rs", r#"allow(non_camel_case_types)"#, 1),
    (
        "src/scsi/mod.rs",
        r#"cfg_attr(not(any(feature="rip",target_os="windows")),allow(dead_code))"#,
        1,
    ),
    (
        "src/scsi/mod.rs",
        r#"cfg_attr(not(any(target_os="linux",target_os="windows")),allow(dead_code))"#,
        1,
    ),
    (
        "src/scsi/mod.rs",
        r#"cfg_attr(not(feature="rip"),allow(dead_code))"#,
        1,
    ),
    (
        "src/scsi/mod.rs",
        r#"cfg_attr(not(target_os="linux"),allow(dead_code))"#,
        5,
    ),
    (
        "src/scsi/mod.rs",
        r#"cfg_attr(not(target_os="windows"),allow(dead_code))"#,
        3,
    ),
    (
        "src/scsi/mod.rs",
        r#"cfg_attr(target_os="macos",allow(dead_code))"#,
        1,
    ),
    ("src/scsi/windows.rs", r#"allow(non_snake_case)"#, 3),
    ("src/udf.rs", r#"allow(clippy::too_many_arguments)"#, 1),
];

/// §5.8 "`#[allow]` only in `halt.rs` and tests." `#[expect]` and `cfg_attr(.., allow)`
/// suppress the same way, so they count too.
#[test]
fn allow_attributes_only_in_halt_and_tests() {
    let srcs = production();
    let mut found: BTreeMap<(String, String), usize> = BTreeMap::new();
    for s in srcs.iter().filter(|s| not_halt(s)) {
        for (start, end, _) in attrs(s.code.as_bytes(), &s.raw) {
            let inner: String = s.raw[start..end].split_whitespace().collect();
            let body = inner.trim_start_matches('#').trim_start_matches('!');
            let body = &body[1..body.len() - 1];
            let lint = body.starts_with("allow(")
                || body.starts_with("expect(")
                || body.starts_with("cfg_attr(")
                    && (body.contains(",allow(") || body.contains(",expect("));
            if lint {
                *found.entry((s.path.clone(), body.to_string())).or_default() += 1;
            }
        }
    }
    let base: BTreeMap<(String, String), usize> = ALLOW_BASELINE
        .iter()
        .map(|(p, a, n)| ((p.to_string(), a.to_string()), *n))
        .collect();
    let new: Vec<_> = found
        .iter()
        .filter(|(k, n)| base.get(*k).is_none_or(|b| *n > b))
        .map(|((p, a), n)| format!("(\"{p}\", r#\"{a}\"#, {n}),"))
        .collect();
    let gone: Vec<_> = base
        .iter()
        .filter(|(k, n)| found.get(*k).is_none_or(|f| f < n))
        .map(|((p, a), n)| format!("{p} {a} x{n}"))
        .collect();
    assert!(
        new.is_empty() && gone.is_empty(),
        "#[allow] outside halt.rs and tests:\n  {}\nfixed (shrink ALLOW_BASELINE):\n  {}",
        new.join("\n  "),
        gone.join("\n  ")
    );
}

// The scanner itself: masking, cfg evaluation and item blanking.
#[test]
fn scanner_sees_production_only() {
    assert!(test_only("test"));
    assert!(test_only("all(test, not(loom))"));
    assert!(test_only("any(test, feature = \"test-util\")"));
    assert!(!test_only("any(target_os = \"linux\", test)"));
    assert!(!test_only("not(test)"));
    let raw = "fn a() { std::thread::sleep(d); } // thread::sleep\n\
               const S: &str = \"thread::sleep\"; const C: char = '{';\n\
               #[cfg(all(test, unix))]\nmod tests { fn b() { thread::sleep(d); } }\n\
               #[cfg(test)]\nuse std::thread::sleep;\nfn c<'a>(x: &'a u8) {}\n\
               #[cfg(test)]\nimpl<A, B> T for S<A, B> { fn d() { thread::sleep(d); } }\n\
               struct F { #[cfg(test)] g: Vec<u8>, h: u8 }\n";
    let mut code = mask(raw);
    drop_test_items(raw, &mut code).unwrap();
    let code = String::from_utf8(code).unwrap();
    assert_eq!(code.matches("thread::sleep").count(), 1, "{code}");
    assert!(!code.contains("sleep;") && code.contains("fn c<'a>"));
    assert!(!code.contains(" g:") && code.contains(" h: u8 }"), "{code}");
    let spans = fn_spans(code.as_bytes());
    assert_eq!(
        spans.iter().map(|s| s.2.as_str()).collect::<Vec<_>>(),
        ["a", "c"]
    );
    let all = production();
    for must in [
        "src/drive/mod.rs",
        "src/scsi/macos.rs",
        "src/scsi/windows.rs",
        "src/halt.rs",
    ] {
        assert!(all.iter().any(|s| s.path == must), "walk lost {must}");
    }
    for never in [
        "src/drive/stop_tests.rs",
        "src/halt/tests.rs",
        "src/test_util_tests.rs",
        "src/whole_disc_tests.rs",
        "src/test_util.rs",
    ] {
        assert!(!all.iter().any(|s| s.path == never), "walk took {never}");
    }
}
