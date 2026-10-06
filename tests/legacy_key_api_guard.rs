//! The structural key guard (keys-upfront design v3.4 §2.2, LK20, KU-X2).
//!
//! §2.2: "One test per repo; any match outside the allow-path fails." Every Rust file under
//! `src/` (`#[cfg(test)]` included) and `tests/` is scanned, comments skipped, for the
//! libfreemkv banned list. The allow-path is §2.2's "`src/keys/**`, `src/decrypt.rs`,
//! `src/sector/decrypting.rs`, `src/aacs/**` (definitions and internal use)".
//!
//! The sanctioned test helper `test_util::decrypt_unit` (§2.2: "Both move to
//! `libfreemkv::test_util::decrypt_unit`") is not the library door: its definition and its
//! callers are exempt. So is `KeySource::resolve_unit_keys`'s default, the trait op's own
//! forward to `get_unit_keys` (§2.2: the "public trait ops ... Stay").
//! Per spec; do not change without a spec citation proving otherwise.

use std::path::{Path, PathBuf};

/// §2.2, the libfreemkv row of the structural guards.
const BANNED: &[&str] = &[
    "get_unit_keys(",
    "get_fmts_indexes(",
    "resolve_unit_keys(",
    "decrypt_with(",
    "AacsKeyMap::from_ranges",
    "with_key_map(",
    "set_key_map(",
    "decrypt_unit(",
];

/// §2.2: "`src/keys/**`, `src/decrypt.rs`, `src/sector/decrypting.rs`, `src/aacs/**`".
fn allowed(rel: &str) -> bool {
    rel.starts_with("src/keys/")
        || rel.starts_with("src/aacs/")
        || rel == "src/decrypt.rs"
        || rel == "src/sector/decrypting.rs"
        || TEST_SIDE_FILES.contains(&rel)
}

/// The `#[cfg(test)]` side files of the allow-path modules above (`#[path]` tests).
const TEST_SIDE_FILES: [&str; 3] = [
    "src/decrypt_tests.rs",
    "src/decrypt_spec_guards_tests.rs",
    "src/sector/decrypting_tests.rs",
];

/// `src` with every comment blanked (newlines kept), and a copy with string and char
/// literal contents blanked too; both keep `src`'s char positions.
fn lex(src: &str) -> (Vec<char>, Vec<char>) {
    let s: Vec<char> = src.chars().collect();
    let mut code = s.clone();
    let mut bare = s.clone();
    let blank = |v: &mut Vec<char>, from: usize, to: usize| {
        for c in &mut v[from..to] {
            if *c != '\n' {
                *c = ' ';
            }
        }
    };
    let word = |c: char| c.is_alphanumeric() || c == '_';
    let (n, mut i) = (s.len(), 0);
    while i < n {
        let at = |k: usize| s.get(k).copied().unwrap_or('\0');
        if at(i) == '/' && at(i + 1) == '/' {
            let end = (i..n).find(|&k| s[k] == '\n').unwrap_or(n);
            blank(&mut code, i, end);
            blank(&mut bare, i, end);
            i = end;
        } else if at(i) == '/' && at(i + 1) == '*' {
            let (mut depth, mut k) = (1, i + 2);
            while k < n && depth > 0 {
                if at(k) == '/' && at(k + 1) == '*' {
                    depth += 1;
                    k += 2;
                } else if at(k) == '*' && at(k + 1) == '/' {
                    depth -= 1;
                    k += 2;
                } else {
                    k += 1;
                }
            }
            blank(&mut code, i, k);
            blank(&mut bare, i, k);
            i = k;
        } else if at(i) == 'r'
            && (i == 0 || !word(s[i - 1]) || (s[i - 1] == 'b' && (i < 2 || !word(s[i - 2]))))
            && {
                let h = (i + 1..n).take_while(|&k| s[k] == '#').count();
                at(i + 1 + h) == '"'
            }
        {
            let h = (i + 1..n).take_while(|&k| s[k] == '#').count();
            let open = i + 2 + h;
            let close: String = std::iter::once('"')
                .chain(std::iter::repeat_n('#', h))
                .collect();
            let close: Vec<char> = close.chars().collect();
            let end = (open..n).find(|&k| s[k..].starts_with(&close)).unwrap_or(n);
            blank(&mut bare, open, end);
            i = (end + close.len()).min(n);
        } else if at(i) == '"' {
            let mut k = i + 1;
            while k < n && s[k] != '"' {
                k += if s[k] == '\\' { 2 } else { 1 };
            }
            blank(&mut bare, i + 1, k.min(n));
            i = k + 1;
        } else if at(i) == '\'' && (at(i + 1) == '\\' || at(i + 2) == '\'') {
            let end = (i + 2..n).find(|&k| s[k] == '\'').unwrap_or(n);
            blank(&mut bare, i + 1, end);
            i = end + 1;
        } else {
            i += 1;
        }
    }
    (code, bare)
}

/// Char spans of the bodies of the `fn`s in `bare` whose name satisfies `pick`.
fn fn_spans(bare: &[char], pick: impl Fn(&str) -> bool) -> Vec<(usize, usize)> {
    let text: String = bare.iter().collect();
    let chars: Vec<(usize, char)> = text.char_indices().collect();
    let to_char = |byte: usize| chars.partition_point(|&(b, _)| b < byte);
    let mut spans = Vec::new();
    for (at, _) in text.match_indices("fn ") {
        let name: String = text[at + 3..]
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect();
        if name.is_empty() || !pick(&name) {
            continue;
        }
        let start = to_char(at);
        let Some(open) = (start..bare.len()).find(|&k| bare[k] == '{') else {
            continue;
        };
        let mut depth = 0;
        for (k, &c) in bare.iter().enumerate().skip(open) {
            depth += (c == '{') as i32 - (c == '}') as i32;
            if depth == 0 {
                spans.push((start, k + 1));
                break;
            }
        }
    }
    spans
}

/// `(line, token)` of every banned match in `src` (file `rel`) outside the allow-path.
fn hits(rel: &str, src: &str) -> Vec<(usize, &'static str)> {
    if allowed(rel) {
        return Vec::new();
    }
    let (code, bare) = lex(src);
    let mut exempt = fn_spans(&bare, |n| rel == "src/test_util.rs" && n == "decrypt_unit");
    exempt.extend(fn_spans(&bare, |n| {
        rel == "src/keysource.rs" && n == "resolve_unit_keys"
    }));
    let inside = |k: usize| exempt.iter().any(|&(a, b)| a <= k && k < b);
    let helper: Vec<char> = "test_util::".chars().collect();
    let mut out = Vec::new();
    for &tok in BANNED {
        let t: Vec<char> = tok.chars().collect();
        for k in 0..code.len().saturating_sub(t.len() - 1) {
            if code[k..k + t.len()] != t[..] || inside(k) {
                continue;
            }
            // The three trait ops are banned as calls (method or UFCS), not as `fn` items.
            let method = matches!(
                tok,
                "get_unit_keys(" | "get_fmts_indexes(" | "resolve_unit_keys("
            );
            let before = code[..k].last().copied().unwrap_or(' ');
            let is_item =
                code[..k].ends_with(&['f', 'n', ' ']) || before.is_alphanumeric() || before == '_';
            if method && is_item {
                continue;
            }
            let via_helper = k >= helper.len() && code[k - helper.len()..k] == helper[..];
            if tok == "decrypt_unit(" && via_helper {
                continue;
            }
            out.push((code[..k].iter().filter(|&&c| c == '\n').count() + 1, tok));
        }
    }
    out.sort();
    out
}

fn rs_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for e in entries.flatten() {
        let p = e.path();
        if p.is_dir() {
            rs_files(&p, out);
        } else if p.extension().is_some_and(|x| x == "rs") {
            out.push(p);
        }
    }
}

/// LK20 (KU §2.2, KU-X2): no AACS decrypt door in `src/` or `tests/` outside the allow-path.
#[test]
fn no_legacy_key_api_outside_the_allow_path() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut files = Vec::new();
    rs_files(&root.join("src"), &mut files);
    rs_files(&root.join("tests"), &mut files);
    assert!(files.len() > 50, "the walk lost files: {}", files.len());
    let mut found = Vec::new();
    for f in &files {
        let rel = f
            .strip_prefix(root)
            .unwrap()
            .to_string_lossy()
            .replace('\\', "/");
        if rel == "tests/legacy_key_api_guard.rs" {
            continue;
        }
        let src = std::fs::read_to_string(f).expect("read source");
        for (line, tok) in hits(&rel, &src) {
            found.push(format!("{rel}:{line}: {tok}"));
        }
    }
    assert!(
        found.is_empty(),
        "legacy key API outside the allow-path:\n{}",
        found.join("\n")
    );
}

/// Self-test: comments are skipped, code and strings are not, the allow-path is exactly
/// §2.2's four, and only the sanctioned helper is exempt.
#[test]
fn the_structural_guard_skips_comments_and_keeps_its_allow_path() {
    let code = "fn f() { m.set_key_map(v); }\n";
    assert_eq!(hits("src/mux/x.rs", code), [(1, "set_key_map(")]);
    let commented =
        "// m.with_key_map(v)\n/* decrypt_with( /* nested */ */\n/// x.get_unit_keys(\n";
    assert!(
        hits("src/x.rs", commented).is_empty(),
        "comments are skipped"
    );
    let url = "let u = \"http://x\"; AacsKeyMap::from_ranges_phased(r);\n";
    assert_eq!(hits("src/x.rs", url), [(1, "AacsKeyMap::from_ranges")]);
    let ufcs = "fn f() { KeySource::get_unit_keys(&s, c); }\n";
    assert_eq!(hits("src/mux/x.rs", ufcs), [(1, "get_unit_keys(")], "UFCS");
    let item = "impl K for S { fn get_unit_keys(&self) {} fn my_resolve_unit_keys(&self) {} }\n";
    assert!(hits("src/x.rs", item).is_empty(), "fn items are not calls");
    let raw = "let s = r#\"x.get_fmts_indexes( // \"#; s.resolve_unit_keys(c);\n";
    assert_eq!(
        hits("src/x.rs", raw).len(),
        2,
        "raw strings are data, not comments"
    );

    for rel in [
        "src/keys/resolve.rs",
        "src/aacs/content.rs",
        "src/decrypt.rs",
        "src/sector/decrypting.rs",
        "src/decrypt_tests.rs",
        "src/sector/decrypting_tests.rs",
    ] {
        assert!(hits(rel, code).is_empty(), "{rel} is on the allow-path");
    }
    for rel in [
        "src/keysource.rs",
        "src/sector/mod.rs",
        "src/decrypt_extra.rs",
        "tests/x.rs",
    ] {
        assert_eq!(hits(rel, code).len(), 1, "{rel} is not on the allow-path");
    }

    let helper = "test_util::decrypt_unit(&mut u, k); content::decrypt_unit(&mut u, k);";
    assert_eq!(hits("tests/x.rs", helper).len(), 1, "only the library door");
    let def = "pub fn decrypt_unit(u: &mut [u8]) { crate::aacs::content::decrypt_unit(u) }\n";
    assert!(
        hits("src/test_util.rs", def).is_empty(),
        "the helper's definition"
    );
    assert_eq!(
        hits("src/other.rs", def).len(),
        2,
        "only in src/test_util.rs"
    );

    let forward = "fn resolve_unit_keys(&self, c: &C) -> R { self.get_unit_keys(c) }\n";
    assert!(
        hits("src/keysource.rs", forward).is_empty(),
        "the trait op's own default"
    );
    let other = "fn other(&self, c: &C) -> R { self.get_unit_keys(c) }\n";
    assert_eq!(hits("src/keysource.rs", other).len(), 1, "only that fn");
    assert_eq!(
        hits("src/session.rs", forward).len(),
        1,
        "only in src/keysource.rs"
    );
}
