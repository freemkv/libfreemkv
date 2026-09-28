//! `spec_quotes_match_registry`: every `libfreemkv::spec` quote (all prefixes)
//! against `tests/spec_quotes.txt`. It proves const == registry only; registry ==
//! source document is a PR-review item (the reviewer opens the PDF page or URL).

use libfreemkv::spec::{self, SpecQuote};
use std::collections::{HashMap, HashSet};

const REGISTRY: &str = include_str!("spec_quotes.txt");
const PREFIXES: &[&str] = &["KS-", "SS-", "MS-"];
const ELISION: &str = " … ";

// `<id>\t<passage>[\t// comment]` per line; `#` lines are the header. Panics on a
// malformed line or a duplicate ID.
fn registry() -> HashMap<&'static str, &'static str> {
    let mut out = HashMap::new();
    for line in REGISTRY
        .lines()
        .filter(|l| !l.starts_with('#') && !l.is_empty())
    {
        let (id, rest) = line
            .split_once('\t')
            .expect("registry line is `<id>\\t<passage>`");
        let passage = rest.split("\t// ").next().unwrap_or(rest);
        assert!(!passage.trim().is_empty(), "{id}: empty registry passage");
        assert!(
            out.insert(id, passage).is_none(),
            "{id}: two registry lines"
        );
    }
    out
}

// Each " … "-separated fragment of `text` occurs in `passage`, in order.
fn fragments_in_order(text: &str, passage: &str) -> Result<(), String> {
    let mut rest = passage;
    for frag in text.split(ELISION) {
        match rest.find(frag) {
            Some(at) => rest = &rest[at + frag.len()..],
            None => return Err(format!("fragment {frag:?} not found (in order)")),
        }
    }
    Ok(())
}

// A known prefix, then a number from 1 (the const name agreeing is checked below).
fn check_id(q: &SpecQuote) {
    let prefix = PREFIXES.iter().find(|p| q.id.starts_with(**p));
    let prefix = prefix.unwrap_or_else(|| panic!("{}: prefix not in {PREFIXES:?}", q.id));
    let n: u32 = q.id[prefix.len()..]
        .parse()
        .unwrap_or_else(|_| panic!("{}: no number after the prefix", q.id));
    assert!(n >= 1, "{}: IDs start at 1", q.id);
}

#[test]
fn spec_quotes_match_registry() {
    let reg = registry();
    let mut seen = HashSet::new();
    let mut n = 0;
    for q in spec::quotes() {
        n += 1;
        check_id(q);
        assert!(seen.insert(q.id), "{}: ID used twice", q.id);
        assert!(!q.section.is_empty(), "{}: empty section", q.id);
        assert!(!q.locator.is_empty(), "{}: empty locator", q.id);
        assert!(!q.source.is_empty(), "{}: empty source", q.id);
        assert!(!q.text.is_empty(), "{}: empty text", q.id);
        let passage = reg
            .get(q.id)
            .unwrap_or_else(|| panic!("{}: no registry line", q.id));
        if let Err(e) = fragments_in_order(q.text, passage) {
            panic!("{}: const text differs from the registry: {e}", q.id);
        }
    }
    let orphans: Vec<_> = reg.keys().filter(|id| !seen.contains(*id)).collect();
    assert!(
        orphans.is_empty(),
        "registry lines with no const: {orphans:?}"
    );
    assert!(n > 0, "spec::ALL is empty");
}

/// Every `pub const <PREFIX>_<n>_<SHORT>: SpecQuote` in `src/spec/` carries `id: "<PREFIX>-<n>"`
/// (KU design §3.6.1 rule 3), and the source defines exactly the quotes `spec::ALL` lists.
#[test]
fn const_names_agree_with_ids() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/spec");
    let mut consts = 0;
    for entry in std::fs::read_dir(&dir).unwrap() {
        let src = std::fs::read_to_string(entry.unwrap().path()).unwrap();
        let mut lines = src.lines();
        while let Some(line) = lines.next() {
            let Some(name) = line
                .strip_prefix("pub const ")
                .and_then(|l| l.strip_suffix(": SpecQuote = SpecQuote {"))
            else {
                continue;
            };
            let id_line = lines.next().unwrap_or_default().trim();
            let mut parts = name.splitn(3, '_');
            let (prefix, n) = (parts.next().unwrap(), parts.next().unwrap_or_default());
            assert_eq!(id_line, format!("id: \"{prefix}-{n}\","), "const {name}");
            consts += 1;
        }
    }
    assert_eq!(
        consts,
        spec::quotes().count(),
        "a quote const missing from spec::ALL"
    );
}

/// `KS-1`…`KS-29` exist, in order, with no gap (KU design §3.6.1 rule 4).
#[test]
fn keys_quotes_are_ks_1_to_29_in_order() {
    let ids: Vec<&str> = spec::keys::ALL.iter().map(|q| q.id).collect();
    let want: Vec<String> = (1..=29).map(|n| format!("KS-{n}")).collect();
    assert_eq!(ids, want);
}

/// The checker itself: in-order fragments pass; a changed, missing or reordered one fails.
#[test]
fn fragment_check_rejects_drift() {
    let passage = "alpha beta gamma delta";
    assert!(fragments_in_order("alpha beta … delta", passage).is_ok());
    assert!(fragments_in_order("alpha beta gamma delta", passage).is_ok());
    assert!(fragments_in_order("alpha … epsilon", passage).is_err());
    assert!(fragments_in_order("delta … alpha", passage).is_err());
    assert!(fragments_in_order("alpha  beta", passage).is_err());
}

/// Registry == source for the libaacs rows (KS-22…KS-24), whitespace-collapsed. Needs a
/// libaacs checkout at `55be92be`: `LIBAACS_AACS_C=…/src/libaacs/aacs.c cargo test
/// --test spec_quotes -- --ignored`. Ignored in CI, which has no libaacs checkout.
#[test]
#[ignore = "needs LIBAACS_AACS_C pointing at libaacs src/libaacs/aacs.c @55be92be"]
fn libaacs_quotes_match_source() {
    let path = std::env::var("LIBAACS_AACS_C").expect("set LIBAACS_AACS_C");
    let collapse = |s: &str| s.split_whitespace().collect::<Vec<_>>().join(" ");
    let src = collapse(&std::fs::read_to_string(path).expect("read aacs.c"));
    let reg = registry();
    for id in ["KS-22", "KS-23", "KS-24"] {
        let passage = collapse(reg[id]);
        assert!(
            src.contains(&passage),
            "{id}: registry passage not in aacs.c: {passage}"
        );
    }
}

const SITES: &str = include_str!("spec_sites.txt");

// The body of the one `fn <name>` in `src`, braces matched outside `//` comments and
// string/char literals. Err when the fn is missing or defined twice.
fn fn_body<'a>(src: &'a str, name: &str) -> Result<&'a str, String> {
    let starts: Vec<usize> = [format!("fn {name}("), format!("fn {name}<")]
        .iter()
        .flat_map(|pat| src.match_indices(pat.as_str()).map(|(i, _)| i))
        .collect();
    let [start] = starts[..] else {
        return Err(format!("fn {name}: {} definitions", starts.len()));
    };
    let bytes = src.as_bytes();
    let (mut i, mut depth, mut open) = (start, 0usize, None);
    while i < bytes.len() {
        match bytes[i] {
            b'/' if bytes.get(i + 1) == Some(&b'/') => {
                i += src[i..].find('\n').unwrap_or(src.len() - i);
            }
            b'"' => {
                i += 1;
                while i < bytes.len() && bytes[i] != b'"' {
                    i += if bytes[i] == b'\\' { 2 } else { 1 };
                }
            }
            b'\'' if bytes.get(i + 2) == Some(&b'\'') => i += 2,
            b'{' => {
                open.get_or_insert(i);
                depth += 1;
            }
            b'}' if open.is_some() => {
                depth -= 1;
                if depth == 0 {
                    return Ok(&src[open.unwrap()..=i]);
                }
            }
            _ => {}
        }
        i += 1;
    }
    Err(format!("fn {name}: unbalanced body"))
}

/// KU design §3.6.1 rule 11: every listed spec-governed site names its quote ID.
#[test]
fn spec_governed_sites_cite_the_spec() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let ids: HashSet<&str> = spec::quotes().map(|q| q.id).collect();
    let mut rows = 0;
    for line in SITES
        .lines()
        .filter(|l| !l.starts_with('#') && !l.is_empty())
    {
        let cols: Vec<&str> = line.split('\t').collect();
        let [file, name, id] = cols[..] else {
            panic!("site row is `<file>\\t<fn>\\t<id>`: {line:?}");
        };
        assert!(ids.contains(id), "{file} {name}: {id} is not a spec quote");
        let src = std::fs::read_to_string(root.join(file)).expect(file);
        let body = fn_body(&src, name).unwrap_or_else(|e| panic!("{file}: {e}"));
        assert!(cites(body, id), "{file} fn {name} does not cite {id}");
        rows += 1;
    }
    assert!(rows > 0, "no spec-governed sites listed");
}

// Does `body` name `id` as a whole ID? `KS-2` must not match inside `KS-22` or `XKS-2`.
fn cites(body: &str, id: &str) -> bool {
    body.match_indices(id).any(|(at, _)| {
        let before = body[..at].chars().next_back();
        let after = body[at + id.len()..].chars().next();
        !before.is_some_and(|c| c.is_alphanumeric() || c == '_')
            && !after.is_some_and(|c| c.is_ascii_digit())
    })
}

/// The site checker itself: a cited ID passes; a missing ID, a missing fn, a braced
/// comment or string, and a twice-defined fn are all caught.
#[test]
fn spec_site_check_finds_the_body() {
    let src = "fn a() { // KS-1 { unbalanced in a comment\n    let s = \"}\"; let c = '{'; }\n\
               fn b() { if x { KS-2 } }\nfn b2() {}\n";
    assert!(fn_body(src, "a").unwrap().contains("KS-1"));
    assert!(fn_body(src, "b").unwrap().contains("KS-2"));
    assert!(!fn_body(src, "b2").unwrap().contains("KS-2"));
    assert!(fn_body(src, "c").is_err());
    assert!(fn_body("fn d() {}\nfn d() {}", "d").is_err());
    // Word boundaries: `KS-2` is not cited by `KS-22`, and is by `KS-2,` / `(KS-2)`.
    assert!(!cites("fn e() { // KS-22 only }", "KS-2"));
    assert!(!cites("fn e() { // XKS-2 }", "KS-2"));
    assert!(cites("fn e() { // KS-22, KS-2, KS-3 }", "KS-2"));
    assert!(cites("fn e() { // (KS-2) }", "KS-2"));
    assert!(cites(fn_body(src, "b").unwrap(), "KS-2"));
}
