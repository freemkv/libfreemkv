#!/usr/bin/env bash
#
# leak-guard.sh — self-contained public-repo leak gate.
#
# This is the LAST line of defense in CI. It is intentionally self-contained:
# public CI cannot reach the private tooling, so this script encodes ONLY the
# generic net — internal infrastructure references, agent-context files, and
# AI-attribution in commit messages. It deliberately contains NO project-
# specific reverse-engineering vocabulary (those words would themselves be a
# leak). The richer private scanner stays private.
#
# Fails (exit 1) if any of the following appear in the repo:
#   1. a tracked CLAUDE.md or .claude/ path (agent context — never public),
#   2. tracked file content matching the internal-infra net,
#   3. a commit message (in the given range) with AI attribution.
#
# Usage:
#   leak-guard.sh [<commit-range>]
#     <commit-range>  optional git rev-list range to scan commit messages
#                     (e.g. "abc..def"). If omitted, commit-message scan is
#                     skipped (path + content checks always run).

set -euo pipefail

# Absolute path to this script, resolved before any cd, so we can exclude it
# from the content scan (it necessarily contains the detection patterns).
SELF_ABS="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")"

REPO="$(git rev-parse --show-toplevel)"
cd "$REPO"

fail=0
note() { printf '  ✗ %s\n' "$1"; fail=1; }

# Internal-infra net — GENERIC ONLY. This script ships in the public repo, so
# the patterns themselves must not name any org-specific identifier (doing so
# would itself leak the infra they guard). We catch the leak *class*:
#   - RFC1918 private IPv4 ranges (10/8, 172.16/12, 192.168/16),
#   - private/internal/non-routable TLDs (.internal/.local/.lan/.corp/.invalid),
#   - docker.internal.
# The full org-specific net (literal hostnames, service names, repo paths,
# vendor tooling, …) lives ONLY in the private scanner and never ships here.
INFRA_RE='\b10\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}|\b172\.(1[6-9]|2[0-9]|3[01])\.[0-9]{1,3}\.[0-9]{1,3}|\b192\.168\.[0-9]{1,3}\.[0-9]{1,3}|\.internal\b|\.local\b|\.lan\b|\.corp\b|\.invalid\b|docker\.internal'
# Home-path net — GENERIC ONLY. Catches an absolute developer home path
# committed into a tracked file (a macOS /Users/<user>/… or Linux /home/<user>/…
# path). This names NO specific user — it matches the leak *class* (any home
# path), so the pattern itself reveals nothing org- or person-specific. A real
# leak (e.g. /Users/alice/Developer/x slipping into a public RELEASE.md) trips
# this regardless of whose machine it came from. The username segment is a
# literal-username class ([A-Za-z0-9._-]) so dynamic/templated paths that build
# the user at runtime — shell `/home/$USER/`, doc `/home/<rip>/`, Rust
# `/home/{user}/` — do NOT false-positive; only a baked-in literal home leaks.
HOMEPATH_RE='/Users/[A-Za-z0-9._-]+/|/home/[A-Za-z0-9._-]+/'
# AI-attribution net (case-insensitive). "claude" matches only as a standalone
# word — NOT preceded by a dot/slash/alnum and NOT followed by .md — so legit
# mentions of CLAUDE.md / .claude/ in a commit message don't false-positive.
ATTR_RE='co-authored-by|generated with|🤖|(?<![.\/A-Za-z0-9])claude(?!\.md)'

echo "── leak-guard: tracked agent-context paths ──"
while IFS= read -r f; do
  case "$f" in
    CLAUDE.md|*/CLAUDE.md|.claude|.claude/*|*/.claude|*/.claude/*)
      note "tracked agent-context file: $f (CLAUDE.md/.claude must never be tracked in a public repo)" ;;
  esac
done < <(git ls-files)

# Match a PCRE against a file, emitting "LINE: MATCH". The pattern is passed as
# an argument (not interpolated into a //) so metacharacters like the "/" in a
# path-style token can't break the regex. Reads raw bytes so non-UTF-8 blobs
# don't abort the scan.
pcre_matches() {
  perl -e '
    my ($file, $re) = @ARGV;
    open(my $fh, "<:raw", $file) or exit 0;
    my $rx; eval { $rx = qr/$re/i }; exit 0 if $@;
    while (my $l = <$fh>) { if ($l =~ /$rx/) { print "$.: $&\n"; } }
  ' "$1" "$2" 2>/dev/null
}

# This script's own source necessarily contains the detection patterns (e.g.
# the regex tokens in INFRA_RE), so scanning it would always self-flag. Skip it.
SELF="$(git ls-files --full-name -- "$SELF_ABS" 2>/dev/null | head -1)"

echo "── leak-guard: internal-infra references in tracked files ──"
while IFS= read -r f; do
  case "$f" in *.png|*.jpg|*.jpeg|*.ico|*.gif|*.bin|*.crate|*.gz|*.zip|*.pdf) continue ;; esac
  [ -n "$SELF" ] && [ "$f" = "$SELF" ] && continue
  [ -f "$f" ] || continue
  while IFS= read -r hit; do
    [ -z "$hit" ] && continue
    note "internal-infra reference: $f:$hit"
  done < <(pcre_matches "$f" "$INFRA_RE")
  while IFS= read -r hit; do
    [ -z "$hit" ] && continue
    note "[HOME-PATH] absolute home path: $f:$hit (no local home path may be committed to a public repo)"
  done < <(pcre_matches "$f" "$HOMEPATH_RE")
done < <(git ls-files)

RANGE="${1:-}"
if [ -n "$RANGE" ]; then
  echo "── leak-guard: AI-attribution in commit messages ($RANGE) ──"
  while IFS= read -r sha; do
    [ -z "$sha" ] && continue
    msg="$(git log -1 --format='%B' "$sha" 2>/dev/null || true)"
    # Pass the pattern as an argument (not interpolated into a //) so the
    # lookbehind char class and "/" don't break the regex.
    hit="$(printf '%s' "$msg" | perl -e '
      my $re = $ARGV[0]; my $rx = qr/$re/i;
      while (my $l = <STDIN>) { if ($l =~ /($rx)/) { print "$1\n"; last; } }
    ' "$ATTR_RE" | head -1 || true)"
    [ -n "$hit" ] && note "commit ${sha:0:12}: message contains \"$hit\" (owner rule: zero AI attribution, ever)"
  done < <(git rev-list "$RANGE" 2>/dev/null || true)
fi

echo
if [ "$fail" -ne 0 ]; then
  echo "✗ leak-guard: blocking finding(s) above — DO NOT MERGE/PUBLISH"
  exit 1
fi
echo "✓ leak-guard: clean"
