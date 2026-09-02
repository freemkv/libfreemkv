# src/labels/jar.rs — relocated comment prose

## `MAX_CLASS_BYTES` rationale

A real BD-J `.class` is far under this ceiling (64 MiB); a lying header
simply gets truncated and the class fails to parse, which is skipped like
any other bad entry. The buffer is grown incrementally and the read is
capped here rather than pre-sized from the declared size.

## `for_each_jar` "top-level" vendor examples

"Top-level" means entries directly under `/BDMV/JAR/`, not nested under a
subdir. Pixelogic, Criterion, Paramount, etc. put their data files inside
`/BDMV/JAR/<x>/`; dbp and Deluxe put their jar directly at
`/BDMV/JAR/<name>.jar`.

## `read_is_bounded_by_cap` test rationale

The read is bounded by `MAX_CLASS_BYTES`: a stored entry whose real payload
exceeds the cap yields only the first `MAX_CLASS_BYTES` bytes to the
parser, never the full (unbounded) entry. Verified here on a small cap via
the entry-count path: the truncated bytes still parse a valid class
prefix, but no read beyond the cap occurs. The test asserts the entry is
still surfaced exactly once (the cap does not drop legitimate entries) and
that the call returns.
