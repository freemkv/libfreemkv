# Deluxe BD-J framework parser (`src/labels/deluxe.rs`)

Detected on discs whose `/BDMV/JAR/<x>.jar` contains a `com/bydeluxe/`
directory entry.

## What this parser reads

Deluxe-authored discs store stream labels as ordinal references into
enum classes whose names are obfuscated per-disc, so a name-based
match won't work. The label data is instead recovered by matching on
the **shape of each enum's `<clinit>`**, which is framework-stable:

| Enum | Signature |
|---|---|
| Language | 70 `ldc` operations in `<clinit>`, sequence starts `English, French, Spanish, Dutch, ...` |
| Purpose | 8 ldcs starting `Normal, Commentary, PiP, Trivia, ...` |
| VideoFormat | 7 ldcs starting `HD, HDR10 Plus, HD Dolby, ...` |
| Region | 22 ldcs starting `USA_D1, LIC1, LIC2, LIC3, ...` |
| Studio | 6 ldcs in `<clinit>` |

Matching on the shape rather than the class name keeps the parser
working across obfuscation variants. Codec strings come from the
standard BD-J `org/bluray/ti/CodingType` enum referenced directly by
the binding constructors (see `StackVal::CodingType`), not from a
Deluxe-internal enum.

## Implementation phases

- **Phase A** — master enum identification (`identify_master_enums`).
  Walks every `.class`'s `<clinit>` ldc sequence and matches against
  the framework-stable fingerprints. Output: `Vec<(label, MasterEnum)>`
  with full ordinal → string-value tables.

- **Phase C** — binding-class identification (`find_binding_classes`).
  The per-stream table is built by some class via repeated
  `getstatic` references to the master enums identified in A.
  That class has the highest such `getstatic` count in the jar.
  Heuristic shape; precise threshold may need tuning.

- **Phase D** — binding-class bytecode decoder (`decode_binding`).
  Walks the binding class's `<clinit>` with a tiny symbolic stack
  machine. For each `new X / dup / ... / invokespecial X.<init>`
  sequence, collects the int values and enum-reference operands
  between the `dup` and the constructor call, then emits a
  `DecodedStream`. The signal-to-StreamLabel mapping (which arg is
  stream index? which is language? audio vs subtitle?) uses a
  heuristic — see `interpret_streams` for the mapping rules.

## Confidence

`parse` returns `Some(ParseResult::high(labels))` when Phases A
through D produce at least one stream: the master enums matched their
framework-stable fingerprints (a strong ordered-prefix signature), the
binding class decoded, and at least one per-stream binding resolved to a
real (language, purpose, codec) tuple — the schema was fully recovered,
not guessed. `None` when the disc isn't Deluxe-authored or when decoding
produces zero streams (a recognized-but-broken state that the analyzer
still surfaces via `parsers_detected`).

## Size caps (Phase A candidate pool)

`MAX_CLINIT_LDC_STRINGS` (cap on `ldc` operands retained per class by
`clinit_ldc_strings`): unlike every other count cap in this crate, the
paired byte cap here cannot be the disc-file size — a `.class` entry
gated only by a `com/bydeluxe/` path prefix deflates from ~100 KB up to
the 64 MiB `MAX_CLASS_BYTES` read ceiling, so ~33M two-byte `ldc`
instructions — one retained `String` each — are reachable from a small
crafted disc. The bound has to be on the decompressed work, so it is
applied here. Headroom: the largest framework-stable enum is `Language`
at 70 values (`FINGERPRINTS`), and no fingerprint matches a count more
than `LDC_COUNT_TOLERANCE` away from its expected size, so anything past
~74 can never identify a master enum. 4096 leaves ~55x headroom over the
largest real enum for framework drift.

`MAX_CLINIT_LDC_BYTES` (companion byte cap): the count cap alone still
admits 4096 x 64 KiB of `Utf8` (a JVMS `CONSTANT_Utf8_info` length is a
u16), i.e. ~268 MB per class from repeated `ldc` of one huge constant.
Headroom: master-enum values are short display names ("English",
"HDR10 Plus", "USA_D1") well under 32 bytes, so a real Language enum
retains ~1 KB. 256 KiB admits 4096 values averaging 64 bytes each.

`MAX_CANDIDATE_TOTAL_BYTES` (aggregate companion, bounds retention across
ALL classes at once): `identify_master_enums` holds every class's
retained strings in one map simultaneously, so the per-class cap alone
still admits `classes x 256 KiB`: a 64 MiB jar of minimal `.class`
entries reaches tens of GiB. This is the same bound one level up.
Headroom: the five `FINGERPRINTS` enums together hold ~113 short values
(~1.2 KB). Every other class in a real BD-J jar contributes only
whatever string constants its own `<clinit>` loads — resource paths,
config keys — so a large authored jar lands in the low hundreds of KB.
16 MiB leaves ~40x headroom over a deliberately generous 400 KB estimate
for real media.

`MAX_CANDIDATE_CLASSES` (entry-count companion to
`MAX_CANDIDATE_TOTAL_BYTES`): the byte budget alone still admits ~16M
map entries when every class retains a single one-byte `ldc`, and the
per-entry `HashMap` + `String` overhead is not counted by that budget.
Headroom: a large retail BD-J title ships on the order of 1-3k classes,
and only those with a non-empty `<clinit>` ldc sequence become
candidates. 65536 leaves >20x headroom over the class count of any real
jar.

## Internal helpers

- `clinit_enum_field_names`: collects the static-field names an enum
  class's `<clinit>` stores its own instances into, in declaration order
  — the ordinal -> field-name mapping that mirrors `clinit_ldc_strings`'s
  ordinal -> value mapping. An obfuscated Deluxe enum constant compiles
  to `new E; dup; ldc "Value"; invokespecial E.<init>(…); putstatic
  E.<field>`, so the `putstatic` whose owning class AND field descriptor
  are both this class's own type names the field for that ordinal. The
  binding class then references that constant as `getstatic E.<field>`;
  without this mapping `MasterEnumTable` cannot turn the obfuscated field
  name back into an ordinal. Bounded by `MAX_CLINIT_LDC_STRINGS` like the
  value walk.

- `clinit_ldc_strings`: walks `<clinit>` and collects every `ldc`/`ldc_w`
  operand that resolves to a `String` or `Utf8` constant, in declaration
  order; `None` if the class has no `<clinit>`. Collection stops at
  `MAX_CLINIT_LDC_STRINGS` operands or `MAX_CLINIT_LDC_BYTES` of retained
  text, whichever comes first: the walk is driven by the decompressed
  class, so it isn't bounded by the disc-file size cap the way the rest
  of this module's counts are. Truncation cannot lose a real match — a
  truncated sequence is far longer than any `FINGERPRINTS` entry's
  `expected_count + LDC_COUNT_TOLERANCE`, so it would have been rejected
  on count anyway.

- `find_binding_classes`: identifies all binding-class candidates by
  getstatic-count to the master enums, using `MIN_GETSTATIC = 4` — the
  binding class on a typical disc has 50+ such getstatic references (one
  per slot x arity); the low floor lets a small disc with few streams
  still qualify, while still filtering out classes that just reference
  the language enum once for a config string. Some Deluxe discs split
  the per-stream table across two binding classes (one for audio, one
  for subtitle — the audio class commonly has the most getstatic refs,
  the subtitle class somewhat fewer, both sharing the master Language +
  Purpose enums), so this returns top-K candidates ordered by descending
  getstatic count, filtered to a minimum concentration of references.

`MAX_CONSTRUCTIONS` (Phase D, cap on `Construction`s retained from a
binding `<clinit>` walk): one entry is appended per matched `new X / dup
/ invokespecial X.<init>` with no other bound — a `.class` gated only by
a `com/bydeluxe/` path prefix deflates to the 64 MiB `MAX_CLASS_BYTES`
ceiling, and the shortest matching sequence is a handful of bytes, so
millions of `Construction`s (each a `String` plus an arg `Vec`) are
reachable from a small crafted disc (~1 GiB). The same cap bounds the
per-class union in `decode_binding_class` (a crafted class may repeat
`<clinit>`, which JVMS §4.6 forbids but this reader tolerates) and the
cross-class union in `parse`, so the whole phase retains at most this
many. Headroom: the BD `STN_table` (BDAV stream-entry counts) admits at
most 32 primary audio and 32 PG streams per playlist, and a Deluxe
binding table covers the disc's playlists — low hundreds of entries on
the largest retail titles. 4096 leaves >20x headroom.

`Decoder::push` (symbolic-stack push during binding `<clinit>` walking):
honours the Code attribute's declared `max_stack`. JVMS 4.7.3 requires
that a method's operand stack never exceed `max_stack` at any point, so
a push past it can only come from bytecode that would fail JVM
verification. Dropping it costs nothing on real bytecode and bounds the
decoder: a `.class` gated only by a `com/bydeluxe/` path prefix deflates
from ~100 KB to the 64 MiB `MAX_CLASS_BYTES` ceiling, i.e. ~67M
single-byte `iconst_0` (~2 GiB of `StackVal`) on an unbounded `Vec`.
Headroom: `max_stack` is exactly what javac computed for the real
binding `<clinit>`, so no real construction can be clipped. The symbolic
stack counts `long`/`double` as one slot where the JVM counts two, so
its depth is never greater than the verified depth.

## interpret_streams: Constructions -> StreamLabels

Converts the per-construction tuples from Phase D into `StreamLabel`s.
Two binding-constructor shapes are handled:

- 5-arg: `BindingType.<init>(I, Lang;, Lpurpose;, I, LCodingType;)V`
- 4-arg: `BindingType.<init>(I, Lang;, Lpurpose;, LCodingType;)V`

Args are identified by **TYPE**, not position:

- First `EnumRef{kind: "Language"}` -> audio/subtitle language
- First `EnumRef{kind: "Purpose"}` -> Deluxe purpose ordinal
- First `CodingType(name)` -> codec field name (translated via
  `coding_type_to_codec_hint`)
- First `Int(n)` -> stream index (preserved as ordering hint;
  per-type sequential stream_number is what actually goes into the
  StreamLabel, since BD spec stream-numbering is anchored on MPLS
  data, not the binding code)

Stream type inference:

- Construction has a `CodingType` arg -> audio stream (subtitles on
  Deluxe don't carry a CodingType; their codec is implicit PGS via
  the BD spec).
- Construction has Language but no CodingType -> subtitle stream.
- Neither, and its binding type never yielded a stream -> not a
  stream (skip). See `slot_kind` for why the binding type is
  consulted rather than the language alone.

`slot_kinds`: which stream list each binding type enumerates, learned
from the constructions that DID resolve a language. A `<clinit>` walk
emits a `Construction` for every `new X; ...; invokespecial X.<init>`
it sees, so the list mixes real stream bindings with whatever else the
class initializer builds. `binding_type` is the constructed class name,
which is how the two are told apart: the stream bindings all share one
class (Deluxe splits audio and subtitle across two), and that class is
identifiable from the slots that resolved. A binding type that resolved
as both kinds is left out — with no consistent answer, guessing a list
to advance would be worse than not advancing.

`slot_kind`: the stream list an unresolved construction occupies a slot
in, or `None` when it is not a stream binding. A
`org.bluray.ti.CodingType` argument is decisive on its own: nothing but
an audio stream binding is handed one. Otherwise falls back to what the
binding type's resolved siblings showed (`slot_kinds`).

## Confirmed studio variants

- **Universal** (`studio="uni"`): Language enum `pd` (65 values,
  `English, French, Spanish, Dutch, …`), Purpose enum `lp`
  (`Normal, Commentary, PiP, Trivia, Descriptive, Score`), audio binding
  `np.<init>(I, Lpd;, Llp;, Lorg/bluray/ti/CodingType;)`, subtitle binding
  `wb.<init>(I, Lpd;, Llp;, Lmi;)`, all built in one binding class's
  `<clinit>` alongside a title-wrapper object whose constructor takes the
  per-stream arrays (filtered out by the array-parameter guard in the
  decoder). SDH/RNIB is encoded as a distinct Language VALUE
  ("English SDH", "English RNIB"), recovered into the qualifier from the
  name. Grounded on fixtures captured from a real Universal Blu-ray release.
- **Disney/Warner**: 70-value Language enum, same binding shape; the
  original corpus this parser was written against.
