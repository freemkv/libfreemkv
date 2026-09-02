# Fox — `dcx.xml` label parser

Older Fox authoring (e.g. a real Fox release, `/BDMV/JAR/05001/dcx.xml`) ships a
human-readable XML manifest alongside the BD-J jar. Its root is `<dcx>`, and
under `<disc>` it lists every playlist the disc plays. The main-feature
playlists carry nested per-stream `<audio>`/`<subtitle>` elements naming
language, editorial purpose and forced/SDH state outright — everything a
label parser wants, in attributes, with no bytecode to walk.

NOT A SPECIFICATION. `/BDMV/JAR/` is application-defined space, so this file
is one authoring house's internal metadata that happens to press onto the
disc. Every field meaning below was read off a real disc; treat an
unfamiliar value as unknown rather than guessing.

## Confirmed schema

A real Fox release, `/BDMV/JAR/05001/dcx.xml`:

```xml
<dcx>
  <disc>
    <playlist id="00001" lang="eng" name="topmenu"/>
    ...
    <playlist id="00800" lang="eng" name="feature" vers="1" durs="7628">
      <audio id="01" lang="eng" type="feature"/>
      <audio id="02" lang="eng" type="rnib"/>
      <audio id="03" lang="spa" dial="lat" type="feature"/>
      ...
      <subtitle id="01" lang="eng" type="feature" form="sdh"/>
      <subtitle id="02" lang="spa" dial="lat" type="embed"/>
      ...
      <subtitle id="11" lang="eng" type="text"/>
      <properties> ...chapter marks... </properties>
    </playlist>
    <playlist id="00801" lang="jpn" name="feature" vers="1" durs="7628"> ... </playlist>
  </disc>
</dcx>
```

## Field meanings (read off the disc, not documented)

* `<audio type>`: `feature` = a normal program track; `rnib` = a
  descriptive/narration track ("Royal National Institute of Blind People"
  described-video), mapped to `LabelPurpose::Descriptive`. A `type`
  containing "comment" maps to `LabelPurpose::Commentary`.
* `<subtitle form>`: `sdh` marks a subtitles-for-the-deaf-and-hard-of-
  hearing track (`LabelQualifier::Sdh`).
* `<subtitle type>`: `embed` marks a dedicated forced/embedded-subtitle
  track (`LabelQualifier::Forced`); `feature`/`text` are full tracks.
* `id` on a nested stream is its 1-based STN slot WITHIN ITS TYPE
  (audio ids 01..N, subtitle ids 01..N independently), matching the vendor
  `stream_number` convention the ordinal binder in `super::apply_labels`
  reads.

The `<audio>`/`<subtitle>` elements are NESTED inside one feature playlist,
so extraction is scoped to a single `<playlist name="feature">` element —
never a document-wide `<audio>` scan, which would merge the regional
`00800` (eng) and `00801` (jpn) tables into one and collide their slots.

## `select_feature_playlist` scoping rationale

Among all `<playlist name="feature">` elements — Fox presses one per
regional variant (`00800` eng, `00801` jpn) — the one carrying the most
`<audio>`/`<subtitle>` streams wins, ties to the first.

Returning ONE element is the whole point: each feature playlist is its own
STN table with its own 1-based slot ids, so merging two would put two
different streams on slot `01`. The richest table is the fullest label set
and anchors to its matching title by language sequence in
`super::apply_labels`.
