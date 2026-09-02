# `disc::profile`

Everything in `DiscProfile` is DERIVED from the `Disc` model at construction:
the `Stream` enum is split into three typed vectors (audio/video/subtitle),
the `qualifier` / `purpose` enums are decomposed into booleans, and the
"default track" selection is precomputed so downstream never recomputes it.
Every field is always populated — a sensible default (`"und"` language, empty
`name`, `false` flag) stands in where the model has nothing, so consumers
never handle a bare `Option`.
