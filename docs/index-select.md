# FMTS index selection

Where the resolved index comes from is a separate concern from disposition
classification (`resolve_disc_index` in `src/aacs/index_select.rs`): today it
is read off the index keys the key source handed us; when Processing Keys are
available it will come from the VK derivation instead. Either way the
disposition logic in `unit_disposition` is identical.
