# `/BDMV/index.bdmv` layout

Binary layout of the BD navigation index (First Play, Top Menu, and the
title table), as parsed by `src/bdnav/index.rs`.

`"INDX"` + version(4) + `indexes_start`(u32 @8) + … ; at `indexes_start`:
`index_len`(u32), First-Play object(12), Top-Menu object(12),
`num_titles`(u16), then `num_titles` title objects(12 each). Every object's
`object_type` is the top two bits of its first byte; for an HDMV object the
`id_ref` into `MovieObject.bdmv` is a big-endian u16 at object offset 6.
