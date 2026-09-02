# disc_tests: capacity/layer derivation

`scan_image_reports_capacity_in_bytes_and_the_layer_count` covers the scan
reporting the medium's size on both axes it exposes, derived from the one
sector count the caller hands in:

* `capacity_bytes` is that sector count times the 2048-byte logical sector
  (ECMA-167 / BD-ROM logical block size). It is what sizes a full-disc image
  read and what the progress percentage divides by, so a wrong scale is a
  wrong ISO length, not a cosmetic number.
* `layers` distinguishes single- from dual-layer media. The threshold sits
  between the two real capacities: a single-layer BD-25 is 12,219,392
  sectors (25,025,314,816 bytes / 2048) and a dual-layer BD-50 is 24,438,784
  sectors, so BD-25 must report 1 layer and BD-50 must report 2.

`scan_image` takes the sector count as a parameter, so this exercises the
real derivation without a 50 GB fixture.
