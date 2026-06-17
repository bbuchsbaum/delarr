# delarr (development version)

## Broadcasting

* Broadcasting a bare length-`n` vector against a **square** `n`-by-`n` matrix
  is ambiguous (the length matches both dimensions). `delarr` continues to
  resolve this to row-aligned (one value per row, matching base R recycling),
  but now emits a warning at operation-construction time so the choice is
  explicit. Pass a conformable matrix (e.g. `matrix(v, n, n, byrow = TRUE)`)
  for column alignment. Silence the warning with
  `options(delarr.warn_ambiguous_broadcast = FALSE)`.

# delarr 0.1.0

First public release.

## Features

* Lazy `delarr` arrays with a fused, chunk-aware execution engine.
* Pipelined verbs: `d_map()`, `d_map2()`, `d_center()`, `d_scale()`,
  `d_zscore()`, `d_detrend()`, `d_reduce()`, `d_where()`, and `d_aperm()`,
  all of which stay lazy until `collect()`.
* Lazy, block-aware matrix products via `d_matmul()`.
* Storage backends: `delarr_mem()` (in-memory matrices and N-d arrays),
  `delarr_hdf5()` (HDF5 datasets, including N-d), `delarr_mmap()`
  (memory-mapped 2D matrices), and `delarr_backend()` for custom pull
  functions.
* Streaming output to disk with `hdf5_writer()` and `collect(into = ...)`.
* `block_apply()` for chunk-wise computation.
* Getting-started and advanced vignettes plus benchmark scripts in `notes/`.

## Dependencies

* The core package depends only on `rlang`. The `hdf5r` and `mmap` backends
  are now optional (`Suggests`); the corresponding constructors raise an
  informative error when the package is not installed.
