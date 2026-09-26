# Changelog

## delarr 0.2.0

### Bug fixes

- Provider descriptors now reject runtime state hidden in attributes or
  language objects. Provider dimensions must be finite, non-negative,
  whole numbers within the integer range; lossy coercion no longer
  changes declared dimensions.

- Unary `-` and `+` on a `delarr` (e.g. `-x`) previously errored because
  the `Ops` group generic was called with `e2` missing. They now stay
  lazy and match their base-matrix counterparts.

- The streaming full-matrix `mean` reduction divided by
  `n_rows * n_cols` computed in integer arithmetic, which overflowed to
  `NA` for matrices with more than `.Machine$integer.max` elements. The
  element count is now computed in double precision.

### New features

- Added reconstructible provider seeds. Storage packages can now keep
  plain, serializable descriptors in lazy plans and supply
  execution-time reads via
  [`delarr_provider_pull()`](https://bbuchsbaum.github.io/delarr/reference/delarr_provider_pull.md)
  without embedding closures or live handles.

- Added a `Math` group generic method for `delarr`, so
  [`sqrt()`](https://rdrr.io/r/base/MathFun.html),
  [`abs()`](https://rdrr.io/r/base/MathFun.html),
  [`exp()`](https://rdrr.io/r/base/Log.html),
  [`log()`](https://rdrr.io/r/base/Log.html),
  [`round()`](https://rdrr.io/r/base/Round.html), the trig functions,
  and the other elementwise math generics work lazily (with extra
  arguments such as `round(x, 2)` and `log(x, base = 2)` forwarded).
  Non-elementwise cumulative generics
  ([`cumsum()`](https://rdrr.io/r/base/cumsum.html) and friends) raise
  an informative error.

### Broadcasting

- Broadcasting a bare length-`n` vector against a **square** `n`-by-`n`
  matrix is ambiguous (the length matches both dimensions). `delarr`
  continues to resolve this to row-aligned (one value per row, matching
  base R recycling), but now emits a warning at operation-construction
  time so the choice is explicit. Pass a conformable matrix
  (e.g. `matrix(v, n, n, byrow = TRUE)`) for column alignment. Silence
  the warning with `options(delarr.warn_ambiguous_broadcast = FALSE)`.

## delarr 0.1.0

CRAN release: 2026-06-30

First public release.

### Features

- Lazy `delarr` arrays with a fused, chunk-aware execution engine.
- Pipelined verbs:
  [`d_map()`](https://bbuchsbaum.github.io/delarr/reference/d_map.md),
  [`d_map2()`](https://bbuchsbaum.github.io/delarr/reference/d_map2.md),
  [`d_center()`](https://bbuchsbaum.github.io/delarr/reference/d_center.md),
  [`d_scale()`](https://bbuchsbaum.github.io/delarr/reference/d_scale.md),
  [`d_zscore()`](https://bbuchsbaum.github.io/delarr/reference/d_zscore.md),
  [`d_detrend()`](https://bbuchsbaum.github.io/delarr/reference/d_detrend.md),
  [`d_reduce()`](https://bbuchsbaum.github.io/delarr/reference/d_reduce.md),
  [`d_where()`](https://bbuchsbaum.github.io/delarr/reference/d_where.md),
  and
  [`d_aperm()`](https://bbuchsbaum.github.io/delarr/reference/d_aperm.md),
  all of which stay lazy until
  [`collect()`](https://bbuchsbaum.github.io/delarr/reference/collect.md).
- Lazy, block-aware matrix products via
  [`d_matmul()`](https://bbuchsbaum.github.io/delarr/reference/d_matmul.md).
- Storage backends:
  [`delarr_mem()`](https://bbuchsbaum.github.io/delarr/reference/delarr_mem.md)
  (in-memory matrices and N-d arrays),
  [`delarr_hdf5()`](https://bbuchsbaum.github.io/delarr/reference/delarr_hdf5.md)
  (HDF5 datasets, including N-d),
  [`delarr_mmap()`](https://bbuchsbaum.github.io/delarr/reference/delarr_mmap.md)
  (memory-mapped 2D matrices), and
  [`delarr_backend()`](https://bbuchsbaum.github.io/delarr/reference/delarr_backend.md)
  for custom pull functions.
- Streaming output to disk with
  [`hdf5_writer()`](https://bbuchsbaum.github.io/delarr/reference/hdf5_writer.md)
  and `collect(into = ...)`.
- [`block_apply()`](https://bbuchsbaum.github.io/delarr/reference/block_apply.md)
  for chunk-wise computation.
- Getting-started and advanced vignettes plus benchmark scripts in
  `notes/`.

### Dependencies

- The core package depends only on `rlang`. The `hdf5r` and `mmap`
  backends are now optional (`Suggests`); the corresponding constructors
  raise an informative error when the package is not installed.
