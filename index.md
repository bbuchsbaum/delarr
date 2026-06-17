# delarr

`delarr` provides a lightweight delayed array type for R with a
tidy-friendly API. It keeps the surface area small—one S3 class plus a
handful of verbs—while offering fused elementwise transforms,
reductions, and streamed materialisation. The package supports ordinary
2D matrices and N-dimensional arrays with `length(dim(x)) >= 2`.
Streamed results can also be written straight to disk via the bundled
HDF5 writer.

## Installation

The package is under active development. Clone the repository and use
[`pkgload::load_all()`](https://pkgload.r-lib.org/reference/load_all.html)
or `devtools::install()` to experiment with the API.

``` r
# install.packages("pkgload")
pkgload::load_all(".")
```

## Getting started

``` r
library(delarr)

mat <- matrix(rnorm(20), 5, 4)
arr <- delarr(mat)

# Lazy pipeline
out <- arr |>
  d_center(dim = "rows", na.rm = TRUE) |>
  d_map(~ .x * 0.5) |>
  d_reduce(mean, dim = "rows")

collect(out)
```

### Multidimensional arrays

`delarr` is not limited to matrices. In-memory arrays and HDF5 datasets
with 3 or more dimensions are supported too.

``` r
library(delarr)

x <- array(rnorm(3 * 4 * 5), dim = c(3, 4, 5))

# Slice lazily and operate along an explicit axis
out <- delarr(x) |>
  d_center(axis = 3L) |>
  d_reduce(mean, axis = 3L)

dim(collect(out))
#> [1] 3 4
```

### Streaming straight to disk

``` r
# assume `X` lives inside an HDF5 file
lzy <- delarr_hdf5("input.h5", "X")

# Apply a transformation lazily and stream the result into a new dataset
# (dim(lzy)[2] supplies the total column count for the writer)
lzy |>
  d_zscore(dim = "cols") |>
  collect(into = hdf5_writer(
    path = "output.h5",
    dataset = "X_zscore",
    ncol = dim(lzy)[2],
    chunk = c(128L, 4096L)
  ))
```

## Backends

- [`delarr_mem()`](https://bbuchsbaum.github.io/delarr/reference/delarr_mem.md)
  wraps any in-memory matrix or array with at least 2 dimensions.
- [`delarr_hdf5()`](https://bbuchsbaum.github.io/delarr/reference/delarr_hdf5.md)
  exposes a dataset through `hdf5r`, including N-dimensional datasets.
- [`delarr_mmap()`](https://bbuchsbaum.github.io/delarr/reference/delarr_mmap.md)
  streams 2D matrices from a memory-mapped binary file via the `mmap`
  package.
- [`delarr_backend()`](https://bbuchsbaum.github.io/delarr/reference/delarr_backend.md)
  lets you create a seed from any `(rows, cols) -> matrix` pull
  function.
- [`hdf5_writer()`](https://bbuchsbaum.github.io/delarr/reference/hdf5_writer.md)
  pairs with `collect(into = ...)` to stream results back to disk
  without materialising the full matrix in memory (supply `ncol` to size
  the destination dataset up front).

The core package depends only on `rlang`. The `hdf5r` and `mmap`
backends are optional: they live in `Suggests`, and the relevant
constructors raise an informative error if the package is not installed.
You can also add new backends yourself via
[`delarr_backend()`](https://bbuchsbaum.github.io/delarr/reference/delarr_backend.md)
without taking on any extra dependency.

## Pipelined verbs

- `d_map()/d_map2()` for elementwise transformations.
- `d_center()/d_scale()/d_zscore()/d_detrend()` for common
  preprocessing, each with optional `na.rm` handling. For N-d arrays,
  use `axis =`.
- [`d_reduce()`](https://bbuchsbaum.github.io/delarr/reference/d_reduce.md)
  for row-wise or column-wise reductions, or explicit axis-based
  reductions on N-d arrays, with streaming `na.rm` support for
  sum/mean/min/max.
- [`d_where()`](https://bbuchsbaum.github.io/delarr/reference/d_where.md)
  for masked updates, optionally replacing masked entries via the `fill`
  argument.
- [`collect()`](https://bbuchsbaum.github.io/delarr/reference/collect.md)
  to realise the data (streamed in chunks), optionally writing to disk
  with
  [`hdf5_writer()`](https://bbuchsbaum.github.io/delarr/reference/hdf5_writer.md),
  and
  [`block_apply()`](https://bbuchsbaum.github.io/delarr/reference/block_apply.md)
  for chunk-wise computation.
- [`d_aperm()`](https://bbuchsbaum.github.io/delarr/reference/d_aperm.md)
  for lazy dimension permutation on N-d arrays.

All verbs return another `delarr`, so pipelines stay lazy until
[`collect()`](https://bbuchsbaum.github.io/delarr/reference/collect.md)
materialises the result.

## Testing

The test suite exercises the core class, slicing, verb fusion,
reductions, chunk-aware execution, and the HDF5 streaming writer. Run it
locally with:

``` r
pkgload::load_all(".")
testthat::test_dir("tests/testthat")
```

## Roadmap

The core abstraction is stable: the in-memory, HDF5, and memory-mapped
backends, the fused verb pipeline, chunk-aware
[`collect()`](https://bbuchsbaum.github.io/delarr/reference/collect.md),
the streaming HDF5 writer, and lazy matrix products
([`d_matmul()`](https://bbuchsbaum.github.io/delarr/reference/d_matmul.md))
are all implemented, documented, and tested. Two vignettes
([`vignette("delarr-getting-started")`](https://bbuchsbaum.github.io/delarr/articles/delarr-getting-started.md)
and
[`vignette("advanced")`](https://bbuchsbaum.github.io/delarr/articles/advanced.md))
cover the workflow end to end, and benchmark scripts live in `notes/`.

Possible future directions, none of which are required for current use:

- Optional sparse-matrix adapters, where a backend can return sparse
  blocks without forcing them dense.
- Writer-style `into=` targets for N-dimensional
  [`collect()`](https://bbuchsbaum.github.io/delarr/reference/collect.md)
  (currently supported for 2D output and via custom
  `into = function(...)` callbacks).
- Promoting the `notes/` benchmarks into a dedicated performance
  article.
