# delarr

`delarr` provides a lightweight delayed matrix type for R with a tidy-friendly
API. It keeps the surface area small—one S3 class plus a handful of verbs—while
offering fused elementwise transforms, reductions, and streaming materialisation
in column chunks. Column blocks can also be streamed straight to disk via the
bundled HDF5 writer.

## Installation

The package is under active development. Clone the repository and use
`pkgload::load_all()` or `devtools::install()` to experiment with the API.

```r
# install.packages("pkgload")
pkgload::load_all(".")
```

## Getting started

```r
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

### Streaming straight to disk

```r
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

- `delarr_mem()` wraps any in-memory matrix.
- `delarr_hdf5()` exposes a dataset through `hdf5r`.
- `delarr_backend()` lets you create a seed from any `(rows, cols) -> matrix`
pull function.
- `hdf5_writer()` pairs with `collect(into = ...)` to stream results back to
  disk without materialising the full matrix in memory (supply `ncol` to size
  the destination dataset up front).

Additional backends (e.g., mmap) can be layered on by supplying a compatible
pull function; no extra dependencies ship in the core package.

## Pipelined verbs

- `d_map()/d_map2()` for elementwise transformations.
- `d_center()/d_scale()/d_zscore()/d_detrend()` for common preprocessing, each
  with optional `na.rm` handling.
- `d_reduce()` for row-wise or column-wise reductions, with streaming
  `na.rm` support for sum/mean/min/max.
- `d_where()` for masked updates, optionally replacing masked entries via the
  `fill` argument.
- `collect()` to realise the data (streamed in chunks), optionally writing to
  disk with `hdf5_writer()`, and `block_apply()` for chunk-wise computation.

All verbs return another `delarr`, so pipelines stay lazy until `collect()`
materialises the result.

## Integration with fmridataset

`delarr_backend()` mirrors the contract of the existing DelayedArray seeds in
*fmridataset*: provide `backend_get_dims()` and `backend_get_data()` style
functions, and wrap them in a simple pull closure. Replacing the S4 seeds with
`as_delarr()` methods becomes a matter of creating `delarr_backend()` objects
and updating call sites to rely on matrix-style subsetting, streaming
`collect()`, and `d_reduce()` for materialisation.

## Testing

The test suite exercises the core class, slicing, verb fusion, reductions,
chunk-aware execution, and the HDF5 streaming writer. Run it locally with:

```r
pkgload::load_all(".")
testthat::test_dir("tests/testthat")
```

## Roadmap

- Add optional mmap and other backends once their APIs are finalised.
- Introduce optional sparse adapters and BLAS helpers where it pays off.
- Expand documentation with vignettes and performance benchmarks.
