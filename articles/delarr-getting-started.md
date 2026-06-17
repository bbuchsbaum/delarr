# Getting Started with delarr

``` r
library(delarr)
```

## What problem does `delarr` solve?

`delarr` lets you write matrix pipelines as if everything were already
in memory while deferring the actual work until
[`collect()`](https://bbuchsbaum.github.io/delarr/reference/collect.md).
That matters when the source matrix lives on disk, when you want to
avoid intermediate allocations, or when you need to stream the result
directly into another backend.

This vignette covers one small lazy pipeline, one streaming write to
HDF5, and one custom backend. For chunk planning, profiling, and
optional shared-memory workers, see
[`vignette("advanced", package = "delarr")`](https://bbuchsbaum.github.io/delarr/articles/advanced.md).

## What does a lazy pipeline look like?

``` r
set.seed(1)
mat <- matrix(
  rnorm(24),
  nrow = 6,
  ncol = 4,
  dimnames = list(paste0("sample_", 1:6), paste0("feature_", 1:4))
)

lazy_mean <- delarr(mat) |>
  d_center(dim = "rows") |>
  d_map(~ .x * 0.5) |>
  d_reduce(mean, dim = "rows")

lazy_mean
#> <delarr> 6 x 1 - ops: center(rows) -> map -> reduce(rows)
```

Nothing has been materialized yet. The object is still a `delarr`, and
the work is only a recorded plan.

``` r
row_summary <- collect(lazy_mean, chunk_size = 2L)
max(abs(row_summary))
#> [1] 6.245005e-17
```

After row-centering, every row has mean zero. The printed value is the
largest absolute row mean after collection.

## How do row and column vectors broadcast?

Scalars, row-sized vectors, and column-sized vectors stay lazy too.
`delarr` infers whether a vector should broadcast across rows or columns
from its length.

``` r
row_bias <- c(-1, 0, 1, 2, 3, 4)
col_scale <- c(1, 0.5, 2, 1.5)

broadcasted <- collect((delarr(mat) + row_bias) * col_scale, chunk_size = 2L)
broadcasted[1:3, , drop = FALSE]
#>           feature_1  feature_2 feature_3  feature_4
#> sample_1 -1.6264538 -0.2562855 -3.242481 -0.2681682
#> sample_2  0.1836433  0.3691624 -4.429400  0.8908520
#> sample_3  0.1643714  0.7878907  4.249862  2.8784661
```

This length-based inference only works when the matrix is non-square, so
that `nrow` and `ncol` are distinct. For a **square** `n`-by-`n` matrix
a bare length-`n` vector is ambiguous — its length matches both
dimensions — and `delarr` resolves the tie to **row-aligned** (one value
per row, broadcast across the columns). This matches base R, where
`matrix + vector` recycles the vector down the columns and so also
aligns element `i` to row `i`. `delarr` emits a one-time warning at this
point to flag the ambiguity:

``` r
sq <- matrix(1:9, 3, 3)
biased <- delarr(sq) + c(10, 20, 30)
#> Warning: Ambiguous broadcast: a length-3 vector against a square 3x3
#> matrix is interpreted as row-aligned (one value per row) ...
collect(biased)
```

If you actually want **column** alignment on a square matrix you cannot
express it with a bare vector; pass an explicit conformable matrix
instead:

``` r
collect(delarr(sq) + matrix(c(10, 20, 30), 3, 3, byrow = TRUE))
```

Silence the warning with
`options(delarr.warn_ambiguous_broadcast = FALSE)` once you have
confirmed the row-aligned default is what you intend.

## How do you stream a result to HDF5?

[`delarr_hdf5()`](https://bbuchsbaum.github.io/delarr/reference/delarr_hdf5.md)
reads a dataset lazily, and
[`hdf5_writer()`](https://bbuchsbaum.github.io/delarr/reference/hdf5_writer.md)
lets you stream the transformed result back to disk without
materializing the full output matrix in R. The HDF5 backend is optional:
install the `hdf5r` package to enable it. The code below runs only when
`hdf5r` is available.

``` r
X <- delarr_hdf5(tf_in, "X")
writer <- hdf5_writer(tf_out, "X_z", ncol = ncol(X), chunk = c(5L, 3L))

collect(X |> d_zscore(dim = "cols"), into = writer, chunk_size = 3L)
```

``` r
z <- read_hdf5(tf_out, "X_z")
rbind(
  mean = round(colMeans(z), 6),
  sd = round(apply(z, 2L, stats::sd), 6)
)
#>      [,1] [,2] [,3] [,4] [,5] [,6]
#> mean    0    0    0    0    0    0
#> sd      1    1    1    1    1    1
```

## How do you wrap your own storage layer?

A custom backend only needs matrix dimensions and a `pull()` function
that can return arbitrary row and column slices. Here the backing store
is just another matrix, but the same pattern works for databases, APIs,
or memory-mapped files.

``` r
custom <- delarr_backend(
  nrow = nrow(source_mat),
  ncol = ncol(source_mat),
  pull = function(rows = NULL, cols = NULL) {
    if (is.null(rows)) rows <- seq_len(nrow(source_mat))
    if (is.null(cols)) cols <- seq_len(ncol(source_mat))
    source_mat[rows, cols, drop = FALSE]
  },
  dimnames = dimnames(source_mat)
)

custom_result <- custom[1:4, 2:5] |>
  d_map(~ .x^2) |>
  collect(chunk_size = 2L)

custom_result
#>       col_2 col_3 col_4 col_5
#> row_1   121   441   961  1681
#> row_2   144   484  1024  1764
#> row_3   169   529  1089  1849
#> row_4   196   576  1156  1936
```

## Where should you go next?

Use
[`collect()`](https://bbuchsbaum.github.io/delarr/reference/collect.md)
when you want to control chunk size or stream into a writer,
[`delarr_backend()`](https://bbuchsbaum.github.io/delarr/reference/delarr_backend.md)
when you need a custom backend, and
[`vignette("advanced", package = "delarr")`](https://bbuchsbaum.github.io/delarr/articles/advanced.md)
for execution plans, streamed multi-reducer summaries, block-wise
workflows, delayed matrix products, and optional shared-memory workers.
