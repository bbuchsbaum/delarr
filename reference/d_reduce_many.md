# Run multiple reductions and collect results

Run multiple reductions and collect results

## Usage

``` r
d_reduce_many(
  x,
  fns,
  dim = c("rows", "cols"),
  na.rm = FALSE,
  chunk_size = NULL,
  simplify = TRUE
)
```

## Arguments

- x:

  A `delarr`.

- fns:

  A named list of reduction functions.

- dim:

  Reduction dimension (`"rows"` or `"cols"`).

- na.rm:

  Logical; remove missing values in each reducer.

- chunk_size:

  Optional chunk size passed to
  [`collect()`](https://bbuchsbaum.github.io/delarr/reference/collect.md).

- simplify:

  Logical; combine equal-length outputs into a matrix.

## Value

A named list (or matrix when `simplify = TRUE`) of reductions.
