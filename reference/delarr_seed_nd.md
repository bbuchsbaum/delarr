# Construct an N-dimensional seed backend for `delarr`

Creates a seed for arrays with 2 or more dimensions. The pull function
receives a list of per-dimension index vectors and returns the
corresponding sub-array.

## Usage

``` r
delarr_seed_nd(
  dims,
  pull,
  chunk_hint = NULL,
  dimnames = NULL,
  begin = NULL,
  end = NULL
)
```

## Arguments

- dims:

  Integer vector of dimension extents (length \>= 2).

- pull:

  A function accepting a single argument `indices` — a named or
  positional list of integer vectors (one per dimension, or `NULL` for
  "all") — and returning an array with the requested sub-dimensions.

- chunk_hint:

  Optional list describing preferred chunk sizes.

- dimnames:

  Optional list of dimnames (one element per dimension).

- begin:

  Optional function invoked before streaming begins.

- end:

  Optional function invoked after streaming completes.

## Value

An object of class `delarr_seed`.

## Examples

``` r
arr <- array(seq_len(24), dim = c(3, 4, 2))
seed <- delarr_seed_nd(
  dims = c(3, 4, 2),
  pull = function(indices) {
    idx <- lapply(seq_along(dim(arr)), function(k) indices[[k]] %||% seq_len(dim(arr)[k]))
    do.call("[", c(list(arr), idx, list(drop = FALSE)))
  }
)
dim(seed)
#> [1] 3 4 2
```
