# Permute dimensions of a delayed array

Permute dimensions of a delayed array

## Usage

``` r
d_aperm(x, perm = rev(seq_along(dim(x))), chunk_size = NULL)
```

## Arguments

- x:

  A `delarr`.

- perm:

  A permutation of `seq_along(dim(x))`.

- chunk_size:

  Optional chunk size used for internal pulls.

## Value

A permuted `delarr`.
