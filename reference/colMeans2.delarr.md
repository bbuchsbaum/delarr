# Column means for a delayed matrix

Computes column means lazily via
[`d_reduce()`](https://bbuchsbaum.github.io/delarr/reference/d_reduce.md);
acts as a drop-in replacement for
[`matrixStats::colMeans2()`](https://rdrr.io/pkg/matrixStats/man/rowMeans2.html).

## Usage

``` r
# S3 method for class 'delarr'
colMeans2(x, ..., na.rm = FALSE)
```

## Arguments

- x:

  A `delarr` object.

- ...:

  Unused.

- na.rm:

  Logical; remove missing values before averaging.

## Value

A numeric vector of column means.
