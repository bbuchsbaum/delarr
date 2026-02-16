# Row means for a delayed matrix

Computes row means lazily via
[`d_reduce()`](https://bbuchsbaum.github.io/delarr/reference/d_reduce.md);
acts as a drop-in replacement for
[`matrixStats::rowMeans2()`](https://rdrr.io/pkg/matrixStats/man/rowMeans2.html).

## Usage

``` r
# S3 method for class 'delarr'
rowMeans2(x, ..., na.rm = FALSE)
```

## Arguments

- x:

  A `delarr` object.

- ...:

  Unused.

- na.rm:

  Logical; remove missing values before averaging.

## Value

A numeric vector of row means.
