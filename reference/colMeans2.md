# Column means for delayed matrices

Generic counterpart to
[`matrixStats::colMeans2()`](https://rdrr.io/pkg/matrixStats/man/rowMeans2.html).
Methods are provided for `delarr` objects, but packages can extend the
generic for their own delayed types.

## Usage

``` r
colMeans2(x, ...)
```

## Arguments

- x:

  An object for which row means should be computed.

- ...:

  Additional arguments passed to methods.

## Value

Typically a numeric vector of column means.

## Examples

``` r
mat <- matrix(1:12, nrow = 3, ncol = 4)
darr <- delarr(mat)

# Compute column means lazily
colMeans2(darr)
#> [1]  2  5  8 11

# Compare with base R
colMeans(mat)
#> [1]  2  5  8 11
```
