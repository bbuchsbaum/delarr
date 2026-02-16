# Row means for delayed matrices

Generic counterpart to
[`matrixStats::rowMeans2()`](https://rdrr.io/pkg/matrixStats/man/rowMeans2.html).
Methods are provided for `delarr` objects, but packages can extend the
generic for their own delayed types.

## Usage

``` r
rowMeans2(x, ...)
```

## Arguments

- x:

  An object for which row means should be computed.

- ...:

  Additional arguments passed to methods.

## Value

Typically a numeric vector of row means.

## Examples

``` r
mat <- matrix(1:12, nrow = 3, ncol = 4)
darr <- delarr(mat)

# Compute row means lazily
rowMeans2(darr)
#> [1] 5.5 6.5 7.5

# Compare with base R
rowMeans(mat)
#> [1] 5.5 6.5 7.5
```
