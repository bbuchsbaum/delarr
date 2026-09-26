# Elementwise math functions for `delarr`

Supports the R `Math` group generics
([`sqrt()`](https://rdrr.io/r/base/MathFun.html),
[`abs()`](https://rdrr.io/r/base/MathFun.html),
[`exp()`](https://rdrr.io/r/base/Log.html),
[`log()`](https://rdrr.io/r/base/Log.html),
[`round()`](https://rdrr.io/r/base/Round.html),
[`floor()`](https://rdrr.io/r/base/Round.html), the trig functions, and
so on) lazily, as fused elementwise maps. Extra arguments are forwarded
to the underlying function, so `round(x, 2)` and `log(x, base = 2)` work
as expected.

## Usage

``` r
# S3 method for class 'delarr'
Math(x, ...)
```

## Arguments

- x:

  A `delarr`.

- ...:

  Additional arguments forwarded to the math generic.

## Value

A `delarr` representing the fused elementwise operation.

## Details

The cumulative generics
([`cumsum()`](https://rdrr.io/r/base/cumsum.html),
[`cumprod()`](https://rdrr.io/r/base/cumsum.html),
[`cummax()`](https://rdrr.io/r/base/cumsum.html),
[`cummin()`](https://rdrr.io/r/base/cumsum.html)) are not elementwise
and cannot be evaluated chunk-by-chunk, so they raise an error rather
than returning silently incorrect results.

## Examples

``` r
mat <- matrix(1:12, nrow = 3, ncol = 4)
darr <- delarr(mat)
collect(sqrt(darr))
#>          [,1]     [,2]     [,3]     [,4]
#> [1,] 1.000000 2.000000 2.645751 3.162278
#> [2,] 1.414214 2.236068 2.828427 3.316625
#> [3,] 1.732051 2.449490 3.000000 3.464102
collect(-darr)
#>      [,1] [,2] [,3] [,4]
#> [1,]   -1   -4   -7  -10
#> [2,]   -2   -5   -8  -11
#> [3,]   -3   -6   -9  -12
```
