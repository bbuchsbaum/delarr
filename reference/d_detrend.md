# Detrend a delayed matrix

Removes a polynomial trend of the specified degree along the chosen
dimension.

## Usage

``` r
d_detrend(x, dim = c("rows", "cols"), degree = 1L)
```

## Arguments

- x:

  A `delarr`.

- dim:

  Dimension along which to fit the trend.

- degree:

  Polynomial degree (default 1).

## Value

A `delarr` with the detrend operation queued.

## Examples

``` r
# Create matrix with linear trend in rows
mat <- matrix(1:12 + rep(1:4, each = 3), nrow = 3, ncol = 4)
darr <- delarr(mat)

# Remove linear trend along rows
detrended <- darr |> d_detrend(dim = "rows", degree = 1L) |> collect()
detrended
#>      [,1] [,2] [,3] [,4]
#> [1,]    0    0    0    0
#> [2,]    0    0    0    0
#> [3,]    0    0    0    0

# Remove quadratic trend
quad_detrend <- darr |> d_detrend(dim = "rows", degree = 2L) |> collect()
quad_detrend
#>      [,1] [,2] [,3] [,4]
#> [1,]    0    0    0    0
#> [2,]    0    0    0    0
#> [3,]    0    0    0    0
```
