# Reduce along rows or columns lazily

Reduce along rows or columns lazily

## Usage

``` r
d_reduce(x, f = base::sum, dim = c("rows", "cols"), na.rm = FALSE)
```

## Arguments

- x:

  A `delarr`.

- f:

  A reduction function (defaults to `sum`).

- dim:

  Dimension to reduce, either "rows" or "cols".

- na.rm:

  Logical; remove missing values while reducing.

## Value

A `delarr` capturing the reduction.

## Examples

``` r
mat <- matrix(1:12, nrow = 3, ncol = 4)
darr <- delarr(mat)

# Row sums (reduce across columns for each row)
row_sums <- darr |> d_reduce(sum, dim = "rows") |> collect()
row_sums
#> [1] 22 26 30

# Column means (reduce across rows for each column)
col_means <- darr |> d_reduce(mean, dim = "cols") |> collect()
col_means
#> [1]  2  5  8 11
```
