# Reduce along a dimension lazily

For 2D arrays use `dim = "rows"` or `"cols"`. For N-d arrays you can
also supply a numeric `axis` indicating which dimension to collapse.

## Usage

``` r
d_reduce(x, f = base::sum, dim = c("rows", "cols"), axis = NULL, na.rm = FALSE)
```

## Arguments

- x:

  A `delarr`.

- f:

  A reduction function (defaults to `sum`).

- dim:

  Dimension to reduce: `"rows"` (keep rows, collapse cols) or `"cols"`
  (keep cols, collapse rows).

- axis:

  Integer axis to collapse (alternative to `dim` for N-d arrays). Takes
  precedence over `dim` when both are supplied.

- na.rm:

  Logical; remove missing values while reducing.

## Value

A `delarr` capturing the reduction.

## Examples

``` r
mat <- matrix(1:12, nrow = 3, ncol = 4)
darr <- delarr(mat)

row_sums <- darr |> d_reduce(sum, dim = "rows") |> collect()
row_sums
#> [1] 22 26 30

col_means <- darr |> d_reduce(mean, dim = "cols") |> collect()
col_means
#> [1]  2  5  8 11
```
