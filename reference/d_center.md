# Center a delayed matrix along rows or columns

Center a delayed matrix along rows or columns

## Usage

``` r
d_center(x, dim = c("rows", "cols"), axis = NULL, na.rm = FALSE)
```

## Arguments

- x:

  A `delarr`.

- dim:

  Dimension along which to subtract the mean.

- axis:

  Integer axis for N-d arrays (alternative to `dim`).

- na.rm:

  Logical; remove missing values when computing the centre.

## Value

A `delarr` with a deferred centering operation.

## Examples

``` r
mat <- matrix(c(1, 2, 3, 10, 20, 30), nrow = 2, ncol = 3)
darr <- delarr(mat)

# Center rows (subtract row means)
centered_rows <- darr |> d_center(dim = "rows") |> collect()
centered_rows
#>      [,1] [,2] [,3]
#> [1,]   -7   -5   12
#> [2,]  -12   -4   16
rowMeans(centered_rows)  # Should be ~0
#> [1] 0 0

# Center columns (subtract column means)
centered_cols <- darr |> d_center(dim = "cols") |> collect()
colMeans(centered_cols)  # Should be ~0
#> [1] 0 0 0
```
