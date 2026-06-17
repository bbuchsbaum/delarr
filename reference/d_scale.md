# Scale a delayed matrix along rows or columns

Scale a delayed matrix along rows or columns

## Usage

``` r
d_scale(
  x,
  dim = c("rows", "cols"),
  axis = NULL,
  center = TRUE,
  scale = TRUE,
  na.rm = FALSE
)
```

## Arguments

- x:

  A `delarr`.

- dim:

  Dimension to scale.

- axis:

  Integer axis for N-d arrays (alternative to `dim`).

- center:

  Logical; subtract the mean before scaling.

- scale:

  Logical; divide by the standard deviation.

- na.rm:

  Logical; remove missing values when computing statistics.

## Value

A `delarr` with a deferred scaling operation.

## Examples

``` r
mat <- matrix(c(1, 2, 3, 10, 20, 30), nrow = 2, ncol = 3)
darr <- delarr(mat)

# Scale rows (center and divide by SD)
scaled <- darr |> d_scale(dim = "rows") |> collect()
scaled
#>            [,1]       [,2]     [,3]
#> [1,] -0.6704784 -0.4789131 1.149392
#> [2,] -0.8320503 -0.2773501 1.109400

# Scale without centering
scaled_only <- darr |> d_scale(dim = "rows", center = FALSE) |> collect()
scaled_only
#>            [,1]      [,2]     [,3]
#> [1,] 0.09578263 0.2873479 1.915653
#> [2,] 0.13867505 0.6933752 2.080126
```
