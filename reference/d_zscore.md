# Z-score a delayed matrix

Equivalent to centering and scaling with unit variance.

## Usage

``` r
d_zscore(x, dim = c("rows", "cols"), axis = NULL, na.rm = FALSE)
```

## Arguments

- x:

  A `delarr`.

- dim:

  Dimension over which to compute the z-score.

- axis:

  Integer axis for N-d arrays (alternative to `dim`).

- na.rm:

  Logical; remove missing values when computing statistics.

## Value

A `delarr` with the z-score applied lazily.

## Examples

``` r
mat <- matrix(c(1, 2, 3, 10, 20, 30), nrow = 2, ncol = 3)
darr <- delarr(mat)

# Z-score normalize rows
zscored <- darr |> d_zscore(dim = "rows") |> collect()
zscored
#>            [,1]       [,2]     [,3]
#> [1,] -0.6704784 -0.4789131 1.149392
#> [2,] -0.8320503 -0.2773501 1.109400

# Row means should be ~0, row SDs should be ~1
rowMeans(zscored)
#> [1] -1.850372e-17  0.000000e+00
```
