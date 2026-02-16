# Apply a boolean mask to a delayed matrix

Elements failing the predicate are replaced with `fill` at
materialisation time.

## Usage

``` r
d_where(x, predicate, fill = 0)
```

## Arguments

- x:

  A `delarr`.

- predicate:

  A function or formula returning a logical matrix.

- fill:

  Replacement value for elements where the predicate is `FALSE`.

## Value

A `delarr` including the mask.

## Examples

``` r
mat <- matrix(c(-1, 2, -3, 4, -5, 6), nrow = 2, ncol = 3)
darr <- delarr(mat)

# Replace negative values with 0
masked <- darr |> d_where(~ .x >= 0, fill = 0) |> collect()
masked
#>      [,1] [,2] [,3]
#> [1,]    0    0    0
#> [2,]    2    4    6

# Replace values below threshold with NA
filtered <- darr |> d_where(~ .x > 1, fill = NA) |> collect()
filtered
#>      [,1] [,2] [,3]
#> [1,]   NA   NA   NA
#> [2,]    2    4    6
```
