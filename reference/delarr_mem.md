# Create a delayed matrix from an in-memory matrix

Create a delayed matrix from an in-memory matrix

## Usage

``` r
delarr_mem(x)
```

## Arguments

- x:

  A numeric or logical matrix, or an array with at least 2 dimensions.

## Value

A `delarr` referencing the original object.

## Examples

``` r
# Wrap an in-memory matrix
mat <- matrix(1:12, nrow = 3, ncol = 4)
darr <- delarr_mem(mat)
darr
#> <delarr> 3 x 4 lazy

# Apply operations lazily
result <- darr |> d_center(dim = "rows") |> collect()
result
#>      [,1] [,2] [,3] [,4]
#> [1,] -4.5 -1.5  1.5  4.5
#> [2,] -4.5 -1.5  1.5  4.5
#> [3,] -4.5 -1.5  1.5  4.5
```
