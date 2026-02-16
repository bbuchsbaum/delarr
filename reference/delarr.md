# Create a delayed matrix

Wraps an existing matrix or `delarr_seed` in the lightweight delayed
pipeline. Matrix inputs are wrapped in a seed that simply slices the
source object, while `delarr` inputs are returned unchanged.

## Usage

``` r
delarr(x, ...)
```

## Arguments

- x:

  A base matrix or a `delarr_seed` to wrap.

- ...:

  Future extensions; currently ignored.

## Value

A `delarr` object representing the delayed matrix.

## Examples

``` r
# Create a delayed matrix from a regular matrix
mat <- matrix(1:12, nrow = 3, ncol = 4)
darr <- delarr(mat)
darr
#> <delarr> 3 x 4 lazy

# Operations are queued lazily
result <- darr * 2
result
#> <delarr> 3 x 4 - ops: map_const

# Materialize with collect()
collect(result)
#>      [,1] [,2] [,3] [,4]
#> [1,]    2    8   14   20
#> [2,]    4   10   16   22
#> [3,]    6   12   18   24
```
