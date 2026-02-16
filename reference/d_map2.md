# Apply a binary elementwise transformation lazily

Apply a binary elementwise transformation lazily

## Usage

``` r
d_map2(x, y, f)
```

## Arguments

- x:

  A `delarr`.

- y:

  Another `delarr`, matrix, or numeric vector/scalar.

- f:

  A function or formula combining two arguments.

## Value

A `delarr` representing the fused binary operation.

## Examples

``` r
mat1 <- matrix(1:12, nrow = 3, ncol = 4)
mat2 <- matrix(12:1, nrow = 3, ncol = 4)
darr1 <- delarr(mat1)
darr2 <- delarr(mat2)

# Binary operation between two delayed matrices
added <- d_map2(darr1, darr2, ~ .x + .y) |> collect()
added
#>      [,1] [,2] [,3] [,4]
#> [1,]   13   13   13   13
#> [2,]   13   13   13   13
#> [3,]   13   13   13   13

# Binary operation with scalar
scaled <- d_map2(darr1, 10, ~ .x * .y) |> collect()
scaled
#>      [,1] [,2] [,3] [,4]
#> [1,]   10   40   70  100
#> [2,]   20   50   80  110
#> [3,]   30   60   90  120
```
