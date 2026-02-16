# Apply an elementwise transformation lazily

Apply an elementwise transformation lazily

## Usage

``` r
d_map(x, f)
```

## Arguments

- x:

  A `delarr`.

- f:

  A function or formula suitable for
  [`rlang::as_function()`](https://rlang.r-lib.org/reference/as_function.html).

## Value

A `delarr` representing the transformation.

## Examples

``` r
mat <- matrix(1:12, nrow = 3, ncol = 4)
darr <- delarr(mat)

# Apply elementwise transformation with formula
squared <- darr |> d_map(~ .x^2) |> collect()
squared
#>      [,1] [,2] [,3] [,4]
#> [1,]    1   16   49  100
#> [2,]    4   25   64  121
#> [3,]    9   36   81  144

# Apply with function
logged <- darr |> d_map(log1p) |> collect()
logged
#>           [,1]     [,2]     [,3]     [,4]
#> [1,] 0.6931472 1.609438 2.079442 2.397895
#> [2,] 1.0986123 1.791759 2.197225 2.484907
#> [3,] 1.3862944 1.945910 2.302585 2.564949
```
