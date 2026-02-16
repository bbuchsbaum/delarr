# Delayed matrix multiplication

Delayed matrix multiplication

## Usage

``` r
d_matmul(x, y, chunk_size = NULL)
```

## Arguments

- x:

  A `delarr` or base matrix.

- y:

  A `delarr` or base matrix.

- chunk_size:

  Optional chunk size used during block pulls.

## Value

A `delarr` representing `%*%`.
