# Subset a delayed array

Performs array-style slicing lazily, capturing the indices in the DAG.
For 2D arrays, standard `x[i, j]` syntax works. For N-d arrays, provide
one index expression per dimension: `x[i, j, k, ...]`.

## Usage

``` r
# S3 method for class 'delarr'
x[..., drop = FALSE]
```

## Arguments

- x:

  A `delarr`.

- ...:

  Index expressions, one per dimension. Missing indices select all.

- drop:

  Logical indicating whether to drop dimensions (ignored lazily).

## Value

A `delarr` containing the slice operation.
