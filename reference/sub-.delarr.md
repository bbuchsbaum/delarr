# Subset a delayed matrix

Performs matrix-style slicing lazily, capturing the indices in the DAG.

## Usage

``` r
# S3 method for class 'delarr'
x[i, j, drop = FALSE]
```

## Arguments

- x:

  A `delarr`.

- i:

  Row indices or `NULL`.

- j:

  Column indices or `NULL`.

- drop:

  Logical indicating whether to drop dimensions (ignored lazily).

## Value

A `delarr` containing the slice operation.
