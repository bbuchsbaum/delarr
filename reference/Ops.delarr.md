# Arithmetic and comparison operators for `delarr`

Supports elementwise operations between delayed matrices or between a
delayed matrix and scalars/matrices.

## Usage

``` r
# S3 method for class 'delarr'
Ops(e1, e2)
```

## Arguments

- e1, e2:

  Operands supplied by the R math group generics.

## Value

A `delarr` representing the fused operation.
