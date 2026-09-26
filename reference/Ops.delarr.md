# Arithmetic and comparison operators for `delarr`

Supports elementwise operations between delayed matrices or between a
delayed matrix and scalars/matrices. The unary `+`/`-` forms (e.g. `-x`)
are also handled and stay lazy.

## Usage

``` r
# S3 method for class 'delarr'
Ops(e1, e2)
```

## Arguments

- e1, e2:

  Operands supplied by the R math group generics. For the unary `+`/`-`
  forms `e2` is missing.

## Value

A `delarr` representing the fused operation.
