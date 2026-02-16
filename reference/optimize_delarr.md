# Optimize a delayed pipeline

Applies lightweight algebraic simplifications to reduce unnecessary work
during
[`collect()`](https://bbuchsbaum.github.io/delarr/reference/collect.md).

## Usage

``` r
optimize_delarr(x)
```

## Arguments

- x:

  A `delarr` object.

## Value

A `delarr` with an optimized operation list.
