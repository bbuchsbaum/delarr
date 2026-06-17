# Create a delayed array backed by shared memory

Wraps a numeric matrix or array into shard's shared memory, returning a
`delarr`. The shared ALTREP vector is stored on the seed so that
[`collect_shard()`](https://bbuchsbaum.github.io/delarr/reference/collect_shard.md)
can reuse it without re-sharing (zero-copy).

## Usage

``` r
delarr_shard(x, backing = "auto")
```

## Arguments

- x:

  A numeric matrix or array.

- backing:

  Backing type passed to
  [`shard::share()`](https://bbuchsbaum.github.io/shard/reference/share.html).

## Value

A `delarr` backed by shared memory.

## Examples

``` r
if (requireNamespace("shard", quietly = TRUE)) {
  mat <- matrix(rnorm(20), 4, 5)
  darr <- delarr_shard(mat)
  collect(darr)
}
#>            [,1]       [,2]       [,3]       [,4]       [,5]
#> [1,] -0.3872136 -1.7562754  0.2271271  0.2585373  0.4248584
#> [2,] -0.7854327 -0.6905379  0.9784549 -0.4417995 -1.6842815
#> [3,] -1.0567369 -0.5585420 -0.2088827  0.5685999  0.2494018
#> [4,] -0.7955414 -0.5366633 -1.3994105  2.1268505  1.0728383
```
