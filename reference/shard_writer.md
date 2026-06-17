# Shared-memory writer for streaming `collect()`

Creates a writer object backed by
[`shard::buffer()`](https://bbuchsbaum.github.io/shard/reference/buffer.html)
conforming to delarr's writer protocol. Pass to
`collect(x, into = shard_writer(...))` to stream results into shared
memory.

## Usage

``` r
shard_writer(nrow, ncol, backing = "auto")
```

## Arguments

- nrow, ncol:

  Dimensions of the output matrix.

- backing:

  Backing type passed to
  [`shard::buffer()`](https://bbuchsbaum.github.io/shard/reference/buffer.html).

## Value

A writer list with `$write()`, `$finalize()`, `$result()`, and
`$close()` methods.

## Note

This writer supports 2D matrices only. N-d array collection does not
currently support writer-style `into` targets.

## Examples

``` r
if (requireNamespace("shard", quietly = TRUE)) {
  mat <- matrix(rnorm(20), 4, 5)
  darr <- delarr(mat)
  w <- shard_writer(4, 5)
  collect(darr |> d_map(~ .x * 2), into = w)
  w$result()
  w$close()
}
```
