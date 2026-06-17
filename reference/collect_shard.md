# Parallel collect using shard's shared-memory workers

Evaluates a `delarr` pipeline in parallel using
[`shard::shard_map()`](https://bbuchsbaum.github.io/shard/reference/shard_map.html).
This gives proper multi-process parallelism with shared-memory I/O,
including parallel reductions.

## Usage

``` r
collect_shard(x, workers = NULL, chunk_size = NULL, optimize = TRUE)
```

## Arguments

- x:

  A `delarr` object.

- workers:

  Number of worker processes. Defaults to `parallel::detectCores() - 1`.

- chunk_size:

  Column chunk size for sharding. If `NULL`, uses the seed's
  `chunk_hint` or a sensible default.

- optimize:

  Logical; run DAG optimizations before evaluation.

## Value

A materialised matrix or vector.

## Details

Pipelines that require full-matrix evaluation (row-wise
center/scale/zscore/ detrend), paired RHS delarrs (`d_map2` with two
delarrs), or generic (user-supplied) reductions automatically fall back
to sequential
[`collect()`](https://bbuchsbaum.github.io/delarr/reference/collect.md).

## Examples

``` r
# \donttest{
if (requireNamespace("shard", quietly = TRUE)) {
  old_conn <- getAllConnections()
  mat <- matrix(rnorm(100), 10, 10)
  darr <- delarr_shard(mat)
  result <- collect_shard(darr |> d_map(~ .x^2), workers = 2)
  all.equal(result, mat^2)
  new_conn <- setdiff(getAllConnections(), old_conn)
  for (con in new_conn) try(close(getConnection(con)), silent = TRUE)
}
# }
```
