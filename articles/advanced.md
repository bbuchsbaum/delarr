# Advanced Chunking and Backends in delarr

``` r
library(delarr)
```

## When should you reach for the advanced tools?

The basic workflow in
[`vignette("delarr-getting-started", package = "delarr")`](https://bbuchsbaum.github.io/delarr/articles/delarr-getting-started.md)
is enough when you only need a lazy pipeline and a final
[`collect()`](https://bbuchsbaum.github.io/delarr/reference/collect.md).
This vignette is for the next step: understanding chunk plans, running
several summaries in one pass, streaming to backends, and checking
whether an optional parallel path behaves the way you expect.

All examples use one small dense matrix and validate the key claims in
code.

``` r
set.seed(11)
mat <- matrix(
  rnorm(96),
  nrow = 12,
  ncol = 8,
  dimnames = list(paste0("sample_", 1:12), paste0("feature_", 1:8))
)
```

## What will the execution plan do?

[`explain()`](https://bbuchsbaum.github.io/delarr/reference/explain.md)
shows the effective output shape, the chunk axis, the chosen chunk size,
and the recorded operations after optimization.

``` r
pipe <- delarr(mat)[, -1] |>
  d_map(~ .x^2 + 1) |>
  d_where(function(x) x > 1.25, fill = 0)

plan <- explain(pipe, chunk_size = 3L)
plan
#> <delarr_explain> in: 12x8  out: 12x7 
#> ops: 2  chunks: 3 (cols=3) 
#> plan: map -> where
```

## How do you let `delarr` choose a chunk size?

If you do not want to hard-code `chunk_size`, you can pass a memory
budget with `target_bytes`.

``` r
adaptive_plan <- explain(pipe, target_bytes = 256)
adaptive_plan
#> <delarr_explain> in: 12x8  out: 12x7 
#> ops: 2  chunks: 4 (cols=2) 
#> plan: map -> where

adaptive_result <- collect(pipe, target_bytes = 256)
dim(adaptive_result)
#> [1] 12  7
```

## How do you compute several summaries in one pass?

[`d_reduce_many()`](https://bbuchsbaum.github.io/delarr/reference/d_reduce_many.md)
runs several built-in reducers together and returns a matrix when the
outputs have a common length.

``` r
row_summary <- d_reduce_many(
  delarr(mat),
  fns = list(sum = sum, mean = mean, max = max),
  dim = "rows",
  chunk_size = 3L
)

row_summary[1:4, , drop = FALSE]
#>                 sum       mean       max
#> sample_1 -0.8688508 -0.1086063 2.2616090
#> sample_2  3.5083317  0.4385415 2.3396931
#> sample_3 -2.9752109 -0.3719014 1.1194619
#> sample_4 -3.9809604 -0.4976200 0.5064512
```

## How do you work block-by-block?

[`block_apply()`](https://bbuchsbaum.github.io/delarr/reference/block_apply.md)
is useful when you want chunk-local summaries or diagnostics without
materializing the whole array.

``` r
col_blocks <- block_apply(
  delarr(mat),
  margin = "cols",
  size = 3L,
  fn = function(block) colMeans(block)
)

block_means <- unlist(col_blocks, use.names = FALSE)
block_means
#> [1] -0.28978420 -0.35858092 -0.45034884 -0.16311026  0.30973503  0.02181969
#> [7] -0.11859494 -0.06352597
```

## How do delayed matrix products fit into a pipeline?

[`d_matmul()`](https://bbuchsbaum.github.io/delarr/reference/d_matmul.md)
returns another `delarr`, so you can materialize only the block you need
from a larger product.

``` r
rhs <- matrix(rnorm(30), nrow = 6, ncol = 5)
product_block <- d_matmul(delarr(mat[, 1:6, drop = FALSE]), delarr(rhs))[1:4, 1:3] |>
  collect(chunk_size = 2L)

product_block
#>                [,1]       [,2]       [,3]
#> sample_1  0.6587435  0.7804025 -0.4433443
#> sample_2 -0.1209847 -1.3106836 -3.0230087
#> sample_3 -0.9759765 -0.2282075 -3.7345642
#> sample_4 -3.0678323  2.4169777  1.4554712
```

## How do you stream a transformed matrix to disk?

The writer interface is useful when the result is still large enough
that you do not want to hold it in memory. The HDF5 backend is optional;
the chunks below run only when the `hdf5r` package is installed.

``` r
X <- delarr_hdf5(tf_in, "X")
scaled <- X |> d_scale(dim = "cols", center = TRUE, scale = TRUE)
writer <- hdf5_writer(tf_out, "X_scaled", ncol = ncol(X), chunk = c(6L, 4L))

collect(scaled, into = writer, chunk_size = 4L)
```

``` r
disk_result <- read_hdf5(tf_out, "X_scaled")
rbind(
  mean = round(colMeans(disk_result), 6),
  sd = round(apply(disk_result, 2L, stats::sd), 6)
)
#>      [,1] [,2] [,3] [,4] [,5] [,6] [,7] [,8]
#> mean    0    0    0    0    0    0    0    0
#> sd      1    1    1    1    1    1    1    1
```

## How do you use shared-memory workers?

If you install the optional `shard` package,
[`collect_shard()`](https://bbuchsbaum.github.io/delarr/reference/collect_shard.md)
can evaluate a supported pipeline in worker processes while keeping the
underlying matrix in shared memory.

``` r
shard_result <- delarr_shard(mat) |>
  d_map(~ .x * 2) |>
  d_reduce(sum, dim = "rows") |>
  collect_shard(workers = 2L, chunk_size = 3L)

head(shard_result)
#>   sample_1   sample_2   sample_3   sample_4   sample_5   sample_6 
#> -1.7377015  7.0166635 -5.9504217 -7.9619208 -5.4479018  0.2611599
```

## How do you profile a candidate pipeline?

[`profile_collect()`](https://bbuchsbaum.github.io/delarr/reference/profile_collect.md)
repeats
[`collect()`](https://bbuchsbaum.github.io/delarr/reference/collect.md)
and records elapsed time plus the size of the realized output.

``` r
profile <- profile_collect(pipe, reps = 2L, chunk_size = 3L)
profile
#> <delarr_profile> reps: 2 min/median/max: 0.0000 / 0.0005 / 0.0010 sec 
#> output size (bytes): 2568
```

## Where should you go after this?

Return to
[`vignette("delarr-getting-started", package = "delarr")`](https://bbuchsbaum.github.io/delarr/articles/delarr-getting-started.md)
for the core lazy workflow, then use
[`explain()`](https://bbuchsbaum.github.io/delarr/reference/explain.md),
[`block_apply()`](https://bbuchsbaum.github.io/delarr/reference/block_apply.md),
[`d_reduce_many()`](https://bbuchsbaum.github.io/delarr/reference/d_reduce_many.md),
and
[`collect_shard()`](https://bbuchsbaum.github.io/delarr/reference/collect_shard.md)
as you tune real pipelines for storage layout, chunking, and execution
strategy.
