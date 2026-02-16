# Materialise a delayed matrix

Streams column chunks from the backing seed, applying deferred
operations and optional reductions on the fly. By default the result is
returned as a base matrix or vector; alternatively, supply a writer via
`into` to stream the output elsewhere (e.g.,
[`hdf5_writer()`](https://bbuchsbaum.github.io/delarr/reference/hdf5_writer.md)).

## Usage

``` r
collect(
  x,
  into = NULL,
  chunk_size = NULL,
  chunk_margin = c("cols", "rows"),
  target_bytes = NULL,
  parallel = FALSE,
  workers = NULL,
  optimize = TRUE
)
```

## Arguments

- x:

  A `delarr` object.

- into:

  Optional writer or callback used to receive streamed chunks.

- chunk_size:

  Optional chunk size along `chunk_margin`.

- chunk_margin:

  Chunking axis for non-reduction collection.

- target_bytes:

  Optional memory budget (bytes) used to adapt chunk size.

- parallel:

  Logical; attempt parallel chunk execution when safe.

- workers:

  Number of worker processes when `parallel = TRUE`.

- optimize:

  Logical; run lightweight DAG optimizations before evaluation.

## Value

A realised matrix/vector, or `NULL` invisibly when writing to `into`.

## Examples

``` r
# Basic materialization
mat <- matrix(1:12, nrow = 3, ncol = 4)
darr <- delarr(mat)
collect(darr)
#>      [,1] [,2] [,3] [,4]
#> [1,]    1    4    7   10
#> [2,]    2    5    8   11
#> [3,]    3    6    9   12

# Collect after lazy operations
result <- darr |>
  d_map(~ .x^2) |>
  collect()
result
#>      [,1] [,2] [,3] [,4]
#> [1,]    1   16   49  100
#> [2,]    4   25   64  121
#> [3,]    9   36   81  144
```
