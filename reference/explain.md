# Explain a delayed execution plan

Explain a delayed execution plan

## Usage

``` r
explain(
  x,
  chunk_size = NULL,
  chunk_margin = c("cols", "rows"),
  target_bytes = NULL,
  optimize = TRUE
)
```

## Arguments

- x:

  A `delarr`.

- chunk_size:

  Optional chunk size hint.

- chunk_margin:

  Chunking axis for non-reduction materialization.

- target_bytes:

  Optional memory budget used for adaptive chunking.

- optimize:

  Logical; whether to explain the optimized DAG.

## Value

An object of class `delarr_explain`.
