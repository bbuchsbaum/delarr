# Apply a function to streamed matrix blocks

Evaluates a `delarr` slice-by-slice, materialising manageable chunks for
further processing without realising the full matrix.

## Usage

``` r
block_apply(
  x,
  margin = c("cols", "rows"),
  size = 16384L,
  fn,
  parallel = FALSE,
  workers = NULL
)
```

## Arguments

- x:

  A `delarr` object.

- margin:

  Dimension along which to chunk (`"cols"` or `"rows"`).

- size:

  Approximate chunk size.

- fn:

  Function applied to each materialised chunk.

- parallel:

  Logical; process chunks in parallel when possible.

- workers:

  Number of worker processes for parallel execution.

## Value

A list of results returned by `fn`.

## Examples

``` r
mat <- matrix(1:20, nrow = 4, ncol = 5)
darr <- delarr(mat)

# Apply function to column chunks
col_maxes <- block_apply(darr, margin = "cols", size = 2L, fn = function(block) {
  apply(block, 2, max)
})
unlist(col_maxes)
#> [1]  4  8 12 16 20

# Apply function to row chunks
row_means <- block_apply(darr, margin = "rows", size = 2L, fn = function(block) {
  rowMeans(block)
})
unlist(row_means)
#> [1]  9 10 11 12
```
