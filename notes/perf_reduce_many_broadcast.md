# Performance Follow-up: Multi-Reduce and Vector Broadcast

This note captures the performance-oriented follow-up from the March 29, 2026
review.

## `d_reduce_many()`

Built-in reducers (`sum`, `mean`, `min`, `max`) now share a single streamed pass
over column chunks instead of replaying the delayed pipeline once per reducer.

Observable effect:

- the pull-count regression in `tests/testthat/test-advanced.R` verifies that a
  3-function row summary over 7 columns and `chunk_size = 3` performs exactly
  `ceiling(7 / 3) = 3` seed pulls, not 9.

## Vector broadcasting

Operator-generated vector broadcasts (`x + row_vec`, `col_vec / x`, comparisons,
etc.) now use margin-aware vector application instead of materializing a dense
broadcast matrix for each chunk.

Observable effect:

- semantics are covered by left/right, row/column, and comparison regressions in
  `tests/testthat/test-edge-cases.R`
- the hot path no longer needs `broadcast_rhs()` to expand row/column vectors to
  full chunk-sized matrices for standard Ops dispatch
