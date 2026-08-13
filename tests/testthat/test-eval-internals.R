# Internal eval helpers and collect edge paths

test_that("broadcast_rhs aligns column vectors by row", {
  mat <- matrix(as.double(1:12), 3, 4)
  rhs <- 1:4
  expect_equal(
    delarr:::broadcast_rhs(mat, rhs),
    matrix(rhs, 3, 4, byrow = TRUE)
  )
})

test_that("fast_vector_broadcast_op handles left-side vector ops", {
  mat <- matrix(as.double(1:12), 3, 4)
  row_vec <- c(10, 20, 30)
  expect_equal(
    delarr:::fast_vector_broadcast_op(mat, row_vec, "-", "left"),
    sweep(mat, 1L, row_vec, FUN = function(x, y) y - x)
  )
})

test_that("fast_vector_broadcast_op returns NULL for unsupported left-side ops", {
  mat <- matrix(1:6, 2, 3)
  expect_null(delarr:::fast_vector_broadcast_op(mat, 1:2, "@", "left"))
})

test_that("subset_rhs_for_chunk subsets N-d rhs arrays", {
  arr <- array(as.double(1:24), dim = c(2, 3, 4))
  chunk_context <- list(
    indices = list(1:2, 2:3, 3:4),
    full_dims = dim(arr)
  )
  expect_equal(
    delarr:::subset_rhs_for_chunk(arr, chunk_context),
    arr[1:2, 2:3, 3:4, drop = FALSE]
  )
})

test_that("apply_ops errors on unknown op types", {
  mat <- matrix(1:6, 2, 3)
  expect_error(
    delarr:::apply_ops(mat, list(list(op = "not_real"))),
    "Unknown op"
  )
})

test_that("apply_ops collects delarr rhs when chunks are absent", {
  lhs <- matrix(as.double(1:12), 3, 4)
  rhs <- matrix(as.double(13:24), 3, 4)
  ops <- list(list(
    op = "emap2",
    op_name = "+",
    rhs = delarr(rhs),
    fn = function(a, b) a + b
  ))
  expect_equal(
    delarr:::apply_ops(lhs, ops),
    lhs + rhs
  )
})

test_that("apply_reduce_full collapses all axes with NA-aware builtins", {
  mat <- matrix(c(1, NA, 3, NA), 2, 2)
  all_na <- matrix(NA_real_, 2, 2)

  expect_equal(
    delarr:::apply_reduce_full(all_na, list(axis = c(1L, 2L), fn = sum, na_rm = TRUE)),
    NA_real_
  )
  expect_equal(
    delarr:::apply_reduce_full(all_na, list(axis = c(1L, 2L), fn = mean, na_rm = TRUE)),
    NA_real_
  )
  expect_equal(
    delarr:::apply_reduce_full(all_na, list(axis = c(1L, 2L), fn = min, na_rm = TRUE)),
    NA_real_
  )
  expect_equal(
    delarr:::apply_reduce_full(all_na, list(axis = c(1L, 2L), fn = max, na_rm = TRUE)),
    NA_real_
  )
  expect_equal(
    delarr:::apply_reduce_full(mat, list(axis = c(1L, 2L), fn = sum, na_rm = TRUE)),
    sum(mat, na.rm = TRUE)
  )
})

test_that("apply_reduce_full returns NA for all-NA slices along margin", {
  mat <- matrix(c(1, NA, NA, 4), 2, 2)
  expect_equal(
    delarr:::apply_reduce_full(mat, list(dim = "rows", fn = sum, na_rm = TRUE)),
    c(1, 4)
  )
  expect_equal(
    delarr:::apply_reduce_full(mat, list(dim = "rows", fn = mean, na_rm = TRUE)),
    c(1, 4)
  )
  expect_equal(
    delarr:::apply_reduce_full(
      matrix(NA_real_, 2, 2),
      list(dim = "rows", fn = min, na_rm = TRUE)
    ),
    c(NA_real_, NA_real_)
  )
})

test_that("apply_reduce_full forwards na.rm to generic reducers", {
  mat <- matrix(c(1, NA, 3, 4), 2, 2)
  generic_sum <- function(x, na.rm = FALSE) sum(x, na.rm = na.rm)
  expect_equal(
    delarr:::apply_reduce_full(mat, list(dim = "rows", fn = generic_sum, na_rm = TRUE)),
    c(4, 4)
  )
})

test_that("apply_result_names attaches vector names for axis reductions", {
  mat <- matrix(
    1:6,
    2,
    3,
    dimnames = list(c("r1", "r2"), c("c1", "c2", "c3"))
  )
  result <- delarr:::apply_result_names(
    c(1, 2, 3),
    dimnames(mat),
    reduce_info = list(dim = "cols")
  )
  expect_equal(names(result), colnames(mat))
})

test_that("collapse_axes_from_reduce handles NULL reduce op", {
  expect_equal(delarr:::collapse_axes_from_reduce(NULL, ndim = 3L), integer())
})

test_that("merge_nd_extrema merges partial extrema with NA semantics", {
  acc <- c(NA_real_, 5, 3)
  partial <- c(2, NA_real_, 4)

  expect_equal(
    delarr:::merge_nd_extrema(acc, partial, type = "min", na_rm = FALSE),
    pmin(acc, partial)
  )
  expect_equal(
    delarr:::merge_nd_extrema(acc, partial, type = "max", na_rm = FALSE),
    pmax(acc, partial)
  )
  expect_equal(
    delarr:::merge_nd_extrema(acc, partial, type = "min", na_rm = TRUE),
    c(2, 5, 3)
  )
  expect_equal(
    delarr:::merge_nd_extrema(NULL, partial, type = "max", na_rm = TRUE),
    partial
  )
})

test_that("collect full-eval path materialises paired delarr rhs chunks", {
  lhs <- matrix(as.double(1:20), 4, 5)
  rhs <- matrix(as.double(21:40), 4, 5)
  pipeline <- d_map2(delarr(lhs), delarr(rhs), ~ .x + .y) |>
    d_center("rows")
  expect_equal(collect(pipeline), delarr:::safe_center(lhs + rhs, "rows"))
})

test_that("collect streams paired delarr rhs with row chunking", {
  lhs <- matrix(as.double(1:20), 4, 5)
  rhs <- matrix(as.double(21:40), 4, 5)
  pipeline <- d_map2(delarr(lhs), delarr(rhs), ~ .x + .y)
  expect_equal(
    collect(pipeline, chunk_margin = "rows", chunk_size = 2L),
    lhs + rhs
  )
})

test_that("collect handle_collect_output accepts list writers for reductions", {
  mat <- matrix(1:12, 3, 4)
  captured <- NULL
  writer <- list(
    write = function(x) {
      captured <<- x
    },
    finalize = function() invisible(NULL)
  )
  result <- collect(d_reduce(delarr(mat), sum, "rows"), into = writer)
  expect_null(result)
  expect_equal(captured, rowSums(mat))
})

test_that("handle_collect_output rejects unsupported into targets", {
  expect_error(
    delarr:::handle_collect_output(matrix(1), "bad"),
    "Unsupported 'into' target"
  )
})

test_that("chunked col min/max reductions handle NA-only slices", {
  mat <- matrix(
    c(NA, NA, NA, NA,
      1, NA, 7, 8,
      5, 6, NA, 12),
    nrow = 4,
    ncol = 3
  )
  x <- delarr(mat)
  expect_equal(
    collect(d_reduce(x, min, "cols", na.rm = TRUE), chunk_size = 1L),
    c(NA_real_, 1, 5)
  )
  expect_equal(
    collect(d_reduce(x, max, "cols", na.rm = TRUE), chunk_size = 1L),
    c(NA_real_, 8, 12)
  )
})

test_that("full-matrix min/max reductions treat all-NA slices as NA", {
  mat <- matrix(c(NA, NA, 1, 2), 2, 2)
  x <- delarr(mat)
  expect_equal(
    collect(d_reduce(x, min, axis = c(1L, 2L), na.rm = TRUE)),
    min(mat, na.rm = TRUE)
  )
  expect_equal(
    collect(d_reduce(x, max, axis = c(1L, 2L), na.rm = TRUE)),
    max(mat, na.rm = TRUE)
  )
})

test_that("N-d collect uses default chunk axis when all axes are blocked", {
  arr <- array(as.double(1:24), dim = c(2, 3, 4))
  pipeline <- delarr(arr) |>
    d_center(axis = 1L) |>
    d_center(axis = 2L)
  expect_equal(
    collect(pipeline, chunk_size = 2L, chunk_margin = 3L),
    collect(pipeline)
  )
})

test_that("apply_result_names returns early when dimnames are NULL", {
  mat <- matrix(1:6, 2, 3)
  expect_identical(
    delarr:::apply_result_names(mat, NULL, reduce_info = list(dim = "rows")),
    mat
  )
})

test_that("N-d streamed mean reductions honor na.rm", {
  arr <- array(
    as.double(c(1, NA, 3, 2, 4, NA, NA, NA, NA, 5, 6, NA)),
    dim = c(2, 3, 2)
  )
  result <- collect(
    d_reduce(delarr(arr), mean, axis = 3L, na.rm = TRUE),
    chunk_size = 1L,
    chunk_margin = 3L
  )
  expected <- apply(arr, c(1, 2), mean, na.rm = TRUE)
  expect_equal(result, expected)
})

test_that("collect warns when row chunking is requested with into writers", {
  mat <- matrix(1:20, 4, 5)
  writer <- list(write = function(...) invisible(NULL))
  expect_warning(
    collect(delarr(mat), into = writer, chunk_margin = "rows", chunk_size = 2L),
    "chunk_margin='rows' is not supported with into="
  )
})

test_that("collect streams paired delarr rhs with seed begin/end hooks", {
  lhs <- matrix(as.double(1:20), 4, 5)
  rhs <- matrix(as.double(21:40), 4, 5)
  tracker <- new.env(parent = emptyenv())
  tracker$rhs_began <- 0L
  rhs_seed <- delarr_backend(
    nrow = 4,
    ncol = 5,
    pull = function(rows = NULL, cols = NULL) {
      rows <- rows %||% seq_len(4)
      cols <- cols %||% seq_len(5)
      rhs[rows, cols, drop = FALSE]
    },
    begin = function() tracker$rhs_began <- tracker$rhs_began + 1L,
    end = function() invisible(NULL)
  )
  pipeline <- d_map2(delarr(lhs), delarr(rhs_seed), ~ .x + .y)
  expect_equal(collect(pipeline, chunk_size = 2L), lhs + rhs)
  expect_gt(tracker$rhs_began, 0L)
})

test_that("collect row-chunks paired delarr rhs blocks", {
  lhs <- matrix(as.double(1:20), 4, 5)
  rhs <- matrix(as.double(21:40), 4, 5)
  pipeline <- d_map2(delarr(lhs), delarr(rhs), ~ .x + .y)
  expect_equal(
    collect(pipeline, chunk_margin = "rows", chunk_size = 2L),
    lhs + rhs
  )
})

test_that("collect row-chunks use precomputed matrix rhs slices", {
  lhs <- matrix(as.double(1:20), 4, 5)
  rhs <- matrix(as.double(21:40), 4, 5)
  pipeline <- d_map2(delarr(lhs), delarr(rhs) |> d_center("rows"), ~ .x + .y)
  expect_equal(
    collect(pipeline, chunk_margin = "rows", chunk_size = 2L),
    collect(pipeline)
  )
})

test_that("full-matrix min/max reductions convert infinite chunk extrema to NA", {
  all_na <- delarr(matrix(NA_real_, 2, 2))
  expect_equal(
    collect(d_reduce(all_na, min, axis = c(1L, 2L), na.rm = TRUE), chunk_size = 1L),
    NA_real_
  )
  expect_equal(
    collect(d_reduce(all_na, max, axis = c(1L, 2L), na.rm = TRUE), chunk_size = 1L),
    NA_real_
  )
})

test_that("collect skips atomic rhs entries when building paired contexts", {
  mat <- matrix(as.double(1:12), 3, 4)
  pipeline <- d_map2(delarr(mat), 1:4, ~ .x + .y)
  expect_equal(
    collect(pipeline, chunk_size = 2L),
    sweep(mat, 2, 1:4, "+")
  )
})

test_that("full-matrix min reductions merge finite partial extrema across chunks", {
  mat <- matrix(c(5, 3, 1, 4), 2, 2)
  expect_equal(
    collect(d_reduce(delarr(mat), min, axis = c(1L, 2L)), chunk_size = 1L),
    min(mat)
  )
})

test_that("column min reductions merge partial extrema across chunks", {
  mat <- matrix(c(5, 1, NA, 2, 3, NA, 4, 6, 7, 8), 5, 2)
  expect_equal(
    collect(d_reduce(delarr(mat), min, "cols", na.rm = TRUE), chunk_size = 1L),
    apply(mat, 2, min, na.rm = TRUE)
  )
  expect_equal(
    collect(d_reduce(delarr(mat), max, "cols", na.rm = TRUE), chunk_size = 1L),
    apply(mat, 2, max, na.rm = TRUE)
  )
})

test_that("collect_reduce_many_streamed handles col extrema directly", {
  mat <- matrix(
    c(NA, NA, NA, NA,
      1, NA, 7, 8,
      5, 6, NA, 12),
    nrow = 4,
    ncol = 3
  )
  out <- delarr:::collect_reduce_many_streamed(
    delarr(mat),
    fns = list(min = min, max = max),
    dim = "cols",
    na.rm = TRUE,
    chunk_size = 1L
  )
  expect_equal(out[[1]], c(NA_real_, 1, 5))
  expect_equal(out[[2]], c(NA_real_, 8, 12))
})

test_that("collect_reduce_many_streamed handles row extrema with na.rm", {
  mat <- matrix(c(5, 1, NA, 2, NA, NA, 4, 6), 4, 2)
  out <- delarr:::collect_reduce_many_streamed(
    delarr(mat),
    fns = list(min = min, max = max),
    dim = "rows",
    na.rm = TRUE,
    chunk_size = 1L
  )
  expect_equal(out[[1]], apply(mat, 1, min, na.rm = TRUE))
  expect_equal(out[[2]], apply(mat, 1, max, na.rm = TRUE))
})

test_that("collect_reduce_many_streamed handles col means with na.rm", {
  mat <- matrix(c(1, NA, 3, 4, NA, 6), 2, 3)
  out <- delarr:::collect_reduce_many_streamed(
    delarr(mat),
    fns = list(m = mean),
    dim = "cols",
    na.rm = TRUE,
    chunk_size = 1L
  )
  expect_equal(out[[1]], colMeans(mat, na.rm = TRUE))
})

test_that("collect_reduce_many_streamed returns NULL for full-eval pipelines", {
  mat <- matrix(as.double(1:12), 3, 4)
  pipeline <- delarr(mat) |> d_center("rows")
  expect_null(delarr:::collect_reduce_many_streamed(
    pipeline,
    fns = list(sum = sum),
    dim = "rows",
    na.rm = FALSE,
    chunk_size = 2L
  ))
})

test_that("collect_reduce_many_streamed returns NULL for empty inputs", {
  empty <- delarr(matrix(numeric(0), nrow = 0, ncol = 3))
  expect_null(delarr:::collect_reduce_many_streamed(
    empty,
    fns = list(sum = sum),
    dim = "rows",
    na.rm = FALSE,
    chunk_size = 1L
  ))
})

test_that("collect_reduce_many_streamed skips non-delarr rhs entries", {
  mat <- matrix(as.double(1:12), 3, 4)
  pipeline <- d_map2(delarr(mat), 1:4, ~ .x + .y)
  out <- delarr:::collect_reduce_many_streamed(
    pipeline,
    fns = list(sum = sum),
    dim = "rows",
    na.rm = FALSE,
    chunk_size = 2L
  )
  expected <- rowSums(sweep(mat, 2, 1:4, "+"))
  expect_equal(out[[1]], expected)
})

test_that("collect_reduce_many_streamed precomputes incompatible delarr rhs", {
  lhs <- matrix(as.double(1:20), 4, 5)
  rhs <- matrix(as.double(21:40), 4, 5)
  pipeline <- d_map2(delarr(lhs), delarr(rhs) |> d_center("rows"), ~ .x + .y)
  out <- delarr:::collect_reduce_many_streamed(
    pipeline,
    fns = list(sum = sum),
    dim = "rows",
    na.rm = FALSE,
    chunk_size = 2L
  )
  expect_equal(out[[1]], rowSums(collect(pipeline)))
})

test_that("collect_reduce_many_streamed invokes main seed begin/end hooks", {
  mat <- matrix(as.double(1:20), 4, 5)
  tracker <- new.env(parent = emptyenv())
  tracker$began <- 0L
  tracker$ended <- 0L
  seed <- delarr_backend(
    nrow = 4,
    ncol = 5,
    pull = function(rows = NULL, cols = NULL) {
      rows <- rows %||% seq_len(4)
      cols <- cols %||% seq_len(5)
      mat[rows, cols, drop = FALSE]
    },
    begin = function() tracker$began <- tracker$began + 1L,
    end = function() tracker$ended <- tracker$ended + 1L
  )
  out <- delarr:::collect_reduce_many_streamed(
    delarr(seed),
    fns = list(sum = sum),
    dim = "rows",
    na.rm = FALSE,
    chunk_size = 2L
  )
  expect_equal(out[[1]], rowSums(mat))
  expect_gt(tracker$began, 0L)
  expect_gt(tracker$ended, 0L)
})
