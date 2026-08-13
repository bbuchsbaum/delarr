# tests/testthat/test-core-extra.R
# Additional coverage for delarr-core.R and delarr-eval.R

test_that("delarr() returns delarr unchanged", {
  mat <- matrix(1:6, 2, 3)
  x <- delarr(mat)
  y <- delarr(x)
  expect_identical(x, y)
})

test_that("delarr() rejects unsupported input", {
  expect_error(delarr("not a matrix"), "Unsupported input")
  expect_error(delarr(list(a = 1)), "Unsupported input")
})

test_that("dimnames.delarr returns seed dimnames", {
  mat <- matrix(1:6, 2, 3, dimnames = list(c("r1", "r2"), c("c1", "c2", "c3")))
  x <- delarr(mat)
  expect_equal(dimnames(x), dimnames(mat))
})

test_that("dimnames.delarr returns NULLs when no dimnames", {
  mat <- matrix(1:6, 2, 3)
  x <- delarr(mat)
  expect_equal(dimnames(x), list(NULL, NULL))
})

test_that("as.matrix.delarr materialises", {
  mat <- matrix(1:12, 3, 4)
  x <- delarr(mat)
  result <- as.matrix(x)
  expect_equal(result, mat)
})

test_that("dimnames.delarr follows slice and reduce semantics", {
  mat <- matrix(
    1:12,
    3,
    4,
    dimnames = list(c("r1", "r2", "r3"), c("c1", "c2", "c3", "c4"))
  )
  x <- delarr(mat)

  sliced <- x[c(3, 1), -2]
  expect_equal(dimnames(sliced), dimnames(mat[c(3, 1), -2, drop = FALSE]))

  row_reduced <- d_reduce(x[-1, 2:4], sum, "rows")
  expect_equal(dimnames(row_reduced), list(rownames(mat[-1, 2:4, drop = FALSE]), NULL))

  col_reduced <- d_reduce(x[c(3, 1), -2], sum, "cols")
  expect_equal(dimnames(col_reduced), list(NULL, colnames(mat[c(3, 1), -2, drop = FALSE])))
})

test_that("collect and as.matrix preserve names", {
  mat <- matrix(
    1:12,
    3,
    4,
    dimnames = list(c("r1", "r2", "r3"), c("c1", "c2", "c3", "c4"))
  )

  sliced <- delarr(mat)[c(3, 1), c(4, 2)]
  expect_equal(collect(sliced), mat[c(3, 1), c(4, 2), drop = FALSE])
  expect_equal(as.matrix(sliced), mat[c(3, 1), c(4, 2), drop = FALSE])

  row_reduced <- d_reduce(delarr(mat)[, c(4, 2)], sum, "rows")
  expect_equal(collect(row_reduced), rowSums(mat[, c(4, 2), drop = FALSE]))

  col_reduced <- d_reduce(delarr(mat)[c(3, 1), ], sum, "cols")
  expect_equal(collect(col_reduced), colSums(mat[c(3, 1), , drop = FALSE]))
})

test_that("print.delarr with no ops prints lazy", {
  mat <- matrix(1:6, 2, 3)
  x <- delarr(mat)
  out <- capture.output(print(x))
  expect_match(paste(out, collapse = ""), "lazy")
})

test_that("describe_op covers all op types", {
  expect_equal(describe_op(list(op = "slice")), "slice")
  expect_equal(describe_op(list(op = "emap")), "map")
  expect_equal(describe_op(list(op = "emap2")), "map2")
  expect_equal(describe_op(list(op = "emap_const")), "map_const")
  expect_equal(describe_op(list(op = "center", dim = "rows")), "center(rows)")
  expect_equal(describe_op(list(op = "scale", dim = "cols")), "scale(cols)")
  expect_equal(describe_op(list(op = "zscore", dim = "rows")), "zscore(rows)")
  expect_equal(describe_op(list(op = "detrend", dim = "cols")), "detrend(cols)")
  expect_equal(describe_op(list(op = "reduce", dim = "rows")), "reduce(rows)")
  expect_equal(describe_op(list(op = "where")), "where")
  expect_equal(describe_op(list(op = "unknown_op")), "")
})

test_that("Ops.delarr with scalar on left side", {
  mat <- matrix(1:12, 3, 4)
  x <- delarr(mat)
  result <- collect(2 * x)
  expect_equal(result, 2 * mat)
})

test_that("Ops.delarr subtraction works", {
  mat <- matrix(1:12, 3, 4)
  x <- delarr(mat)
  result <- collect(x - 5)
  expect_equal(result, mat - 5)
})

test_that("Ops.delarr with two delarr objects", {
  m1 <- matrix(1:12, 3, 4)
  m2 <- matrix(12:1, 3, 4)
  x <- delarr(m1)
  y <- delarr(m2)
  expect_equal(collect(x * y), m1 * m2)
})

test_that("print.delarr with ops prints op names", {
  mat <- matrix(1:6, 2, 3)
  x <- delarr(mat) |> d_map(~ .x * 2) |> d_where(~ .x > 0)
  out <- paste(capture.output(print(x)), collapse = "\n")
  expect_match(out, "map")
  expect_match(out, "where")
})

test_that("dim.delarr accounts for slice and reduce ops", {
  mat <- matrix(1:20, 4, 5)
  x <- delarr(mat)

  sliced <- x[1:2, 1:3]
  expect_equal(dim(sliced), c(2, 3))

  neg_sliced <- x[-1, ]
  expect_equal(dim(neg_sliced), c(3, 5))

  logical_sliced <- x[c(TRUE, FALSE, TRUE, FALSE), c(FALSE, TRUE, TRUE, FALSE, TRUE)]
  expect_equal(dim(logical_sliced), c(2, 3))

  reordered <- x[c(4, 2, 2), c(5, 3)]
  expect_equal(dim(reordered), c(3, 2))

  reduced <- d_reduce(x, sum, "rows")
  expect_equal(dim(reduced), c(4, 1))

  reduced_cols <- d_reduce(x, sum, "cols")
  expect_equal(dim(reduced_cols), c(1, 5))
})

test_that("d_map2 with matrix rhs works", {
  m1 <- matrix(1:12, 3, 4)
  m2 <- matrix(12:1, 3, 4)
  x <- delarr(m1)
  result <- collect(d_map2(x, m2, ~ .x + .y))
  expect_equal(result, m1 + m2)
})

test_that("d_map2 rejects invalid rhs", {
  x <- delarr(matrix(1:6, 2, 3))
  expect_error(d_map2(x, "bad", ~ .x + .y), "y must be a delarr or numeric")
})

test_that("d_scale with center=FALSE scale=TRUE", {
  set.seed(10)
  mat <- matrix(rnorm(20), 4, 5)
  x <- delarr(mat)
  result <- collect(d_scale(x, dim = "cols", center = FALSE, scale = TRUE))
  expect_true(is.matrix(result))
  expect_equal(dim(result), dim(mat))
})

test_that("d_scale with center=TRUE scale=FALSE", {
  set.seed(10)
  mat <- matrix(rnorm(20), 4, 5)
  x <- delarr(mat)
  result <- collect(d_scale(x, dim = "cols", center = TRUE, scale = FALSE))
  expect_true(all(abs(colMeans(result)) < 1e-10))
})

test_that("d_detrend along cols works", {
  mat <- matrix(as.double(1:12), 3, 4)
  x <- delarr(mat)
  result <- collect(d_detrend(x, dim = "cols", degree = 1L))
  expect_equal(dim(result), dim(mat))
})

test_that("block_apply with margin='rows' works", {
  mat <- matrix(1:20, 4, 5)
  x <- delarr(mat)
  res <- block_apply(x, margin = "rows", size = 2L, fn = function(chunk) {
    rowMeans(chunk)
  })
  expect_equal(length(res), 2L)
  expect_equal(res[[1]], rowMeans(mat[1:2, , drop = FALSE]))
  expect_equal(res[[2]], rowMeans(mat[3:4, , drop = FALSE]))
})

test_that("block_apply with row chunks respects sliced dimensions", {
  mat <- matrix(1:20, 4, 5)
  x <- delarr(mat)[-1, ]
  res <- block_apply(x, margin = "rows", size = 2L, fn = function(chunk) {
    rowSums(chunk)
  })
  expect_equal(length(res), 2L)
  expect_equal(res[[1]], rowSums(mat[-1, , drop = FALSE][1:2, , drop = FALSE]))
  expect_equal(res[[2]], rowSums(mat[-1, , drop = FALSE][3, , drop = FALSE]))
})

test_that("block_apply rejects non-function fn", {
  x <- delarr(matrix(1:6, 2, 3))
  expect_error(block_apply(x, fn = "not_a_fn"), "fn must be a function")
})

test_that("collect with generic reduce falls back to full eval", {
  mat <- matrix(1:12, 3, 4)
  x <- delarr(mat)
  result <- collect(d_reduce(x, stats::median, dim = "rows"))
  expected <- apply(mat, 1, stats::median)
  expect_equal(result, expected)
})

test_that("collect with generic reduce on cols", {
  mat <- matrix(1:12, 3, 4)
  x <- delarr(mat)
  result <- collect(d_reduce(x, stats::median, dim = "cols"))
  expected <- apply(mat, 2, stats::median)
  expect_equal(result, expected)
})

test_that("compile_plan rejects multiple reduces", {
  mat <- matrix(1:6, 2, 3)
  x <- delarr(mat)
  r <- d_reduce(x, sum, "rows")
  r2 <- d_reduce(r, sum, "rows")
  expect_error(collect(r2), "Only one reduce")
})

test_that("broadcast_rhs errors on NULL rhs", {
  mat <- matrix(1:6, 2, 3)
  x <- delarr(mat)
  op_node <- list(op = "emap_const", const = NULL, side = "right",
                  fn = function(a, b) a + b)
  bad <- new_delarr(x$seed, append(x$ops, list(op_node)))
  expect_error(collect(bad), "Binary operation requires a RHS")
})

test_that("d_map that returns non-matrix errors", {
  mat <- matrix(1:6, 2, 3)
  x <- delarr(mat)
  bad <- d_map(x, function(m) as.vector(m))
  expect_error(collect(bad), "d_map functions must return a matrix")
})

test_that("row-oriented center requires full eval but works across chunks", {
  set.seed(99)
  mat <- matrix(rnorm(40), 5, 8)
  x <- delarr(mat)
  result <- collect(d_center(x, dim = "rows"), chunk_size = 3L)
  expect_true(all(abs(rowMeans(result)) < 1e-10))
})

test_that("empty subscript returns delarr unchanged", {
  mat <- matrix(1:12, 3, 4)
  x <- delarr(mat)
  expect_identical(do.call("[", list(x)), x)
  expect_equal(collect(x[]), mat)
})

test_that("N-d subscript pads missing trailing indices", {
  arr <- array(as.double(1:24), dim = c(2, 3, 4))
  darr <- delarr(arr)
  result <- collect(darr[1:2, ])
  expect_equal(result, arr[1:2, , , drop = FALSE])
})

test_that("dimnames.delarr pads short seed dimnames", {
  arr <- array(as.double(1:24), dim = c(2, 3, 4))
  seed <- delarr_seed_nd(
    dims = dim(arr),
    pull = function(indices) {
      idx <- lapply(seq_along(dim(arr)), function(k) {
        indices[[k]] %||% seq_len(dim(arr)[[k]])
      })
      do.call(`[`, c(list(arr), idx, list(drop = FALSE)))
    },
    dimnames = list(c("a", "b"))
  )
  darr <- delarr(seed)
  expect_equal(dimnames(darr)[[1]], c("a", "b"))
  expect_null(dimnames(darr)[[3]])
})

test_that("d_scale accepts axis= for N-d arrays", {
  arr <- array(rnorm(60), dim = c(3, 4, 5))
  darr <- delarr(arr)
  result <- collect(d_scale(darr, axis = 2L, center = TRUE, scale = FALSE))
  expect_equal(dim(result), dim(arr))
  means <- apply(result, 2, mean)
  expect_true(all(abs(means) < 1e-10))
})

test_that("block_apply supports parallel execution on Unix", {
  skip_on_os("windows")
  mat <- matrix(1:20, 4, 5)
  x <- delarr(mat)
  res <- block_apply(
    x,
    margin = "cols",
    size = 2L,
    fn = colSums,
    parallel = TRUE,
    workers = 2L
  )
  expect_equal(unlist(res), colSums(mat))
})

test_that("collect into list writer finalizes target", {
  mat <- matrix(1:12, 3, 4)
  captured <- NULL
  writer <- list(
    write = function(block, ...) {
      captured <<- block
    },
    finalize = function() {
      captured <<- captured + 100
    }
  )
  result <- collect(delarr(mat) |> d_map(~ .x + 1), into = writer)
  expect_null(result)
  expect_equal(captured, mat + 1 + 100)
})

test_that("2D full-matrix min/max reductions handle NA chunks", {
  mat <- matrix(c(1, NA, 3, NA, 5, 6), 2, 3)
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
