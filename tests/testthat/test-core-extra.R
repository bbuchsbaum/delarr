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
