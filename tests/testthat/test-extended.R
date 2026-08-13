# ---- d_matmul ----------------------------------------------------------------

test_that("d_matmul produces correct matrix product", {
  A <- matrix(1:6, 2, 3)
  B <- matrix(7:12, 3, 2)
  result <- collect(d_matmul(delarr(A), delarr(B)))
  expect_equal(result, A %*% B)
})

test_that("d_matmul accepts raw matrices", {
  A <- matrix(rnorm(6), 2, 3)
  B <- matrix(rnorm(6), 3, 2)
  result <- collect(d_matmul(A, B))
  expect_equal(result, A %*% B)
})

test_that("d_matmul preserves dimnames", {
  A <- matrix(1:6, 2, 3, dimnames = list(c("r1", "r2"), NULL))
  B <- matrix(1:6, 3, 2, dimnames = list(NULL, c("c1", "c2")))
  result <- collect(d_matmul(delarr(A), delarr(B)))
  expect_equal(rownames(result), c("r1", "r2"))
  expect_equal(colnames(result), c("c1", "c2"))
})

test_that("d_matmul errors on non-conformable dimensions", {
  A <- matrix(1:6, 2, 3)
  B <- matrix(1:8, 2, 4)
  expect_error(d_matmul(delarr(A), delarr(B)), "Non-conformable")
})

test_that("d_matmul works with sliced delarrs", {
  A <- matrix(rnorm(20), 4, 5)
  B <- matrix(rnorm(15), 5, 3)
  darr_a <- delarr(A)[1:2, ]
  darr_b <- delarr(B)
  result <- collect(d_matmul(darr_a, darr_b))
  expect_equal(result, A[1:2, ] %*% B)
})

test_that("d_matmul handles empty output slices", {
  A <- matrix(rnorm(20), 4, 5)
  B <- matrix(rnorm(15), 5, 3)
  product <- d_matmul(delarr(A), delarr(B))

  empty_rows <- collect(product[integer(), ])
  expect_equal(dim(empty_rows), c(0L, 3L))

  empty_cols <- collect(product[, integer()])
  expect_equal(dim(empty_cols), c(4L, 0L))
})

# ---- d_reduce_many -----------------------------------------------------------

test_that("d_reduce_many computes multiple reductions in one pass", {
  mat <- matrix(rnorm(12), 3, 4)
  darr <- delarr(mat)
  result <- d_reduce_many(darr, list(s = sum, m = mean), dim = "rows")
  expect_true(is.matrix(result))
  expect_equal(ncol(result), 2L)
  expect_equal(colnames(result), c("s", "m"))
  expect_equal(result[, "s"], rowSums(mat))
  expect_equal(result[, "m"], rowMeans(mat))
})

test_that("d_reduce_many works with dim=cols", {
  mat <- matrix(1:12, 3, 4)
  darr <- delarr(mat)
  result <- d_reduce_many(darr, list(s = sum, m = mean), dim = "cols")
  expect_equal(result[, "s"], colSums(mat))
  expect_equal(result[, "m"], colMeans(mat))
})

test_that("d_reduce_many handles min and max", {
  mat <- matrix(c(1, 5, 3, 2, 4, 6), 2, 3)
  darr <- delarr(mat)
  result <- d_reduce_many(darr, list(mn = min, mx = max), dim = "rows")
  expect_equal(result[, "mn"], apply(mat, 1, min))
  expect_equal(result[, "mx"], apply(mat, 1, max))
})

test_that("d_reduce_many with na.rm handles NAs", {
  mat <- matrix(c(1, NA, 3, 4, NA, 6), 2, 3)
  darr <- delarr(mat)
  result <- d_reduce_many(darr, list(s = sum, m = mean), dim = "rows", na.rm = TRUE)
  expect_equal(result[, "s"], rowSums(mat, na.rm = TRUE))
  expect_equal(result[, "m"], rowMeans(mat, na.rm = TRUE))
})

test_that("d_reduce_many with single function works", {
  mat <- matrix(1:12, 3, 4)
  darr <- delarr(mat)
  result <- d_reduce_many(darr, sum, dim = "rows")
  expect_equal(as.numeric(result), rowSums(mat))
})

test_that("d_reduce_many with unnamed functions gets auto-names", {
  mat <- matrix(1:12, 3, 4)
  darr <- delarr(mat)
  result <- d_reduce_many(darr, list(sum, mean), dim = "rows")
  expect_equal(colnames(result), c("fn1", "fn2"))
})

test_that("d_reduce_many with generic function falls back correctly", {
  mat <- matrix(as.double(1:12), 3, 4)
  darr <- delarr(mat)
  result <- d_reduce_many(darr, list(s = sum, med = median), dim = "rows")
  expect_equal(result[, "s"], rowSums(mat))
  expect_equal(result[, "med"], apply(mat, 1, median))
})

test_that("d_reduce_many falls back when all reducers are generic", {
  mat <- matrix(as.double(1:15), 3, 5)
  darr <- delarr(mat)
  result <- d_reduce_many(darr, list(med = median, iqr = IQR), dim = "rows")

  expect_equal(result[, "med"], apply(mat, 1L, median))
  expect_equal(result[, "iqr"], apply(mat, 1L, IQR))
})

test_that("d_reduce_many streams paired delarr rhs operations", {
  lhs <- matrix(as.double(1:20), 4, 5)
  rhs <- matrix(as.double(21:40), 4, 5)
  pipeline <- d_map2(delarr(lhs), delarr(rhs), ~ .x + .y)

  result <- d_reduce_many(
    pipeline,
    fns = list(sum = sum, mean = mean),
    dim = "rows",
    chunk_size = 2L
  )

  expected <- lhs + rhs
  expect_equal(result[, "sum"], rowSums(expected))
  expect_equal(result[, "mean"], rowMeans(expected))
})

test_that("d_reduce_many handles column extrema with all-NA chunks", {
  mat <- matrix(
    c(NA, NA, NA, NA,
      1, NA, 7, 8,
      5, 6, NA, 12),
    nrow = 4,
    ncol = 3
  )
  result <- d_reduce_many(
    delarr(mat),
    fns = list(min = min, max = max),
    dim = "cols",
    na.rm = TRUE,
    chunk_size = 1L
  )

  expect_equal(result[, "min"], c(NA_real_, 1, 5))
  expect_equal(result[, "max"], c(NA_real_, 8, 12))
})

test_that("d_reduce_many simplify=FALSE returns list", {
  mat <- matrix(1:12, 3, 4)
  darr <- delarr(mat)
  result <- d_reduce_many(darr, list(s = sum), dim = "rows", simplify = FALSE)
  expect_true(is.list(result))
  expect_equal(result$s, rowSums(mat))
})

test_that("d_reduce_many validates fns argument", {
  darr <- delarr(matrix(1:6, 2, 3))
  expect_error(d_reduce_many(darr, list(), dim = "rows"), "non-empty list")
})

test_that("d_reduce_many preserves rownames when simplifying", {
  mat <- matrix(
    as.double(1:12),
    3,
    4,
    dimnames = list(c("r1", "r2", "r3"), NULL)
  )
  result <- d_reduce_many(
    delarr(mat),
    list(s = sum, m = mean),
    dim = "rows"
  )
  expect_equal(rownames(result), rownames(mat))
})

test_that("d_reduce_many falls back when rhs delarr requires full evaluation", {
  lhs <- matrix(as.double(1:20), 4, 5)
  rhs <- matrix(as.double(21:40), 4, 5)
  pipeline <- d_map2(delarr(lhs), delarr(rhs) |> d_center("rows"), ~ .x + .y)
  result <- d_reduce_many(
    pipeline,
    fns = list(sum = sum, mean = mean),
    dim = "rows",
    chunk_size = 2L
  )
  materialized <- collect(pipeline)
  expect_equal(result[, "sum"], rowSums(materialized))
  expect_equal(result[, "mean"], rowMeans(materialized))
})

test_that("d_reduce_many streams col extrema with na.rm across chunks", {
  mat <- matrix(
    c(NA, NA, NA, NA,
      1, NA, 7, 8,
      5, 6, NA, 12),
    nrow = 4,
    ncol = 3
  )
  result <- d_reduce_many(
    delarr(mat),
    fns = list(min = min, max = max),
    dim = "cols",
    na.rm = TRUE,
    chunk_size = 1L
  )
  expect_equal(result[, "min"], c(NA_real_, 1, 5))
  expect_equal(result[, "max"], c(NA_real_, 8, 12))
})

test_that("d_reduce_many streams paired delarr rhs with begin/end hooks", {
  lhs <- matrix(as.double(1:20), 4, 5)
  rhs <- matrix(as.double(21:40), 4, 5)
  tracker <- new.env(parent = emptyenv())
  tracker$began <- 0L
  tracker$ended <- 0L
  rhs_seed <- delarr_backend(
    nrow = 4,
    ncol = 5,
    pull = function(rows = NULL, cols = NULL) {
      rows <- rows %||% seq_len(4)
      cols <- cols %||% seq_len(5)
      rhs[rows, cols, drop = FALSE]
    },
    begin = function() tracker$began <- tracker$began + 1L,
    end = function() tracker$ended <- tracker$ended + 1L
  )
  pipeline <- d_map2(delarr(lhs), delarr(rhs_seed), ~ .x + .y)
  result <- d_reduce_many(
    pipeline,
    fns = list(sum = sum),
    dim = "rows",
    chunk_size = 2L
  )
  expect_equal(as.numeric(result), rowSums(lhs + rhs))
  expect_gt(tracker$began, 0L)
  expect_gt(tracker$ended, 0L)
})

test_that("d_reduce_many precomputes matrix rhs slices for incompatible chunks", {
  lhs <- matrix(as.double(1:20), 4, 5)
  rhs <- matrix(as.double(21:40), 4, 5)
  pipeline <- d_map2(delarr(lhs), delarr(rhs), ~ .x + .y)
  precomputed <- collect(pipeline)
  result <- d_reduce_many(
    pipeline,
    fns = list(sum = sum, mean = mean),
    dim = "cols",
    chunk_size = 1L
  )
  expect_equal(result[, "sum"], colSums(precomputed))
  expect_equal(result[, "mean"], colMeans(precomputed))
})

# ---- d_transpose / d_aperm ---------------------------------------------------

test_that("d_transpose returns correct transposed matrix", {
  mat <- matrix(1:12, 3, 4)
  darr <- delarr(mat)
  result <- collect(d_transpose(darr))
  expect_equal(result, t(mat))
})

test_that("t.delarr dispatches to d_transpose", {
  mat <- matrix(1:12, 3, 4)
  darr <- delarr(mat)
  result <- collect(t(darr))
  expect_equal(result, t(mat))
})

test_that("d_aperm with identity permutation returns unchanged", {
  arr <- array(1:24, dim = c(2, 3, 4))
  darr <- delarr(arr)
  result <- d_aperm(darr, c(1L, 2L, 3L))
  expect_identical(result, darr)
})

test_that("d_aperm preserves 2D chunk hints and dimnames", {
  mat <- matrix(
    as.double(1:12),
    nrow = 3,
    ncol = 4,
    dimnames = list(paste0("r", 1:3), paste0("c", 1:4))
  )
  seed <- delarr_seed(
    nrow = nrow(mat),
    ncol = ncol(mat),
    pull = function(rows = NULL, cols = NULL) {
      rows <- rows %||% seq_len(nrow(mat))
      cols <- cols %||% seq_len(ncol(mat))
      mat[rows, cols, drop = FALSE]
    },
    chunk_hint = list(rows = 2L, cols = 3L),
    dimnames = dimnames(mat)
  )

  result <- d_aperm(delarr(seed), c(2L, 1L))

  expect_equal(collect(result, chunk_size = 2L), t(mat))
  expect_equal(dimnames(result), rev(dimnames(mat)))
  expect_equal(result$seed$chunk_hint$rows, 3L)
  expect_equal(result$seed$chunk_hint$cols, 2L)
})

test_that("d_aperm on 4D array works correctly", {
  arr <- array(seq_len(120), dim = c(3, 4, 5, 2))
  darr <- delarr(arr)
  perm <- c(4L, 2L, 1L, 3L)
  result <- collect(d_aperm(darr, perm))
  expect_equal(result, aperm(arr, perm))
  expect_equal(dim(result), dim(arr)[perm])
})

test_that("d_aperm rejects invalid permutation", {
  darr <- delarr(array(1:24, dim = c(2, 3, 4)))
  expect_error(d_aperm(darr, c(1L, 2L)), "one entry per dimension")
  expect_error(d_aperm(darr, c(1L, 1L, 3L)), "permutation")
})
