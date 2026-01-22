# tests/testthat/test-edge-cases.R
# Edge case tests for indices, broadcasting, and chunk boundaries

# --- Negative Index Tests (TEST-03) ---

test_that("negative indices drop first row correctly", {
  mat <- matrix(1:20, 4, 5)
  x <- delarr(mat)

  result <- collect(x[-1, ])
  expect_equal(result, mat[-1, ])
  expect_equal(dim(result), c(3, 5))
})

test_that("negative indices drop last row correctly", {
  mat <- matrix(1:20, 4, 5)
  x <- delarr(mat)

  result <- collect(x[-nrow(x), ])
  expect_equal(result, mat[-nrow(mat), ])
  expect_equal(dim(result), c(3, 5))
})

test_that("negative indices drop first column correctly", {
  mat <- matrix(1:20, 4, 5)
  x <- delarr(mat)

  result <- collect(x[, -1])
  expect_equal(result, mat[, -1])
  expect_equal(dim(result), c(4, 4))
})

test_that("negative indices drop last column correctly", {
  mat <- matrix(1:20, 4, 5)
  x <- delarr(mat)

  result <- collect(x[, -ncol(x)])
  expect_equal(result, mat[, -ncol(mat)])
  expect_equal(dim(result), c(4, 4))
})

test_that("negative indices drop multiple rows", {
  mat <- matrix(1:20, 4, 5)
  x <- delarr(mat)

  result <- collect(x[-c(1, 3), ])
  expect_equal(result, mat[-c(1, 3), ])
  expect_equal(dim(result), c(2, 5))
})

test_that("negative indices drop multiple columns", {
  mat <- matrix(1:20, 4, 5)
  x <- delarr(mat)

  result <- collect(x[, -c(2, 4)])
  expect_equal(result, mat[, -c(2, 4)])
  expect_equal(dim(result), c(4, 3))
})

test_that("negative indices drop all but one row", {
  mat <- matrix(1:20, 4, 5)
  x <- delarr(mat)

  drop_rows <- seq_len(nrow(mat) - 1)
  result <- collect(x[-drop_rows, ])
  expect_equal(result, mat[-drop_rows, , drop = FALSE])
  expect_equal(dim(result), c(1, 5))
})

test_that("negative indices work with both row and column simultaneously", {
  mat <- matrix(1:20, 4, 5)
  x <- delarr(mat)

  result <- collect(x[-1, -ncol(x)])
  expect_equal(result, mat[-1, -ncol(mat)])
  expect_equal(dim(result), c(3, 4))
})

test_that("negative indices combined with lazy operations", {
  mat <- matrix(1:20, 4, 5)
  x <- delarr(mat)

  result <- x[-1, ] |> d_map(~ .x * 2) |> collect()
  expect_equal(result, mat[-1, ] * 2)
})

# --- Chunk Size Boundary Tests (TEST-05) ---

test_that("chunk_size=1 produces correct results", {
  mat <- matrix(1:30, 5, 6)
  x <- delarr(mat)

  result <- collect(x, chunk_size = 1L)
  expect_equal(result, mat)
})

test_that("chunk_size equal to ncol produces correct results", {
  mat <- matrix(1:30, 5, 6)
  x <- delarr(mat)

  result <- collect(x, chunk_size = ncol(mat))
  expect_equal(result, mat)
})

test_that("chunk_size larger than ncol produces correct results", {
  mat <- matrix(1:30, 5, 6)
  x <- delarr(mat)

  result <- collect(x, chunk_size = ncol(mat) * 2L)
  expect_equal(result, mat)
})

test_that("chunk_size that evenly divides ncol produces correct results", {
  mat <- matrix(1:30, 5, 6)  # 6 cols
  x <- delarr(mat)

  result <- collect(x, chunk_size = 2L)  # 6 / 2 = 3 chunks
  expect_equal(result, mat)

  result <- collect(x, chunk_size = 3L)  # 6 / 3 = 2 chunks
  expect_equal(result, mat)
})

test_that("chunk_size that doesn't evenly divide ncol produces correct results", {
  mat <- matrix(1:35, 5, 7)  # 7 cols
  x <- delarr(mat)

  result <- collect(x, chunk_size = 2L)  # 4 chunks (2+2+2+1)
  expect_equal(result, mat)

  result <- collect(x, chunk_size = 3L)  # 3 chunks (3+3+1)
  expect_equal(result, mat)

  result <- collect(x, chunk_size = 4L)  # 2 chunks (4+3)
  expect_equal(result, mat)
})

test_that("different chunk sizes produce identical reduction results", {
  set.seed(42)
  mat <- matrix(runif(100), 10, 10)
  x <- delarr(mat)

  # Row reductions with different chunk sizes
  r1 <- collect(d_reduce(x, sum, "rows"), chunk_size = 1L)
  r2 <- collect(d_reduce(x, sum, "rows"), chunk_size = 3L)
  r3 <- collect(d_reduce(x, sum, "rows"), chunk_size = 5L)
  r4 <- collect(d_reduce(x, sum, "rows"), chunk_size = 10L)
  r5 <- collect(d_reduce(x, sum, "rows"), chunk_size = 100L)

  expect_equal(r1, r2)
  expect_equal(r2, r3)
  expect_equal(r3, r4)
  expect_equal(r4, r5)
  expect_equal(r1, rowSums(mat))
})

test_that("chunk boundaries don't affect d_map results", {
  mat <- matrix(1:30, 5, 6)
  x <- delarr(mat)

  result_1 <- x |> d_map(~ .x^2) |> collect(chunk_size = 1L)
  result_2 <- x |> d_map(~ .x^2) |> collect(chunk_size = 2L)
  result_3 <- x |> d_map(~ .x^2) |> collect(chunk_size = 3L)
  result_6 <- x |> d_map(~ .x^2) |> collect(chunk_size = 6L)

  expect_equal(result_1, mat^2)
  expect_equal(result_2, mat^2)
  expect_equal(result_3, mat^2)
  expect_equal(result_6, mat^2)
})

test_that("chunk boundaries don't affect binary operations", {
  mat1 <- matrix(1:30, 5, 6)
  mat2 <- matrix(31:60, 5, 6)
  x <- delarr(mat1)
  y <- delarr(mat2)

  result_1 <- collect(x + y, chunk_size = 1L)
  result_2 <- collect(x + y, chunk_size = 2L)
  result_6 <- collect(x + y, chunk_size = 6L)

  expect_equal(result_1, mat1 + mat2)
  expect_equal(result_2, mat1 + mat2)
  expect_equal(result_6, mat1 + mat2)
})

# --- Broadcasting Edge Case Tests (TEST-04) ---

test_that("broadcasting with vector matching nrow works", {
  mat <- matrix(1:12, 3, 4)
  x <- delarr(mat)
  row_vec <- 1:3

  result <- collect(x + row_vec)
  expected <- sweep(mat, 1L, row_vec, "+")
  expect_equal(result, expected)
})

test_that("broadcasting with vector matching ncol works", {
  mat <- matrix(1:12, 3, 4)
  x <- delarr(mat)
  col_vec <- 1:4

  result <- collect(x + col_vec)
  expected <- sweep(mat, 2L, col_vec, "+")
  expect_equal(result, expected)
})

test_that("broadcasting rejects non-conformable vectors", {
  mat <- matrix(1:12, 3, 4)  # 3 rows, 4 cols
  x <- delarr(mat)
  bad_vec <- 1:5  # Neither 3 nor 4

  expect_error(collect(x + bad_vec), "Non-conformable")
})

test_that("broadcasting with scalar works", {
  mat <- matrix(1:12, 3, 4)
  x <- delarr(mat)

  expect_equal(collect(x + 10), mat + 10)
  expect_equal(collect(x * 2), mat * 2)
  expect_equal(collect(x - 1), mat - 1)
})

test_that("broadcasting handles NaN correctly", {
  mat <- matrix(1:12, 3, 4)
  x <- delarr(mat)

  result <- collect(x + NaN)
  expect_true(all(is.nan(result)))
})

test_that("broadcasting handles Inf correctly", {
  mat <- matrix(1:12, 3, 4)
  x <- delarr(mat)

  result <- collect(x + Inf)
  expect_true(all(is.infinite(result)))
  expect_true(all(result > 0))

  result_neg <- collect(x + (-Inf))
  expect_true(all(is.infinite(result_neg)))
  expect_true(all(result_neg < 0))
})

test_that("broadcasting with matrix of same dimensions works", {
  mat1 <- matrix(1:12, 3, 4)
  mat2 <- matrix(13:24, 3, 4)
  x <- delarr(mat1)

  result <- collect(x + mat2)
  expect_equal(result, mat1 + mat2)
})

test_that("broadcasting rejects matrix of wrong dimensions", {
  mat <- matrix(1:12, 3, 4)
  x <- delarr(mat)
  wrong_mat <- matrix(1:6, 2, 3)

  expect_error(collect(x + wrong_mat), "Non-conformable")
})

test_that("broadcasting with delarr RHS of same dimensions works", {
  mat1 <- matrix(1:12, 3, 4)
  mat2 <- matrix(13:24, 3, 4)
  x <- delarr(mat1)
  y <- delarr(mat2)

  result <- collect(x + y)
  expect_equal(result, mat1 + mat2)
})

test_that("broadcasting in d_map2 with scalar works", {
  mat <- matrix(1:12, 3, 4)
  x <- delarr(mat)

  result <- collect(d_map2(x, 2, `*`))
  expect_equal(result, mat * 2)
})
