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
