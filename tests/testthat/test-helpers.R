# tests/testthat/test-helpers.R
# Tests for delarr-helpers.R: detrend_matrix, where_mask

test_that("detrend_matrix along rows removes linear trend", {
  mat <- matrix(as.double(1:12), 3, 4)
  result <- detrend_matrix(mat, "rows", 1L)
  expect_equal(dim(result), dim(mat))
})

test_that("detrend_matrix along cols removes linear trend", {
  mat <- matrix(as.double(1:12), 3, 4)
  result <- detrend_matrix(mat, "cols", 1L)
  expect_equal(dim(result), dim(mat))
})

test_that("detrend_matrix rejects degree < 1", {
  mat <- matrix(1:6, 2, 3)
  expect_error(detrend_matrix(mat, "rows", 0L), "degree must be >= 1")
})

test_that("detrend_matrix degree 2 works", {
  set.seed(7)
  mat <- matrix(rnorm(30), 5, 6)
  result <- detrend_matrix(mat, "rows", 2L)
  expect_equal(dim(result), dim(mat))
  expect_true(is.matrix(result))
})

test_that("where_mask replaces values not matching predicate", {
  mat <- matrix(c(-1, 2, -3, 4, -5, 6), 2, 3)
  result <- where_mask(mat, function(x) x > 0, fill = 0)
  expect_equal(result[mat > 0], mat[mat > 0])
  expect_equal(result[mat <= 0], rep(0, sum(mat <= 0)))
})

test_that("where_mask with NA fill", {
  mat <- matrix(1:6, 2, 3)
  result <- where_mask(mat, function(x) x > 3, fill = NA)
  expect_true(all(is.na(result[mat <= 3])))
  expect_equal(result[mat > 3], mat[mat > 3])
})

test_that("where_mask coerces non-logical mask", {
  mat <- matrix(1:6, 2, 3)
  result <- where_mask(mat, function(x) as.integer(x > 3), fill = 0)
  expect_equal(result[mat <= 3], rep(0, sum(mat <= 3)))
})

test_that("where_mask rejects non-scalar fill", {
  mat <- matrix(1:6, 2, 3)
  expect_error(where_mask(mat, function(x) x > 0, fill = c(1, 2)),
               "fill must be a scalar")
})
