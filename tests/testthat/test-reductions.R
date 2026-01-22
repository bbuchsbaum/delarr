# tests/testthat/test-reductions.R
# Edge case tests for d_reduce() operations

test_that("all-NA row reductions return NA not NaN/Inf", {
  mat <- matrix(NA_real_, 3, 4)
  x <- delarr(mat)

  # Test all standard reduction operations
  for (fn_name in c("sum", "mean", "min", "max")) {
    fn <- get(fn_name, envir = baseenv())
    result <- collect(d_reduce(x, fn, "rows", na.rm = TRUE))

    expect_true(all(is.na(result)),
                info = sprintf("%s(all-NA, na.rm=TRUE) rows should be NA", fn_name))
    expect_false(any(is.nan(result)),
                 info = sprintf("%s(all-NA) rows should not be NaN", fn_name))
    expect_false(any(is.infinite(result)),
                 info = sprintf("%s(all-NA) rows should not be Inf", fn_name))
  }
})

test_that("all-NA column reductions return NA not NaN/Inf", {
  mat <- matrix(NA_real_, 3, 4)
  x <- delarr(mat)

  for (fn_name in c("sum", "mean", "min", "max")) {
    fn <- get(fn_name, envir = baseenv())
    result <- collect(d_reduce(x, fn, "cols", na.rm = TRUE))

    expect_true(all(is.na(result)),
                info = sprintf("%s(all-NA, na.rm=TRUE) cols should be NA", fn_name))
    expect_false(any(is.nan(result)),
                 info = sprintf("%s(all-NA) cols should not be NaN", fn_name))
    expect_false(any(is.infinite(result)),
                 info = sprintf("%s(all-NA) cols should not be Inf", fn_name))
  }
})

test_that("mixed-NA reductions return correct values", {
  # First row all NA, second row has values, third row mixed
  mat <- matrix(c(NA, 1, NA,
                  NA, 2, 3,
                  NA, NA, 4,
                  NA, 5, NA), nrow = 3, ncol = 4)
  x <- delarr(mat)

  # Row sums with na.rm
  row_sums <- collect(d_reduce(x, sum, "rows", na.rm = TRUE))
  expect_equal(row_sums[1], NA_real_)  # All NA row
  expect_equal(row_sums[2], 1 + 2 + 5) # Has values
  expect_equal(row_sums[3], 3 + 4)     # Mixed

  # Row means with na.rm
  row_means <- collect(d_reduce(x, mean, "rows", na.rm = TRUE))
  expect_equal(row_means[1], NA_real_)
  expect_equal(row_means[2], mean(c(1, 2, 5)))
  expect_equal(row_means[3], mean(c(3, 4)))
})

test_that("chunked reductions match non-chunked for edge cases", {
  mat <- matrix(NA_real_, 4, 8)
  mat[2, ] <- 1:8  # One row with values
  x <- delarr(mat)

  # Compare different chunk sizes
  result_1 <- collect(d_reduce(x, sum, "rows", na.rm = TRUE), chunk_size = 1L)
  result_4 <- collect(d_reduce(x, sum, "rows", na.rm = TRUE), chunk_size = 4L)
  result_8 <- collect(d_reduce(x, sum, "rows", na.rm = TRUE), chunk_size = 8L)

  expect_equal(result_1, result_4)
  expect_equal(result_4, result_8)
  expect_equal(result_1[1], NA_real_)  # All-NA row
  expect_equal(result_1[2], sum(1:8))  # Row with values
})

test_that("reductions without na.rm return NA when any NA present", {
  # Matrix fills column-wise: row 1 = [1, 3], row 2 = [NA, 4]
  mat <- matrix(c(1, NA, 3, 4), 2, 2)
  x <- delarr(mat)

  # Without na.rm, any NA propagates
  row_sums <- collect(d_reduce(x, sum, "rows", na.rm = FALSE))
  expect_equal(row_sums[1], 1 + 3)  # No NA in row 1
  expect_true(is.na(row_sums[2]))   # Has NA in row 2
})
