# tests/testthat/test-seed.R
# Tests for delarr_seed, dim.delarr_seed, pull_seed

test_that("delarr_seed creates correct structure", {
  data <- matrix(1:6, 2, 3)
  seed <- delarr_seed(
    nrow = 2, ncol = 3,
    pull = function(rows = NULL, cols = NULL) {
      rows <- rows %||% seq_len(2)
      cols <- cols %||% seq_len(3)
      data[rows, cols, drop = FALSE]
    }
  )
  expect_s3_class(seed, "delarr_seed")
  expect_equal(seed$nrow, 2L)
  expect_equal(seed$ncol, 3L)
})

test_that("delarr_seed validates nrow", {
  pfn <- function(rows = NULL, cols = NULL) matrix(0, 1, 1)
  expect_error(delarr_seed(nrow = -1, ncol = 2, pull = pfn),
               "non-negative")
  expect_error(delarr_seed(nrow = "a", ncol = 2, pull = pfn),
               "non-negative")
  expect_error(delarr_seed(nrow = c(1, 2), ncol = 2, pull = pfn),
               "non-negative")
})

test_that("delarr_seed validates ncol", {
  pfn <- function(rows = NULL, cols = NULL) matrix(0, 1, 1)
  expect_error(delarr_seed(nrow = 2, ncol = -1, pull = pfn),
               "non-negative")
})

test_that("delarr_seed validates pull is a function", {
  expect_error(delarr_seed(nrow = 2, ncol = 3, pull = "not_fn"),
               "pull must be a function")
})

test_that("delarr_seed stores optional fields", {
  pfn <- function(rows = NULL, cols = NULL) matrix(0, 2, 3)
  dn <- list(c("a", "b"), c("x", "y", "z"))
  bfn <- function() invisible(NULL)
  efn <- function() invisible(NULL)
  ch <- list(cols = 100L)

  seed <- delarr_seed(nrow = 2, ncol = 3, pull = pfn,
                      chunk_hint = ch, dimnames = dn,
                      begin = bfn, end = efn)
  expect_equal(seed$chunk_hint, ch)
  expect_equal(seed$dimnames, dn)
  expect_identical(seed$begin, bfn)
  expect_identical(seed$end, efn)
})

test_that("dim.delarr_seed returns dimensions", {
  pfn <- function(rows = NULL, cols = NULL) matrix(0, 3, 5)
  seed <- delarr_seed(nrow = 3, ncol = 5, pull = pfn)
  expect_equal(dim(seed), c(3L, 5L))
})

test_that("pull_seed extracts full matrix", {
  data <- matrix(1:12, 3, 4)
  seed <- delarr_seed(
    nrow = 3, ncol = 4,
    pull = function(rows = NULL, cols = NULL) {
      rows <- rows %||% seq_len(3)
      cols <- cols %||% seq_len(4)
      data[rows, cols, drop = FALSE]
    }
  )
  result <- pull_seed(seed)
  expect_equal(result, data)
})

test_that("pull_seed extracts subsets", {
  data <- matrix(1:12, 3, 4)
  seed <- delarr_seed(
    nrow = 3, ncol = 4,
    pull = function(rows = NULL, cols = NULL) {
      rows <- rows %||% seq_len(3)
      cols <- cols %||% seq_len(4)
      data[rows, cols, drop = FALSE]
    }
  )
  result <- pull_seed(seed, rows = 1:2, cols = 2:3)
  expect_equal(result, data[1:2, 2:3])
})

test_that("pull_seed errors if pull does not return matrix", {
  seed <- delarr_seed(
    nrow = 2, ncol = 2,
    pull = function(rows = NULL, cols = NULL) 42
  )
  expect_error(pull_seed(seed), "Seed pull must return a matrix")
})
