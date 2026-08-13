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

test_that("pull_seed errors if pull returns the wrong shape", {
  seed <- delarr_seed(
    nrow = 3, ncol = 4,
    pull = function(rows = NULL, cols = NULL) matrix(0, 1, 1)
  )
  expect_error(pull_seed(seed, rows = 1:2, cols = 1:3), "expected 2x3")
})

test_that("provider seeds are reconstructible and execute through dispatch", {
  data <- matrix(1:20, 4, 5)
  seed <- delarr_provider_seed(
    provider = data,
    dims = dim(data),
    chunk_hint = list(rows = 2L, cols = 3L)
  )

  expect_s3_class(seed, "delarr_provider_seed")
  expect_false(any(vapply(seed, is.function, logical(1))))
  restored <- unserialize(serialize(seed, NULL))
  expect_equal(pull_seed(restored, c(4, 1), c(5, 2)), data[c(4, 1), c(5, 2)])
  expect_equal(
    collect(delarr(restored)[c(3, 1), c(4, 2)]),
    data[c(3, 1), c(4, 2), drop = FALSE]
  )
})

test_that("provider seeds support N-dimensional reconstructible plans", {
  data <- array(seq_len(24), dim = c(2, 3, 4))
  x <- delarr_provider(data, dims = dim(data))
  restored <- unserialize(serialize(x, NULL))

  expect_equal(
    collect(restored[2:1, 1:2, c(4, 2)]),
    data[2:1, 1:2, c(4, 2), drop = FALSE]
  )
})

test_that("provider seeds reject runtime state and malformed contracts", {
  expect_error(
    delarr_provider_seed(list(loader = function() NULL), c(2, 2)),
    "cannot contain functions"
  )
  expect_error(
    delarr_provider_seed(new.env(), c(2, 2)),
    "cannot contain functions"
  )
  expect_error(delarr_provider_seed(list(), 2L), "length >= 2")
  expect_error(delarr_provider_seed(list(), c(2, 2), chunk_hint = 2L), "list")

  missing_method <- delarr_provider_seed(structure(list(), class = "unknown_provider"), c(2, 2))
  expect_error(pull_seed(missing_method), "No delarr_provider_pull")
})

test_that("provider pull enforces exact returned dimensions", {
  local_provider <- structure(list(), class = "bad_shape_provider")
  registerS3method(
    "delarr_provider_pull",
    "bad_shape_provider",
    function(provider, indices, ...) matrix(0, 1, 1),
    envir = asNamespace("delarr")
  )
  seed <- delarr_provider_seed(local_provider, c(3, 4))
  expect_error(pull_seed(seed, 1:2, 1:3), "expected 2x3")
})

test_that("delarr_seed_nd validates dimensions and pull function", {
  expect_error(
    delarr_seed_nd(dims = 4L, pull = function(indices) array(0, dim = c(4))),
    "length >= 2"
  )
  expect_error(
    delarr_seed_nd(dims = c(2L, -1L), pull = function(indices) matrix(0, 2, 1)),
    "non-negative"
  )
  expect_error(
    delarr_seed_nd(dims = c(2L, 3L), pull = "not a function"),
    "pull must be a function"
  )
})

test_that("pull_seed_nd delegates to pull for 2D seeds without pull_nd", {
  mat <- matrix(as.double(1:6), 2, 3)
  seed <- delarr_seed(
    nrow = 2,
    ncol = 3,
    pull = function(rows = NULL, cols = NULL) {
      rows <- rows %||% seq_len(2)
      cols <- cols %||% seq_len(3)
      mat[rows, cols, drop = FALSE]
    }
  )
  expect_equal(
    pull_seed_nd(seed, list(1:2, 2:3)),
    mat[1:2, 2:3, drop = FALSE]
  )
})

test_that("pull_seed_nd validates index length and result shape", {
  arr <- array(seq_len(24), dim = c(2, 3, 4))
  seed <- delarr_seed_nd(
    dims = dim(arr),
    pull = function(indices) {
      idx <- lapply(seq_along(dim(arr)), function(k) {
        indices[[k]] %||% seq_len(dim(arr)[[k]])
      })
      do.call(`[`, c(list(arr), idx, list(drop = FALSE)))
    }
  )

  expect_error(pull_seed_nd(seed, list(1L, 1L)), "length 3")
  expect_equal(pull_seed_nd(seed, list(1:2, 1:3, 1:4)), arr)

  vector_seed <- delarr_seed_nd(
    dims = c(2L, 3L, 4L),
    pull = function(indices) as.vector(arr)
  )
  reshaped <- pull_seed_nd(vector_seed, list(NULL, NULL, NULL))
  expect_equal(dim(reshaped), dim(arr))
  expect_equal(as.vector(reshaped), as.vector(arr))

  wrong_length_seed <- delarr_seed_nd(
    dims = c(2L, 3L),
    pull = function(indices) seq_len(5)
  )
  expect_error(
    pull_seed_nd(wrong_length_seed, list(NULL, NULL)),
    "expected 6"
  )

  wrong_dim_seed <- delarr_seed_nd(
    dims = c(2L, 3L, 4L),
    pull = function(indices) array(0, dim = c(2L, 3L, 3L))
  )
  expect_error(
    pull_seed_nd(wrong_dim_seed, list(NULL, NULL, NULL)),
    "expected \\[2,3,4\\]"
  )
})
