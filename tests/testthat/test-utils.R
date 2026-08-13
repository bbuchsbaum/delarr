# tests/testthat/test-utils.R
# Tests for utility functions

test_that("%||% returns x when non-null", {
  expect_equal(5 %||% 10, 5)
  expect_equal("a" %||% "b", "a")
})

test_that("%||% returns y when x is null", {
  expect_equal(NULL %||% 10, 10)
  expect_equal(NULL %||% "b", "b")
})

test_that("normalize_index returns seq_len for NULL", {
  expect_equal(normalize_index(NULL, 5), 1:5)
})

test_that("normalize_index handles logical indices", {
  expect_equal(normalize_index(c(TRUE, FALSE, TRUE), 3), c(1L, 3L))
})

test_that("normalize_index rejects logical with NA", {
  expect_error(normalize_index(c(TRUE, NA, FALSE), 3), "Logical index contains NA")
})

test_that("normalize_index rejects logical of wrong length", {
  expect_error(normalize_index(c(TRUE, FALSE), 3), "Logical index length must match")
})

test_that("normalize_index handles positive integer indices", {
  expect_equal(normalize_index(c(2L, 4L), 5), c(2L, 4L))
})

test_that("normalize_index rejects NA in integer indices", {
  expect_error(normalize_index(c(1L, NA_integer_), 5), "Index contains NA")
})

test_that("normalize_index rejects zero in indices", {
  expect_error(normalize_index(c(0L, 1L), 5), "Index cannot contain zero")
})

test_that("normalize_index handles negative indices", {
  expect_equal(normalize_index(-2L, 5), c(1L, 3L, 4L, 5L))
  expect_equal(normalize_index(c(-1L, -3L), 4), c(2L, 4L))
})

test_that("normalize_index rejects mixed positive and negative", {
  expect_error(normalize_index(c(-1L, 2L), 5), "Cannot mix positive and negative")
})

test_that("normalize_index rejects out-of-bounds", {
  expect_error(normalize_index(c(1L, 10L), 5), "Index out of bounds")
})

test_that("seq_chunk returns empty list for n <= 0", {
  expect_equal(seq_chunk(0, 5), list())
  expect_equal(seq_chunk(-1, 5), list())
})

test_that("seq_chunk splits correctly", {
  chunks <- seq_chunk(7, 3)
  expect_equal(length(chunks), 3)
  expect_equal(unlist(chunks, use.names = FALSE), 1:7)
})

test_that("seq_chunk with size >= n gives single chunk", {
  chunks <- seq_chunk(5, 10)
  expect_equal(length(chunks), 1)
  expect_equal(unlist(chunks, use.names = FALSE), 1:5)
})

test_that("safe_mean computes row means", {
  mat <- matrix(1:12, 3, 4)
  expect_equal(safe_mean(mat, "rows"), rowMeans(mat))
})

test_that("safe_mean computes col means", {
  mat <- matrix(1:12, 3, 4)
  expect_equal(safe_mean(mat, "cols"), colMeans(mat))
})

test_that("safe_sd computes row standard deviations", {
  mat <- matrix(as.double(1:12), 3, 4)
  expected <- apply(mat, 1, sd)
  expect_equal(safe_sd(mat, "rows"), expected)
})

test_that("safe_sd computes col standard deviations", {
  mat <- matrix(as.double(1:12), 3, 4)
  expected <- apply(mat, 2, sd)
  expect_equal(safe_sd(mat, "cols"), expected)
})

test_that("safe_scale_matrix with no center or scale returns unchanged", {
  mat <- matrix(1:12, 3, 4)
  expect_equal(safe_scale_matrix(mat, "rows", center = FALSE, scale = FALSE), mat)
})

test_that("safe_scale_matrix centers and scales rows", {
  mat <- matrix(as.double(1:12), 3, 4)
  result <- safe_scale_matrix(mat, "rows", center = TRUE, scale = TRUE)
  expect_true(all(abs(rowMeans(result)) < 1e-10))
})

test_that("safe_scale_matrix centers and scales cols", {
  mat <- matrix(as.double(1:12), 3, 4)
  result <- safe_scale_matrix(mat, "cols", center = TRUE, scale = TRUE)
  expect_true(all(abs(colMeans(result)) < 1e-10))
})

test_that("safe_scale_matrix handles zero sd by setting to 1", {
  mat <- matrix(c(5, 5, 5, 1, 2, 3), 3, 2)
  result <- safe_scale_matrix(mat, "cols", center = TRUE, scale = TRUE)
  expect_true(all(is.finite(result)))
  expect_equal(result[, 1], rep(0, 3))
})

test_that("safe_min computes row mins", {
  mat <- matrix(c(3, 1, 2, 5, 4, 6), 3, 2)
  expect_equal(safe_min(mat, "rows"), apply(mat, 1, min))
})

test_that("safe_min computes col mins", {
  mat <- matrix(c(3, 1, 2, 5, 4, 6), 3, 2)
  expect_equal(safe_min(mat, "cols"), apply(mat, 2, min))
})

test_that("safe_max computes row maxes", {
  mat <- matrix(c(3, 1, 2, 5, 4, 6), 3, 2)
  expect_equal(safe_max(mat, "rows"), apply(mat, 1, max))
})

test_that("safe_max computes col maxes", {
  mat <- matrix(c(3, 1, 2, 5, 4, 6), 3, 2)
  expect_equal(safe_max(mat, "cols"), apply(mat, 2, max))
})

test_that("safe_min/max handle NA with na.rm", {
  mat <- matrix(c(1, NA, 3, 4, 5, NA), 3, 2)
  expect_equal(safe_min(mat, "rows", na.rm = TRUE),
               c(min(1, 4), 5, 3))
  expect_equal(safe_max(mat, "rows", na.rm = TRUE),
               c(max(1, 4), 5, 3))
})

test_that("normalize_chunk_margin resolves character and numeric axes", {
  expect_equal(delarr:::normalize_chunk_margin("cols", 3), "cols")
  expect_equal(delarr:::normalize_chunk_margin("rows", 2), "rows")
  expect_equal(delarr:::normalize_chunk_margin(3L, 4), 3L)
  expect_equal(delarr:::normalize_chunk_margin(1L, 2), "rows")
})

test_that("normalize_chunk_margin rejects multi-axis numeric input", {
  expect_error(
    delarr:::normalize_chunk_margin(c(1L, 2L), 4),
    "single axis"
  )
})

test_that("resolve_chunk_axis validates character length and multi-axis input", {
  expect_error(
    delarr:::resolve_chunk_axis(c("rows", "cols"), 3),
    "single character value"
  )
  expect_error(
    delarr:::resolve_chunk_axis(c(1L, 2L), 4),
    "single axis"
  )
})

test_that("infer_nd_chunk_size uses rows/cols hints and default fallback", {
  seed <- list(chunk_hint = list(rows = 2L, cols = 3L))
  expect_equal(
    delarr:::infer_nd_chunk_size(seed, c(5L, 6L, 7L), 1L, NULL),
    2L
  )
  expect_equal(
    delarr:::infer_nd_chunk_size(seed, c(5L, 6L, 7L), 2L, NULL),
    3L
  )

  seed_no_hint <- list(chunk_hint = NULL)
  fallback <- delarr:::infer_nd_chunk_size(seed_no_hint, c(100L, 4L), 1L, NULL)
  expect_true(fallback >= 1L && fallback <= 100L)
})

test_that("safe_mean/sd/min/max fall back when matrixStats is unavailable", {
  local_mocked_bindings(
    requireNamespace = function(pkg, quietly = TRUE) {
      if (identical(pkg, "matrixStats")) return(FALSE)
      base::requireNamespace(pkg, quietly = quietly)
    },
    .package = "base"
  )
  mat <- matrix(as.double(1:12), 3, 4)
  expect_equal(delarr:::safe_mean(mat, "rows"), rowMeans(mat))
  expect_equal(delarr:::safe_mean(mat, "cols"), colMeans(mat))
  expect_equal(delarr:::safe_sd(mat, "rows"), apply(mat, 1, sd))
  expect_equal(delarr:::safe_sd(mat, "cols"), apply(mat, 2, sd))
  expect_equal(delarr:::safe_min(mat, "rows"), apply(mat, 1, min))
  expect_equal(delarr:::safe_max(mat, "cols"), apply(mat, 2, max))
})

test_that("axis utilities use N-d apply paths for arrays", {
  arr <- array(as.double(1:24), dim = c(2, 3, 4))
  expect_equal(
    delarr:::axis_sds(arr, 3L),
    apply(arr, 3, stats::sd)
  )
  expect_equal(
    delarr:::axis_sweep(arr, 2L, apply(arr, 2, mean), FUN = "-"),
    sweep(arr, 2, apply(arr, 2, mean), "-")
  )
  scaled <- delarr:::axis_scale(arr, 3L, center = TRUE, scale = TRUE)
  expect_equal(dim(scaled), dim(arr))
  slice_means <- apply(scaled, 3, mean)
  expect_true(all(abs(slice_means) < 1e-10))
})

test_that("axis_scale returns unchanged arrays when center and scale are FALSE", {
  arr <- array(as.double(1:24), dim = c(2, 3, 4))
  expect_identical(
    delarr:::axis_scale(arr, 2L, center = FALSE, scale = FALSE),
    arr
  )
})

test_that("infer_nd_chunk_size uses adaptive fallback for 1D extents", {
  seed <- list(chunk_hint = NULL)
  expect_equal(
    delarr:::infer_nd_chunk_size(seed, c(100L), 1L, NULL, target_bytes = 200),
    min(100L, floor(200 / 8))
  )
  expect_equal(
    delarr:::infer_nd_chunk_size(seed, c(100L), 1L, NULL),
    min(100L, floor((8 * 16384) / 8))
  )
})
