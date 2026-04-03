# ---- normalize_axes ----------------------------------------------------------

test_that("normalize_axes validates and sorts", {
  expect_equal(delarr:::normalize_axes(c(3, 1), 4), c(1L, 3L))
  expect_error(delarr:::normalize_axes(c(0), 3), "between 1")
  expect_error(delarr:::normalize_axes(c(5), 3), "between 1")
  expect_error(delarr:::normalize_axes(c(1, 1), 3), "duplicates")
  expect_error(delarr:::normalize_axes(integer(0), 3), "at least one")
  expect_error(delarr:::normalize_axes(NA_integer_, 3), "NA")
})

# ---- resolve_chunk_axis ------------------------------------------------------

test_that("resolve_chunk_axis handles character and numeric", {
  expect_equal(delarr:::resolve_chunk_axis("cols", 3), 2L)
  expect_equal(delarr:::resolve_chunk_axis("rows", 3), 1L)
  expect_equal(delarr:::resolve_chunk_axis(3L, 4), 3L)
  expect_equal(delarr:::resolve_chunk_axis(NULL, 3, default = 3L), 3L)
})

# ---- infer_nd_chunk_size -----------------------------------------------------

test_that("infer_nd_chunk_size respects explicit chunk_size", {
  seed <- list(chunk_hint = NULL)
  expect_equal(delarr:::infer_nd_chunk_size(seed, c(3, 4, 5), 3L, 2L), 2L)
  expect_equal(delarr:::infer_nd_chunk_size(seed, c(3, 4, 5), 3L, 100L), 5L)
})

test_that("infer_nd_chunk_size uses chunk_hint", {
  seed <- list(chunk_hint = list(axis3 = 2L))
  expect_equal(delarr:::infer_nd_chunk_size(seed, c(3, 4, 5), 3L, NULL), 2L)
})

test_that("infer_nd_chunk_size respects target_bytes", {
  seed <- list(chunk_hint = NULL)
  # 3 x 4 = 12 doubles per slice along axis 3 = 96 bytes
  # target_bytes = 200 → can fit floor(200/96) = 2 slices
  result <- delarr:::infer_nd_chunk_size(seed, c(3, 4, 5), 3L, NULL,
                                         target_bytes = 200)
  expect_equal(result, 2L)
})

# ---- dim_to_axis -------------------------------------------------------------

test_that("dim_to_axis converts correctly", {
  expect_equal(delarr:::dim_to_axis("rows"), 1L)
  expect_equal(delarr:::dim_to_axis("cols"), 2L)
  expect_equal(delarr:::dim_to_axis(3L), 3L)
  expect_error(delarr:::dim_to_axis("foo"), "must be")
})

# ---- safe_min / safe_max ----------------------------------------------------

test_that("safe_min and safe_max work without matrixStats", {
  mat <- matrix(c(3, 1, 2, 5, 4, 6), 2, 3)
  expect_equal(delarr:::safe_min(mat, "rows"), apply(mat, 1, min))
  expect_equal(delarr:::safe_min(mat, "cols"), apply(mat, 2, min))
  expect_equal(delarr:::safe_max(mat, "rows"), apply(mat, 1, max))
  expect_equal(delarr:::safe_max(mat, "cols"), apply(mat, 2, max))
})

test_that("safe_min/max handle NA with na.rm", {
  mat <- matrix(c(1, NA, 3, 4, NA, 6), 2, 3)
  expect_equal(
    delarr:::safe_min(mat, "rows", na.rm = TRUE),
    apply(mat, 1, min, na.rm = TRUE)
  )
  expect_equal(
    delarr:::safe_max(mat, "cols", na.rm = TRUE),
    apply(mat, 2, max, na.rm = TRUE)
  )
})

# ---- axis_means / axis_sds / axis_sums (2D fast paths) ----------------------

test_that("axis_means 2D fast path matches apply", {
  mat <- matrix(rnorm(12), 3, 4)
  expect_equal(delarr:::axis_means(mat, 1L), rowMeans(mat))
  expect_equal(delarr:::axis_means(mat, 2L), colMeans(mat))
})

test_that("axis_sds 2D fast path matches apply", {
  mat <- matrix(rnorm(12), 3, 4)
  expect_equal(delarr:::axis_sds(mat, 1L), apply(mat, 1, sd))
  expect_equal(delarr:::axis_sds(mat, 2L), apply(mat, 2, sd))
})

test_that("axis_sums 2D fast path matches rowSums/colSums", {
  mat <- matrix(1:12, 3, 4)
  expect_equal(delarr:::axis_sums(mat, 1L), rowSums(mat))
  expect_equal(delarr:::axis_sums(mat, 2L), colSums(mat))
})

# ---- axis_center / axis_scale 2D fast paths ----------------------------------

test_that("axis_center 2D matches safe_center", {
  mat <- matrix(rnorm(12), 3, 4)
  expect_equal(
    delarr:::axis_center(mat, 1L),
    delarr:::safe_center(mat, "rows")
  )
  expect_equal(
    delarr:::axis_center(mat, 2L),
    delarr:::safe_center(mat, "cols")
  )
})

test_that("axis_scale 2D matches safe_scale_matrix", {
  mat <- matrix(rnorm(12), 3, 4)
  expect_equal(
    delarr:::axis_scale(mat, 1L),
    delarr:::safe_scale_matrix(mat, "rows", center = TRUE, scale = TRUE)
  )
})

# ---- axis_detrend 2D fast path -----------------------------------------------

test_that("axis_detrend 2D matches detrend_matrix", {
  mat <- matrix(1:12 + rep(1:4, each = 3), 3, 4)
  expect_equal(
    delarr:::axis_detrend(mat, 1L, 1L),
    delarr:::detrend_matrix(mat, "rows", 1L)
  )
  expect_equal(
    delarr:::axis_detrend(mat, 2L, 1L),
    delarr:::detrend_matrix(mat, "cols", 1L)
  )
})
