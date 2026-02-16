skip_if_not_installed("shard")

# ---- Round-trip and basic construction --------------------------------------

test_that("delarr_shard round-trip", {
  set.seed(1)
  mat <- matrix(rnorm(20), 4, 5)
  darr <- delarr_shard(mat)
  expect_s3_class(darr, "delarr")
  expect_equal(dim(darr), dim(mat))
  out <- collect_shard(darr, workers = 2)
  expect_equal(out, mat)
})

test_that("delarr_shard rejects non-numeric input", {
  expect_error(delarr_shard("abc"), "numeric matrix")
  expect_error(delarr_shard(1:10), "numeric matrix")
  expect_error(delarr_shard(matrix(TRUE, 2, 2)), "numeric matrix")
})

test_that("delarr_shard preserves dimnames", {
  mat <- matrix(1:12, 3, 4, dimnames = list(c("a", "b", "c"), paste0("V", 1:4)))
  darr <- delarr_shard(mat)
  expect_equal(dimnames(darr), dimnames(mat))
})

# ---- Elementwise pipelines (Path A) ----------------------------------------

test_that("collect_shard with ops pipeline matches sequential", {
  set.seed(2)
  mat <- matrix(rnorm(40), 5, 8)
  darr <- delarr_shard(mat)
  pipeline <- darr |> d_map(~ .x^2) |> d_center("cols")
  par_result <- collect_shard(pipeline, workers = 2)
  seq_result <- collect(pipeline)
  expect_equal(par_result, seq_result)
})

test_that("collect_shard with arithmetic ops", {
  set.seed(3)
  mat <- matrix(rnorm(24), 4, 6)
  darr <- delarr_shard(mat)
  result <- collect_shard(darr * 2 + 1, workers = 2)
  expect_equal(result, mat * 2 + 1)
})

test_that("collect_shard with d_where", {
  set.seed(4)
  mat <- matrix(rnorm(20), 4, 5)
  darr <- delarr_shard(mat)
  pipeline <- darr |> d_where(~ .x > 0, fill = 0)
  result <- collect_shard(pipeline, workers = 2)
  expect_equal(result, collect(pipeline))
})

test_that("collect_shard with slicing", {
  set.seed(5)
  mat <- matrix(rnorm(40), 5, 8)
  darr <- delarr_shard(mat)
  result <- collect_shard(darr[2:4, 3:6], workers = 2)
  expect_equal(result, mat[2:4, 3:6])
})

test_that("collect_shard with chunk boundary edge case", {
  set.seed(6)
  mat <- matrix(rnorm(35), 5, 7)
  darr <- delarr_shard(mat)
  # Force chunk_size that doesn't divide n_cols evenly
  result <- collect_shard(darr |> d_map(~ .x + 1), workers = 2, chunk_size = 3L)
  expect_equal(result, mat + 1)
})

# ---- Row reductions (Path B) -----------------------------------------------

test_that("row-reduction sum via collect_shard", {
  set.seed(10)
  mat <- matrix(rnorm(30), 5, 6)
  darr <- delarr_shard(mat)
  result <- collect_shard(darr |> d_reduce(sum, "rows"), workers = 2)
  expect_equal(result, rowSums(mat))
})

test_that("row-reduction mean via collect_shard", {
  set.seed(11)
  mat <- matrix(rnorm(30), 5, 6)
  darr <- delarr_shard(mat)
  result <- collect_shard(darr |> d_reduce(mean, "rows"), workers = 2)
  expect_equal(result, rowMeans(mat))
})

test_that("row-reduction min/max via collect_shard", {
  set.seed(12)
  mat <- matrix(rnorm(30), 5, 6)
  darr <- delarr_shard(mat)
  expect_equal(
    collect_shard(darr |> d_reduce(min, "rows"), workers = 2),
    apply(mat, 1, min)
  )
  expect_equal(
    collect_shard(darr |> d_reduce(max, "rows"), workers = 2),
    apply(mat, 1, max)
  )
})

test_that("row-reduction with ops before reduce", {
  set.seed(13)
  mat <- matrix(rnorm(40), 5, 8)
  darr <- delarr_shard(mat)
  pipeline <- darr |> d_map(~ .x^2) |> d_reduce(sum, "rows")
  result <- collect_shard(pipeline, workers = 2)
  expect_equal(result, rowSums(mat^2))
})

# ---- Column reductions (Path C) --------------------------------------------

test_that("col-reduction sum via collect_shard", {
  set.seed(20)
  mat <- matrix(rnorm(30), 5, 6)
  darr <- delarr_shard(mat)
  result <- collect_shard(darr |> d_reduce(sum, "cols"), workers = 2)
  expect_equal(result, colSums(mat))
})

test_that("col-reduction mean via collect_shard", {
  set.seed(21)
  mat <- matrix(rnorm(30), 5, 6)
  darr <- delarr_shard(mat)
  result <- collect_shard(darr |> d_reduce(mean, "cols"), workers = 2)
  expect_equal(result, colMeans(mat))
})

test_that("col-reduction min/max via collect_shard", {
  set.seed(22)
  mat <- matrix(rnorm(30), 5, 6)
  darr <- delarr_shard(mat)
  expect_equal(
    collect_shard(darr |> d_reduce(min, "cols"), workers = 2),
    apply(mat, 2, min)
  )
  expect_equal(
    collect_shard(darr |> d_reduce(max, "cols"), workers = 2),
    apply(mat, 2, max)
  )
})

# ---- NA handling ------------------------------------------------------------

test_that("row-reduction sum with NA and na_rm", {
  mat <- matrix(c(1, NA, 3, 4, NA, 6, 7, 8, NA, NA, NA, NA), 4, 3)
  darr <- delarr_shard(mat)
  result <- collect_shard(darr |> d_reduce(sum, "rows", na.rm = TRUE), workers = 2)
  expected <- rowSums(mat, na.rm = TRUE)
  # All-NA rows should be NA, not 0
  expected[apply(mat, 1, function(r) all(is.na(r)))] <- NA_real_
  expect_equal(result, expected)
})

test_that("row-reduction mean with NA and na_rm", {
  mat <- matrix(c(1, NA, 3, 4, NA, 6, 7, 8, NA, NA, NA, NA), 4, 3)
  darr <- delarr_shard(mat)
  result <- collect_shard(darr |> d_reduce(mean, "rows", na.rm = TRUE), workers = 2)
  seq_result <- collect(darr |> d_reduce(mean, "rows", na.rm = TRUE))
  expect_equal(result, seq_result)
})

test_that("col-reduction sum with NA and na_rm", {
  mat <- matrix(c(1, NA, NA, 4, 5, NA), 3, 2)
  darr <- delarr_shard(mat)
  result <- collect_shard(darr |> d_reduce(sum, "cols", na.rm = TRUE), workers = 2)
  seq_result <- collect(darr |> d_reduce(sum, "cols", na.rm = TRUE))
  expect_equal(result, seq_result)
})

test_that("col-reduction mean with NA and na_rm", {
  mat <- matrix(c(1, NA, NA, 4, 5, NA), 3, 2)
  darr <- delarr_shard(mat)
  result <- collect_shard(darr |> d_reduce(mean, "cols", na.rm = TRUE), workers = 2)
  seq_result <- collect(darr |> d_reduce(mean, "cols", na.rm = TRUE))
  expect_equal(result, seq_result)
})

test_that("row-reduction min/max with NA and na_rm", {
  mat <- matrix(c(1, NA, 3, NA, 5, NA, NA, NA, 9), 3, 3)
  darr <- delarr_shard(mat)
  min_result <- collect_shard(darr |> d_reduce(min, "rows", na.rm = TRUE), workers = 2)
  max_result <- collect_shard(darr |> d_reduce(max, "rows", na.rm = TRUE), workers = 2)
  expect_equal(min_result, collect(darr |> d_reduce(min, "rows", na.rm = TRUE)))
  expect_equal(max_result, collect(darr |> d_reduce(max, "rows", na.rm = TRUE)))
})

# ---- Fallback paths ---------------------------------------------------------

test_that("fallback for row-center (requires_full_eval)", {
  set.seed(30)
  mat <- matrix(rnorm(20), 4, 5)
  darr <- delarr_shard(mat)
  result <- collect_shard(darr |> d_center("rows"), workers = 2)
  expect_equal(result, collect(darr |> d_center("rows")))
})

test_that("fallback for row-zscore (requires_full_eval)", {
  set.seed(31)
  mat <- matrix(rnorm(20), 4, 5)
  darr <- delarr_shard(mat)
  result <- collect_shard(darr |> d_zscore("rows"), workers = 2)
  expect_equal(result, collect(darr |> d_zscore("rows")))
})

test_that("fallback for row-scale (requires_full_eval)", {
  set.seed(32)
  mat <- matrix(rnorm(20), 4, 5)
  darr <- delarr_shard(mat)
  result <- collect_shard(darr |> d_scale("rows"), workers = 2)
  expect_equal(result, collect(darr |> d_scale("rows")))
})

test_that("fallback for row-detrend (requires_full_eval)", {
  set.seed(33)
  mat <- matrix(rnorm(20), 4, 5)
  darr <- delarr_shard(mat)
  result <- collect_shard(darr |> d_detrend("rows"), workers = 2)
  expect_equal(result, collect(darr |> d_detrend("rows")))
})

test_that("fallback for d_map2 with paired delarrs", {
  set.seed(34)
  mat1 <- matrix(rnorm(20), 4, 5)
  mat2 <- matrix(rnorm(20), 4, 5)
  darr1 <- delarr_shard(mat1)
  darr2 <- delarr_shard(mat2)
  pipeline <- darr1 + darr2
  result <- collect_shard(pipeline, workers = 2)
  expect_equal(result, mat1 + mat2)
})

test_that("fallback for generic reduction", {
  set.seed(35)
  mat <- matrix(rnorm(20), 4, 5)
  darr <- delarr_shard(mat)
  my_fn <- function(x) sum(x^2)
  result <- collect_shard(darr |> d_reduce(my_fn, "rows"), workers = 2)
  expect_equal(result, apply(mat, 1, my_fn))
})

# ---- Non-shard seed ---------------------------------------------------------

test_that("non-shard seed works with collect_shard", {
  set.seed(40)
  mat <- matrix(rnorm(20), 4, 5)
  darr <- delarr_mem(mat)
  result <- collect_shard(darr |> d_map(~ .x * 3), workers = 2)
  expect_equal(result, mat * 3)
})

test_that("non-shard seed with reduction via collect_shard", {
  set.seed(41)
  mat <- matrix(rnorm(30), 5, 6)
  darr <- delarr_mem(mat)
  result <- collect_shard(darr |> d_reduce(sum, "rows"), workers = 2)
  expect_equal(result, rowSums(mat))
})

# ---- shard_writer -----------------------------------------------------------

test_that("shard_writer works with collect(into=...)", {
  set.seed(50)
  mat <- matrix(rnorm(20), 4, 5)
  darr <- delarr(mat) |> d_map(~ .x^2)
  w <- shard_writer(4, 5)
  collect(darr, into = w)
  result <- w$result()
  w$close()
  expect_equal(result, mat^2)
})

test_that("shard_writer receives chunked output correctly", {
  set.seed(51)
  mat <- matrix(rnorm(35), 5, 7)
  darr <- delarr(mat) |> d_map(~ .x + 10)
  w <- shard_writer(5, 7)
  collect(darr, into = w, chunk_size = 2L)
  result <- w$result()
  w$close()
  expect_equal(result, mat + 10)
})

# ---- Larger matrices / stress -----------------------------------------------

test_that("collect_shard with wider matrix and small chunks", {
  set.seed(60)
  mat <- matrix(rnorm(500), 10, 50)
  darr <- delarr_shard(mat)
  result <- collect_shard(darr |> d_map(~ .x^2), workers = 2, chunk_size = 7L)
  expect_equal(result, mat^2)
})

test_that("col-reduction on wide matrix matches base R", {
  set.seed(61)
  mat <- matrix(rnorm(500), 10, 50)
  darr <- delarr_shard(mat)
  result <- collect_shard(darr |> d_reduce(mean, "cols"), workers = 2, chunk_size = 11L)
  expect_equal(result, colMeans(mat))
})

test_that("row-reduction on wide matrix matches base R", {
  set.seed(62)
  mat <- matrix(rnorm(500), 10, 50)
  darr <- delarr_shard(mat)
  result <- collect_shard(darr |> d_reduce(sum, "rows"), workers = 2, chunk_size = 11L)
  expect_equal(result, rowSums(mat))
})
