skip_if_not_installed("shard")

test_that("delarr_shard round-trip", {
  mat <- matrix(rnorm(20), 4, 5)
  darr <- delarr_shard(mat)
  expect_s3_class(darr, "delarr")
  expect_equal(dim(darr), dim(mat))
  out <- collect_shard(darr, workers = 2)
  expect_equal(out, mat)
})

test_that("collect_shard with ops pipeline matches sequential", {
  mat <- matrix(rnorm(40), 5, 8)
  darr <- delarr_shard(mat)
  pipeline <- darr |> d_map(~ .x^2) |> d_center("cols")
  par_result <- collect_shard(pipeline, workers = 2)
  seq_result <- collect(pipeline)
  expect_equal(par_result, seq_result)
})

test_that("row-reduction sum via collect_shard", {
  mat <- matrix(rnorm(30), 5, 6)
  darr <- delarr_shard(mat)
  result <- collect_shard(darr |> d_reduce(sum, "rows"), workers = 2)
  expect_equal(result, rowSums(mat))
})

test_that("row-reduction mean via collect_shard", {
  mat <- matrix(rnorm(30), 5, 6)
  darr <- delarr_shard(mat)
  result <- collect_shard(darr |> d_reduce(mean, "rows"), workers = 2)
  expect_equal(result, rowMeans(mat))
})

test_that("col-reduction sum via collect_shard", {
  mat <- matrix(rnorm(30), 5, 6)
  darr <- delarr_shard(mat)
  result <- collect_shard(darr |> d_reduce(sum, "cols"), workers = 2)
  expect_equal(result, colSums(mat))
})

test_that("col-reduction mean via collect_shard", {
  mat <- matrix(rnorm(30), 5, 6)
  darr <- delarr_shard(mat)
  result <- collect_shard(darr |> d_reduce(mean, "cols"), workers = 2)
  expect_equal(result, colMeans(mat))
})

test_that("row-reduction min/max via collect_shard", {
  set.seed(42)
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

test_that("col-reduction min/max via collect_shard", {
  set.seed(43)
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

test_that("NA handling in row-reduction with na_rm", {
  mat <- matrix(c(1, NA, 3, 4, NA, 6, 7, 8, NA, NA, NA, NA), 4, 3)
  darr <- delarr_shard(mat)
  result <- collect_shard(darr |> d_reduce(sum, "rows", na.rm = TRUE), workers = 2)
  expected <- rowSums(mat, na.rm = TRUE)
  # Row 4 is all-NA: should be NA not 0
  expected[apply(mat, 1, function(r) all(is.na(r)))] <- NA_real_
  expect_equal(result, expected)
})

test_that("fallback for row-center pipeline (requires_full_eval)", {
  mat <- matrix(rnorm(20), 4, 5)
  darr <- delarr_shard(mat)
  pipeline <- darr |> d_center("rows")
  # Should gracefully fall back to sequential collect
  result <- collect_shard(pipeline, workers = 2)
  expect_equal(result, collect(pipeline))
})

test_that("non-shard seed works with collect_shard (materializes then shares)", {
  mat <- matrix(rnorm(20), 4, 5)
  darr <- delarr_mem(mat)
  result <- collect_shard(darr |> d_map(~ .x * 3), workers = 2)
  expect_equal(result, mat * 3)
})

test_that("shard_writer works with collect(into=...)", {
  mat <- matrix(rnorm(20), 4, 5)
  darr <- delarr(mat) |> d_map(~ .x^2)
  w <- shard_writer(4, 5)
  collect(darr, into = w)
  result <- w$result()
  w$close()
  expect_equal(result, mat^2)
})

test_that("collect_shard with slicing", {
  mat <- matrix(rnorm(40), 5, 8)
  darr <- delarr_shard(mat)
  pipeline <- darr[2:4, 3:6]
  result <- collect_shard(pipeline, workers = 2)
  expect_equal(result, mat[2:4, 3:6])
})

test_that("collect_shard with ops + reduction", {
  mat <- matrix(rnorm(40), 5, 8)
  darr <- delarr_shard(mat)
  pipeline <- darr |> d_map(~ .x^2) |> d_reduce(sum, "rows")
  result <- collect_shard(pipeline, workers = 2)
  expect_equal(result, rowSums(mat^2))
})

test_that("collect_shard falls back for generic reduction", {
  mat <- matrix(rnorm(20), 4, 5)
  darr <- delarr_shard(mat)
  my_fn <- function(x) sum(x^2)
  pipeline <- darr |> d_reduce(my_fn, "rows")
  result <- collect_shard(pipeline, workers = 2)
  expect_equal(result, apply(mat, 1, my_fn))
})

test_that("delarr_shard rejects non-numeric input", {
  expect_error(delarr_shard("abc"), "numeric matrix")
  expect_error(delarr_shard(1:10), "numeric matrix")
})
