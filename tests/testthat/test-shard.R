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

test_that("delarr_shard round-trip for 3D arrays", {
  arr <- array(as.double(1:60), dim = c(3, 4, 5))
  darr <- delarr_shard(arr)
  expect_s3_class(darr, "delarr")
  expect_equal(dim(darr), dim(arr))
  expect_equal(collect_shard(darr, workers = 2, chunk_size = 2L), arr)
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
  pipeline <- darr |> d_map(function(x) x^2) |> d_center("cols")
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

test_that("collect_shard preserves logical output type for comparisons", {
  set.seed(3)
  mat <- matrix(rnorm(24), 4, 6)
  darr <- delarr_shard(mat)
  result <- collect_shard(darr > 0, workers = 2)
  expect_type(result, "logical")
  expect_identical(result, mat > 0)
})

test_that("collect_shard preserves integer storage for integer matrices", {
  mat <- matrix(1L:12L, 3, 4)
  darr <- delarr_shard(mat)
  result <- collect_shard(darr, workers = 2)
  expect_type(result, "integer")
  expect_identical(result, mat)
})

test_that("collect_shard with d_where", {
  set.seed(4)
  mat <- matrix(rnorm(20), 4, 5)
  darr <- delarr_shard(mat)
  pipeline <- darr |> d_where(function(x) x > 0, fill = 0)
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
  result <- collect_shard(darr |> d_map(function(x) x + 1), workers = 2, chunk_size = 3L)
  expect_equal(result, mat + 1)
})

test_that("collect_shard supports N-d elementwise pipelines", {
  arr <- array(as.double(1:60), dim = c(3, 4, 5))
  darr <- delarr_shard(arr)
  pipeline <- darr |> d_map(function(x) x + 1) |> d_where(function(x) x %% 2 == 0, fill = -1)
  expect_equal(
    collect_shard(pipeline, workers = 2, chunk_size = 2L),
    collect(pipeline, chunk_size = 2L, chunk_margin = 3L)
  )
})

test_that("collect_shard supports N-d reductions", {
  arr <- array(as.double(1:120), dim = c(3, 4, 5, 2))
  darr <- delarr_shard(arr)
  pipeline <- darr |> d_reduce(mean, axis = c(2L, 4L))
  expect_equal(
    collect_shard(pipeline, workers = 2, chunk_size = 1L),
    collect(pipeline, chunk_size = 1L, chunk_margin = 4L)
  )
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
  pipeline <- darr |> d_map(function(x) x^2) |> d_reduce(sum, "rows")
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
  result <- collect_shard(darr |> d_map(function(x) x * 3), workers = 2)
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
  darr <- delarr(mat) |> d_map(function(x) x^2)
  w <- shard_writer(4, 5)
  collect(darr, into = w)
  result <- w$result()
  w$close()
  expect_equal(result, mat^2)
})

test_that("shard_writer receives chunked output correctly", {
  set.seed(51)
  mat <- matrix(rnorm(35), 5, 7)
  darr <- delarr(mat) |> d_map(function(x) x + 10)
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
  result <- collect_shard(darr |> d_map(function(x) x^2), workers = 2, chunk_size = 7L)
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

# ---- Edge cases and degenerate inputs ---------------------------------------

test_that("single-column matrix round-trip", {
  mat <- matrix(1:5, 5, 1)
  darr <- delarr_shard(mat)
  expect_equal(collect_shard(darr, workers = 2), mat)
})

test_that("single-row matrix round-trip", {
  mat <- matrix(as.double(1:8), 1, 8)
  darr <- delarr_shard(mat)
  expect_equal(collect_shard(darr, workers = 2), mat)
})

test_that("1x1 matrix round-trip", {
  mat <- matrix(42.0, 1, 1)
  darr <- delarr_shard(mat)
  expect_equal(collect_shard(darr, workers = 2), mat)
})

test_that("more workers than columns still works", {
  mat <- matrix(rnorm(6), 3, 2)
  darr <- delarr_shard(mat)
  result <- collect_shard(darr, workers = 8)
  expect_equal(result, mat)
})

test_that("integer matrix is accepted and round-trips", {
  mat <- matrix(1:20, 4, 5)
  darr <- delarr_shard(mat)
  result <- collect_shard(darr, workers = 2)
  expect_equal(result, mat)
})

# ---- Column-wise center/scale/zscore (should parallelize, not fallback) -----

test_that("col-center via collect_shard matches sequential", {
  set.seed(70)
  mat <- matrix(rnorm(40), 5, 8)
  darr <- delarr_shard(mat)
  pipeline <- darr |> d_center("cols")
  par_result <- collect_shard(pipeline, workers = 2)
  seq_result <- collect(pipeline)
  expect_equal(par_result, seq_result)
  expect_true(all(abs(colMeans(par_result)) < 1e-10))
})

test_that("col-scale via collect_shard matches sequential", {
  set.seed(71)
  mat <- matrix(rnorm(40), 5, 8)
  darr <- delarr_shard(mat)
  pipeline <- darr |> d_scale("cols")
  par_result <- collect_shard(pipeline, workers = 2)
  seq_result <- collect(pipeline)
  expect_equal(par_result, seq_result)
})

test_that("col-zscore via collect_shard matches sequential", {
  set.seed(72)
  mat <- matrix(rnorm(40), 5, 8)
  darr <- delarr_shard(mat)
  pipeline <- darr |> d_zscore("cols")
  par_result <- collect_shard(pipeline, workers = 2)
  seq_result <- collect(pipeline)
  expect_equal(par_result, seq_result)
})

# ---- Chunk-size invariance --------------------------------------------------

test_that("different chunk sizes produce identical results (elementwise)", {
  set.seed(80)
  mat <- matrix(rnorm(60), 6, 10)
  darr <- delarr_shard(mat)
  pipeline <- darr |> d_map(function(x) log1p(abs(x)))
  r3 <- collect_shard(pipeline, workers = 2, chunk_size = 3L)
  r7 <- collect_shard(pipeline, workers = 2, chunk_size = 7L)
  rall <- collect_shard(pipeline, workers = 2, chunk_size = 10L)
  expect_equal(r3, r7)
  expect_equal(r7, rall)
})

test_that("different chunk sizes produce identical results (row-reduction)", {
  set.seed(81)
  mat <- matrix(rnorm(60), 6, 10)
  darr <- delarr_shard(mat)
  pipeline <- darr |> d_reduce(sum, "rows")
  r5 <- collect_shard(pipeline, workers = 2, chunk_size = 5L)
  r10 <- collect_shard(pipeline, workers = 2, chunk_size = 10L)
  expect_equal(r5, r10)
  expect_equal(r10, rowSums(mat))
})

test_that("different chunk sizes produce identical results (col-reduction)", {
  set.seed(82)
  mat <- matrix(rnorm(60), 6, 10)
  darr <- delarr_shard(mat)
  pipeline <- darr |> d_reduce(mean, "cols")
  r5 <- collect_shard(pipeline, workers = 2, chunk_size = 5L)
  r10 <- collect_shard(pipeline, workers = 2, chunk_size = 10L)
  expect_equal(r5, r10)
  expect_equal(r10, colMeans(mat))
})

# ---- Chained operations ----------------------------------------------------

test_that("multiple chained d_map ops", {
  set.seed(90)
  mat <- matrix(rnorm(30), 5, 6)
  darr <- delarr_shard(mat)
  pipeline <- darr |>
    d_map(function(x) x^2) |>
    d_map(function(x) sqrt(x)) |>
    d_map(function(x) x + 1)
  result <- collect_shard(pipeline, workers = 2)
  expect_equal(result, abs(mat) + 1)
})

test_that("ops + col-center + more ops", {
  set.seed(91)
  mat <- matrix(rnorm(40), 5, 8)
  darr <- delarr_shard(mat)
  pipeline <- darr |> d_map(function(x) x * 2) |> d_center("cols") |> d_map(function(x) x^2)
  par_result <- collect_shard(pipeline, workers = 2)
  seq_result <- collect(pipeline)
  expect_equal(par_result, seq_result)
})

# ---- Slicing + ops + reduction combos --------------------------------------

test_that("slice then reduce (row)", {
  set.seed(100)
  mat <- matrix(rnorm(40), 5, 8)
  darr <- delarr_shard(mat)
  pipeline <- darr[2:4, 1:6] |> d_reduce(sum, "rows")
  result <- collect_shard(pipeline, workers = 2)
  expect_equal(result, rowSums(mat[2:4, 1:6]))
})

test_that("slice then reduce (col)", {
  set.seed(101)
  mat <- matrix(rnorm(40), 5, 8)
  darr <- delarr_shard(mat)
  pipeline <- darr[, 2:7] |> d_reduce(mean, "cols")
  result <- collect_shard(pipeline, workers = 2)
  expect_equal(result, colMeans(mat[, 2:7]))
})

test_that("slice + ops + reduce", {
  set.seed(102)
  mat <- matrix(rnorm(40), 5, 8)
  darr <- delarr_shard(mat)
  pipeline <- darr[1:3, ] |> d_map(function(x) x^2) |> d_reduce(max, "rows")
  result <- collect_shard(pipeline, workers = 2)
  expect_equal(result, apply(mat[1:3, ]^2, 1, max))
})

# ---- Scalar and vector broadcast --------------------------------------------

test_that("scalar addition via Ops dispatches correctly", {
  set.seed(110)
  mat <- matrix(rnorm(20), 4, 5)
  darr <- delarr_shard(mat)
  expect_equal(collect_shard(darr + 10, workers = 2), mat + 10)
  expect_equal(collect_shard(3 * darr, workers = 2), 3 * mat)
  expect_equal(collect_shard(darr / 2, workers = 2), mat / 2)
})

test_that("comparison ops produce correct values via collect_shard", {
  set.seed(111)
  mat <- matrix(rnorm(20), 4, 5)
  darr <- delarr_shard(mat)
  result <- collect_shard(darr > 0, workers = 2)
  expect_type(result, "logical")
  expect_identical(result, mat > 0)
})

# ---- All-NA edge cases ------------------------------------------------------

test_that("row-reduction on all-NA matrix", {
  mat <- matrix(NA_real_, 3, 4)
  darr <- delarr_shard(mat)
  result <- collect_shard(darr |> d_reduce(sum, "rows", na.rm = TRUE), workers = 2)
  expect_true(all(is.na(result)))
})

test_that("col-reduction on all-NA matrix", {
  mat <- matrix(NA_real_, 3, 4)
  darr <- delarr_shard(mat)
  result <- collect_shard(darr |> d_reduce(mean, "cols", na.rm = TRUE), workers = 2)
  expect_true(all(is.na(result)))
})

test_that("min row-reduction with all-NA rows returns NA", {
  mat <- matrix(c(1, NA, NA, 2, NA, NA, 3, NA, NA), 3, 3)
  darr <- delarr_shard(mat)
  result <- collect_shard(darr |> d_reduce(min, "rows", na.rm = TRUE), workers = 2)
  seq_result <- collect(darr |> d_reduce(min, "rows", na.rm = TRUE))
  expect_equal(result, seq_result)
  # Row 2 and 3 are all-NA
  expect_true(is.na(result[2]))
  expect_true(is.na(result[3]))
  expect_equal(result[1], 1)
})

# ---- Systematic parallel vs sequential agreement ---------------------------

test_that("collect_shard agrees with collect across pipeline shapes", {
  set.seed(120)
  mat <- matrix(rnorm(60), 6, 10)
  darr <- delarr_shard(mat)

  pipelines <- list(
    identity = darr,
    squared = darr |> d_map(function(x) x^2),
    times_two = darr * 2,
    col_centered = darr |> d_center("cols"),
    col_zscored = darr |> d_zscore("cols"),
    sliced = darr[2:5, 3:8],
    chained = darr |> d_map(function(x) abs(x)) |> d_map(function(x) log1p(x))
  )

  for (nm in names(pipelines)) {
    p <- pipelines[[nm]]
    par_r <- collect_shard(p, workers = 2, chunk_size = 3L)
    seq_r <- collect(p)
    expect_equal(par_r, seq_r, info = paste("pipeline:", nm))
  }
})

test_that("reduction agreement across pipeline shapes", {
  set.seed(121)
  mat <- matrix(rnorm(60), 6, 10)
  darr <- delarr_shard(mat)

  configs <- list(
    list(fn = sum,  dim = "rows"),
    list(fn = mean, dim = "rows"),
    list(fn = min,  dim = "rows"),
    list(fn = max,  dim = "rows"),
    list(fn = sum,  dim = "cols"),
    list(fn = mean, dim = "cols"),
    list(fn = min,  dim = "cols"),
    list(fn = max,  dim = "cols")
  )

  for (cfg in configs) {
    pipeline <- d_reduce(darr, cfg$fn, cfg$dim)
    par_r <- collect_shard(pipeline, workers = 2, chunk_size = 3L)
    seq_r <- collect(pipeline)
    label <- paste(deparse(cfg$fn)[[1]], cfg$dim)
    expect_equal(par_r, seq_r, info = label)
  }
})

test_that("check_shard_available errors when shard is missing", {
  local_mocked_bindings(
    requireNamespace = function(pkg, quietly = TRUE) {
      if (identical(pkg, "shard")) return(FALSE)
      base::requireNamespace(pkg, quietly = quietly)
    },
    .package = "base"
  )
  expect_error(delarr:::check_shard_available(), "The 'shard' package is required")
})

test_that("shard_writer accepts column-oriented writes without positions", {
  w <- shard_writer(4, 5)
  block <- matrix(as.double(1:12), 4, 3)
  w$write(block, cols = 1:3)
  expect_equal(w$result()[, 1:3], block)
  w$close()
})

test_that("shard_writer fills leading columns when only a block is supplied", {
  w <- shard_writer(4, 5)
  block <- matrix(as.double(1:8), 4, 2)
  w$write(block)
  expect_equal(w$result()[, 1:2], block)
  w$close()
})

test_that("collect_shard col min/max with na.rm merges partial results", {
  mat <- matrix(
    c(NA, NA, NA, NA,
      1, NA, 7, 8,
      5, 6, NA, 12),
    nrow = 4,
    ncol = 3
  )
  darr <- delarr_shard(mat)
  expect_equal(
    collect_shard(darr |> d_reduce(min, "cols", na.rm = TRUE), workers = 2, chunk_size = 1L),
    collect(darr |> d_reduce(min, "cols", na.rm = TRUE), chunk_size = 1L)
  )
  expect_equal(
    collect_shard(darr |> d_reduce(max, "cols", na.rm = TRUE), workers = 2, chunk_size = 1L),
    collect(darr |> d_reduce(max, "cols", na.rm = TRUE), chunk_size = 1L)
  )
})

test_that("shard_buffer_type maps storage modes", {
  expect_equal(delarr:::shard_buffer_type(logical()), "logical")
  expect_equal(delarr:::shard_buffer_type(1:3), "integer")
  expect_equal(delarr:::shard_buffer_type(as.double(1:3)), "double")
})

test_that("collect_shard preserves logical output buffers", {
  mat <- matrix(as.double(1:20), 4, 5)
  darr <- delarr_shard(mat)
  result <- collect_shard(darr > 0, workers = 2)
  expect_type(result, "logical")
  expect_identical(result, mat > 0)
})

test_that("collect_shard materializes non-shard seeds with begin/end hooks", {
  mat <- matrix(as.double(1:20), 4, 5)
  tracker <- new.env(parent = emptyenv())
  tracker$began <- 0L
  tracker$ended <- 0L
  seed <- delarr_backend(
    nrow = 4,
    ncol = 5,
    pull = function(rows = NULL, cols = NULL) {
      rows <- rows %||% seq_len(4)
      cols <- cols %||% seq_len(5)
      mat[rows, cols, drop = FALSE]
    },
    begin = function() tracker$began <- tracker$began + 1L,
    end = function() tracker$ended <- tracker$ended + 1L
  )
  result <- collect_shard(delarr(seed) |> d_map(~ .x + 1), workers = 2)
  expect_equal(result, mat + 1)
  expect_gt(tracker$began, 0L)
  expect_gt(tracker$ended, 0L)
})
