test_that("d_transpose matches base transpose", {
  set.seed(21)
  mat <- matrix(rnorm(24), 4, 6)
  x <- delarr(mat)
  expect_equal(collect(d_transpose(x), chunk_size = 2L), t(mat))
  expect_equal(collect(t(x), chunk_size = 3L), t(mat))
})

test_that("d_matmul matches base matrix multiplication", {
  set.seed(22)
  a <- matrix(rnorm(20), 4, 5)
  b <- matrix(rnorm(15), 5, 3)
  x <- delarr(a)
  y <- delarr(b)
  expect_equal(collect(d_matmul(x, y), chunk_size = 2L), a %*% b)
  expect_equal(collect(d_matmul(x, b), chunk_size = 2L), a %*% b)
})

test_that("d_reduce_many returns named matrix summary", {
  set.seed(23)
  mat <- matrix(rnorm(30), 5, 6)
  x <- delarr(mat)
  out <- d_reduce_many(
    x,
    fns = list(sum = sum, mean = mean, max = max),
    dim = "rows",
    na.rm = FALSE
  )
  expect_true(is.matrix(out))
  expect_equal(colnames(out), c("sum", "mean", "max"))
  expect_equal(out[, "sum"], rowSums(mat))
  expect_equal(out[, "mean"], rowMeans(mat))
  expect_equal(out[, "max"], apply(mat, 1L, max))
})

test_that("optimize_delarr removes no-op constants", {
  set.seed(24)
  mat <- matrix(rnorm(12), 3, 4)
  x <- delarr(mat) |> d_map(~ .x + 1) |> (\(z) z + 0)() |> (\(z) z * 1)()
  before <- length(x$ops)
  x_opt <- optimize_delarr(x)
  after <- length(x_opt$ops)
  expect_lt(after, before)
  expect_equal(collect(x_opt), collect(x))
})

test_that("collect supports row chunking", {
  set.seed(25)
  mat <- matrix(rnorm(48), 6, 8)
  x <- delarr(mat) |> d_map(~ .x^2 + 1)
  expect_equal(
    collect(x, chunk_margin = "rows", chunk_size = 2L),
    collect(x, chunk_margin = "cols", chunk_size = 3L)
  )
})

test_that("collect supports adaptive chunk sizing", {
  set.seed(26)
  mat <- matrix(rnorm(60), 6, 10)
  x <- delarr(mat)
  out <- collect(x, target_bytes = 128)
  expect_equal(out, mat)
})

test_that("collect parallel matches sequential for simple pipelines", {
  skip_on_os("windows")
  set.seed(27)
  mat <- matrix(rnorm(70), 7, 10)
  x <- delarr(mat) |> d_map(~ .x * 3 + 2)
  seq_out <- collect(x, chunk_size = 2L, parallel = FALSE)
  par_out <- collect(x, chunk_size = 2L, parallel = TRUE, workers = 2L)
  expect_equal(par_out, seq_out)
})

test_that("explain returns chunk plan metadata", {
  mat <- matrix(1:20, 4, 5)
  x <- delarr(mat) |> d_map(~ .x + 1)
  info <- explain(x, chunk_size = 2L)
  expect_s3_class(info, "delarr_explain")
  expect_equal(info$chunk_size, 2L)
  expect_equal(info$output_dim, c(4, 5))
  expect_true(info$chunk_count >= 2L)
})

test_that("profile_collect runs repeated timings", {
  mat <- matrix(1:20, 4, 5)
  x <- delarr(mat) |> d_map(~ .x + 1)
  prof <- profile_collect(x, reps = 2L, chunk_size = 2L)
  expect_s3_class(prof, "delarr_profile")
  expect_equal(length(prof$elapsed), 2L)
  expect_true(all(prof$elapsed >= 0))
})
