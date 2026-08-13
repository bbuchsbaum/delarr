test_that("d_transpose matches base transpose", {
  set.seed(21)
  mat <- matrix(rnorm(24), 4, 6)
  x <- delarr(mat)
  expect_equal(collect(d_transpose(x), chunk_size = 2L), t(mat))
  expect_equal(collect(t(x), chunk_size = 3L), t(mat))

  sliced <- x[-1, ]
  expect_equal(collect(d_transpose(sliced), chunk_size = 2L), t(mat[-1, , drop = FALSE]))
})

test_that("d_matmul matches base matrix multiplication", {
  set.seed(22)
  a <- matrix(rnorm(20), 4, 5)
  b <- matrix(rnorm(15), 5, 3)
  rownames(a) <- paste0("r", seq_len(nrow(a)))
  colnames(b) <- paste0("c", seq_len(ncol(b)))
  x <- delarr(a)
  y <- delarr(b)
  out <- collect(d_matmul(x, y), chunk_size = 2L)
  expect_equal(out, a %*% b)
  expect_equal(dimnames(out), list(rownames(a), colnames(b)))
  expect_equal(collect(d_matmul(x, b), chunk_size = 2L), a %*% b)

  sliced <- x[-1, ]
  expect_equal(collect(d_matmul(sliced, y), chunk_size = 2L), a[-1, , drop = FALSE] %*% b)
})

test_that("d_matmul reuses lhs panels across output column chunks", {
  set.seed(220)
  a <- matrix(rnorm(24), 4, 6)
  b <- matrix(rnorm(30), 6, 5)
  lhs_tracker <- new.env(parent = emptyenv())
  rhs_tracker <- new.env(parent = emptyenv())
  lhs_tracker$pulls <- 0L
  rhs_tracker$pulls <- 0L

  x <- delarr(delarr_seed(
    nrow = nrow(a),
    ncol = ncol(a),
    pull = function(rows = NULL, cols = NULL) {
      lhs_tracker$pulls <- lhs_tracker$pulls + 1L
      rows <- rows %||% seq_len(nrow(a))
      cols <- cols %||% seq_len(ncol(a))
      a[rows, cols, drop = FALSE]
    }
  ))
  y <- delarr(delarr_seed(
    nrow = nrow(b),
    ncol = ncol(b),
    pull = function(rows = NULL, cols = NULL) {
      rhs_tracker$pulls <- rhs_tracker$pulls + 1L
      rows <- rows %||% seq_len(nrow(b))
      cols <- cols %||% seq_len(ncol(b))
      b[rows, cols, drop = FALSE]
    }
  ))

  inner_chunk <- 2L
  outer_chunk <- 2L
  out <- collect(d_matmul(x, y, chunk_size = inner_chunk), chunk_size = outer_chunk)

  expect_equal(out, a %*% b)
  expect_equal(lhs_tracker$pulls, ceiling(ncol(a) / inner_chunk))
  expect_equal(
    rhs_tracker$pulls,
    ceiling(ncol(a) / inner_chunk) * ceiling(ncol(b) / outer_chunk)
  )
})

test_that("d_matmul reuses rhs panels across output row chunks", {
  set.seed(221)
  a <- matrix(rnorm(24), 4, 6)
  b <- matrix(rnorm(30), 6, 5)
  lhs_tracker <- new.env(parent = emptyenv())
  rhs_tracker <- new.env(parent = emptyenv())
  lhs_tracker$pulls <- 0L
  rhs_tracker$pulls <- 0L

  x <- delarr(delarr_seed(
    nrow = nrow(a),
    ncol = ncol(a),
    pull = function(rows = NULL, cols = NULL) {
      lhs_tracker$pulls <- lhs_tracker$pulls + 1L
      rows <- rows %||% seq_len(nrow(a))
      cols <- cols %||% seq_len(ncol(a))
      a[rows, cols, drop = FALSE]
    }
  ))
  y <- delarr(delarr_seed(
    nrow = nrow(b),
    ncol = ncol(b),
    pull = function(rows = NULL, cols = NULL) {
      rhs_tracker$pulls <- rhs_tracker$pulls + 1L
      rows <- rows %||% seq_len(nrow(b))
      cols <- cols %||% seq_len(ncol(b))
      b[rows, cols, drop = FALSE]
    }
  ))

  inner_chunk <- 2L
  outer_chunk <- 2L
  out <- collect(
    d_matmul(x, y, chunk_size = inner_chunk),
    chunk_margin = "rows",
    chunk_size = outer_chunk
  )

  expect_equal(out, a %*% b)
  expect_equal(lhs_tracker$pulls, ceiling(nrow(a) / outer_chunk) * ceiling(ncol(a) / inner_chunk))
  expect_equal(rhs_tracker$pulls, ceiling(ncol(a) / inner_chunk))
})

test_that("d_matmul blocks the contracted dimension instead of pulling full panels", {
  set.seed(222)
  a <- matrix(rnorm(35), 5, 7)
  b <- matrix(rnorm(42), 7, 6)
  lhs_tracker <- new.env(parent = emptyenv())
  rhs_tracker <- new.env(parent = emptyenv())
  lhs_tracker$max_cols <- 0L
  rhs_tracker$max_rows <- 0L

  x <- delarr(delarr_seed(
    nrow = nrow(a),
    ncol = ncol(a),
    pull = function(rows = NULL, cols = NULL) {
      rows <- rows %||% seq_len(nrow(a))
      cols <- cols %||% seq_len(ncol(a))
      lhs_tracker$max_cols <- max(lhs_tracker$max_cols, length(cols))
      a[rows, cols, drop = FALSE]
    }
  ))
  y <- delarr(delarr_seed(
    nrow = nrow(b),
    ncol = ncol(b),
    pull = function(rows = NULL, cols = NULL) {
      rows <- rows %||% seq_len(nrow(b))
      cols <- cols %||% seq_len(ncol(b))
      rhs_tracker$max_rows <- max(rhs_tracker$max_rows, length(rows))
      b[rows, cols, drop = FALSE]
    }
  ))

  inner_chunk <- 2L
  out <- collect(d_matmul(x, y, chunk_size = inner_chunk), chunk_size = 3L)

  expect_equal(out, a %*% b)
  expect_lte(lhs_tracker$max_cols, inner_chunk)
  expect_lte(rhs_tracker$max_rows, inner_chunk)
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

test_that("d_reduce_many streams built-in reducers in one pass", {
  set.seed(23)
  mat <- matrix(rnorm(35), 5, 7)
  tracker <- new.env(parent = emptyenv())
  tracker$pulls <- 0L
  seed <- delarr_seed(
    nrow = nrow(mat),
    ncol = ncol(mat),
    pull = function(rows = NULL, cols = NULL) {
      tracker$pulls <- tracker$pulls + 1L
      rows <- rows %||% seq_len(nrow(mat))
      cols <- cols %||% seq_len(ncol(mat))
      mat[rows, cols, drop = FALSE]
    }
  )
  x <- delarr(seed)
  chunk <- 3L
  out <- d_reduce_many(
    x,
    fns = list(sum = sum, mean = mean, max = max),
    dim = "rows",
    na.rm = FALSE,
    chunk_size = chunk,
    simplify = FALSE
  )
  expect_equal(out$sum, rowSums(mat))
  expect_equal(out$mean, rowMeans(mat))
  expect_equal(out$max, apply(mat, 1L, max))
  expect_equal(tracker$pulls, ceiling(ncol(mat) / chunk))
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

test_that("optimize_delarr removes subtract-zero and divide-by-one no-ops", {
  mat <- matrix(as.double(1:12), 3, 4)
  x <- delarr(mat) |>
    (\(z) z - 0)() |>
    (\(z) z / 1)()
  x_opt <- optimize_delarr(x)
  expect_lt(length(x_opt$ops), length(x$ops))
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

  sliced <- explain(delarr(mat)[-1, ], chunk_size = 2L)
  expect_equal(sliced$output_dim, c(3, 5))
  expect_equal(sliced$selected_rows, 3L)
})

test_that("collect and explain accept numeric chunk axes for 2D inputs", {
  mat <- matrix(rnorm(24), 4, 6)
  x <- delarr(mat) |> d_map(~ .x + 1)

  expect_equal(
    collect(x, chunk_margin = 1L, chunk_size = 2L),
    collect(x, chunk_margin = "rows", chunk_size = 2L)
  )
  expect_equal(
    collect(x, chunk_margin = 2L, chunk_size = 3L),
    collect(x, chunk_margin = "cols", chunk_size = 3L)
  )

  info_rows <- explain(x, chunk_margin = 1L, chunk_size = 2L)
  info_cols <- explain(x, chunk_margin = 2L, chunk_size = 3L)
  expect_equal(info_rows$chunk_margin, "rows")
  expect_equal(info_cols$chunk_margin, "cols")
})

test_that("profile_collect runs repeated timings", {
  mat <- matrix(1:20, 4, 5)
  x <- delarr(mat) |> d_map(~ .x + 1)
  prof <- profile_collect(x, reps = 2L, chunk_size = 2L)
  expect_s3_class(prof, "delarr_profile")
  expect_equal(length(prof$elapsed), 2L)
  expect_true(all(prof$elapsed >= 0))
})
