test_that("delarr constructs from matrix", {
  mat <- matrix(1:9, 3, 3)
  x <- delarr(mat)
  expect_s3_class(x, "delarr")
  expect_equal(dim(x), c(3, 3))
  expect_equal(collect(x), mat)
})

test_that("slicing is deferred and collected correctly", {
  mat <- matrix(1:16, 4, 4)
  x <- delarr(mat)
  y <- x[2:3, 1:2]
  expect_equal(dim(y), c(2, 2))
  expect_equal(collect(y), mat[2:3, 1:2])
})

test_that("map and arithmetic ops fuse", {
  mat <- matrix(runif(9), 3, 3)
  arr <- delarr(mat)
  y <- d_map(arr, ~ .x * 2)
  y <- y + 1
  y <- d_where(y, ~ .x > 0)
  expect_equal(collect(y), ifelse((mat * 2 + 1) > 0, mat * 2 + 1, 0))
})

test_that("binary ops stream without materialising RHS", {
  set.seed(5)
  lhs <- matrix(rnorm(25), 5, 5)
  rhs <- matrix(rnorm(25), 5, 5)
  x <- delarr(lhs)
  y <- delarr(rhs)
  expect_equal(collect(x + y, chunk_size = 2L), lhs + rhs)
  expect_identical(collect(x > 0), lhs > 0)
})

test_that("emap2 with delarr RHS pulls matching chunks", {
  set.seed(11)
  lhs <- matrix(rnorm(20), 4, 5)
  rhs <- matrix(rnorm(20), 4, 5)
  tracker <- new.env(parent = emptyenv())
  tracker$pulls <- 0L
  seed_rhs <- delarr_seed(
    nrow = nrow(rhs),
    ncol = ncol(rhs),
    pull = function(rows = NULL, cols = NULL) {
      tracker$pulls <- tracker$pulls + 1L
      rows <- rows %||% seq_len(nrow(rhs))
      cols <- cols %||% seq_len(ncol(rhs))
      rhs[rows, cols, drop = FALSE]
    }
  )
  x <- delarr(lhs)
  y <- delarr(seed_rhs)
  chunk <- 2L
  expect_equal(collect(x + y, chunk_size = chunk), lhs + rhs)
  expect_equal(tracker$pulls, ceiling(ncol(lhs) / chunk))
})

test_that("broadcasting in binary ops works for vectors", {
  mat <- matrix(1:12, 3, 4)
  x <- delarr(mat)
  col_vec <- 1:4
  row_vec <- 1:3
  expect_equal(collect(x + col_vec), sweep(mat, 2L, col_vec, "+"))
  expect_equal(collect(row_vec + x), sweep(mat, 1L, row_vec, "+"))
})

test_that("comparisons return logical matrices", {
  set.seed(3)
  mat <- matrix(rnorm(15), 5, 3)
  x <- delarr(mat)
  out <- collect(x > 0)
  expect_type(out, "logical")
  expect_identical(out, mat > 0)
})

test_that("reduce returns vector", {
  mat <- matrix(1:12, 3, 4)
  x <- delarr(mat)
  r <- d_reduce(x, base::sum, dim = "rows")
  expect_equal(collect(r), rowSums(mat))
})

test_that("reduce dims update lazily", {
  mat <- matrix(1:12, 3, 4)
  x <- delarr(mat)
  expect_equal(dim(d_reduce(x, sum, dim = "rows")), c(1L, ncol(mat)))
  expect_equal(dim(d_reduce(x, sum, dim = "cols")), c(nrow(mat), 1L))
})

test_that("reductions support na.rm", {
  mat <- matrix(c(1, NA, 3, 4, 5, NA), 3, 2)
  x <- delarr(mat)
  expect_equal(collect(d_reduce(x, sum, dim = "rows", na.rm = TRUE)), rowSums(mat, na.rm = TRUE))
  expect_equal(collect(d_reduce(x, mean, dim = "cols", na.rm = TRUE)), colMeans(mat, na.rm = TRUE))
})

test_that("center and scale operate along requested dimension", {
  set.seed(1)
  mat <- matrix(rnorm(20), 5, 4)
  x <- delarr(mat)
  centered <- collect(d_center(x, dim = "rows"))
  expect_true(all(abs(rowMeans(centered)) < 1e-8))

  z <- collect(d_zscore(x, dim = "cols"))
  expect_true(all(abs(colMeans(z)) < 1e-8))
})

test_that("center and zscore honour na.rm", {
  mat <- matrix(c(1, 2, NA, 4, 5, NA), 3, 2)
  x <- delarr(mat)
  centered <- collect(d_center(x, dim = "cols", na.rm = TRUE))
  expect_equal(colMeans(centered, na.rm = TRUE), rep(0, ncol(centered)))
  z <- collect(d_zscore(x, dim = "rows", na.rm = TRUE))
  z_means <- rowMeans(z, na.rm = TRUE)
  expect_true(all(is.nan(z_means) | abs(z_means) < 1e-8))
})

test_that("d_where can use alternative fill", {
  mat <- matrix(c(-1, 2, -3, 4), 2, 2)
  x <- delarr(mat)
  out <- collect(d_where(x, ~ .x > 0, fill = NA_real_))
  expect_true(all(out[mat > 0] == mat[mat > 0]))
  expect_true(all(is.na(out[mat <= 0])))
})

test_that("block_apply iterates over chunks", {
  mat <- matrix(1:20, 5, 4)
  x <- delarr(mat)
  res <- block_apply(x, margin = "cols", size = 2L, fn = function(chunk) {
    colSums(chunk)
  })
  expect_equal(length(res), 2L)
  expect_equal(res[[1]], colSums(mat[, 1:2]))
  expect_equal(res[[2]], colSums(mat[, 3:4]))
})

test_that("collect streams in column chunks", {
  mat <- matrix(seq_len(35), 5, 7)
  x <- delarr(mat)
  out <- collect(x, chunk_size = 2L)
  expect_equal(out, mat)
})

test_that("chunked reductions over rows and cols agree with base", {
  set.seed(2)
  mat <- matrix(runif(60), 6, 10)
  x <- delarr(mat)
  expect_equal(collect(d_reduce(x, sum, dim = "rows"), chunk_size = 3L), rowSums(mat))
  expect_equal(collect(d_reduce(x, mean, dim = "rows"), chunk_size = 3L), rowMeans(mat))
  expect_equal(collect(d_reduce(x, sum, dim = "cols"), chunk_size = 4L), colSums(mat))
  expect_equal(collect(d_reduce(x, max, dim = "cols"), chunk_size = 4L), apply(mat, 2L, max))
})

test_that("rowMeans2 and colMeans2 respect na.rm", {
  mat <- matrix(c(1, 2, NA, 4), 2, 2)
  x <- delarr(mat)
  expect_equal(rowMeans2(x, na.rm = TRUE), rowMeans(mat, na.rm = TRUE))
  expect_equal(colMeans2(x, na.rm = TRUE), colMeans(mat, na.rm = TRUE))
})

test_that("print summarises pipeline", {
  mat <- matrix(1:6, 3, 2)
  x <- delarr(mat) |> d_center("rows") |> d_reduce(mean, "rows")
  out <- paste(capture.output(print(x)), collapse = "\n")
  expect_match(out, "<delarr> 1 x 2", fixed = TRUE)
  expect_match(out, "center(rows)", fixed = TRUE)
  expect_match(out, "reduce(rows)", fixed = TRUE)
})

test_that("emap2 with delarr RHS is pair-chunked and correct", {
  set.seed(123)
  lhs <- matrix(rnorm(35), 5, 7)
  rhs <- matrix(rnorm(35), 5, 7)
  tracker <- new.env(parent = emptyenv())
  tracker$pulls <- 0L
  seed_rhs <- delarr_seed(
    nrow = nrow(rhs),
    ncol = ncol(rhs),
    pull = function(rows = NULL, cols = NULL) {
      tracker$pulls <- tracker$pulls + 1L
      rows <- rows %||% seq_len(nrow(rhs))
      cols <- cols %||% seq_len(ncol(rhs))
      rhs[rows, cols, drop = FALSE]
    }
  )
  x <- delarr(lhs)
  y <- delarr(seed_rhs)
  chunk <- 3L
  expect_equal(collect(x + y, chunk_size = chunk), lhs + rhs)
  expect_equal(tracker$pulls, ceiling(ncol(lhs) / chunk))
})

test_that("broadcasting works for scalars and vectors", {
  set.seed(4)
  M <- matrix(rnorm(12), 3, 4)
  X <- delarr(M)
  row_vec <- rnorm(nrow(M))
  col_vec <- rnorm(ncol(M))
  expect_equal(collect(X + 2), M + 2)
  expect_equal(collect(X + row_vec), M + matrix(row_vec, nrow(M), ncol(M)))
  expect_equal(collect(X + col_vec), M + matrix(col_vec, nrow(M), ncol(M), byrow = TRUE))
})

test_that("comparisons return logical matrices", {
  set.seed(5)
  M <- matrix(rnorm(15), 5, 3)
  X <- delarr(M)
  out <- collect(X > 0)
  expect_type(out, "logical")
  expect_equal(out, M > 0)
})

test_that("reduce dims update lazily", {
  M <- matrix(1:12, 3, 4)
  X <- delarr(M)
  expect_equal(dim(d_reduce(X, sum, dim = "rows")), c(1L, ncol(M)))
  expect_equal(dim(d_reduce(X, sum, dim = "cols")), c(nrow(M), 1L))
})

test_that("NA-aware reductions work", {
  M <- matrix(c(1, NA, 3, 4, NA, 6), 3, 2, byrow = TRUE)
  X <- delarr(M)
  expect_equal(collect(d_reduce(X, sum, dim = "rows", na.rm = TRUE)), rowSums(M, na.rm = TRUE))
  expect_equal(collect(d_reduce(X, mean, dim = "cols", na.rm = TRUE)), colMeans(M, na.rm = TRUE))
  expect_equal(collect(d_reduce(X, max, dim = "rows", na.rm = TRUE)), apply(M, 1L, max, na.rm = TRUE))
})

test_that("print displays pipeline sketch", {
  M <- matrix(1:9, 3, 3)
  X <- delarr(M) |> d_center("rows") |> d_zscore("rows")
  txt <- paste(capture.output(print(X)), collapse = "\n")
  expect_true(grepl("ops:", txt, fixed = TRUE))
})

test_that("HDF5 writer streams results to disk", {
  skip_if_not_installed("hdf5r")
  path <- tempfile(fileext = ".h5")
  on.exit(unlink(path), add = TRUE)
  input <- matrix(runif(30), 5, 6)
  file <- hdf5r::H5File$new(path, mode = "w")
  file$create_dataset("X", robj = input)
  file$close_all()

  X <- delarr_hdf5(path, "X")
  centred <- X |> d_center("cols")
  out_path <- tempfile(fileext = ".h5")
  on.exit(unlink(out_path), add = TRUE)
  writer <- hdf5_writer(out_path, "Y", ncol = ncol(input), chunk = c(2L, 3L))
  collect(centred, into = writer, chunk_size = 3L)

  file_out <- hdf5r::H5File$new(out_path, mode = "r")
  on.exit(file_out$close_all(), add = TRUE)
  expect_equal(file_out[["Y"]]$read(), collect(d_center(delarr(input), "cols")))
})
