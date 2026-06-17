# tests/testthat/test-backends.R
# Tests for delarr_mem, delarr_hdf5, delarr_mmap, delarr_backend

test_that("delarr_mem wraps a matrix correctly", {
  mat <- matrix(1:12, 3, 4)
  x <- delarr_mem(mat)
  expect_s3_class(x, "delarr")
  expect_equal(dim(x), c(3, 4))
  expect_equal(collect(x), mat)
})

test_that("delarr_mem rejects non-matrix input", {
  expect_error(delarr_mem(1:10), "x must be a matrix")
  expect_error(delarr_mem(data.frame(a = 1:3)), "x must be a matrix")
})

test_that("delarr_mem supports lazy operations", {
  mat <- matrix(1:12, 3, 4)
  x <- delarr_mem(mat)
  result <- x |> d_center(dim = "cols") |> collect()
  expect_equal(colMeans(result), rep(0, 4))
})

test_that("delarr_backend creates a working delarr", {
  data <- matrix(1:20, 4, 5)
  darr <- delarr_backend(
    nrow = 4, ncol = 5,
    pull = function(rows = NULL, cols = NULL) {
      rows <- rows %||% seq_len(4)
      cols <- cols %||% seq_len(5)
      data[rows, cols, drop = FALSE]
    }
  )
  expect_s3_class(darr, "delarr")
  expect_equal(collect(darr), data)
})

test_that("delarr_backend with begin/end hooks", {
  data <- matrix(1:12, 3, 4)
  tracker <- new.env(parent = emptyenv())
  tracker$began <- FALSE
  tracker$ended <- FALSE

  darr <- delarr_backend(
    nrow = 3, ncol = 4,
    pull = function(rows = NULL, cols = NULL) {
      rows <- rows %||% seq_len(3)
      cols <- cols %||% seq_len(4)
      data[rows, cols, drop = FALSE]
    },
    begin = function() tracker$began <- TRUE,
    end = function() tracker$ended <- TRUE
  )

  result <- collect(darr)
  expect_equal(result, data)
  expect_true(tracker$began)
  expect_true(tracker$ended)
})

test_that("delarr_backend with dimnames", {
  data <- matrix(1:6, 2, 3)
  dn <- list(c("r1", "r2"), c("c1", "c2", "c3"))
  darr <- delarr_backend(
    nrow = 2, ncol = 3,
    pull = function(rows = NULL, cols = NULL) {
      rows <- rows %||% seq_len(2)
      cols <- cols %||% seq_len(3)
      data[rows, cols, drop = FALSE]
    },
    dimnames = dn
  )
  expect_equal(dimnames(darr), dn)
})

test_that("delarr_hdf5 reads data correctly", {
  skip_if_not_installed("hdf5r")
  tf <- tempfile(fileext = ".h5")
  on.exit(unlink(tf), add = TRUE)
  data <- matrix(as.double(1:20), 4, 5)
  f <- hdf5r::H5File$new(tf, mode = "w")
  f$create_dataset("X", robj = data)
  f$close_all()

  darr <- delarr_hdf5(tf, "X")
  expect_s3_class(darr, "delarr")
  expect_equal(dim(darr), c(4, 5))
  expect_equal(collect(darr), data)
})

test_that("delarr_hdf5 supports lazy pipeline", {
  skip_if_not_installed("hdf5r")
  tf <- tempfile(fileext = ".h5")
  on.exit(unlink(tf), add = TRUE)
  data <- matrix(as.double(1:20), 4, 5)
  f <- hdf5r::H5File$new(tf, mode = "w")
  f$create_dataset("X", robj = data)
  f$close_all()

  darr <- delarr_hdf5(tf, "X")
  result <- darr |> d_map(~ .x * 2) |> collect()
  expect_equal(result, data * 2)
})

test_that("delarr_hdf5 chunked streaming works", {
  skip_if_not_installed("hdf5r")
  tf <- tempfile(fileext = ".h5")
  on.exit(unlink(tf), add = TRUE)
  data <- matrix(as.double(1:30), 5, 6)
  f <- hdf5r::H5File$new(tf, mode = "w")
  f$create_dataset("X", robj = data)
  f$close_all()

  darr <- delarr_hdf5(tf, "X")
  result <- collect(darr, chunk_size = 2L)
  expect_equal(result, data)
})

test_that("delarr_hdf5 reads 3D datasets correctly", {
  skip_if_not_installed("hdf5r")
  tf <- tempfile(fileext = ".h5")
  on.exit(unlink(tf), add = TRUE)
  data <- array(as.double(1:60), dim = c(3, 4, 5))
  f <- hdf5r::H5File$new(tf, mode = "w")
  f$create_dataset("X", robj = data)
  f$close_all()

  darr <- delarr_hdf5(tf, "X")
  expect_equal(dim(darr), c(3, 4, 5))
  expect_equal(collect(darr, chunk_size = 2L, chunk_margin = 3L), data)
})

test_that("delarr_hdf5 supports 3D lazy pipelines", {
  skip_if_not_installed("hdf5r")
  tf <- tempfile(fileext = ".h5")
  on.exit(unlink(tf), add = TRUE)
  data <- array(as.double(1:60), dim = c(3, 4, 5))
  f <- hdf5r::H5File$new(tf, mode = "w")
  f$create_dataset("X", robj = data)
  f$close_all()

  darr <- delarr_hdf5(tf, "X")
  result <- collect(d_reduce(darr * 2, mean, axis = c(2L, 3L)), chunk_size = 2L, chunk_margin = 3L)
  expect_equal(result, apply(data * 2, 1, mean))
})

test_that("delarr_hdf5 rejects scalar datasets", {
  skip_if_not_installed("hdf5r")
  tf <- tempfile(fileext = ".h5")
  on.exit(unlink(tf), add = TRUE)
  f <- hdf5r::H5File$new(tf, mode = "w")
  f$create_dataset("scalar", robj = 1)
  f$close_all()

  expect_error(delarr_hdf5(tf, "scalar"), "at least 2 dimensions")
})

test_that("delarr_hdf5 pull functions work before collect opens the file", {
  skip_if_not_installed("hdf5r")
  tf <- tempfile(fileext = ".h5")
  on.exit(unlink(tf), add = TRUE)
  mat <- matrix(as.double(1:20), 4, 5)
  arr <- array(as.double(1:24), dim = c(2, 3, 4))
  f <- hdf5r::H5File$new(tf, mode = "w")
  f$create_dataset("mat", robj = mat)
  f$create_dataset("arr", robj = arr)
  f$close_all()

  mat_darr <- delarr_hdf5(tf, "mat")
  expect_equal(mat_darr$seed$pull(rows = 2:3, cols = 1:2), mat[2:3, 1:2])

  arr_darr <- delarr_hdf5(tf, "arr")
  expect_equal(
    arr_darr$seed$pull_nd(list(1:2, 2:3, 1:2)),
    arr[1:2, 2:3, 1:2, drop = FALSE]
  )
})

test_that("delarr_mmap reads binary data correctly", {
  skip_if_not_installed("mmap")
  mat <- matrix(as.double(1:20), 4, 5)
  tf <- tempfile()
  on.exit(unlink(tf), add = TRUE)
  writeBin(as.double(mat), tf)

  darr <- delarr_mmap(tf, nrow = 4, ncol = 5)
  expect_s3_class(darr, "delarr")
  expect_equal(dim(darr), c(4, 5))
  expect_equal(collect(darr), mat)
})

test_that("delarr_mmap supports lazy operations", {
  skip_if_not_installed("mmap")
  mat <- matrix(as.double(1:20), 4, 5)
  tf <- tempfile()
  on.exit(unlink(tf), add = TRUE)
  writeBin(as.double(mat), tf)

  darr <- delarr_mmap(tf, nrow = 4, ncol = 5)
  result <- darr |> d_map(~ .x * 2) |> collect()
  expect_equal(result, mat * 2)
})

test_that("delarr_mmap validates missing file", {
  skip_if_not_installed("mmap")
  expect_error(delarr_mmap("/nonexistent/file.bin", nrow = 2, ncol = 3),
               "File not found")
})

test_that("delarr_mmap validates bad dimensions", {
  skip_if_not_installed("mmap")
  tf <- tempfile()
  on.exit(unlink(tf), add = TRUE)
  writeBin(as.double(1:10), tf)
  expect_error(delarr_mmap(tf, nrow = -1, ncol = 5),
               "positive integers")
})

test_that("delarr_mmap validates file too small", {
  skip_if_not_installed("mmap")
  tf <- tempfile()
  on.exit(unlink(tf), add = TRUE)
  writeBin(as.double(1:4), tf)
  expect_error(delarr_mmap(tf, nrow = 10, ncol = 10),
               "File too small")
})

test_that("delarr_mmap slicing works", {
  skip_if_not_installed("mmap")
  mat <- matrix(as.double(1:20), 4, 5)
  tf <- tempfile()
  on.exit(unlink(tf), add = TRUE)
  writeBin(as.double(mat), tf)

  darr <- delarr_mmap(tf, nrow = 4, ncol = 5)
  result <- collect(darr[2:3, 1:3])
  expect_equal(result, mat[2:3, 1:3])
})
