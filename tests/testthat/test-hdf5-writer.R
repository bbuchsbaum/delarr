# tests/testthat/test-hdf5-writer.R
# Tests for write_hdf5, read_hdf5, hdf5_writer

test_that("write_hdf5 and read_hdf5 roundtrip", {
  skip_if_not_installed("hdf5r")
  mat <- matrix(as.double(1:20), 4, 5)
  tf <- tempfile(fileext = ".h5")
  on.exit(unlink(tf), add = TRUE)

  write_hdf5(mat, tf, "data")
  result <- read_hdf5(tf, "data")
  expect_equal(result, mat)
})

test_that("write_hdf5 rejects non-matrix", {
  skip_if_not_installed("hdf5r")
  tf <- tempfile(fileext = ".h5")
  on.exit(unlink(tf), add = TRUE)
  expect_error(write_hdf5(1:10, tf, "data"), "x must be a matrix")
})

test_that("write_hdf5 validates compression argument", {
  skip_if_not_installed("hdf5r")
  mat <- matrix(1:4, 2, 2)
  tf <- tempfile(fileext = ".h5")
  on.exit(unlink(tf), add = TRUE)
  expect_error(write_hdf5(mat, tf, "data", compression = 15),
               "compression must be NULL or an integer between 0 and 9")
  expect_error(write_hdf5(mat, tf, "data", compression = -1),
               "compression must be NULL or an integer between 0 and 9")
})

test_that("write_hdf5 with compression=NULL works", {
  skip_if_not_installed("hdf5r")
  mat <- matrix(as.double(1:6), 2, 3)
  tf <- tempfile(fileext = ".h5")
  on.exit(unlink(tf), add = TRUE)

  write_hdf5(mat, tf, "data", compression = NULL)
  result <- read_hdf5(tf, "data")
  expect_equal(result, mat)
})

test_that("hdf5_writer validates chunk argument", {
  skip_if_not_installed("hdf5r")
  expect_error(hdf5_writer("foo.h5", "X", ncol = 10, chunk = c(1, 2, 3)),
               "chunk must be a length-2")
})

test_that("hdf5_writer validates ncol argument", {
  skip_if_not_installed("hdf5r")
  expect_error(hdf5_writer("foo.h5", "X", ncol = -1),
               "ncol must be a positive integer")
  expect_error(hdf5_writer("foo.h5", "X", ncol = c(1, 2)),
               "ncol must be a positive integer")
})

test_that("hdf5_writer validates compression", {
  skip_if_not_installed("hdf5r")
  expect_error(hdf5_writer("f.h5", "X", ncol = 5, compression = 20),
               "compression must be NULL or an integer between 0 and 9")
})

test_that("hdf5_writer with NULL compression works", {
  skip_if_not_installed("hdf5r")
  tf_in <- tempfile(fileext = ".h5")
  tf_out <- tempfile(fileext = ".h5")
  on.exit(unlink(c(tf_in, tf_out)), add = TRUE)

  data <- matrix(as.double(1:12), 3, 4)
  f <- hdf5r::H5File$new(tf_in, mode = "w")
  f$create_dataset("X", robj = data)
  f$close_all()

  darr <- delarr_hdf5(tf_in, "X")
  writer <- hdf5_writer(tf_out, "Y", ncol = 4, compression = NULL)
  collect(darr, into = writer, chunk_size = 2L)

  result <- read_hdf5(tf_out, "Y")
  expect_equal(result, data)
})

test_that("hdf5_writer creates datasets from integer blocks without materializing output", {
  skip_if_not_installed("hdf5r")
  tf_out <- tempfile(fileext = ".h5")
  on.exit(unlink(tf_out), add = TRUE)

  data <- matrix(1L:12L, 3, 4)
  writer <- hdf5_writer(tf_out, "Y", ncol = ncol(data), compression = NULL)
  collect(delarr(data), into = writer, chunk_size = 2L)

  result <- read_hdf5(tf_out, "Y")
  expect_equal(result, data)
  expect_type(result, "integer")
})

test_that("collect with into=function callback works for reduce", {
  mat <- matrix(1:12, 3, 4)
  darr <- delarr(mat) |> d_reduce(sum, dim = "rows")
  captured <- NULL
  result <- collect(darr, into = function(x) { captured <<- x })
  expect_null(result)
  expect_equal(captured, rowSums(mat))
})

test_that("collect with into=function receives streamed matrix chunks", {
  mat <- matrix(1:20, 4, 5)
  chunks <- list()

  result <- collect(
    delarr(mat) |> d_map(~ .x + 1),
    into = function(block, rows, cols, positions) {
      chunks[[length(chunks) + 1L]] <<- list(
        block = block,
        rows = rows,
        cols = cols,
        positions = positions
      )
    },
    chunk_size = 2L
  )

  expect_null(result)
  expect_equal(length(chunks), 3L)
  expect_equal(chunks[[1L]]$block, mat[, 1:2, drop = FALSE] + 1)
  expect_equal(chunks[[1L]]$positions, 1:2)
  expect_equal(chunks[[3L]]$block, mat[, 5, drop = FALSE] + 1)
})

test_that("collect rejects unsupported into targets", {
  expect_error(
    collect(delarr(matrix(1:6, 2, 3)), into = "not a writer"),
    "Unsupported 'into' target"
  )
})

test_that("hdf5_writer fails clearly for reduction outputs", {
  skip_if_not_installed("hdf5r")
  tf <- tempfile(fileext = ".h5")
  on.exit(unlink(tf), add = TRUE)
  darr <- delarr(matrix(1:12, 3, 4)) |> d_reduce(sum, dim = "rows")
  writer <- hdf5_writer(tf, "X", ncol = 1)
  expect_error(
    collect(darr, into = writer),
    "only supports matrix block outputs"
  )
})

test_that("hdf5_writer validates streamed write positions", {
  skip_if_not_installed("hdf5r")
  tf <- tempfile(fileext = ".h5")
  on.exit(unlink(tf), add = TRUE)
  writer <- hdf5_writer(tf, "X", ncol = 4)
  expect_error(
    writer$write(matrix(1:6, 2, 3), rows = 1:2, cols = 1:3),
    "requires column positions"
  )
  expect_error(
    writer$write(matrix(1:10, 2, 5), rows = 1:2, cols = 1:5, positions = 1:5),
    "beyond declared ncol"
  )
})

test_that(".require_hdf5r errors when hdf5r is unavailable", {
  local_mocked_bindings(
    requireNamespace = function(pkg, quietly = TRUE) {
      if (identical(pkg, "hdf5r")) return(FALSE)
      base::requireNamespace(pkg, quietly = quietly)
    },
    .package = "base"
  )
  expect_error(
    delarr:::.require_hdf5r(),
    "Package 'hdf5r' is required for HDF5 backends"
  )
})
