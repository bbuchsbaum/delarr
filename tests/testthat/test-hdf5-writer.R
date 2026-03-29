# tests/testthat/test-hdf5-writer.R
# Tests for write_hdf5, read_hdf5, hdf5_writer

test_that("write_hdf5 and read_hdf5 roundtrip", {
  mat <- matrix(as.double(1:20), 4, 5)
  tf <- tempfile(fileext = ".h5")
  on.exit(unlink(tf), add = TRUE)

  write_hdf5(mat, tf, "data")
  result <- read_hdf5(tf, "data")
  expect_equal(result, mat)
})

test_that("write_hdf5 rejects non-matrix", {
  tf <- tempfile(fileext = ".h5")
  on.exit(unlink(tf), add = TRUE)
  expect_error(write_hdf5(1:10, tf, "data"), "x must be a matrix")
})

test_that("write_hdf5 validates compression argument", {
  mat <- matrix(1:4, 2, 2)
  tf <- tempfile(fileext = ".h5")
  on.exit(unlink(tf), add = TRUE)
  expect_error(write_hdf5(mat, tf, "data", compression = 15),
               "compression must be NULL or an integer between 0 and 9")
  expect_error(write_hdf5(mat, tf, "data", compression = -1),
               "compression must be NULL or an integer between 0 and 9")
})

test_that("write_hdf5 with compression=NULL works", {
  mat <- matrix(as.double(1:6), 2, 3)
  tf <- tempfile(fileext = ".h5")
  on.exit(unlink(tf), add = TRUE)

  write_hdf5(mat, tf, "data", compression = NULL)
  result <- read_hdf5(tf, "data")
  expect_equal(result, mat)
})

test_that("hdf5_writer validates chunk argument", {
  expect_error(hdf5_writer("foo.h5", "X", ncol = 10, chunk = c(1, 2, 3)),
               "chunk must be a length-2")
})

test_that("hdf5_writer validates ncol argument", {
  expect_error(hdf5_writer("foo.h5", "X", ncol = -1),
               "ncol must be a positive integer")
  expect_error(hdf5_writer("foo.h5", "X", ncol = c(1, 2)),
               "ncol must be a positive integer")
})

test_that("hdf5_writer validates compression", {
  expect_error(hdf5_writer("f.h5", "X", ncol = 5, compression = 20),
               "compression must be NULL or an integer between 0 and 9")
})

test_that("hdf5_writer with NULL compression works", {
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

test_that("collect with into=function callback works for reduce", {
  mat <- matrix(1:12, 3, 4)
  darr <- delarr(mat) |> d_reduce(sum, dim = "rows")
  captured <- NULL
  result <- collect(darr, into = function(x) { captured <<- x })
  expect_null(result)
  expect_equal(captured, rowSums(mat))
})

test_that("hdf5_writer fails clearly for reduction outputs", {
  tf <- tempfile(fileext = ".h5")
  on.exit(unlink(tf), add = TRUE)
  darr <- delarr(matrix(1:12, 3, 4)) |> d_reduce(sum, dim = "rows")
  writer <- hdf5_writer(tf, "X", ncol = 1)
  expect_error(
    collect(darr, into = writer),
    "only supports matrix block outputs"
  )
})
