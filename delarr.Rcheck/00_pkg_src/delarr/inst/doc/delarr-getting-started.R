## ----setup, include=FALSE-----------------------------------------------------
knitr::opts_chunk$set(
  collapse = TRUE,
  comment = "#>",
  fig.width = 7,
  fig.height = 4
)

## ----library, message=FALSE---------------------------------------------------
library(delarr)

## ----lazy-pipeline------------------------------------------------------------
set.seed(1)
mat <- matrix(rnorm(20), 5, 4)
arr <- delarr(mat)

result <- arr |>
  d_center(dim = "rows", na.rm = FALSE) |>
  d_map(~ .x * 0.5) |>
  d_reduce(mean, dim = "rows") |>
  collect()

result

## ----broadcasting-------------------------------------------------------------
row_bias <- rnorm(nrow(mat))

delarr(mat) |>
  (`+`)(row_bias) |>
  (`*`)(1.5) |>
  d_reduce(mean, dim = "cols") |>
  collect(chunk_size = 2)

## ----hdf5, eval=knitr::is_html_output(), message=FALSE------------------------
if (requireNamespace("hdf5r", quietly = TRUE)) {
  tf_in <- tempfile(fileext = ".h5")
  on.exit(unlink(tf_in), add = TRUE)
  input <- matrix(runif(30), 5, 6)
  f <- hdf5r::H5File$new(tf_in, mode = "w")
  f$create_dataset("X", robj = input)
  f$close_all()

  X <- delarr_hdf5(tf_in, "X")
  centred <- X |> d_center("cols")

  tf_out <- tempfile(fileext = ".h5")
  on.exit(unlink(tf_out), add = TRUE)
  writer <- hdf5_writer(tf_out, "X_centered", ncol = dim(centred)[2])
  collect(centred, into = writer, chunk_size = 3L)

  g <- hdf5r::H5File$new(tf_out, mode = "r")
  centred_back <- g[["X_centered"]]$read()
  g$close_all()

  centred_back
} else {
  "Install the 'hdf5r' package to run the streaming example."
}

## ----custom-backend-----------------------------------------------------------
random_backend <- list(
  pull = function(rows = NULL, cols = NULL) {
    rows <- if (is.null(rows)) seq_len(100) else rows
    cols <- if (is.null(cols)) seq_len(50) else cols
    matrix(rnorm(length(rows) * length(cols)), length(rows), length(cols))
  }
)

seed <- delarr_seed(
  nrow = 100,
  ncol = 50,
  pull = function(rows, cols) random_backend$pull(rows, cols)
)

rand_arr <- delarr(seed)
rand_arr |>
  d_map(~ .x^2) |>
  d_reduce(mean, dim = "cols") |>
  collect()

