#' HDF5 writer for streaming `collect()`
#'
#' Creates or extends an HDF5 dataset so that `collect(x, into = writer)` can
#' stream column blocks directly to disk without materialising the full matrix
#' in memory.
#'
#' @param path Path to the HDF5 file. The file is created if it does not exist.
#' @param dataset Name of the dataset to create or update.
#' @param ncol Total number of columns that will be written. The writer uses
#'   this to size the target dataset up-front.
#' @param chunk Integer vector of length two giving the chunk size
#'   `(rows, cols)` for the target dataset (optional).
#' @param compression Gzip compression level (0-9). Use 0 for no compression,
#'   higher values for better compression at cost of speed. Default is 4.
#'   Use NULL to disable compression entirely.
#'
#' @return A writer object with `$write()` and `$finalize()` methods understood
#'   by `collect()`.
#' @export
#' @examples
#' # Create source data in a temp HDF5 file
#' tf_in <- tempfile(fileext = ".h5")
#' data <- matrix(1:20, nrow = 4, ncol = 5)
#' f <- hdf5r::H5File$new(tf_in, mode = "w")
#' f$create_dataset("X", robj = data)
#' f$close_all()
#'
#' # Load, transform, and stream to output file
#' darr <- delarr_hdf5(tf_in, "X")
#' transformed <- darr |> d_center(dim = "cols")
#'
#' tf_out <- tempfile(fileext = ".h5")
#' writer <- hdf5_writer(tf_out, "result", ncol = ncol(transformed), compression = 4L)
#' collect(transformed, into = writer)
#'
#' # Verify output
#' g <- hdf5r::H5File$new(tf_out, mode = "r")
#' result <- g[["result"]]$read()
#' g$close_all()
#' result
#'
#' # Clean up
#' unlink(c(tf_in, tf_out))
hdf5_writer <- function(path, dataset, ncol, chunk = c(128L, 4096L), compression = 4L) {
  if (length(chunk) != 2L) {
    stop("chunk must be a length-2 integer vector", call. = FALSE)
  }
  if (!is.numeric(ncol) || length(ncol) != 1L || ncol < 1) {
    stop("ncol must be a positive integer", call. = FALSE)
  }
  gzip_level <- NULL
  if (!is.null(compression)) {
    if (!is.numeric(compression) || length(compression) != 1L ||
        compression < 0 || compression > 9) {
      stop("compression must be NULL or an integer between 0 and 9", call. = FALSE)
    }
    gzip_level <- as.integer(compression)
  }
  env <- new.env(parent = emptyenv())
  env$file <- NULL
  env$dset <- NULL
  env$nrow <- NULL
  env$ncol <- as.integer(ncol)
  env$gzip_level <- gzip_level

  open_file <- function(mode = "a") {
    if (is.null(env$file)) {
      env$file <- hdf5r::H5File$new(path, mode = mode)
    }
  }

  ensure_dataset <- function(block, positions) {
    if (!is.null(env$dset)) {
      return(invisible(NULL))
    }
    env$nrow <- nrow(block)
    open_file("a")
    empty <- matrix(
      vector(mode = typeof(block), length = env$nrow * env$ncol),
      nrow = env$nrow,
      ncol = env$ncol
    )
    env$dset <- env$file$create_dataset(
      name = dataset,
      robj = empty,
      chunk_dims = as.integer(chunk),
      gzip_level = env$gzip_level
    )
  }

  list(
    write = function(block, rows, cols, positions) {
      ensure_dataset(block, positions)
      need_cols <- max(positions)
      if (need_cols > env$ncol) {
        stop("Attempting to write beyond declared ncol", call. = FALSE)
      }
      env$dset[seq_len(env$nrow), positions] <- block
      invisible(NULL)
    },
    finalize = function() {
      if (!is.null(env$file)) {
        env$file$close_all()
        env$file <- NULL
        env$dset <- NULL
      }
      invisible(NULL)
    }
  )
}
