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
#' @param compression Compression level passed to `hdf5r` (currently unused but
#'   kept for forward compatibility).
#'
#' @return A writer object with `$write()` and `$finalize()` methods understood
#'   by `collect()`.
#' @export
hdf5_writer <- function(path, dataset, ncol, chunk = c(128L, 4096L), compression = NULL) {
  if (!requireNamespace("hdf5r", quietly = TRUE)) {
    stop("Package 'hdf5r' is required for hdf5_writer()", call. = FALSE)
  }
  if (length(chunk) != 2L) {
    stop("chunk must be a length-2 integer vector", call. = FALSE)
  }
  if (!is.numeric(ncol) || length(ncol) != 1L || ncol < 1) {
    stop("ncol must be a positive integer", call. = FALSE)
  }
  if (length(chunk) != 2L) {
    stop("chunk must be a length-2 integer vector", call. = FALSE)
  }
  env <- new.env(parent = emptyenv())
  env$file <- NULL
  env$dset <- NULL
  env$nrow <- NULL
  env$ncol <- as.integer(ncol)

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
      chunk = as.integer(chunk)
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
