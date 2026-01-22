#' Wrap a custom backend as a delayed matrix
#'
#' Provides a convenience helper that turns a user-supplied slice function into
#' a ready-to-use `delarr` object.
#'
#' @param nrow,ncol Dimensions of the logical matrix.
#' @param pull Function of `rows` and `cols` returning a base matrix slice.
#' @param chunk_hint Optional preferred chunking metadata.
#' @param dimnames Optional dimnames to expose lazily.
#' @param begin Optional function invoked before streaming.
#' @param end Optional function invoked after streaming.
#'
#' @return A `delarr` backed by the provided pull function.
#' @export
#' @examples
#' # Create a custom backend from a pull function
#' data <- matrix(1:20, nrow = 4, ncol = 5)
#'
#' darr <- delarr_backend(
#'   nrow = 4,
#'   ncol = 5,
#'   pull = function(rows = NULL, cols = NULL) {
#'     rows <- rows %||% seq_len(4)
#'     cols <- cols %||% seq_len(5)
#'     data[rows, cols, drop = FALSE]
#'   }
#' )
#' darr
#'
#' # Use like any delarr
#' result <- darr |> d_map(~ .x * 2) |> collect()
#' result
delarr_backend <- function(nrow, ncol, pull, chunk_hint = NULL, dimnames = NULL,
                          begin = NULL, end = NULL) {
  seed <- delarr_seed(
    nrow = nrow,
    ncol = ncol,
    pull = pull,
    chunk_hint = chunk_hint,
    dimnames = dimnames,
    begin = begin,
    end = end
  )
  delarr(seed)
}

#' Create a delayed matrix from an in-memory matrix
#'
#' @param x A numeric or logical matrix.
#'
#' @return A `delarr` referencing the original matrix.
#' @export
#' @examples
#' # Wrap an in-memory matrix
#' mat <- matrix(1:12, nrow = 3, ncol = 4)
#' darr <- delarr_mem(mat)
#' darr
#'
#' # Apply operations lazily
#' result <- darr |> d_center(dim = "rows") |> collect()
#' result
delarr_mem <- function(x) {
  if (!is.matrix(x)) {
    stop("x must be a matrix", call. = FALSE)
  }
  delarr(x)
}

#' Create a delayed matrix sourced from an HDF5 dataset
#'
#' Uses `hdf5r` to lazily read slices from disk on demand.
#'
#' @param path Path to the HDF5 file.
#' @param dataset Name of the dataset within the file.
#'
#' @return A `delarr` that streams data from the HDF5 dataset.
#' @export
#' @examples
#' if (requireNamespace("hdf5r", quietly = TRUE)) {
#'   # Create a temporary HDF5 file
#'   tf <- tempfile(fileext = ".h5")
#'   data <- matrix(1:20, nrow = 4, ncol = 5)
#'
#'   # Write test data
#'   f <- hdf5r::H5File$new(tf, mode = "w")
#'   f$create_dataset("X", robj = data)
#'   f$close_all()
#'
#'   # Load as delayed array
#'   darr <- delarr_hdf5(tf, "X")
#'   darr
#'
#'   # Apply operations and collect
#'   result <- darr |> d_map(~ .x * 2) |> collect()
#'   result
#'
#'   # Clean up
#'   unlink(tf)
#' }
delarr_hdf5 <- function(path, dataset) {
  if (!requireNamespace("hdf5r", quietly = TRUE)) {
    stop("Package 'hdf5r' is required for delarr_hdf5", call. = FALSE)
  }
  file <- hdf5r::H5File$new(path, mode = "r")
  on.exit(file$close_all())
  dset <- file[[dataset]]
  dims <- dset$dims
  chunk_dims <- tryCatch(dset$chunk_dims, error = function(e) NULL)

  state <- new.env(parent = emptyenv())
  state$file <- NULL
  state$dset <- NULL

  begin <- function() {
    if (!is.null(state$file)) {
      return(invisible(NULL))
    }
    state$file <- hdf5r::H5File$new(path, mode = "r")
    state$dset <- state$file[[dataset]]
    invisible(NULL)
  }

  end <- function() {
    if (!is.null(state$file)) {
      state$file$close_all()
      state$file <- NULL
      state$dset <- NULL
    }
    invisible(NULL)
  }

  pull <- function(rows = NULL, cols = NULL) {
    rows <- rows %||% seq_len(dims[1])
    cols <- cols %||% seq_len(dims[2])
    if (!is.null(state$dset)) {
      return(state$dset[rows, cols, drop = FALSE])
    }
    file <- hdf5r::H5File$new(path, mode = "r")
    on.exit(file$close_all())
    dset <- file[[dataset]]
    dset[rows, cols, drop = FALSE]
  }

  chunk_hint <- NULL
  if (!is.null(chunk_dims) && length(chunk_dims) >= 2L) {
    chunk_hint <- list(cols = as.integer(chunk_dims[[2L]]))
  }

  delarr_backend(
    nrow = dims[1],
    ncol = dims[2],
    pull = pull,
    chunk_hint = chunk_hint,
    begin = begin,
    end = end
  )
}

#' Placeholder for a memory-mapped backend
#'
#' The mmap helper is not yet implemented; users can supply their own backend
#' via `delarr_backend()` in the meantime.
#'
#' @param ... Reserved for future options.
#'
#' @return No return value; the function always errors.
#' @export
#' @examples
#' # delarr_mmap() is not yet implemented
#' \dontrun{
#' delarr_mmap()  # Throws an error
#' }
#'
#' # Use delarr_backend() with a custom pull function instead
#' mat <- matrix(1:12, nrow = 3, ncol = 4)
#' darr <- delarr_backend(
#'   nrow = 3, ncol = 4,
#'   pull = function(rows = NULL, cols = NULL) {
#'     rows <- rows %||% seq_len(3)
#'     cols <- cols %||% seq_len(4)
#'     mat[rows, cols, drop = FALSE]
#'   }
#' )
#' collect(darr)
delarr_mmap <- function(...) {
  stop("delarr_mmap() is not implemented yet. Use delarr_backend() with a custom pull function.", call. = FALSE)
}
