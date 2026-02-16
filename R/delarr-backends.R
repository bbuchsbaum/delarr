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
#' # Create a temporary HDF5 file
#' tf <- tempfile(fileext = ".h5")
#' data <- matrix(1:20, nrow = 4, ncol = 5)
#'
#' # Write test data
#' f <- hdf5r::H5File$new(tf, mode = "w")
#' f$create_dataset("X", robj = data)
#' f$close_all()
#'
#' # Load as delayed array
#' darr <- delarr_hdf5(tf, "X")
#' darr
#'
#' # Apply operations and collect
#' result <- darr |> d_map(~ .x * 2) |> collect()
#' result
#'
#' # Clean up
#' unlink(tf)
delarr_hdf5 <- function(path, dataset) {
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

#' Create a delayed matrix from a memory-mapped file
#'
#' Uses the mmap package to lazily read slices from a binary file on demand.
#' The file must contain raw numeric data in column-major order (R's default).
#'
#' @param path Path to the binary file containing matrix data.
#' @param nrow Number of rows in the matrix.
#' @param ncol Number of columns in the matrix.
#' @param mode mmap mode object specifying data type. Default is double().
#'
#' @return A `delarr` that streams data from the memory-mapped file.
#' @export
#' @examples
#' # Create a binary file with matrix data
#' mat <- matrix(1:20, nrow = 4, ncol = 5)
#' tf <- tempfile()
#' writeBin(as.double(mat), tf)
#'
#' # Load as delayed array
#' darr <- delarr_mmap(tf, nrow = 4, ncol = 5)
#' darr
#'
#' # Apply operations and collect
#' result <- darr |> d_map(~ .x * 2) |> collect()
#' result
#'
#' # Clean up
#' unlink(tf)
delarr_mmap <- function(path, nrow, ncol, mode = NULL) {
  # Validate inputs
  if (!file.exists(path)) {
    stop("File not found: ", path, call. = FALSE)
  }
  if (!is.numeric(nrow) || !is.numeric(ncol) || nrow < 1 || ncol < 1) {
    stop("nrow and ncol must be positive integers", call. = FALSE)
  }
  nrow <- as.integer(nrow)
  ncol <- as.integer(ncol)

  # Default to double precision
  if (is.null(mode)) {
    mode <- mmap::real64()
  }

  # Calculate expected file size
  elem_size <- mmap::sizeof(mode)
  expected_bytes <- nrow * ncol * elem_size
  actual_bytes <- file.info(path)$size

  if (actual_bytes < expected_bytes) {
    stop(sprintf("File too small: expected %d bytes for %dx%d matrix, got %d",
                 expected_bytes, nrow, ncol, actual_bytes), call. = FALSE)
  }

  # State for lazy file access
  state <- new.env(parent = emptyenv())
  state$m <- NULL

  begin <- function() {
    if (!is.null(state$m)) return(invisible(NULL))
    state$m <- mmap::mmap(path, mode = mode)
    invisible(NULL)
  }

  end <- function() {
    if (!is.null(state$m)) {
      mmap::munmap(state$m)
      state$m <- NULL
    }
    invisible(NULL)
  }

  pull <- function(rows = NULL, cols = NULL) {
    rows <- rows %||% seq_len(nrow)
    cols <- cols %||% seq_len(ncol)
    rows <- as.integer(rows)
    cols <- as.integer(cols)
    idx <- rep.int(rows, times = length(cols)) +
      rep((cols - 1L) * nrow, each = length(rows))

    if (!is.null(state$m)) {
      vals <- state$m[idx]
    } else {
      m <- mmap::mmap(path, mode = mode)
      on.exit(mmap::munmap(m))
      vals <- m[idx]
    }

    matrix(vals, nrow = length(rows), ncol = length(cols))
  }

  delarr_backend(
    nrow = nrow,
    ncol = ncol,
    pull = pull,
    begin = begin,
    end = end
  )
}
