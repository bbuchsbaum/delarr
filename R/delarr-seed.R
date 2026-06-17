#' Construct a seed backend for `delarr`
#'
#' Seeds encapsulate storage access for delayed matrices. They define matrix
#' dimensions and a `pull()` function that returns materialised slices.
#'
#' @param nrow,ncol Number of rows and columns.
#' @param pull A function accepting `rows` and `cols` indices and returning a
#'   base matrix slice.
#' @param chunk_hint Optional list describing preferred chunk sizes
#'   (e.g. `list(cols = 4096L)`).
#' @param dimnames Optional list of dimnames to expose lazily.
#' @param begin Optional function invoked before streaming begins.
#' @param end Optional function invoked after streaming completes.
#'
#' @return An object of class `delarr_seed`.
#'
#' @examples
#' # Create a custom seed with a pull function
#' data <- matrix(1:12, nrow = 3, ncol = 4)
#'
#' seed <- delarr_seed(
#'   nrow = 3,
#'   ncol = 4,
#'   pull = function(rows = NULL, cols = NULL) {
#'     rows <- rows %||% seq_len(3)
#'     cols <- cols %||% seq_len(4)
#'     data[rows, cols, drop = FALSE]
#'   }
#' )
#' seed
#'
#' # Wrap in delarr() to use with lazy operations
#' darr <- delarr(seed)
#' result <- darr |> d_map(~ .x * 2) |> collect()
#' result
#' @export
delarr_seed <- function(nrow, ncol, pull, chunk_hint = NULL, dimnames = NULL,
                        begin = NULL, end = NULL) {
  if (!is.numeric(nrow) || length(nrow) != 1L || nrow < 0) {
    stop("nrow must be a single non-negative number", call. = FALSE)
  }
  if (!is.numeric(ncol) || length(ncol) != 1L || ncol < 0) {
    stop("ncol must be a single non-negative number", call. = FALSE)
  }
  if (!is.function(pull)) {
    stop("pull must be a function", call. = FALSE)
  }
  structure(
    list(
      nrow = as.integer(nrow),
      ncol = as.integer(ncol),
      dims = c(as.integer(nrow), as.integer(ncol)),
      pull = pull,
      chunk_hint = chunk_hint,
      dimnames = dimnames,
      begin = begin,
      end = end
    ),
    class = "delarr_seed"
  )
}

#' Dimensions for a `delarr_seed`
#'
#' @param x A `delarr_seed`.
#'
#' @return An integer vector of dimension extents.
#' @export
dim.delarr_seed <- function(x) {
  x$dims
}

pull_seed <- function(seed, rows = NULL, cols = NULL) {
  rows <- if (!is.null(rows)) as.integer(rows) else rows
  cols <- if (!is.null(cols)) as.integer(cols) else cols
  res <- seed$pull(rows = rows, cols = cols)
  if (!is.matrix(res)) {
    stop("Seed pull must return a matrix", call. = FALSE)
  }
  expected_nrow <- if (is.null(rows)) seed$nrow else length(rows)
  expected_ncol <- if (is.null(cols)) seed$ncol else length(cols)
  if (!identical(dim(res), c(expected_nrow, expected_ncol))) {
    stop(
      sprintf(
        "Seed pull returned a %dx%d matrix, expected %dx%d",
        nrow(res),
        ncol(res),
        expected_nrow,
        expected_ncol
      ),
      call. = FALSE
    )
  }
  res
}

# ---- N-d seed ----------------------------------------------------------------

#' Construct an N-dimensional seed backend for `delarr`
#'
#' Creates a seed for arrays with 2 or more dimensions. The pull function
#' receives a list of per-dimension index vectors and returns the
#' corresponding sub-array.
#'
#' @param dims Integer vector of dimension extents (length >= 2).
#' @param pull A function accepting a single argument `indices` — a named or
#'   positional list of integer vectors (one per dimension, or `NULL` for "all")
#'   — and returning an array with the requested sub-dimensions.
#' @param chunk_hint Optional list describing preferred chunk sizes.
#' @param dimnames Optional list of dimnames (one element per dimension).
#' @param begin Optional function invoked before streaming begins.
#' @param end Optional function invoked after streaming completes.
#'
#' @return An object of class `delarr_seed`.
#'
#' @examples
#' arr <- array(seq_len(24), dim = c(3, 4, 2))
#' seed <- delarr_seed_nd(
#'   dims = c(3, 4, 2),
#'   pull = function(indices) {
#'     idx <- lapply(seq_along(dim(arr)), function(k) indices[[k]] %||% seq_len(dim(arr)[k]))
#'     do.call("[", c(list(arr), idx, list(drop = FALSE)))
#'   }
#' )
#' dim(seed)
#' @export
delarr_seed_nd <- function(dims, pull, chunk_hint = NULL, dimnames = NULL,
                           begin = NULL, end = NULL) {
  dims <- as.integer(dims)
  if (length(dims) < 2L || any(dims < 0L)) {
    stop("dims must be an integer vector of length >= 2 with non-negative values",
         call. = FALSE)
  }
  if (!is.function(pull)) {
    stop("pull must be a function", call. = FALSE)
  }
  structure(
    list(
      nrow = dims[1L],
      ncol = dims[2L],
      dims = dims,
      pull = pull,
      pull_nd = pull,
      chunk_hint = chunk_hint,
      dimnames = dimnames,
      begin = begin,
      end = end
    ),
    class = "delarr_seed"
  )
}

#' Pull a sub-array from an N-d seed
#'
#' @param seed A `delarr_seed` with N-d support.
#' @param indices A list of per-dimension index vectors (NULL = all).
#'
#' @return An array of the requested dimensions.
#' @keywords internal
pull_seed_nd <- function(seed, indices) {
  ndim <- length(seed$dims)
  if (length(indices) != ndim) {
    stop(sprintf("indices must have length %d (one per dimension)", ndim),
         call. = FALSE)
  }

  # If this is a 2D seed without a native pull_nd, delegate to pull(rows, cols)
  if (ndim == 2L && is.null(seed$pull_nd)) {
    return(pull_seed(seed, rows = indices[[1L]], cols = indices[[2L]]))
  }

  # Use the N-d pull function
  pull_fn <- seed$pull_nd %||% seed$pull
  res <- pull_fn(indices)

  # Validate result dimensions
  expected_dims <- vapply(seq_len(ndim), function(k) {
    if (is.null(indices[[k]])) seed$dims[k] else length(indices[[k]])
  }, integer(1))

  res_dims <- dim(res)
  if (is.null(res_dims)) {
    # Scalar or vector result — try to reshape
    if (length(res) == prod(expected_dims)) {
      dim(res) <- expected_dims
    } else {
      stop(sprintf("Seed pull returned %d elements, expected %d",
                   length(res), prod(expected_dims)), call. = FALSE)
    }
  } else if (!identical(as.integer(res_dims), expected_dims)) {
    stop(sprintf("Seed pull returned array with dim [%s], expected [%s]",
                 paste(res_dims, collapse = ","),
                 paste(expected_dims, collapse = ",")),
         call. = FALSE)
  }
  res
}

#' Check if a seed is N-dimensional (ndim > 2)
#' @keywords internal
is_nd_seed <- function(seed) {
  length(seed$dims) > 2L
}
