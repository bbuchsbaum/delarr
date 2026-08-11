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
  res <- if (inherits(seed, "delarr_provider_seed")) {
    delarr_provider_pull(seed$provider, list(rows, cols))
  } else {
    seed$pull(rows = rows, cols = cols)
  }
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

.provider_has_runtime_state <- function(x) {
  if (is.environment(x) || is.function(x) || typeof(x) == "externalptr") {
    return(TRUE)
  }
  if (is.pairlist(x)) {
    return(any(vapply(as.list(x), .provider_has_runtime_state, logical(1))))
  }
  if (is.list(x)) {
    return(any(vapply(unclass(x), .provider_has_runtime_state, logical(1))))
  }
  FALSE
}

#' Pull an array slice from a reconstructible provider
#'
#' Storage packages implement this generic for a serializable provider class.
#' The provider descriptor remains inside the lazy plan; live file handles and
#' closures are created, used, and closed inside the method at execution time.
#'
#' @param provider A reconstructible provider descriptor.
#' @param indices A list containing one integer selector per array dimension;
#'   `NULL` selects an entire dimension.
#' @param ... Provider-specific arguments.
#' @return A matrix or array with dimensions matching `indices`.
#' @export
delarr_provider_pull <- function(provider, indices, ...) {
  UseMethod("delarr_provider_pull")
}

#' @export
delarr_provider_pull.default <- function(provider, indices, ...) {
  stop(
    "No delarr_provider_pull() method is registered for provider class '",
    class(provider)[1L],
    "'.",
    call. = FALSE
  )
}

#' @export
delarr_provider_pull.matrix <- function(provider, indices, ...) {
  resolved <- lapply(seq_along(dim(provider)), function(axis) {
    indices[[axis]] %||% seq_len(dim(provider)[[axis]])
  })
  do.call(`[`, c(list(provider), resolved, list(drop = FALSE)))
}

#' @export
delarr_provider_pull.array <- function(provider, indices, ...) {
  delarr_provider_pull.matrix(provider, indices, ...)
}

#' Construct a reconstructible provider seed
#'
#' Unlike [delarr_seed()], this seed stores no pull, begin, or end closures.
#' It stores a plain provider descriptor and dispatches reads through
#' `delarr_provider_pull()` only when the plan executes. This makes untouched
#' provider-backed plans safe to serialize and reconstruct in another process.
#'
#' @param provider A serializable provider descriptor containing no functions,
#'   environments, or external pointers.
#' @param dims Integer vector of logical dimensions, with length at least two.
#' @param chunk_hint Optional list of preferred chunk sizes.
#' @param dimnames Optional list of dimension names.
#' @return A `delarr_provider_seed` inheriting from `delarr_seed`.
#' @export
delarr_provider_seed <- function(provider, dims, chunk_hint = NULL, dimnames = NULL) {
  dims <- as.integer(dims)
  if (length(dims) < 2L || anyNA(dims) || any(dims < 0L)) {
    stop(
      "dims must be an integer vector of length >= 2 with non-negative values",
      call. = FALSE
    )
  }
  if (.provider_has_runtime_state(provider)) {
    stop(
      "provider descriptors cannot contain functions, environments, or external pointers",
      call. = FALSE
    )
  }
  if (!is.null(chunk_hint) && !is.list(chunk_hint)) {
    stop("chunk_hint must be NULL or a list", call. = FALSE)
  }
  if (!is.null(dimnames) && length(dimnames) != length(dims)) {
    stop("dimnames must have one element per provider dimension", call. = FALSE)
  }
  structure(
    list(
      provider = provider,
      nrow = dims[[1L]],
      ncol = dims[[2L]],
      dims = dims,
      chunk_hint = chunk_hint,
      dimnames = dimnames,
      begin = NULL,
      end = NULL
    ),
    class = c("delarr_provider_seed", "delarr_seed")
  )
}

#' Create a delayed array from a reconstructible provider
#'
#' @inheritParams delarr_provider_seed
#' @return A lazy `delarr` backed by the provider descriptor.
#' @export
delarr_provider <- function(provider, dims, chunk_hint = NULL, dimnames = NULL) {
  delarr(delarr_provider_seed(
    provider = provider,
    dims = dims,
    chunk_hint = chunk_hint,
    dimnames = dimnames
  ))
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

  if (inherits(seed, "delarr_provider_seed")) {
    res <- delarr_provider_pull(seed$provider, indices)
    expected_dims <- vapply(seq_len(ndim), function(k) {
      if (is.null(indices[[k]])) seed$dims[k] else length(indices[[k]])
    }, integer(1))
    if (!identical(as.integer(dim(res)), expected_dims)) {
      stop(sprintf(
        "Provider pull returned array with dim [%s], expected [%s]",
        paste(dim(res), collapse = ","),
        paste(expected_dims, collapse = ",")
      ), call. = FALSE)
    }
    return(res)
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
