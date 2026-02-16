#' Transpose a delayed matrix
#'
#' @param x A `delarr`.
#' @param chunk_size Optional chunk size used for internal pulls.
#'
#' @return A transposed `delarr`.
#' @export
d_transpose <- function(x, chunk_size = NULL) {
  stopifnot(inherits(x, "delarr"))
  dx <- dim(x)
  delarr_backend(
    nrow = dx[2],
    ncol = dx[1],
    pull = function(rows = NULL, cols = NULL) {
      rows <- rows %||% seq_len(dx[2])
      cols <- cols %||% seq_len(dx[1])
      block <- collect(x[cols, rows, drop = FALSE], chunk_size = chunk_size)
      t(block)
    },
    chunk_hint = list(cols = x$seed$chunk_hint$rows %||% x$seed$chunk_hint$cols)
  )
}

#' @export
t.delarr <- function(x) {
  d_transpose(x)
}

#' Delayed matrix multiplication
#'
#' @param x A `delarr` or base matrix.
#' @param y A `delarr` or base matrix.
#' @param chunk_size Optional chunk size used during block pulls.
#'
#' @return A `delarr` representing `%*%`.
#' @export
d_matmul <- function(x, y, chunk_size = NULL) {
  if (!inherits(x, "delarr")) {
    x <- delarr(x)
  }
  if (!inherits(y, "delarr")) {
    y <- delarr(y)
  }
  dx <- dim(x)
  dy <- dim(y)
  if (!identical(dx[2], dy[1])) {
    stop("Non-conformable arguments for matrix multiplication", call. = FALSE)
  }
  delarr_backend(
    nrow = dx[1],
    ncol = dy[2],
    pull = function(rows = NULL, cols = NULL) {
      rows <- rows %||% seq_len(dx[1])
      cols <- cols %||% seq_len(dy[2])
      lhs <- collect(x[rows, , drop = FALSE], chunk_size = chunk_size)
      rhs <- collect(y[, cols, drop = FALSE], chunk_size = chunk_size)
      lhs %*% rhs
    },
    chunk_hint = list(cols = y$seed$chunk_hint$cols %||% 1024L)
  )
}

#' Run multiple reductions and collect results
#'
#' @param x A `delarr`.
#' @param fns A named list of reduction functions.
#' @param dim Reduction dimension (`"rows"` or `"cols"`).
#' @param na.rm Logical; remove missing values in each reducer.
#' @param chunk_size Optional chunk size passed to `collect()`.
#' @param simplify Logical; combine equal-length outputs into a matrix.
#'
#' @return A named list (or matrix when `simplify = TRUE`) of reductions.
#' @export
d_reduce_many <- function(x, fns, dim = c("rows", "cols"), na.rm = FALSE,
                          chunk_size = NULL, simplify = TRUE) {
  stopifnot(inherits(x, "delarr"))
  dim <- match.arg(dim)
  if (is.function(fns)) {
    fns <- list(fn1 = fns)
  }
  if (!is.list(fns) || length(fns) == 0L) {
    stop("fns must be a non-empty list of functions", call. = FALSE)
  }
  if (is.null(names(fns)) || any(names(fns) == "")) {
    names(fns) <- paste0("fn", seq_along(fns))
  }

  out <- lapply(fns, function(fn) {
    collect(d_reduce(x, fn, dim = dim, na.rm = na.rm), chunk_size = chunk_size)
  })
  names(out) <- names(fns)

  lengths <- vapply(out, length, integer(1))
  if (isTRUE(simplify) && length(unique(lengths)) == 1L) {
    mat <- do.call(cbind, out)
    colnames(mat) <- names(out)
    return(mat)
  }
  out
}
