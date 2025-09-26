#' Create a delayed matrix
#'
#' Wraps an existing matrix or `delarr_seed` in the lightweight delayed
#' pipeline. Matrix inputs are wrapped in a seed that simply slices the source
#' object, while `delarr` inputs are returned unchanged.
#'
#' @param x A base matrix or a `delarr_seed` to wrap.
#' @param ... Future extensions; currently ignored.
#'
#' @return A `delarr` object representing the delayed matrix.
#' @export
delarr <- function(x, ...) {
  if (inherits(x, "delarr")) {
    return(x)
  }
  if (inherits(x, "delarr_seed")) {
    return(new_delarr(seed = x, ops = list()))
  }
  if (is.matrix(x)) {
    seed <- delarr_seed(
      nrow = nrow(x),
      ncol = ncol(x),
      pull = function(rows = NULL, cols = NULL) {
        rows <- rows %||% seq_len(nrow(x))
        cols <- cols %||% seq_len(ncol(x))
        x[rows, cols, drop = FALSE]
      },
      dimnames = dimnames(x)
    )
    return(new_delarr(seed = seed, ops = list()))
  }
  stop("Unsupported input for delarr()", call. = FALSE)
}

#' Internal constructor for `delarr`
#'
#' @param seed A `delarr_seed` backend.
#' @param ops A list of deferred operations.
#'
#' @keywords internal
new_delarr <- function(seed, ops = list()) {
  structure(
    list(
      seed = seed,
      ops = ops
    ),
    class = "delarr"
  )
}

#' Subset a delayed matrix
#'
#' Performs matrix-style slicing lazily, capturing the indices in the DAG.
#'
#' @param x A `delarr`.
#' @param i Row indices or `NULL`.
#' @param j Column indices or `NULL`.
#' @param drop Logical indicating whether to drop dimensions (ignored lazily).
#'
#' @return A `delarr` containing the slice operation.
#' @export
`[.delarr` <- function(x, i, j, drop = FALSE) {
  op <- list(
    op = "slice",
    rows = if (missing(i)) NULL else i,
    cols = if (missing(j)) NULL else j,
    drop = drop
  )
  add_op(x, op)
}

#' Dimensions of a delayed matrix
#'
#' Computes the realised dimensions after taking queued slice and reduce
#' operations into account.
#'
#' @param x A `delarr`.
#'
#' @return An integer vector of length two.
#' @export
dim.delarr <- function(x) {
  dims <- c(x$seed$nrow, x$seed$ncol)
  for (op in x$ops) {
    if (op$op == "slice") {
      if (!is.null(op$rows)) {
        dims[1] <- length(op$rows)
      }
      if (!is.null(op$cols)) {
        dims[2] <- length(op$cols)
      }
    }
    if (op$op == "reduce") {
      if (identical(op$dim, "rows")) {
        dims <- c(1L, dims[2])
      } else {
        dims <- c(dims[1], 1L)
      }
    }
  }
  dims
}

#' Dimension names for a delayed matrix
#'
#' @param x A `delarr`.
#'
#' @return A list of row and column names or `NULL` placeholders.
dimnames.delarr <- function(x) {
  x$seed$dimnames %||% list(NULL, NULL)
}

#' Pretty-print a delayed matrix
#'
#' @param x A `delarr`.
#' @param ... Unused.
#'
#' @return The original object, invisibly.
#' @export
print.delarr <- function(x, ...) {
  d <- dim(x)
  if (length(x$ops)) {
    labels <- vapply(x$ops, describe_op, character(1))
    labels <- labels[labels != ""]
    if (length(labels)) {
      cat("<delarr> ", d[1], " x ", d[2], " • ops: ", paste(labels, collapse = " -> "), "\n", sep = "")
      return(invisible(x))
    }
  }
  cat("<delarr> ", d[1], " x ", d[2], " lazy\n", sep = "")
  invisible(x)
}

describe_op <- function(op) {
  switch(op$op,
    slice = "slice",
    emap = "map",
    emap2 = "map2",
    emap_const = "map_const",
    center = paste0("center(", op$dim, ")"),
    scale = paste0("scale(", op$dim, ")"),
    zscore = paste0("zscore(", op$dim, ")"),
    detrend = paste0("detrend(", op$dim, ")"),
    reduce = paste0("reduce(", op$dim, ")"),
    where = "where",
    ""
  )
}

#' Materialise a delayed matrix as a base matrix
#'
#' @param x A `delarr`.
#' @param ... Passed to `collect()`.
#'
#' @return A base matrix containing the realised data.
#' @export
as.matrix.delarr <- function(x, ...) {
  collect(x)
}

#' Arithmetic and comparison operators for `delarr`
#'
#' Supports elementwise operations between delayed matrices or between a
#' delayed matrix and scalars/matrices.
#'
#' @param e1,e2 Operands supplied by the R math group generics.
#'
#' @return A `delarr` representing the fused operation.
#' @export
Ops.delarr <- function(e1, e2) {
  op <- .Generic
  if (inherits(e1, "delarr") && inherits(e2, "delarr")) {
    return(add_op(e1, list(op = "emap2", rhs = e2, fn = function(a, b) do.call(op, list(a, b)))))
  }
  if (inherits(e1, "delarr")) {
    return(add_op(e1, list(op = "emap_const", const = e2, side = "right", fn = function(a, b) do.call(op, list(a, b)))))
  }
  if (inherits(e2, "delarr")) {
    return(add_op(e2, list(op = "emap_const", const = e1, side = "left", fn = function(a, b) do.call(op, list(a, b)))))
  }
  stop("Operation not supported", call. = FALSE)
}

add_op <- function(x, op) {
  stopifnot(inherits(x, "delarr"))
  new_delarr(x$seed, append(x$ops, list(op)))
}
