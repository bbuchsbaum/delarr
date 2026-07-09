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
#'
#' @examples
#' # Create a delayed matrix from a regular matrix
#' mat <- matrix(1:12, nrow = 3, ncol = 4)
#' darr <- delarr(mat)
#' darr
#'
#' # Operations are queued lazily
#' result <- darr * 2
#' result
#'
#' # Materialize with collect()
#' collect(result)
#'
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
  if (is.array(x) && length(dim(x)) >= 2L) {
    d <- dim(x)
    seed <- delarr_seed_nd(
      dims = d,
      pull = function(indices) {
        idx <- lapply(seq_along(d), function(k) {
          indices[[k]] %||% seq_len(d[k])
        })
        do.call(`[`, c(list(x), idx, list(drop = FALSE)))
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

#' Subset a delayed array
#'
#' Performs array-style slicing lazily, capturing the indices in the DAG.
#' For 2D arrays, standard `x[i, j]` syntax works. For N-d arrays, provide
#' one index expression per dimension: `x[i, j, k, ...]`.
#'
#' @param x A `delarr`.
#' @param ... Index expressions, one per dimension. Missing indices select all.
#' @param drop Logical indicating whether to drop dimensions (ignored lazily).
#'
#' @return A `delarr` containing the slice operation.
#' @export
`[.delarr` <- function(x, ..., drop = FALSE) {
  ndim <- length(x$seed$dims)

  # Parse sys.call() to handle missing index args (e.g., x[, j] or x[i, , k])
  sc <- as.list(sys.call())
  # sc[[1]] = `[`, sc[[2]] = x, sc[[3..]] = index exprs, possibly drop
  sc <- sc[-(1:2)]  # remove function name and x
  # Remove named 'drop' if present
  drop_pos <- match("drop", names(sc))
  if (!is.na(drop_pos)) sc <- sc[-drop_pos]

  n_idx <- length(sc)

  if (n_idx == 0L) {
    return(x)
  }

  pf <- parent.frame()

  # For 2D with up to 2 index args, use legacy rows/cols path
  if (ndim == 2L && n_idx <= 2L) {
    rows <- if (n_idx >= 1L && !identical(sc[[1L]], quote(expr = ))) {
      eval(sc[[1L]], pf)
    } else {
      NULL
    }
    cols <- if (n_idx >= 2L && !identical(sc[[2L]], quote(expr = ))) {
      eval(sc[[2L]], pf)
    } else {
      NULL
    }
    op <- list(op = "slice", rows = rows, cols = cols, drop = drop)
    return(add_op(x, op))
  }

  # N-d path: build indices list
  indices <- lapply(sc, function(a) {
    if (identical(a, quote(expr = ))) NULL else eval(a, pf)
  })

  # Pad with NULLs if fewer indices than dimensions
  if (length(indices) < ndim) {
    indices <- c(indices, rep(list(NULL), ndim - length(indices)))
  }

  op <- list(op = "slice", indices = indices, drop = drop)
  add_op(x, op)
}

#' Dimensions of a delayed array
#'
#' Computes the realised dimensions after taking queued slice and reduce
#' operations into account.
#'
#' @param x A `delarr`.
#'
#' @return An integer vector of dimension extents.
#' @export
dim.delarr <- function(x) {
  plan <- compile_plan(x)
  dims <- vapply(plan$indices, length, integer(1))
  if (!is.null(plan$reduce)) {
    collapse_axis <- collapse_axes_from_reduce(plan$reduce, ndim = length(dims))
    dims[collapse_axis] <- 1L
  }
  dims
}

#' Dimension names for a delayed array
#'
#' @param x A `delarr`.
#'
#' @return A list of per-dimension names or `NULL` placeholders.
#' @export
dimnames.delarr <- function(x) {
  ndim <- length(x$seed$dims)
  seed_dimnames <- x$seed$dimnames %||% rep(list(NULL), ndim)
  if (length(seed_dimnames) < ndim) {
    seed_dimnames <- c(seed_dimnames,
                       rep(list(NULL), ndim - length(seed_dimnames)))
  }

  plan <- compile_plan(x)
  result <- lapply(seq_len(ndim), function(k) {
    dn <- seed_dimnames[[k]]
    if (is.null(dn)) NULL else dn[plan$indices[[k]]]
  })
  if (!is.null(plan$reduce)) {
    collapse_axis <- collapse_axes_from_reduce(plan$reduce, ndim = ndim)
    result[collapse_axis] <- rep(list(NULL), length(collapse_axis))
  }
  result
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
  dim_str <- paste(d, collapse = " x ")
  if (length(x$ops)) {
    labels <- vapply(x$ops, describe_op, character(1))
    labels <- labels[labels != ""]
    if (length(labels)) {
      cat("<delarr> ", dim_str, " - ops: ", paste(labels, collapse = " -> "), "\n", sep = "")
      return(invisible(x))
    }
  }
  cat("<delarr> ", dim_str, " lazy\n", sep = "")
  invisible(x)
}

describe_op <- function(op) {
  dim_label <- op$dim %||% if (!is.null(op$axis)) {
    paste0("axis=", paste(op$axis, collapse = ","))
  } else {
    "?"
  }
  switch(op$op,
    slice = "slice",
    emap = "map",
    emap2 = "map2",
    emap_const = "map_const",
    center = paste0("center(", dim_label, ")"),
    scale = paste0("scale(", dim_label, ")"),
    zscore = paste0("zscore(", dim_label, ")"),
    detrend = paste0("detrend(", dim_label, ")"),
    reduce = paste0("reduce(", dim_label, ")"),
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
#' delayed matrix and scalars/matrices. The unary `+`/`-` forms (e.g. `-x`)
#' are also handled and stay lazy.
#'
#' @param e1,e2 Operands supplied by the R math group generics. For the unary
#'   `+`/`-` forms `e2` is missing.
#'
#' @return A `delarr` representing the fused operation.
#' @export
Ops.delarr <- function(e1, e2) {
  op <- .Generic
  # Unary `+`/`-` are dispatched through the Ops group generic with `e2`
  # missing. Apply the operator lazily as an elementwise map so that `-x`
  # (and `+x`) behave like their base-matrix counterparts.
  if (missing(e2)) {
    return(add_op(e1, list(
      op = "emap",
      fn = function(a) do.call(op, list(a))
    )))
  }
  if (inherits(e1, "delarr") && inherits(e2, "delarr")) {
    return(add_op(e1, list(
      op = "emap2",
      op_name = op,
      rhs = e2,
      fn = function(a, b) do.call(op, list(a, b))
    )))
  }
  if (inherits(e1, "delarr")) {
    warn_if_ambiguous_broadcast(e1, e2)
    return(add_op(e1, list(
      op = "emap_const",
      op_name = op,
      const = e2,
      side = "right",
      fn = function(a, b) do.call(op, list(a, b))
    )))
  }
  if (inherits(e2, "delarr")) {
    warn_if_ambiguous_broadcast(e2, e1)
    return(add_op(e2, list(
      op = "emap_const",
      op_name = op,
      const = e1,
      side = "left",
      fn = function(a, b) do.call(op, list(a, b))
    )))
  }
  stop("Operation not supported", call. = FALSE)
}

# Warn, at lazy-op construction time, when a bare atomic vector is broadcast
# against a square matrix. In that case the row/column orientation cannot be
# inferred from length alone (`length(rhs) == nrow == ncol`), and delarr
# resolves the tie to row-aligned (one value per row), matching base R's
# `matrix + vector` recycling down columns. The check fires here -- not inside
# the chunk-evaluation hot path -- so the warning is emitted once per operation
# rather than once per chunk. Silence with
# `options(delarr.warn_ambiguous_broadcast = FALSE)`.
warn_if_ambiguous_broadcast <- function(x, rhs) {
  if (!isTRUE(getOption("delarr.warn_ambiguous_broadcast", TRUE))) {
    return(invisible(NULL))
  }
  if (!is.atomic(rhs) || is.array(rhs) || length(rhs) <= 1L) {
    return(invisible(NULL))
  }
  dims <- dim(x)
  if (length(dims) != 2L || dims[1L] != dims[2L] || length(rhs) != dims[1L]) {
    return(invisible(NULL))
  }
  warning(
    sprintf(
      paste0(
        "Ambiguous broadcast: a length-%d vector against a square %dx%d ",
        "matrix is interpreted as row-aligned (one value per row), matching ",
        "base R recycling. For column alignment pass an explicit matrix, ",
        "e.g. matrix(v, %d, %d, byrow = TRUE). Silence with ",
        "options(delarr.warn_ambiguous_broadcast = FALSE)."
      ),
      length(rhs), dims[1L], dims[2L], dims[1L], dims[2L]
    ),
    call. = FALSE
  )
  invisible(NULL)
}

#' Elementwise math functions for `delarr`
#'
#' Supports the R `Math` group generics (`sqrt()`, `abs()`, `exp()`, `log()`,
#' `round()`, `floor()`, the trig functions, and so on) lazily, as fused
#' elementwise maps. Extra arguments are forwarded to the underlying function,
#' so `round(x, 2)` and `log(x, base = 2)` work as expected.
#'
#' The cumulative generics (`cumsum()`, `cumprod()`, `cummax()`, `cummin()`)
#' are not elementwise and cannot be evaluated chunk-by-chunk, so they raise an
#' error rather than returning silently incorrect results.
#'
#' @param x A `delarr`.
#' @param ... Additional arguments forwarded to the math generic.
#'
#' @return A `delarr` representing the fused elementwise operation.
#' @export
#' @examples
#' mat <- matrix(1:12, nrow = 3, ncol = 4)
#' darr <- delarr(mat)
#' collect(sqrt(darr))
#' collect(-darr)
Math.delarr <- function(x, ...) {
  g <- .Generic
  if (g %in% c("cumsum", "cumprod", "cummax", "cummin")) {
    stop(sprintf("%s() is not supported for delarr (not an elementwise op)", g),
         call. = FALSE)
  }
  args <- list(...)
  add_op(x, list(
    op = "emap",
    fn = function(a) do.call(g, c(list(a), args))
  ))
}

add_op <- function(x, op) {
  stopifnot(inherits(x, "delarr"))
  new_delarr(x$seed, append(x$ops, list(op)))
}
