#' Explain a delayed execution plan
#'
#' @param x A `delarr`.
#' @param chunk_size Optional chunk size hint.
#' @param chunk_margin Chunking axis for non-reduction materialization.
#' @param target_bytes Optional memory budget used for adaptive chunking.
#' @param optimize Logical; whether to explain the optimized DAG.
#'
#' @return An object of class `delarr_explain`.
#' @export
explain <- function(x, chunk_size = NULL, chunk_margin = c("cols", "rows"),
                    target_bytes = NULL, optimize = TRUE) {
  stopifnot(inherits(x, "delarr"))
  obj <- if (isTRUE(optimize)) optimize_delarr(x) else x
  plan <- compile_plan(obj)
  ndim <- length(obj$seed$dims)
  chunk_margin_missing <- missing(chunk_margin)
  chunk_margin <- if (chunk_margin_missing) {
    if (ndim <= 2L) "cols" else NULL
  } else {
    normalize_chunk_margin(chunk_margin, ndim)
  }
  rows <- plan$rows %||% seq_len(obj$seed$nrow)
  cols <- plan$cols %||% seq_len(obj$seed$ncol)
  n_rows <- length(rows)
  n_cols <- length(cols)
  reduce_dim <- if (is.null(plan$reduce)) NULL else plan$reduce$dim
  selected_dims <- vapply(plan$indices, length, integer(1))
  if (ndim <= 2L) {
    effective_margin <- if (is.null(plan$reduce)) chunk_margin else "cols"
    resolved_chunk <- infer_chunk_size(
      seed = obj$seed,
      requested_rows = n_rows,
      requested_cols = n_cols,
      chunk_size = chunk_size,
      margin = effective_margin,
      target_bytes = target_bytes
    )
    chunk_extent <- if (identical(effective_margin, "cols")) n_cols else n_rows
  } else {
    blocked_axes <- blocked_chunk_axes(plan$ops, ndim)
    effective_margin <- NULL
    resolved_chunk <- NA_integer_
    chunk_extent <- 1L
    reduce_info <- classify_reduce(plan$reduce)
    if (is.null(plan$reduce)) {
      safe_axes <- setdiff(seq_len(ndim), blocked_axes)
      default_axis <- if (length(safe_axes)) safe_axes[[length(safe_axes)]] else ndim
      effective_margin <- resolve_chunk_axis(chunk_margin, ndim, default = default_axis)
      resolved_chunk <- infer_nd_chunk_size(
        seed = obj$seed,
        requested_dims = selected_dims,
        axis = effective_margin,
        chunk_size = chunk_size,
        target_bytes = target_bytes
      )
      chunk_extent <- selected_dims[[effective_margin]]
    } else if (!is.null(reduce_info) && !identical(reduce_info$type, "generic")) {
      collapse_axes <- collapse_axes_from_reduce(plan$reduce, ndim = ndim)
      safe_axes <- setdiff(collapse_axes, blocked_axes)
      if (length(safe_axes)) {
        default_axis <- safe_axes[[length(safe_axes)]]
        effective_margin <- resolve_chunk_axis(chunk_margin, ndim, default = default_axis)
        if (effective_margin %in% safe_axes) {
          resolved_chunk <- infer_nd_chunk_size(
            seed = obj$seed,
            requested_dims = selected_dims,
            axis = effective_margin,
            chunk_size = chunk_size,
            target_bytes = target_bytes
          )
          chunk_extent <- selected_dims[[effective_margin]]
        } else {
          effective_margin <- NULL
        }
      }
    }
  }
  labels <- vapply(plan$ops, describe_op, character(1))
  structure(
    list(
      input_dim = obj$seed$dims,
      output_dim = dim(obj),
      selected_rows = n_rows,
      selected_cols = n_cols,
      selected_dim = selected_dims,
      op_count = length(plan$ops),
      ops = labels[labels != ""],
      has_reduce = !is.null(plan$reduce),
      reduce_dim = reduce_dim,
      chunk_margin = effective_margin,
      chunk_size = resolved_chunk,
      chunk_count = if (is.null(effective_margin) || is.na(resolved_chunk) || chunk_extent == 0L) {
        1L
      } else {
        ceiling(chunk_extent / resolved_chunk)
      },
      pair_rhs_ops = length(plan$rhs_indices),
      optimized = isTRUE(optimize)
    ),
    class = "delarr_explain"
  )
}

#' @export
print.delarr_explain <- function(x, ...) {
  chunk_label <- if (is.null(x$chunk_margin) || is.na(x$chunk_size)) {
    "full"
  } else {
    sprintf("(%s=%d)", x$chunk_margin, x$chunk_size)
  }
  cat(
    "<delarr_explain> in:", paste(x$input_dim, collapse = "x"),
    " out:", paste(x$output_dim, collapse = "x"), "\n"
  )
  cat(
    "ops:", x$op_count,
    " chunks:", x$chunk_count,
    chunk_label, "\n"
  )
  if (length(x$ops)) {
    cat("plan:", paste(x$ops, collapse = " -> "), "\n")
  }
  if (isTRUE(x$has_reduce)) {
    cat("reduce:", x$reduce_dim, "\n")
  }
  invisible(x)
}

#' Profile `collect()` runtime
#'
#' @param x A `delarr`.
#' @param reps Number of repetitions.
#' @param ... Additional arguments forwarded to `collect()`.
#'
#' @return An object of class `delarr_profile`.
#' @export
profile_collect <- function(x, reps = 3L, ...) {
  stopifnot(inherits(x, "delarr"))
  reps <- as.integer(reps)
  if (reps < 1L) {
    stop("reps must be >= 1", call. = FALSE)
  }
  elapsed <- numeric(reps)
  sizes <- numeric(reps)
  sizes[] <- NA_real_
  last_value <- NULL

  for (i in seq_len(reps)) {
    gc()
    t0 <- proc.time()[["elapsed"]]
    value <- collect(x, ...)
    elapsed[i] <- proc.time()[["elapsed"]] - t0
    if (!is.null(value)) {
      sizes[i] <- as.numeric(utils::object.size(value))
    }
    last_value <- value
  }

  structure(
    list(
      reps = reps,
      elapsed = elapsed,
      min_sec = min(elapsed),
      median_sec = stats::median(elapsed),
      max_sec = max(elapsed),
      output_size_bytes = sizes,
      output_class = if (is.null(last_value)) "NULL" else class(last_value)[1]
    ),
    class = "delarr_profile"
  )
}

#' @export
print.delarr_profile <- function(x, ...) {
  cat(
    "<delarr_profile>",
    "reps:", x$reps,
    sprintf("min/median/max: %.4f / %.4f / %.4f sec", x$min_sec, x$median_sec, x$max_sec),
    "\n"
  )
  if (any(!is.na(x$output_size_bytes))) {
    cat("output size (bytes):", round(stats::median(x$output_size_bytes, na.rm = TRUE)), "\n")
  }
  invisible(x)
}
