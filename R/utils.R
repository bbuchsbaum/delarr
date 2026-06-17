`%||%` <- function(x, y) {
  if (is.null(x)) y else x
}

normalize_chunk_margin <- function(chunk_margin, ndim, arg = "chunk_margin") {
  if (is.character(chunk_margin)) {
    return(match.arg(chunk_margin, c("cols", "rows")))
  }
  axes <- normalize_axes(chunk_margin, ndim, arg = arg)
  if (length(axes) != 1L) {
    stop(sprintf("%s must resolve to a single axis", arg), call. = FALSE)
  }
  if (ndim <= 2L) {
    return(if (axes[[1L]] == 1L) "rows" else "cols")
  }
  axes[[1L]]
}

normalize_axes <- function(axis, ndim, arg = "axis") {
  axis <- as.integer(axis)
  if (!length(axis)) {
    stop(sprintf("%s must contain at least one axis", arg), call. = FALSE)
  }
  if (anyNA(axis)) {
    stop(sprintf("%s contains NA", arg), call. = FALSE)
  }
  if (any(axis < 1L | axis > ndim)) {
    stop(sprintf("%s must be between 1 and %d", arg, ndim), call. = FALSE)
  }
  if (anyDuplicated(axis)) {
    stop(sprintf("%s cannot contain duplicates", arg), call. = FALSE)
  }
  sort(axis)
}

resolve_chunk_axis <- function(chunk_margin, ndim, default = NULL) {
  if (is.null(chunk_margin)) {
    return(default %||% min(2L, ndim))
  }
  if (is.character(chunk_margin)) {
    if (length(chunk_margin) != 1L) {
      stop("chunk_margin must be a single character value or numeric axis", call. = FALSE)
    }
    return(dim_to_axis(chunk_margin))
  }
  axes <- normalize_axes(chunk_margin, ndim, arg = "chunk_margin")
  if (length(axes) != 1L) {
    stop("chunk_margin must resolve to a single axis", call. = FALSE)
  }
  axes[[1L]]
}

infer_nd_chunk_size <- function(seed, requested_dims, axis, chunk_size,
                                target_bytes = NULL) {
  axis <- as.integer(axis)
  requested <- requested_dims[[axis]]
  if (!is.null(chunk_size) && chunk_size > 0L) {
    return(as.integer(min(chunk_size, requested)))
  }
  if (!is.null(target_bytes) && is.finite(target_bytes) && target_bytes > 0) {
    bytes_per_value <- 8L
    fixed_extent <- if (length(requested_dims) > 1L) {
      prod(requested_dims[-axis])
    } else {
      1L
    }
    denom <- max(1L, as.integer(fixed_extent)) * bytes_per_value
    adaptive <- floor(as.numeric(target_bytes) / denom)
    if (is.finite(adaptive) && adaptive >= 1L) {
      return(as.integer(min(requested, adaptive)))
    }
  }
  hint <- seed$chunk_hint
  hint_size <- NULL
  if (is.list(hint)) {
    hint_size <- hint[[paste0("axis", axis)]]
    if (axis == 1L) hint_size <- hint_size %||% hint[["rows"]]
    if (axis == 2L) hint_size <- hint_size %||% hint[["cols"]]
  }
  if (!is.null(hint_size)) {
    size <- as.integer(hint_size)
    if (!is.na(size) && size > 0L) {
      return(as.integer(min(size, requested)))
    }
  }
  default_target_bytes <- 8L * 16384L
  fixed_extent <- if (length(requested_dims) > 1L) {
    prod(requested_dims[-axis])
  } else {
    1L
  }
  fallback <- floor(default_target_bytes / max(1, fixed_extent * 8L))
  as.integer(min(requested, max(1L, fallback)))
}

normalize_index <- function(idx, n) {
  if (is.null(idx)) {
    return(seq_len(n))
  }
  if (is.logical(idx)) {
    if (anyNA(idx)) {
      stop("Logical index contains NA", call. = FALSE)
    }
    if (length(idx) != n) {
      stop("Logical index length must match dimension", call. = FALSE)
    }
    return(which(idx))
  }
  idx <- as.integer(idx)
  if (any(is.na(idx))) {
    stop("Index contains NA", call. = FALSE)
  }
  if (any(idx == 0L)) {
    stop("Index cannot contain zero", call. = FALSE)
  }
  neg <- idx[idx < 0L]
  pos <- idx[idx > 0L]
  if (length(neg) && length(pos)) {
    stop("Cannot mix positive and negative indices", call. = FALSE)
  }
  if (length(neg)) {
    return(setdiff(seq_len(n), abs(neg)))
  }
  if (any(pos > n)) {
    stop("Index out of bounds", call. = FALSE)
  }
  pos
}

seq_chunk <- function(n, size) {
  if (n <= 0L) {
    return(list())
  }
  split(seq_len(n), ceiling(seq_along(seq_len(n)) / size))
}

safe_mean <- function(x, dim, na.rm = FALSE) {
  if (requireNamespace("matrixStats", quietly = TRUE)) {
    if (identical(dim, "rows")) {
      return(matrixStats::rowMeans2(x, na.rm = na.rm))
    }
    return(matrixStats::colMeans2(x, na.rm = na.rm))
  }
  if (identical(dim, "rows")) {
    return(rowMeans(x, na.rm = na.rm))
  }
  colMeans(x, na.rm = na.rm)
}

safe_sd <- function(x, dim, na.rm = FALSE) {
  if (requireNamespace("matrixStats", quietly = TRUE)) {
    if (identical(dim, "rows")) {
      return(matrixStats::rowSds(x, na.rm = na.rm))
    }
    return(matrixStats::colSds(x, na.rm = na.rm))
  }
  if (identical(dim, "rows")) {
    return(apply(x, 1L, stats::sd, na.rm = na.rm))
  }
  apply(x, 2L, stats::sd, na.rm = na.rm)
}

safe_center <- function(x, dim, na.rm = FALSE) {
  if (identical(dim, "rows")) {
    means <- safe_mean(x, "rows", na.rm = na.rm)
    sweep(x, 1L, means, FUN = "-")
  } else {
    means <- safe_mean(x, "cols", na.rm = na.rm)
    sweep(x, 2L, means, FUN = "-")
  }
}

safe_scale_matrix <- function(x, dim, center, scale, na.rm = FALSE) {
  if (!center && !scale) {
    return(x)
  }
  if (center) {
    x <- safe_center(x, dim, na.rm = na.rm)
  }
  if (scale) {
    sds <- safe_sd(x, if (identical(dim, "rows")) "rows" else "cols", na.rm = na.rm)
    sds[sds == 0] <- 1
    margin <- if (identical(dim, "rows")) 1L else 2L
    x <- sweep(x, margin, sds, FUN = "/")
  }
  x
}

# ---- N-d axis utilities ------------------------------------------------------

#' Convert legacy dim name to integer axis
#' @keywords internal
dim_to_axis <- function(dim) {
  if (is.numeric(dim)) return(as.integer(dim))
  switch(dim, rows = 1L, cols = 2L,
         stop("dim must be 'rows', 'cols', or a numeric axis", call. = FALSE))
}

#' Compute means along an axis (2D fast path + N-d fallback)
#' @keywords internal
axis_means <- function(x, axis, na.rm = FALSE) {
  axis <- as.integer(axis)
  if (is.matrix(x)) {
    if (axis == 1L) return(safe_mean(x, "rows", na.rm = na.rm))
    if (axis == 2L) return(safe_mean(x, "cols", na.rm = na.rm))
  }
  apply(x, axis, mean, na.rm = na.rm)
}

#' Compute sds along an axis (2D fast path + N-d fallback)
#' @keywords internal
axis_sds <- function(x, axis, na.rm = FALSE) {
  axis <- as.integer(axis)
  if (is.matrix(x)) {
    if (axis == 1L) return(safe_sd(x, "rows", na.rm = na.rm))
    if (axis == 2L) return(safe_sd(x, "cols", na.rm = na.rm))
  }
  apply(x, axis, stats::sd, na.rm = na.rm)
}

#' Compute sums along an axis (2D fast path + N-d fallback)
#' @keywords internal
axis_sums <- function(x, axis, na.rm = FALSE) {
  axis <- as.integer(axis)
  if (is.matrix(x) && axis == 1L) return(rowSums(x, na.rm = na.rm))
  if (is.matrix(x) && axis == 2L) return(colSums(x, na.rm = na.rm))
  apply(x, axis, sum, na.rm = na.rm)
}

#' Sweep along an axis (generalised sweep)
#' @keywords internal
axis_sweep <- function(x, axis, stats, FUN = "-") {
  sweep(x, MARGIN = as.integer(axis), STATS = stats, FUN = FUN)
}

#' Center along an axis
#' @keywords internal
axis_center <- function(x, axis, na.rm = FALSE) {
  axis <- as.integer(axis)
  if (is.matrix(x)) {
    dim_name <- if (axis == 1L) "rows" else "cols"
    return(safe_center(x, dim_name, na.rm = na.rm))
  }
  means <- apply(x, axis, mean, na.rm = na.rm)
  sweep(x, axis, means, FUN = "-")
}

#' Scale (and optionally center) along an axis
#' @keywords internal
axis_scale <- function(x, axis, center = TRUE, scale = TRUE, na.rm = FALSE) {
  axis <- as.integer(axis)
  if (is.matrix(x)) {
    dim_name <- if (axis == 1L) "rows" else "cols"
    return(safe_scale_matrix(x, dim_name, center = center, scale = scale,
                             na.rm = na.rm))
  }
  if (!center && !scale) return(x)
  if (center) {
    means <- apply(x, axis, mean, na.rm = na.rm)
    x <- sweep(x, axis, means, FUN = "-")
  }
  if (scale) {
    sds <- apply(x, axis, stats::sd, na.rm = na.rm)
    sds[sds == 0] <- 1
    x <- sweep(x, axis, sds, FUN = "/")
  }
  x
}

#' Detrend along an axis
#' @keywords internal
axis_detrend <- function(x, axis, degree) {
  axis <- as.integer(axis)
  if (is.matrix(x)) {
    dim_name <- if (axis == 1L) "rows" else "cols"
    return(detrend_matrix(x, dim_name, degree))
  }
  # For N-d: the "along" dimension provides the sequence for detrending.
  # We apply over all other dimensions, fitting polynomial along axis.
  along_len <- dim(x)[axis]
  seq_along_axis <- seq_len(along_len)
  design <- cbind(1, stats::poly(seq_along_axis, degree, raw = TRUE))
  ndim <- length(dim(x))
  other_axes <- setdiff(seq_len(ndim), axis)
  res <- apply(x, other_axes, function(y) {
    fit <- stats::lm.fit(design, y)
    y - as.vector(design %*% fit$coefficients)
  })
  # apply() puts the collapsed axis first; permute back to original order
  # Result has dim c(along_len, other_dims...)
  # We need to move the first axis back to position `axis`
  if (ndim > 2L) {
    perm <- integer(ndim)
    perm[axis] <- 1L
    perm[other_axes] <- seq_along(other_axes) + 1L
    res <- aperm(res, perm)
  }
  res
}

#' Extract a sub-array along one axis
#' @keywords internal
extract_axis_chunk <- function(x, axis, positions) {
  ndim <- length(dim(x))
  idx <- rep(list(TRUE), ndim)
  idx[[axis]] <- positions
  do.call(`[`, c(list(x), idx, list(drop = FALSE)))
}

#' Assign a chunk into an array along one axis
#' @keywords internal
assign_axis_chunk <- function(x, block, axis, positions) {
  ndim <- length(dim(x))
  idx <- rep(list(TRUE), ndim)
  idx[[axis]] <- positions
  do.call(`[<-`, c(list(x), idx, list(value = block)))
}

# ---- 2D helpers (original) ---------------------------------------------------

safe_min <- function(x, dim, na.rm = FALSE) {
  if (requireNamespace("matrixStats", quietly = TRUE)) {
    if (identical(dim, "rows")) {
      return(matrixStats::rowMins(x, na.rm = na.rm))
    }
    return(matrixStats::colMins(x, na.rm = na.rm))
  }
  margin <- if (identical(dim, "rows")) 1L else 2L
  suppressWarnings(apply(x, margin, min, na.rm = na.rm))
}

safe_max <- function(x, dim, na.rm = FALSE) {
  if (requireNamespace("matrixStats", quietly = TRUE)) {
    if (identical(dim, "rows")) {
      return(matrixStats::rowMaxs(x, na.rm = na.rm))
    }
    return(matrixStats::colMaxs(x, na.rm = na.rm))
  }
  margin <- if (identical(dim, "rows")) 1L else 2L
  suppressWarnings(apply(x, margin, max, na.rm = na.rm))
}
