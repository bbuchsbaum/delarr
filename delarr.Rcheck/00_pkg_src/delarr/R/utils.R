`%||%` <- function(x, y) {
  if (is.null(x)) y else x
}

normalize_index <- function(idx, n) {
  if (is.null(idx)) {
    return(seq_len(n))
  }
  if (is.logical(idx)) {
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
