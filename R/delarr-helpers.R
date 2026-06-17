detrend_matrix <- function(mat, dim, degree) {
  if (degree < 1L) {
    stop("degree must be >= 1", call. = FALSE)
  }
  if (identical(dim, "rows")) {
    x <- seq_len(ncol(mat))
    design <- stats::poly(x, degree, raw = TRUE)
    design <- cbind(1, design)
    res <- t(apply(mat, 1L, function(y) {
      fit <- stats::lm.fit(design, y)
      y - as.vector(design %*% fit$coefficients)
    }))
    return(res)
  }
  x <- seq_len(nrow(mat))
  design <- stats::poly(x, degree, raw = TRUE)
  design <- cbind(1, design)
  apply(mat, 2L, function(y) {
    fit <- stats::lm.fit(design, y)
    y - as.vector(design %*% fit$coefficients)
  })
}

where_mask <- function(mat, predicate, fill) {
  mask <- predicate(mat)
  mask_dims <- dim(mask)
  if (!is.logical(mask)) {
    mask <- as.logical(mask)
    if (!is.null(mask_dims)) {
      dim(mask) <- mask_dims
    }
  }
  if (is.null(mask_dims)) {
    if (length(mask) != length(mat)) {
      stop("predicate must return a conformable mask with one value per element", call. = FALSE)
    }
    dim(mask) <- dim(mat)
  } else if (!identical(mask_dims, dim(mat))) {
    stop("predicate must return a mask conformable to the input", call. = FALSE)
  }
  if (length(fill) != 1L) {
    stop("fill must be a scalar value", call. = FALSE)
  }
  mat[!mask] <- fill
  mat
}
