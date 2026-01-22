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
  if (!is.logical(mask)) {
    mask <- as.logical(mask)
  }
  if (!all(dim(mask) == dim(mat))) {
    mask <- matrix(mask, nrow = nrow(mat), ncol = ncol(mat))
  }
  if (length(fill) != 1L) {
    stop("fill must be a scalar value", call. = FALSE)
  }
  mat[!mask] <- fill
  mat
}
