#' Apply an elementwise transformation lazily
#'
#' @param x A `delarr`.
#' @param f A function or formula suitable for `rlang::as_function()`.
#'
#' @return A `delarr` representing the transformation.
#' @export
d_map <- function(x, f) {
  stopifnot(inherits(x, "delarr"))
  fn <- rlang::as_function(f)
  add_op(x, list(op = "emap", fn = fn))
}

#' Apply a binary elementwise transformation lazily
#'
#' @param x A `delarr`.
#' @param y Another `delarr`, matrix, or numeric vector/scalar.
#' @param f A function or formula combining two arguments.
#'
#' @return A `delarr` representing the fused binary operation.
#' @export
d_map2 <- function(x, y, f) {
  stopifnot(inherits(x, "delarr"))
  fn <- rlang::as_function(f)
  if (!(inherits(y, "delarr") || is.numeric(y) || is.matrix(y))) {
    stop("y must be a delarr or numeric", call. = FALSE)
  }
  add_op(x, list(op = "emap2", rhs = y, fn = fn))
}

#' Reduce along rows or columns lazily
#'
#' @param x A `delarr`.
#' @param f A reduction function (defaults to `sum`).
#' @param dim Dimension to reduce, either "rows" or "cols".
#' @param na.rm Logical; remove missing values while reducing.
#'
#' @return A `delarr` capturing the reduction.
#' @export
d_reduce <- function(x, f = base::sum, dim = c("rows", "cols"), na.rm = FALSE) {
  stopifnot(inherits(x, "delarr"))
  dim <- match.arg(dim)
  fn <- match.fun(f)
  add_op(x, list(op = "reduce", fn = fn, dim = dim, na_rm = isTRUE(na.rm)))
}

#' Center a delayed matrix along rows or columns
#'
#' @param x A `delarr`.
#' @param dim Dimension along which to subtract the mean.
#' @param na.rm Logical; remove missing values when computing the centre.
#'
#' @return A `delarr` with a deferred centering operation.
#' @export
d_center <- function(x, dim = c("rows", "cols"), na.rm = FALSE) {
  stopifnot(inherits(x, "delarr"))
  dim <- match.arg(dim)
  add_op(x, list(op = "center", dim = dim, na_rm = isTRUE(na.rm)))
}

#' Scale a delayed matrix along rows or columns
#'
#' @param x A `delarr`.
#' @param dim Dimension to scale.
#' @param center Logical; subtract the mean before scaling.
#' @param scale Logical; divide by the standard deviation.
#' @param na.rm Logical; remove missing values when computing statistics.
#'
#' @return A `delarr` with a deferred scaling operation.
#' @export
d_scale <- function(x, dim = c("rows", "cols"), center = TRUE, scale = TRUE,
                    na.rm = FALSE) {
  stopifnot(inherits(x, "delarr"))
  dim <- match.arg(dim)
  add_op(x, list(
    op = "scale",
    dim = dim,
    center = center,
    scale = scale,
    na_rm = isTRUE(na.rm)
  ))
}

#' Z-score a delayed matrix
#'
#' Equivalent to centering and scaling with unit variance.
#'
#' @param x A `delarr`.
#' @param dim Dimension over which to compute the z-score.
#' @param na.rm Logical; remove missing values when computing statistics.
#'
#' @return A `delarr` with the z-score applied lazily.
#' @export
d_zscore <- function(x, dim = c("rows", "cols"), na.rm = FALSE) {
  stopifnot(inherits(x, "delarr"))
  dim <- match.arg(dim)
  add_op(x, list(op = "zscore", dim = dim, na_rm = isTRUE(na.rm)))
}

#' Detrend a delayed matrix
#'
#' Removes a polynomial trend of the specified degree along the chosen
#' dimension.
#'
#' @param x A `delarr`.
#' @param dim Dimension along which to fit the trend.
#' @param degree Polynomial degree (default 1).
#'
#' @return A `delarr` with the detrend operation queued.
#' @export
d_detrend <- function(x, dim = c("rows", "cols"), degree = 1L) {
  stopifnot(inherits(x, "delarr"))
  dim <- match.arg(dim)
  add_op(x, list(op = "detrend", dim = dim, degree = as.integer(degree)))
}

#' Apply a boolean mask to a delayed matrix
#'
#' Elements failing the predicate are replaced with `fill` at materialisation
#' time.
#'
#' @param x A `delarr`.
#' @param predicate A function or formula returning a logical matrix.
#' @param fill Replacement value for elements where the predicate is `FALSE`.
#'
#' @return A `delarr` including the mask.
#' @export
d_where <- function(x, predicate, fill = 0) {
  stopifnot(inherits(x, "delarr"))
  fn <- rlang::as_function(predicate)
  add_op(x, list(op = "where", predicate = fn, fill = fill))
}

#' Row means for a delayed matrix
#'
#' Computes row means lazily via `d_reduce()`; acts as a drop-in replacement for
#' `matrixStats::rowMeans2()`.
#'
#' @param x A `delarr` object.
#' @param ... Unused.
#' @param na.rm Logical; remove missing values before averaging.
#'
#' @return A numeric vector of row means.
rowMeans2.delarr <- function(x, ..., na.rm = FALSE) {
  res <- d_reduce(x, base::mean, dim = "rows", na.rm = na.rm)
  collect(res)
}

#' Column means for a delayed matrix
#'
#' Computes column means lazily via `d_reduce()`; acts as a drop-in replacement
#' for `matrixStats::colMeans2()`.
#'
#' @param x A `delarr` object.
#' @param ... Unused.
#' @param na.rm Logical; remove missing values before averaging.
#'
#' @return A numeric vector of column means.
colMeans2.delarr <- function(x, ..., na.rm = FALSE) {
  res <- d_reduce(x, base::mean, dim = "cols", na.rm = na.rm)
  collect(res)
}
