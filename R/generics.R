#' Row means for delayed matrices
#'
#' Generic counterpart to `matrixStats::rowMeans2()`. Methods are provided for
#' `delarr` objects, but packages can extend the generic for their own delayed
#' types.
#'
#' @param x An object for which row means should be computed.
#' @param ... Additional arguments passed to methods.
#'
#' @return Typically a numeric vector of row means.
#' @export
rowMeans2 <- function(x, ...) {
  UseMethod("rowMeans2")
}

#' Column means for delayed matrices
#'
#' Generic counterpart to `matrixStats::colMeans2()`. Methods are provided for
#' `delarr` objects, but packages can extend the generic for their own delayed
#' types.
#'
#' @inheritParams rowMeans2
#'
#' @return Typically a numeric vector of column means.
#' @export
colMeans2 <- function(x, ...) {
  UseMethod("colMeans2")
}
