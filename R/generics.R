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
#' @examples
#' mat <- matrix(1:12, nrow = 3, ncol = 4)
#' darr <- delarr(mat)
#'
#' # Compute row means lazily
#' rowMeans2(darr)
#'
#' # Compare with base R
#' rowMeans(mat)
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
#' @examples
#' mat <- matrix(1:12, nrow = 3, ncol = 4)
#' darr <- delarr(mat)
#'
#' # Compute column means lazily
#' colMeans2(darr)
#'
#' # Compare with base R
#' colMeans(mat)
colMeans2 <- function(x, ...) {
  UseMethod("colMeans2")
}
