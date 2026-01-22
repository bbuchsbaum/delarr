#' Construct a seed backend for `delarr`
#'
#' Seeds encapsulate storage access for delayed matrices. They define matrix
#' dimensions and a `pull()` function that returns materialised slices.
#'
#' @param nrow,ncol Number of rows and columns.
#' @param pull A function accepting `rows` and `cols` indices and returning a
#'   base matrix slice.
#' @param chunk_hint Optional list describing preferred chunk sizes
#'   (e.g. `list(cols = 4096L)`).
#' @param dimnames Optional list of dimnames to expose lazily.
#' @param begin Optional function invoked before streaming begins.
#' @param end Optional function invoked after streaming completes.
#'
#' @return An object of class `delarr_seed`.
#' @export
delarr_seed <- function(nrow, ncol, pull, chunk_hint = NULL, dimnames = NULL,
                        begin = NULL, end = NULL) {
  if (!is.numeric(nrow) || length(nrow) != 1L || nrow < 0) {
    stop("nrow must be a single non-negative number", call. = FALSE)
  }
  if (!is.numeric(ncol) || length(ncol) != 1L || ncol < 0) {
    stop("ncol must be a single non-negative number", call. = FALSE)
  }
  if (!is.function(pull)) {
    stop("pull must be a function", call. = FALSE)
  }
  structure(
    list(
      nrow = as.integer(nrow),
      ncol = as.integer(ncol),
      pull = pull,
      chunk_hint = chunk_hint,
      dimnames = dimnames,
      begin = begin,
      end = end
    ),
    class = "delarr_seed"
  )
}

#' Dimensions for a `delarr_seed`
#'
#' @param x A `delarr_seed`.
#'
#' @return A two-element integer vector.
dim.delarr_seed <- function(x) {
  c(x$nrow, x$ncol)
}

pull_seed <- function(seed, rows = NULL, cols = NULL) {
  rows <- if (!is.null(rows)) as.integer(rows) else rows
  cols <- if (!is.null(cols)) as.integer(cols) else cols
  res <- seed$pull(rows = rows, cols = cols)
  if (!is.matrix(res)) {
    stop("Seed pull must return a matrix", call. = FALSE)
  }
  res
}
