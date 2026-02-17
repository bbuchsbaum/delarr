is_scalar_atomic <- function(x) {
  is.atomic(x) && length(x) == 1L && !is.na(x)
}

is_noop_const_op <- function(op) {
  if (!identical(op$op, "emap_const") || !is_scalar_atomic(op$const)) {
    return(FALSE)
  }
  name <- op$op_name %||% ""
  val <- op$const
  if (identical(name, "+") && identical(val, 0)) {
    return(TRUE)
  }
  if (identical(name, "-") && identical(op$side, "right") && identical(val, 0)) {
    return(TRUE)
  }
  if (identical(name, "*") && identical(val, 1)) {
    return(TRUE)
  }
  if (identical(name, "/") && identical(op$side, "right") && identical(val, 1)) {
    return(TRUE)
  }
  FALSE
}

# Helper to fuse two emap functions without the closure-in-loop bug.
# By calling a function, `f1` and `f2` are forced and captured in a fresh
# execution frame, avoiding stale references when the loop continues.
make_fused_fn <- function(f1, f2) {
  force(f1)
  force(f2)
  function(m) f2(f1(m))
}

optimize_ops <- function(ops) {
  if (!length(ops)) {
    return(ops)
  }
  out <- list()
  for (op in ops) {
    if (is_noop_const_op(op)) {
      next
    }
    if (identical(op$op, "emap") &&
        length(out) > 0 &&
        identical(out[[length(out)]]$op, "emap")) {
      prev <- out[[length(out)]]
      out[[length(out)]] <- list(
        op = "emap",
        fn = make_fused_fn(prev$fn, op$fn)
      )
      next
    }
    out <- append(out, list(op))
  }
  out
}

#' Optimize a delayed pipeline
#'
#' Applies lightweight algebraic simplifications to reduce unnecessary work
#' during `collect()`.
#'
#' @param x A `delarr` object.
#'
#' @return A `delarr` with an optimized operation list.
#' @export
optimize_delarr <- function(x) {
  stopifnot(inherits(x, "delarr"))
  new_delarr(seed = x$seed, ops = optimize_ops(x$ops))
}
