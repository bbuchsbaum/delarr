#' Transpose a delayed matrix
#'
#' @param x A `delarr`.
#' @param chunk_size Optional chunk size used for internal pulls.
#'
#' @return A transposed `delarr`.
#' @export
d_transpose <- function(x, chunk_size = NULL) {
  d_aperm(x, c(2L, 1L), chunk_size = chunk_size)
}

pull_delarr_block <- function(x, indices, chunk_size = NULL) {
  if (!length(x$ops)) {
    if (length(indices) == 2L && !is_nd_seed(x$seed)) {
      return(pull_seed(x$seed, rows = indices[[1L]], cols = indices[[2L]]))
    }
    return(pull_seed_nd(x$seed, indices))
  }
  collect(do.call(`[`, c(list(x), indices, list(drop = FALSE))),
          chunk_size = chunk_size)
}

#' Permute dimensions of a delayed array
#'
#' @param x A `delarr`.
#' @param perm A permutation of `seq_along(dim(x))`.
#' @param chunk_size Optional chunk size used for internal pulls.
#'
#' @return A permuted `delarr`.
#' @export
d_aperm <- function(x, perm = rev(seq_along(dim(x))), chunk_size = NULL) {
  stopifnot(inherits(x, "delarr"))
  dx <- dim(x)
  ndim <- length(dx)
  perm <- as.integer(perm)
  if (!length(perm) || length(perm) != ndim) {
    stop("perm must have one entry per dimension", call. = FALSE)
  }
  if (anyNA(perm) || !setequal(perm, seq_len(ndim))) {
    stop("perm must be a permutation of seq_along(dim(x))", call. = FALSE)
  }
  if (identical(perm, seq_len(ndim))) {
    return(x)
  }

  source_dimnames <- dimnames(x)
  out_dimnames <- source_dimnames[perm]
  hint <- x$seed$chunk_hint
  chunk_hint <- NULL
  if (is.list(hint)) {
    chunk_hint <- stats::setNames(vector("list", ndim), paste0("axis", seq_len(ndim)))
    for (out_axis in seq_len(ndim)) {
      src_axis <- perm[[out_axis]]
      hint_value <- hint[[paste0("axis", src_axis)]]
      if (is.null(hint_value) && src_axis == 1L) hint_value <- hint[["rows"]]
      if (is.null(hint_value) && src_axis == 2L) hint_value <- hint[["cols"]]
      chunk_hint[[out_axis]] <- hint_value
    }
    chunk_hint$rows <- chunk_hint[[1L]]
    if (ndim >= 2L) {
      chunk_hint$cols <- chunk_hint[[2L]]
    }
  }

  if (ndim == 2L) {
    return(delarr_backend(
      nrow = dx[[perm[[1L]]]],
      ncol = dx[[perm[[2L]]]],
      pull = function(rows = NULL, cols = NULL) {
        out_indices <- list(
          rows %||% seq_len(dx[[perm[[1L]]]]),
          cols %||% seq_len(dx[[perm[[2L]]]])
        )
        src_indices <- vector("list", 2L)
        src_indices[perm] <- out_indices
        block <- pull_delarr_block(x, src_indices, chunk_size = chunk_size)
        aperm(block, perm = perm)
      },
      chunk_hint = chunk_hint,
      dimnames = out_dimnames
    ))
  }

  delarr(delarr_seed_nd(
    dims = dx[perm],
    pull = function(indices) {
      src_indices <- vector("list", ndim)
      src_indices[perm] <- indices
      block <- pull_delarr_block(x, src_indices, chunk_size = chunk_size)
      aperm(block, perm = perm)
    },
    chunk_hint = chunk_hint,
    dimnames = out_dimnames
  ))
}

#' @export
t.delarr <- function(x) {
  d_transpose(x)
}

#' Delayed matrix multiplication
#'
#' @param x A `delarr` or base matrix.
#' @param y A `delarr` or base matrix.
#' @param chunk_size Optional chunk size used during block pulls.
#'
#' @return A `delarr` representing `%*%`.
#' @export
d_matmul <- function(x, y, chunk_size = NULL) {
  if (!inherits(x, "delarr")) {
    x <- delarr(x)
  }
  if (!inherits(y, "delarr")) {
    y <- delarr(y)
  }
  dx <- dim(x)
  dy <- dim(y)
  if (!identical(dx[2], dy[1])) {
    stop("Non-conformable arguments for matrix multiplication", call. = FALSE)
  }
  state <- new.env(parent = emptyenv())
  state$lhs_rows_key <- NULL
  state$lhs_blocks <- list()
  state$rhs_cols_key <- NULL
  state$rhs_blocks <- list()
  inner_chunk <- as.integer(max(
    1L,
    min(
      dx[2],
      chunk_size %||%
        x$seed$chunk_hint$cols %||%
        y$seed$chunk_hint$rows %||%
        256L
    )
  ))
  inner_chunks <- seq_chunk(dx[2], inner_chunk)

  fetch_lhs_block <- function(rows, kpos) {
    rows_key <- paste(rows, collapse = ",")
    k_key <- paste(kpos, collapse = ",")
    if (!identical(state$lhs_rows_key, rows_key)) {
      state$lhs_rows_key <- rows_key
      state$lhs_blocks <- list()
    }
    if (!is.null(state$lhs_blocks[[k_key]])) {
      return(state$lhs_blocks[[k_key]])
    }
    block <- collect(
      x[rows, kpos, drop = FALSE],
      chunk_size = max(1L, length(kpos)),
      chunk_margin = "cols"
    )
    state$lhs_blocks[[k_key]] <- block
    block
  }

  fetch_rhs_block <- function(kpos, cols) {
    cols_key <- paste(cols, collapse = ",")
    k_key <- paste(kpos, collapse = ",")
    if (!identical(state$rhs_cols_key, cols_key)) {
      state$rhs_cols_key <- cols_key
      state$rhs_blocks <- list()
    }
    if (!is.null(state$rhs_blocks[[k_key]])) {
      return(state$rhs_blocks[[k_key]])
    }
    block <- collect(
      y[kpos, cols, drop = FALSE],
      chunk_size = max(1L, length(cols)),
      chunk_margin = "cols"
    )
    state$rhs_blocks[[k_key]] <- block
    block
  }

  delarr_backend(
    nrow = dx[1],
    ncol = dy[2],
    pull = function(rows = NULL, cols = NULL) {
      rows <- rows %||% seq_len(dx[1])
      cols <- cols %||% seq_len(dy[2])
      if (!length(rows) || !length(cols)) {
        return(matrix(0, nrow = length(rows), ncol = length(cols)))
      }
      out <- NULL
      for (kpos in inner_chunks) {
        lhs <- fetch_lhs_block(rows, kpos)
        rhs <- fetch_rhs_block(kpos, cols)
        partial <- lhs %*% rhs
        out <- if (is.null(out)) partial else out + partial
      }
      out
    },
    chunk_hint = list(cols = y$seed$chunk_hint$cols %||% 1024L),
    dimnames = list(dimnames(x)[[1L]], dimnames(y)[[2L]])
  )
}

#' Run multiple reductions and collect results
#'
#' @param x A `delarr`.
#' @param fns A named list of reduction functions.
#' @param dim Reduction dimension (`"rows"` or `"cols"`).
#' @param na.rm Logical; remove missing values in each reducer.
#' @param chunk_size Optional chunk size passed to `collect()`.
#' @param simplify Logical; combine equal-length outputs into a matrix.
#'
#' @return A named list (or matrix when `simplify = TRUE`) of reductions.
#' @export
d_reduce_many <- function(x, fns, dim = c("rows", "cols"), na.rm = FALSE,
                          chunk_size = NULL, simplify = TRUE) {
  stopifnot(inherits(x, "delarr"))
  dim <- match.arg(dim)
  if (is.function(fns)) {
    fns <- list(fn1 = fns)
  }
  if (!is.list(fns) || length(fns) == 0L) {
    stop("fns must be a non-empty list of functions", call. = FALSE)
  }
  if (is.null(names(fns)) || any(names(fns) == "")) {
    names(fns) <- paste0("fn", seq_along(fns))
  }

  out <- collect_reduce_many_streamed(
    x,
    fns = fns,
    dim = dim,
    na.rm = isTRUE(na.rm),
    chunk_size = chunk_size
  )
  if (is.null(out)) {
    out <- lapply(fns, function(fn) {
      collect(d_reduce(x, fn, dim = dim, na.rm = na.rm), chunk_size = chunk_size)
    })
  }
  names(out) <- names(fns)

  lengths <- vapply(out, length, integer(1))
  if (isTRUE(simplify) && length(unique(lengths)) == 1L) {
    mat <- do.call(cbind, out)
    colnames(mat) <- names(out)
    first_names <- names(out[[1L]])
    if (!is.null(first_names)) {
      rownames(mat) <- first_names
    }
    return(mat)
  }
  out
}

collect_reduce_many_streamed <- function(x, fns, dim, na.rm, chunk_size) {
  x <- optimize_delarr(x)
  plan <- compile_plan(x)
  if (!is.null(plan$reduce) || requires_full_eval(plan$ops)) {
    return(NULL)
  }

  specs <- lapply(fns, function(fn) {
    fn <- match.fun(fn)
    info <- classify_reduce(list(fn = fn, dim = dim, na_rm = na.rm))
    list(fn = fn, type = info$type)
  })
  builtin <- vapply(specs, function(spec) spec$type != "generic", logical(1))
  if (!any(builtin)) {
    return(NULL)
  }

  seed <- x$seed
  rows <- plan$rows %||% seq_len(seed$nrow)
  cols <- plan$cols %||% seq_len(seed$ncol)
  n_rows <- length(rows)
  n_cols <- length(cols)
  if (n_rows == 0L || n_cols == 0L) {
    return(NULL)
  }

  rhs_contexts <- vector("list", length(plan$ops))
  rhs_precomputed <- vector("list", length(plan$ops))
  if (plan$pair_rhs) {
    for (idx in plan$rhs_indices) {
      rhs_obj <- plan$ops[[idx]]$rhs
      if (!inherits(rhs_obj, "delarr")) {
        next
      }
      rhs_plan <- compile_plan(rhs_obj)
      rhs_seed <- rhs_obj$seed
      rhs_rows <- rhs_plan$rows %||% seq_len(rhs_seed$nrow)
      rhs_cols <- rhs_plan$cols %||% seq_len(rhs_seed$ncol)
      chunk_compatible <- is.null(rhs_plan$reduce) &&
        !requires_full_eval(rhs_plan$ops) &&
        length(rhs_rows) == n_rows &&
        length(rhs_cols) == n_cols
      if (chunk_compatible) {
        rhs_contexts[[idx]] <- list(
          seed = rhs_seed,
          plan = rhs_plan,
          rows = rhs_rows,
          cols = rhs_cols
        )
        if (is.function(rhs_seed$begin)) rhs_seed$begin()
        on.exit({
          if (is.function(rhs_seed$end)) rhs_seed$end()
        }, add = TRUE)
      } else {
        rhs_precomputed[[idx]] <- collect(rhs_obj)
      }
    }
  }

  rhs_chunks_for <- function(pos) {
    chunks <- vector("list", length(plan$ops))
    for (idx in plan$rhs_indices) {
      ctx <- rhs_contexts[[idx]]
      if (!is.null(ctx)) {
        rhs_cols <- ctx$cols[pos]
        rhs_block <- pull_seed(ctx$seed, rows = ctx$rows, cols = rhs_cols)
        rhs_block <- apply_ops(
          rhs_block,
          ctx$plan$ops,
          chunk_context = list(
            rows = seq_len(length(ctx$rows)),
            cols = pos,
            full_nrow = length(ctx$rows),
            full_ncol = length(ctx$cols)
          )
        )
        chunks[[idx]] <- rhs_block
        next
      }
      rhs_val <- rhs_precomputed[[idx]]
      if (is.null(rhs_val)) {
        next
      }
      if (is.matrix(rhs_val) && all(dim(rhs_val) == c(n_rows, n_cols))) {
        chunks[[idx]] <- rhs_val[, pos, drop = FALSE]
      } else {
        chunks[[idx]] <- rhs_val
      }
    }
    if (!any(vapply(chunks, Negate(is.null), logical(1)))) {
      return(NULL)
    }
    chunks
  }

  if (is.function(seed$begin)) seed$begin()
  on.exit({
    if (is.function(seed$end)) seed$end()
  }, add = TRUE)

  resolved_chunk <- infer_chunk_size(
    seed = seed,
    requested_rows = n_rows,
    requested_cols = n_cols,
    chunk_size = chunk_size,
    margin = "cols"
  )
  chunks <- seq_chunk(n_cols, resolved_chunk)

  types <- vapply(specs, `[[`, character(1), "type")
  need_sum <- any(types %in% c("sum", "mean"))
  need_min <- any(types == "min")
  need_max <- any(types == "max")
  need_counts <- na.rm && any(types %in% c("sum", "mean", "min", "max"))
  need_mean_counts <- any(types == "mean")

  if (identical(dim, "rows")) {
    sum_acc <- if (need_sum) numeric(n_rows) else NULL
    counts_acc <- if (need_counts || need_mean_counts) numeric(n_rows) else NULL
    min_acc <- NULL
    max_acc <- if (need_max) NULL else NULL
    for (pos in chunks) {
      block <- pull_seed(seed, rows = rows, cols = cols[pos])
      rhs_chunks <- rhs_chunks_for(pos)
      block <- apply_ops(
        block,
        plan$ops,
        rhs_chunks,
        chunk_context = list(
          rows = seq_len(n_rows),
          cols = pos,
          full_nrow = n_rows,
          full_ncol = n_cols
        )
      )
      if (need_sum) {
        sum_acc <- sum_acc + rowSums(block, na.rm = na.rm)
      }
      if (!is.null(counts_acc)) {
        counts_acc <- counts_acc + rowSums(!is.na(block))
      }
      if (need_min) {
        partial <- safe_min(block, "rows", na.rm = na.rm)
        min_acc <- if (is.null(min_acc)) partial else pmin(min_acc, partial, na.rm = na.rm)
      }
      if (need_max) {
        partial <- safe_max(block, "rows", na.rm = na.rm)
        max_acc <- if (is.null(max_acc)) partial else pmax(max_acc, partial, na.rm = na.rm)
      }
    }
    result_names <- dimnames(x)[[1L]]
    builtins <- lapply(specs, function(spec) {
      res <- switch(spec$type,
        sum = {
          out <- sum_acc
          if (na.rm && !is.null(counts_acc)) out[counts_acc == 0] <- NA_real_
          out
        },
        mean = {
          out <- sum_acc
          if (na.rm && !is.null(counts_acc)) {
            out[counts_acc == 0] <- NA_real_
            idx <- counts_acc > 0
            out[idx] <- out[idx] / counts_acc[idx]
          } else {
            out <- out / n_cols
          }
          out
        },
        min = {
          out <- min_acc
          if (na.rm && !is.null(counts_acc)) out[counts_acc == 0] <- NA_real_
          out
        },
        max = {
          out <- max_acc
          if (na.rm && !is.null(counts_acc)) out[counts_acc == 0] <- NA_real_
          out
        },
        NULL
      )
      if (is.null(res)) {
        return(NULL)
      }
      names(res) <- result_names
      res
    })
  } else {
    sum_acc <- if (need_sum) numeric(n_cols) else NULL
    counts_acc <- if (need_counts || need_mean_counts) numeric(n_cols) else NULL
    min_acc <- if (need_min) rep(NA_real_, n_cols) else NULL
    max_acc <- if (need_max) rep(NA_real_, n_cols) else NULL
    for (pos in chunks) {
      block <- pull_seed(seed, rows = rows, cols = cols[pos])
      rhs_chunks <- rhs_chunks_for(pos)
      block <- apply_ops(
        block,
        plan$ops,
        rhs_chunks,
        chunk_context = list(
          rows = seq_len(n_rows),
          cols = pos,
          full_nrow = n_rows,
          full_ncol = n_cols
        )
      )
      if (need_sum) {
        sum_acc[pos] <- sum_acc[pos] + colSums(block, na.rm = na.rm)
      }
      if (!is.null(counts_acc)) {
        counts_acc[pos] <- counts_acc[pos] + colSums(!is.na(block))
      }
      if (need_min) {
        partial <- safe_min(block, "cols", na.rm = na.rm)
        missing <- is.na(min_acc[pos])
        if (any(missing)) {
          min_acc[pos][missing] <- partial[missing]
        }
        if (any(!missing)) {
          min_acc[pos][!missing] <- pmin(min_acc[pos][!missing], partial[!missing], na.rm = na.rm)
        }
      }
      if (need_max) {
        partial <- safe_max(block, "cols", na.rm = na.rm)
        missing <- is.na(max_acc[pos])
        if (any(missing)) {
          max_acc[pos][missing] <- partial[missing]
        }
        if (any(!missing)) {
          max_acc[pos][!missing] <- pmax(max_acc[pos][!missing], partial[!missing], na.rm = na.rm)
        }
      }
    }
    result_names <- dimnames(x)[[2L]]
    builtins <- lapply(specs, function(spec) {
      res <- switch(spec$type,
        sum = {
          out <- sum_acc
          if (na.rm && !is.null(counts_acc)) out[counts_acc == 0] <- NA_real_
          out
        },
        mean = {
          out <- sum_acc
          if (na.rm && !is.null(counts_acc)) {
            out[counts_acc == 0] <- NA_real_
            idx <- counts_acc > 0
            out[idx] <- out[idx] / counts_acc[idx]
          } else {
            out <- out / n_rows
          }
          out
        },
        min = {
          out <- min_acc
          if (na.rm && !is.null(counts_acc)) out[counts_acc == 0] <- NA_real_
          out
        },
        max = {
          out <- max_acc
          if (na.rm && !is.null(counts_acc)) out[counts_acc == 0] <- NA_real_
          out
        },
        NULL
      )
      if (is.null(res)) {
        return(NULL)
      }
      names(res) <- result_names
      res
    })
  }

  out <- vector("list", length(specs))
  for (i in seq_along(specs)) {
    if (builtin[[i]]) {
      out[[i]] <- builtins[[i]]
    } else {
      out[[i]] <- collect(
        d_reduce(x, specs[[i]]$fn, dim = dim, na.rm = na.rm),
        chunk_size = chunk_size,
        optimize = FALSE
      )
    }
  }
  out
}
