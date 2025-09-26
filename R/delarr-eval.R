compile_plan <- function(x) {
  stopifnot(inherits(x, "delarr"))
  current_rows <- seq_len(x$seed$nrow)
  current_cols <- seq_len(x$seed$ncol)
  ops <- list()
  reduce_op <- NULL
  rhs_indices <- integer()
  for (op in x$ops) {
    if (identical(op$op, "slice")) {
      if (!is.null(op$rows)) {
        current_rows <- current_rows[normalize_index(op$rows, length(current_rows))]
      }
      if (!is.null(op$cols)) {
        current_cols <- current_cols[normalize_index(op$cols, length(current_cols))]
      }
      next
    }
    if (identical(op$op, "reduce")) {
      if (!is.null(reduce_op)) {
        stop("Only one reduce() is supported in a pipeline", call. = FALSE)
      }
      reduce_op <- op
      next
    }
    if (identical(op$op, "emap2") && inherits(op$rhs, "delarr")) {
      rhs_indices <- c(rhs_indices, length(ops) + 1L)
    }
    ops <- append(ops, list(op))
  }
list(
    rows = current_rows,
    cols = current_cols,
    ops = ops,
    reduce = reduce_op,
    rhs_indices = rhs_indices,
    pair_rhs = length(rhs_indices) > 0
  )
}

requires_full_eval <- function(ops) {
  any(vapply(ops, function(op) {
    op$op %in% c("center", "scale", "zscore") && identical(op$dim, "rows")
  }, logical(1)))
}

broadcast_rhs <- function(lhs, rhs) {
  if (is.null(rhs)) {
    stop("Binary operation requires a RHS", call. = FALSE)
  }
  if (length(rhs) == 1L && is.atomic(rhs)) {
    return(rhs)
  }
  if (is.matrix(rhs)) {
    if (!all(dim(rhs) == dim(lhs))) {
      stop("Non-conformable RHS for binary op", call. = FALSE)
    }
    return(rhs)
  }
  if (is.atomic(rhs)) {
    len <- length(rhs)
    nr <- nrow(lhs)
    nc <- ncol(lhs)
    if (len == nr) {
      return(matrix(rhs, nr, nc))
    }
    if (len == nc) {
      return(matrix(rhs, nr, nc, byrow = TRUE))
    }
  }
  stop("Non-conformable RHS for binary operation", call. = FALSE)
}

apply_ops <- function(mat, ops, rhs_chunks = NULL) {
  if (!length(ops)) {
    return(mat)
  }
  for (i in seq_along(ops)) {
    op <- ops[[i]]
    mat <- switch(op$op,
      emap = {
        res <- op$fn(mat)
        if (!is.matrix(res)) {
          stop("d_map functions must return a matrix", call. = FALSE)
        }
        res
      },
      emap_const = {
        const <- broadcast_rhs(mat, op$const)
        if (identical(op$side, "right")) op$fn(mat, const) else op$fn(const, mat)
      },
      emap2 = {
        rhs <- op$rhs
        if (inherits(rhs, "delarr")) {
          if (!is.null(rhs_chunks) && !is.null(rhs_chunks[[i]])) {
            rhs <- rhs_chunks[[i]]
          } else {
            rhs <- collect(rhs)
          }
        }
        rhs <- broadcast_rhs(mat, rhs)
        op$fn(mat, rhs)
      },
      center = safe_center(mat, op$dim, op$na_rm %||% FALSE),
      scale = safe_scale_matrix(mat, op$dim, center = op$center, scale = op$scale, na.rm = op$na_rm %||% FALSE),
      zscore = safe_scale_matrix(mat, op$dim, center = TRUE, scale = TRUE, na.rm = op$na_rm %||% FALSE),
      detrend = detrend_matrix(mat, op$dim, op$degree),
      where = where_mask(mat, op$predicate, op$fill),
      stop(sprintf("Unknown op '%s'", op$op), call. = FALSE)
    )
  }
  mat
}

apply_reduce_full <- function(mat, reduce_op) {
  if (is.null(reduce_op)) {
    return(mat)
  }
  margin <- if (identical(reduce_op$dim, "rows")) 1L else 2L
  fn <- reduce_op$fn
  na_rm <- reduce_op$na_rm %||% FALSE
  if (identical(fn, base::sum)) {
    return(apply(mat, margin, sum, na.rm = na_rm))
  }
  if (identical(fn, base::mean)) {
    return(apply(mat, margin, mean, na.rm = na_rm))
  }
  if (identical(fn, base::min)) {
    return(apply(mat, margin, min, na.rm = na_rm))
  }
  if (identical(fn, base::max)) {
    return(apply(mat, margin, max, na.rm = na_rm))
  }
  formals_fn <- tryCatch(names(formals(fn)), error = function(e) character())
  if (na_rm && "na.rm" %in% formals_fn) {
    return(apply(mat, margin, function(x) fn(x, na.rm = na_rm)))
  }
  apply(mat, margin, fn)
}

classify_reduce <- function(reduce_op) {
  if (is.null(reduce_op)) {
    return(NULL)
  }
  fn <- reduce_op$fn
  dim <- reduce_op$dim
  type <- if (identical(fn, base::sum)) {
    "sum"
  } else if (identical(fn, base::mean)) {
    "mean"
  } else if (identical(fn, base::min)) {
    "min"
  } else if (identical(fn, base::max)) {
    "max"
  } else {
    "generic"
  }
  list(type = type, dim = dim, op = reduce_op, na.rm = reduce_op$na_rm %||% FALSE)
}

infer_chunk_size <- function(seed, requested_cols, chunk_size) {
  if (!is.null(chunk_size) && chunk_size > 0L) {
    return(as.integer(chunk_size))
  }
  hint <- seed$chunk_hint
  if (is.list(hint) && !is.null(hint$cols)) {
    size <- as.integer(hint$cols)
    if (!is.na(size) && size > 0L) {
      return(min(size, requested_cols))
    }
  }
  default <- 16384L
  as.integer(min(default, requested_cols))
}

collect <- function(x, into = NULL, chunk_size = NULL) {
  seed <- x$seed
  if (is.function(seed$begin)) seed$begin()
  on.exit({
    if (is.function(seed$end)) seed$end()
  }, add = TRUE)

  plan <- compile_plan(x)
  rows <- plan$rows %||% seq_len(seed$nrow)
  cols <- plan$cols %||% seq_len(seed$ncol)
  n_rows <- length(rows)
  n_cols <- length(cols)

  if (requires_full_eval(plan$ops)) {
    mat <- pull_seed(seed, rows = rows, cols = cols)
    rhs_chunks <- NULL
    if (length(plan$rhs_indices)) {
      rhs_chunks <- vector("list", length(plan$ops))
      for (idx in plan$rhs_indices) {
        rhs_obj <- plan$ops[[idx]]$rhs
        rhs_plan <- compile_plan(rhs_obj)
        rhs_seed <- rhs_obj$seed
        rhs_rows <- rhs_plan$rows %||% seq_len(rhs_seed$nrow)
        rhs_cols <- rhs_plan$cols %||% seq_len(rhs_seed$ncol)
        rhs_mat <- pull_seed(rhs_seed, rows = rhs_rows, cols = rhs_cols)
        rhs_mat <- apply_ops(rhs_mat, rhs_plan$ops)
        rhs_chunks[[idx]] <- rhs_mat
      }
    }
    mat <- apply_ops(mat, plan$ops, rhs_chunks)
    res <- apply_reduce_full(mat, plan$reduce)
    return(handle_collect_output(res, into))
  }

  rhs_ctx <- NULL
  if (plan$pair_rhs) {
    rhs_idx <- plan$rhs_indices[1]
    rhs_obj <- plan$ops[[rhs_idx]]$rhs
    rhs_plan <- compile_plan(rhs_obj)
    rhs_seed <- rhs_obj$seed
    rhs_rows <- rhs_plan$rows %||% seq_len(rhs_seed$nrow)
    rhs_cols <- rhs_plan$cols %||% seq_len(rhs_seed$ncol)
    if (length(rhs_rows) == n_rows && length(rhs_cols) == n_cols) {
      if (is.function(rhs_seed$begin)) rhs_seed$begin()
      on.exit({
        if (is.function(rhs_seed$end)) rhs_seed$end()
      }, add = TRUE)
      matched <- plan$rhs_indices[vapply(plan$ops[plan$rhs_indices], function(op) {
        inherits(op$rhs, "delarr") && identical(op$rhs, rhs_obj)
      }, logical(1))]
      rhs_ctx <- list(
        seed = rhs_seed,
        plan = rhs_plan,
        rows = rhs_rows,
        cols = rhs_cols,
        indices = matched
      )
    }
  }

  rhs_chunks_for <- function(pos) {
    if (is.null(rhs_ctx)) {
      return(NULL)
    }
    rhs_cols <- rhs_ctx$cols[pos]
    rhs_block <- pull_seed(rhs_ctx$seed, rows = rhs_ctx$rows, cols = rhs_cols)
    rhs_block <- apply_ops(rhs_block, rhs_ctx$plan$ops)
    chunks <- vector("list", length(plan$ops))
    for (idx in rhs_ctx$indices) {
      chunks[[idx]] <- rhs_block
    }
    chunks
  }

  reduce_info <- classify_reduce(plan$reduce)
  if (!is.null(reduce_info) && identical(reduce_info$type, "generic")) {
    block <- pull_seed(seed, rows = rows, cols = cols)
    rhs_chunks <- rhs_chunks_for(seq_len(n_cols))
    block <- apply_ops(block, plan$ops, rhs_chunks)
    res <- apply_reduce_full(block, plan$reduce)
    return(handle_collect_output(res, into))
  }

  chunk_size <- infer_chunk_size(seed, n_cols, chunk_size)
  chunks <- seq_chunk(n_cols, chunk_size)

  if (is.null(reduce_info)) {
    result <- NULL
    for (pos in chunks) {
      pull_cols <- cols[pos]
      block <- pull_seed(seed, rows = rows, cols = pull_cols)
      rhs_chunks <- rhs_chunks_for(pos)
      block <- apply_ops(block, plan$ops, rhs_chunks)
      if (is.null(into)) {
        if (is.null(result)) {
          result <- matrix(vector(mode = typeof(block), length = n_rows * n_cols), nrow = n_rows, ncol = n_cols)
        }
        result[, pos] <- block
      } else {
        assign_chunk(into, block, rows = rows, cols = pull_cols, positions = pos)
      }
    }
    if (is.null(into)) {
      return(result)
    }
    finalize_target(into)
    return(invisible(NULL))
  }

  type <- reduce_info$type
  na_rm <- reduce_info$na.rm

  if (identical(reduce_info$dim, "rows")) {
    if (type %in% c("sum", "mean")) {
      acc <- numeric(n_rows)
      counts <- if (na_rm || identical(type, "mean")) numeric(n_rows) else NULL
    } else {
      acc <- NULL
      counts <- if (na_rm) numeric(n_rows) else NULL
    }
    for (pos in chunks) {
      pull_cols <- cols[pos]
      block <- pull_seed(seed, rows = rows, cols = pull_cols)
      rhs_chunks <- rhs_chunks_for(pos)
      block <- apply_ops(block, plan$ops, rhs_chunks)
      if (type %in% c("sum", "mean")) {
        partial <- rowSums(block, na.rm = na_rm)
        acc <- acc + partial
        if (!is.null(counts)) {
          counts <- counts + rowSums(!is.na(block))
        }
      } else if (identical(type, "min")) {
        partial <- apply(block, 1L, min, na.rm = na_rm)
        if (is.null(acc)) {
          acc <- partial
        } else {
          acc <- pmin(acc, partial, na.rm = na_rm)
        }
        if (!is.null(counts)) {
          counts <- counts + rowSums(!is.na(block))
        }
      } else if (identical(type, "max")) {
        partial <- apply(block, 1L, max, na.rm = na_rm)
        if (is.null(acc)) {
          acc <- partial
        } else {
          acc <- pmax(acc, partial, na.rm = na_rm)
        }
        if (!is.null(counts)) {
          counts <- counts + rowSums(!is.na(block))
        }
      }
    }
    if (identical(type, "sum")) {
      if (!is.null(counts) && na_rm) {
        acc[counts == 0] <- NA_real_
      }
      return(handle_collect_output(acc, into))
    }
    if (identical(type, "mean")) {
      if (!is.null(counts) && na_rm) {
        acc[counts == 0] <- NA_real_
        idx <- counts > 0
        acc[idx] <- acc[idx] / counts[idx]
      } else {
        acc <- acc / n_cols
      }
      return(handle_collect_output(acc, into))
    }
    if (!is.null(counts) && na_rm) {
      acc[counts == 0] <- NA_real_
    }
    return(handle_collect_output(acc, into))
  }

  # column reductions
  if (type %in% c("sum", "mean")) {
    acc <- numeric(n_cols)
    counts <- if (na_rm || identical(type, "mean")) numeric(n_cols) else NULL
  } else {
    acc <- rep(NA_real_, n_cols)
    counts <- if (na_rm) numeric(n_cols) else NULL
  }
  for (pos in chunks) {
    pull_cols <- cols[pos]
    block <- pull_seed(seed, rows = rows, cols = pull_cols)
    rhs_chunks <- rhs_chunks_for(pos)
    block <- apply_ops(block, plan$ops, rhs_chunks)
    if (type %in% c("sum", "mean")) {
      partial <- colSums(block, na.rm = na_rm)
      acc[pos] <- acc[pos] + partial
      if (!is.null(counts)) {
        counts[pos] <- counts[pos] + colSums(!is.na(block))
      }
    } else if (identical(type, "min")) {
      partial <- apply(block, 2L, min, na.rm = na_rm)
      missing <- is.na(acc[pos])
      if (any(missing)) {
        acc[pos][missing] <- partial[missing]
      }
      if (any(!missing)) {
        acc[pos][!missing] <- pmin(acc[pos][!missing], partial[!missing], na.rm = na_rm)
      }
      if (!is.null(counts)) {
        counts[pos] <- counts[pos] + colSums(!is.na(block))
      }
    } else if (identical(type, "max")) {
      partial <- apply(block, 2L, max, na.rm = na_rm)
      missing <- is.na(acc[pos])
      if (any(missing)) {
        acc[pos][missing] <- partial[missing]
      }
      if (any(!missing)) {
        acc[pos][!missing] <- pmax(acc[pos][!missing], partial[!missing], na.rm = na_rm)
      }
      if (!is.null(counts)) {
        counts[pos] <- counts[pos] + colSums(!is.na(block))
      }
    }
  }
  if (identical(type, "sum") && na_rm && !is.null(counts)) {
    acc[counts == 0] <- NA_real_
  }
  if (identical(type, "mean")) {
    if (!is.null(counts) && na_rm) {
      acc[counts == 0] <- NA_real_
      idx <- counts > 0
      acc[idx] <- acc[idx] / counts[idx]
    } else {
      acc <- acc / n_rows
    }
  }
  if (type %in% c("min", "max") && !is.null(counts) && na_rm) {
    acc[counts == 0] <- NA_real_
  }
  handle_collect_output(acc, into)
}

assign_chunk <- function(target, block, rows, cols, positions) {
  if (is.function(target)) {
    target(block, rows = rows, cols = cols, positions = positions)
    return(invisible(NULL))
  }
  if (is.list(target) && is.function(target$write)) {
    target$write(block, rows = rows, cols = cols, positions = positions)
    return(invisible(NULL))
  }
  stop("Unsupported 'into' target", call. = FALSE)
}

handle_collect_output <- function(result, into) {
  if (is.null(into)) {
    return(result)
  }
  if (is.function(into)) {
    into(result)
    return(invisible(NULL))
  }
  if (is.list(into) && is.function(into$write)) {
    into$write(result)
    finalize_target(into)
    return(invisible(NULL))
  }
  stop("Unsupported 'into' target", call. = FALSE)
}

finalize_target <- function(target) {
  if (is.list(target) && is.function(target$finalize)) {
    target$finalize()
  }
  invisible(NULL)
}

block_apply <- function(x, margin = c("cols", "rows"), size = 16384L, fn) {
  margin <- match.arg(margin)
  if (!is.function(fn)) {
    stop("fn must be a function", call. = FALSE)
  }
  dims <- dim(x)
  total <- if (margin == "cols") dims[2] else dims[1]
  chunks <- seq_chunk(total, size)
  out <- vector("list", length(chunks))
  for (i in seq_along(chunks)) {
    indices <- chunks[[i]]
    slice_arr <- if (margin == "cols") {
      x[, indices, drop = FALSE]
    } else {
      x[indices, , drop = FALSE]
    }
    block <- collect(slice_arr, chunk_size = size)
    out[[i]] <- fn(block)
  }
  out
}
