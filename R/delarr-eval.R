compile_plan <- function(x) {
  stopifnot(inherits(x, "delarr"))
  seed <- x$seed
  ndim <- length(seed$dims)

  # N-d index tracking (always populated)
  current_indices <- lapply(seed$dims, seq_len)

  ops <- list()
  reduce_op <- NULL
  rhs_indices <- integer()
  for (op in x$ops) {
    if (identical(op$op, "slice")) {
      # N-d slice: op$indices is a list of per-dim indices
      if (!is.null(op$indices)) {
        for (k in seq_along(op$indices)) {
          if (!is.null(op$indices[[k]])) {
            current_indices[[k]] <- current_indices[[k]][
              normalize_index(op$indices[[k]], length(current_indices[[k]]))
            ]
          }
        }
      } else {
        # Legacy 2D slice: op$rows / op$cols
        if (!is.null(op$rows)) {
          current_indices[[1L]] <- current_indices[[1L]][
            normalize_index(op$rows, length(current_indices[[1L]]))
          ]
        }
        if (!is.null(op$cols)) {
          current_indices[[2L]] <- current_indices[[2L]][
            normalize_index(op$cols, length(current_indices[[2L]]))
          ]
        }
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
    rows = current_indices[[1L]],
    cols = current_indices[[2L]],
    indices = current_indices,
    ops = ops,
    reduce = reduce_op,
    rhs_indices = rhs_indices,
    pair_rhs = length(rhs_indices) > 0
  )
}

requires_full_eval <- function(ops) {
  any(vapply(ops, function(op) {
    if (!(op$op %in% c("center", "scale", "zscore", "detrend"))) return(FALSE)
    # Row-wise ops need all columns at once (full eval)
    identical(op$dim, "rows") || identical(op$axis, 1L)
  }, logical(1)))
}

blocked_chunk_axes <- function(ops, ndim) {
  # center/scale/zscore/detrend along axis K need full cross-sections across

  # all OTHER axes.  Only axis K itself is safe to chunk along.
  blocked <- integer()
  for (op in ops) {
    if (!(op$op %in% c("center", "scale", "zscore", "detrend"))) {
      next
    }
    op_axis <- op$axis %||% dim_to_axis(op$dim)
    # Block every axis EXCEPT the op's own axis
    blocked <- c(blocked, setdiff(seq_len(ndim), op_axis))
  }
  if (!length(blocked)) {
    return(integer())
  }
  unique(as.integer(blocked))
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

fast_vector_broadcast_op <- function(mat, rhs, op_name, side = c("right", "left")) {
  side <- match.arg(side)
  if (!is.atomic(rhs) || is.matrix(rhs) || length(rhs) <= 1L) {
    return(NULL)
  }

  nr <- nrow(mat)
  nc <- ncol(mat)
  margin <- if (length(rhs) == nr) {
    1L
  } else if (length(rhs) == nc) {
    2L
  } else {
    return(NULL)
  }

  if (identical(side, "right")) {
    return(sweep(mat, margin, rhs, FUN = op_name))
  }

  op_fun <- switch(op_name,
    "+" = function(x, y) y + x,
    "-" = function(x, y) y - x,
    "*" = function(x, y) y * x,
    "/" = function(x, y) y / x,
    "^" = function(x, y) y ^ x,
    "%%" = function(x, y) y %% x,
    "%/%" = function(x, y) y %/% x,
    "&" = function(x, y) y & x,
    "|" = function(x, y) y | x,
    "==" = function(x, y) y == x,
    "!=" = function(x, y) y != x,
    "<" = function(x, y) y < x,
    "<=" = function(x, y) y <= x,
    ">" = function(x, y) y > x,
    ">=" = function(x, y) y >= x,
    NULL
  )
  if (is.null(op_fun)) {
    return(NULL)
  }
  sweep(mat, margin, rhs, FUN = op_fun)
}

subset_rhs_for_chunk <- function(rhs, chunk_context = NULL) {
  if (is.null(rhs) || is.null(chunk_context)) {
    return(rhs)
  }

  if (!is.null(chunk_context$indices) && is.array(rhs) &&
      !is.null(chunk_context$full_dims) &&
      identical(as.integer(dim(rhs)), as.integer(chunk_context$full_dims))) {
    return(do.call(`[`, c(list(rhs), chunk_context$indices, list(drop = FALSE))))
  }

  row_pos <- chunk_context$rows
  col_pos <- chunk_context$cols
  full_nrow <- chunk_context$full_nrow
  full_ncol <- chunk_context$full_ncol

  if (is.matrix(rhs) && all(dim(rhs) == c(full_nrow, full_ncol))) {
    return(rhs[row_pos, col_pos, drop = FALSE])
  }

  if (is.atomic(rhs) && length(rhs) > 1L) {
    if (!is.null(row_pos) && length(rhs) == full_nrow) {
      return(rhs[row_pos])
    }
    if (!is.null(col_pos) && length(rhs) == full_ncol) {
      return(rhs[col_pos])
    }
  }

  rhs
}

apply_ops <- function(mat, ops, rhs_chunks = NULL, chunk_context = NULL) {
  if (!length(ops)) {
    return(mat)
  }
  nd <- !is.matrix(mat) && is.array(mat) && length(dim(mat)) > 2L
  for (i in seq_along(ops)) {
    op <- ops[[i]]
    mat <- switch(op$op,
      emap = {
        res <- op$fn(mat)
        if (!is.matrix(res) && !is.array(res)) {
          stop("d_map functions must return a matrix or array", call. = FALSE)
        }
        res
      },
      emap_const = {
        const <- subset_rhs_for_chunk(op$const, chunk_context)
        fast <- NULL
        if (!nd && !is.null(op$op_name)) {
          fast <- fast_vector_broadcast_op(mat, const, op$op_name, op$side)
        }
        if (!is.null(fast)) {
          fast
        } else {
          if (!nd) const <- broadcast_rhs(mat, const)
          if (identical(op$side, "right")) op$fn(mat, const) else op$fn(const, mat)
        }
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
        rhs <- subset_rhs_for_chunk(rhs, chunk_context)
        fast <- NULL
        if (!nd && !is.null(op$op_name)) {
          fast <- fast_vector_broadcast_op(mat, rhs, op$op_name, "right")
        }
        if (!is.null(fast)) {
          fast
        } else {
          if (!nd) rhs <- broadcast_rhs(mat, rhs)
          op$fn(mat, rhs)
        }
      },
      center = {
        if (nd) {
          axis_center(mat, op$axis %||% dim_to_axis(op$dim), na.rm = op$na_rm %||% FALSE)
        } else {
          safe_center(mat, op$dim, op$na_rm %||% FALSE)
        }
      },
      scale = {
        if (nd) {
          axis_scale(mat, op$axis %||% dim_to_axis(op$dim), center = op$center, scale = op$scale, na.rm = op$na_rm %||% FALSE)
        } else {
          safe_scale_matrix(mat, op$dim, center = op$center, scale = op$scale, na.rm = op$na_rm %||% FALSE)
        }
      },
      zscore = {
        if (nd) {
          axis_scale(mat, op$axis %||% dim_to_axis(op$dim), center = TRUE, scale = TRUE, na.rm = op$na_rm %||% FALSE)
        } else {
          safe_scale_matrix(mat, op$dim, center = TRUE, scale = TRUE, na.rm = op$na_rm %||% FALSE)
        }
      },
      detrend = {
        if (nd) {
          axis_detrend(mat, op$axis %||% dim_to_axis(op$dim), op$degree)
        } else {
          detrend_matrix(mat, op$dim, op$degree)
        }
      },
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
  ndim <- length(dim(mat))
  collapse_axis <- collapse_axes_from_reduce(reduce_op, ndim = ndim)
  margin <- setdiff(seq_len(ndim), collapse_axis)
  fn <- reduce_op$fn
  na_rm <- reduce_op$na_rm %||% FALSE
  if (!length(margin)) {
    vals <- as.vector(mat)
    if (identical(fn, base::sum)) {
      out <- sum(vals, na.rm = na_rm)
      if (na_rm && all(is.na(vals))) out <- NA_real_
      return(out)
    }
    if (identical(fn, base::mean)) {
      out <- mean(vals, na.rm = na_rm)
      if (na_rm && all(is.na(vals))) out <- NA_real_
      return(out)
    }
    if (identical(fn, base::min)) {
      out <- suppressWarnings(min(vals, na.rm = na_rm))
      if (na_rm && is.infinite(out)) out <- NA_real_
      return(out)
    }
    if (identical(fn, base::max)) {
      out <- suppressWarnings(max(vals, na.rm = na_rm))
      if (na_rm && is.infinite(out)) out <- NA_real_
      return(out)
    }
    formals_fn <- tryCatch(names(formals(fn)), error = function(e) character())
    if (na_rm && "na.rm" %in% formals_fn) {
      return(fn(vals, na.rm = na_rm))
    }
    return(fn(vals))
  }
  if (identical(fn, base::sum)) {
    result <- apply(mat, margin, sum, na.rm = na_rm)
    # When na.rm=TRUE and all values are NA, sum should return NA not 0
    if (na_rm) {
      all_na <- apply(mat, margin, function(x) all(is.na(x)))
      result[all_na] <- NA_real_
    }
    return(result)
  }
  if (identical(fn, base::mean)) {
    result <- apply(mat, margin, mean, na.rm = na_rm)
    # When na.rm=TRUE and all values are NA, mean should return NA not NaN
    if (na_rm) {
      all_na <- apply(mat, margin, function(x) all(is.na(x)))
      result[all_na] <- NA_real_
    }
    return(result)
  }
  if (identical(fn, base::min)) {
    result <- suppressWarnings(apply(mat, margin, min, na.rm = na_rm))
    # When na.rm=TRUE and all values are NA, min returns Inf - should be NA
    if (na_rm) {
      result[is.infinite(result)] <- NA_real_
    }
    return(result)
  }
  if (identical(fn, base::max)) {
    result <- suppressWarnings(apply(mat, margin, max, na.rm = na_rm))
    # When na.rm=TRUE and all values are NA, max returns -Inf - should be NA
    if (na_rm) {
      result[is.infinite(result)] <- NA_real_
    }
    return(result)
  }
  formals_fn <- tryCatch(names(formals(fn)), error = function(e) character())
  if (na_rm && "na.rm" %in% formals_fn) {
    return(apply(mat, margin, function(x) fn(x, na.rm = na_rm)))
  }
  apply(mat, margin, fn)
}

apply_result_names <- function(result, out_dimnames, reduce_info = NULL) {
  if (is.null(out_dimnames)) {
    return(result)
  }
  if (is.matrix(result) || (is.array(result) && length(dim(result)) >= 2L)) {
    if (!any(vapply(out_dimnames, Negate(is.null), logical(1)))) {
      return(result)
    }
    dimnames(result) <- out_dimnames
    return(result)
  }
  if (!is.null(reduce_info) && is.atomic(result)) {
    if (identical(reduce_info$dim, "rows")) {
      names(result) <- out_dimnames[[1L]]
    } else if (identical(reduce_info$dim, "cols")) {
      names(result) <- out_dimnames[[2L]]
    } else if (!is.null(out_dimnames)) {
      non_null <- which(vapply(out_dimnames, Negate(is.null), logical(1)))
      if (length(non_null) == 1L && length(out_dimnames[[non_null]]) == length(result)) {
        names(result) <- out_dimnames[[non_null]]
      }
    }
  }
  result
}

collapse_axes_from_reduce <- function(reduce_op, ndim = NULL) {
  if (is.null(reduce_op)) {
    return(integer())
  }
  axes <- reduce_op$axis
  if (is.null(axes)) {
    ndim <- as.integer(ndim %||% 2L)
    keep_axis <- if (identical(reduce_op$dim, "rows")) 1L else 2L
    return(setdiff(seq_len(ndim), keep_axis))
  }
  as.integer(axes)
}

merge_nd_extrema <- function(acc, partial, type, na_rm) {
  if (is.null(acc)) {
    return(partial)
  }
  if (!na_rm) {
    return(if (identical(type, "min")) {
      pmin(acc, partial)
    } else {
      pmax(acc, partial)
    })
  }

  out <- acc
  acc_na <- is.na(acc)
  partial_na <- is.na(partial)
  both <- !acc_na & !partial_na
  if (any(both)) {
    out[both] <- if (identical(type, "min")) {
      pmin(acc[both], partial[both])
    } else {
      pmax(acc[both], partial[both])
    }
  }
  adopt <- acc_na & !partial_na
  if (any(adopt)) {
    out[adopt] <- partial[adopt]
  }
  out
}

reduce_block_builtin_nd <- function(block, collapse_axes, type, na_rm) {
  keep_axes <- setdiff(seq_along(dim(block)), collapse_axes)
  counts <- NULL

  sum_over_axes <- function(x, na.rm = FALSE) {
    if (!length(keep_axes)) {
      return(sum(x, na.rm = na.rm))
    }
    apply(x, keep_axes, sum, na.rm = na.rm)
  }

  partial <- switch(type,
    sum = sum_over_axes(block, na.rm = na_rm),
    mean = sum_over_axes(block, na.rm = na_rm),
    min = apply_reduce_full(block, list(
      axis = collapse_axes,
      fn = base::min,
      na_rm = na_rm
    )),
    max = apply_reduce_full(block, list(
      axis = collapse_axes,
      fn = base::max,
      na_rm = na_rm
    )),
    stop(sprintf("Unsupported N-d reduction type '%s'", type), call. = FALSE)
  )

  if (na_rm || identical(type, "mean")) {
    counts <- if (!length(keep_axes)) {
      sum(!is.na(block))
    } else {
      apply(!is.na(block), keep_axes, sum)
    }
  }

  if (na_rm && type %in% c("min", "max")) {
    partial[is.infinite(partial)] <- NA_real_
  }

  list(partial = partial, counts = counts)
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

infer_chunk_size <- function(seed, requested_rows, requested_cols, chunk_size,
                             margin = c("cols", "rows"), target_bytes = NULL) {
  margin <- match.arg(margin)
  requested <- if (identical(margin, "cols")) requested_cols else requested_rows
  if (!is.null(chunk_size) && chunk_size > 0L) {
    return(as.integer(min(chunk_size, requested)))
  }
  if (!is.null(target_bytes) && is.finite(target_bytes) && target_bytes > 0) {
    bytes_per_value <- 8L
    fixed_extent <- if (identical(margin, "cols")) requested_rows else requested_cols
    denom <- max(1L, as.integer(fixed_extent)) * bytes_per_value
    adaptive <- floor(as.numeric(target_bytes) / denom)
    if (is.finite(adaptive) && adaptive >= 1L) {
      return(as.integer(min(requested, adaptive)))
    }
  }
  hint <- seed$chunk_hint
  hint_key <- if (identical(margin, "cols")) "cols" else "rows"
  hint_size <- if (is.list(hint)) hint[[hint_key]] else NULL
  if (!is.null(hint_size)) {
    size <- as.integer(hint_size)
    if (!is.na(size) && size > 0L) {
      return(min(size, requested))
    }
  }
  default <- if (identical(margin, "cols")) 16384L else 4096L
  as.integer(min(default, requested))
}

#' Materialise a delayed matrix
#'
#' Streams column chunks from the backing seed, applying deferred operations
#' and optional reductions on the fly. By default the result is returned as a
#' base matrix or vector; alternatively, supply a writer via `into` to stream
#' the output elsewhere (e.g., `hdf5_writer()`).
#'
#' @param x A `delarr` object.
#' @param into Optional writer or callback used to receive streamed chunks.
#' @param chunk_size Optional chunk size along `chunk_margin`.
#' @param chunk_margin Chunking axis for non-reduction collection.
#' @param target_bytes Optional memory budget (bytes) used to adapt chunk size.
#' @param parallel Logical; attempt parallel chunk execution when safe.
#' @param workers Number of worker processes when `parallel = TRUE`.
#' @param optimize Logical; run lightweight DAG optimizations before evaluation.
#'
#' @return A realised matrix/vector, or `NULL` invisibly when writing to
#'   `into`.
#'
#' @examples
#' # Basic materialization
#' mat <- matrix(1:12, nrow = 3, ncol = 4)
#' darr <- delarr(mat)
#' collect(darr)
#'
#' # Collect after lazy operations
#' result <- darr |>
#'   d_map(~ .x^2) |>
#'   collect()
#' result
#'
#' @export
collect <- function(x, into = NULL, chunk_size = NULL,
                    chunk_margin = c("cols", "rows"),
                    target_bytes = NULL,
                    parallel = FALSE,
                    workers = NULL,
                    optimize = TRUE) {
  stopifnot(inherits(x, "delarr"))
  chunk_margin_missing <- missing(chunk_margin)
  ndim <- length(x$seed$dims)
  if (chunk_margin_missing) {
    chunk_margin <- if (ndim <= 2L) "cols" else NULL
  } else {
    chunk_margin <- normalize_chunk_margin(chunk_margin, ndim)
  }
  if (isTRUE(optimize)) {
    x <- optimize_delarr(x)
  }

  seed <- x$seed
  plan <- compile_plan(x)
  out_dimnames <- dimnames(x)
  reduce_info <- classify_reduce(plan$reduce)
  reduce_axes <- if (!is.null(plan$reduce) && !is_nd_seed(seed)) {
    collapse_axes_from_reduce(plan$reduce, ndim = 2L)
  } else {
    NULL
  }
  rows <- plan$rows %||% seq_len(seed$nrow)
  cols <- plan$cols %||% seq_len(seed$ncol)
  n_rows <- length(rows)
  n_cols <- length(cols)
  full_chunk_context <- list(
    rows = seq_len(n_rows),
    cols = seq_len(n_cols),
    full_nrow = n_rows,
    full_ncol = n_cols
  )

  # ---- N-d path: full materialization for arrays with ndim > 2 ----------------
  if (is_nd_seed(seed)) {
    if (is.function(seed$begin)) seed$begin()
    on.exit({
      if (is.function(seed$end)) seed$end()
    }, add = TRUE)

    indices <- plan$indices
    selected_dims <- vapply(indices, length, integer(1))
    blocked_axes <- blocked_chunk_axes(plan$ops, length(selected_dims))

    rhs_chunks <- NULL
    if (length(plan$rhs_indices)) {
      rhs_chunks <- vector("list", length(plan$ops))
      for (idx in plan$rhs_indices) {
        rhs_obj <- plan$ops[[idx]]$rhs
        if (inherits(rhs_obj, "delarr")) {
          rhs_chunks[[idx]] <- collect(rhs_obj)
        }
      }
    }

    eval_nd_chunk <- function(pos, chunk_axis) {
      chunk_indices <- lapply(seq_along(indices), function(k) {
        if (k == chunk_axis) indices[[k]][pos] else indices[[k]]
      })
      chunk_context <- list(
        indices = lapply(seq_along(selected_dims), function(k) {
          if (k == chunk_axis) pos else seq_len(selected_dims[[k]])
        }),
        full_dims = selected_dims
      )
      block <- pull_seed_nd(seed, chunk_indices)
      block <- apply_ops(
        block,
        plan$ops,
        rhs_chunks = rhs_chunks,
        chunk_context = chunk_context
      )
      list(block = block, positions = pos, indices = chunk_context$indices)
    }

    allow_parallel_nd <- isTRUE(parallel) &&
      is.null(into) &&
      !is.function(seed$begin) &&
      !is.function(seed$end) &&
      identical(.Platform$OS.type, "unix")

    if (is.null(plan$reduce)) {
      safe_axes <- setdiff(seq_along(selected_dims), blocked_axes)
      default_axis <- if (length(safe_axes)) {
        safe_axes[[length(safe_axes)]]
      } else {
        length(selected_dims)
      }
      chunk_axis <- resolve_chunk_axis(
        if (isTRUE(chunk_margin_missing)) NULL else chunk_margin,
        length(selected_dims),
        default = default_axis
      )
      if (!(chunk_axis %in% blocked_axes)) {
        if (is.list(into) && is.function(into$write)) {
          stop("Writer-style into targets are not yet supported for N-d collect(); use into=function(...) instead", call. = FALSE)
        }

        resolved_chunk <- infer_nd_chunk_size(
          seed = seed,
          requested_dims = selected_dims,
          axis = chunk_axis,
          chunk_size = chunk_size,
          target_bytes = target_bytes
        )
        chunks <- seq_chunk(selected_dims[[chunk_axis]], resolved_chunk)

        if (allow_parallel_nd) {
          avail <- suppressWarnings(parallel::detectCores(logical = FALSE))
          default_cores <- if (is.na(avail)) 1L else max(1L, avail - 1L)
          cores <- as.integer(workers %||% default_cores)
          pieces <- parallel::mclapply(
            chunks,
            eval_nd_chunk,
            chunk_axis = chunk_axis,
            mc.cores = max(1L, cores)
          )
          result <- array(
            vector(mode = typeof(pieces[[1L]]$block), length = prod(selected_dims)),
            dim = selected_dims
          )
          for (piece in pieces) {
            result <- assign_axis_chunk(result, piece$block, chunk_axis, piece$positions)
          }
          result <- apply_result_names(result, out_dimnames)
          return(result)
        }

        result <- NULL
        for (pos in chunks) {
          piece <- eval_nd_chunk(pos, chunk_axis = chunk_axis)
          if (is.null(into)) {
            if (is.null(result)) {
              result <- array(
                vector(mode = typeof(piece$block), length = prod(selected_dims)),
                dim = selected_dims
              )
            }
            result <- assign_axis_chunk(result, piece$block, chunk_axis, piece$positions)
          } else {
            into(piece$block, indices = piece$indices, axis = chunk_axis, positions = piece$positions)
          }
        }
        if (is.null(into)) {
          result <- apply_result_names(result, out_dimnames)
          return(result)
        }
        return(invisible(NULL))
      }
    } else if (!is.null(reduce_info) && !identical(reduce_info$type, "generic")) {
      collapse_axes <- collapse_axes_from_reduce(
        plan$reduce,
        ndim = length(selected_dims)
      )
      safe_axes <- setdiff(collapse_axes, blocked_axes)
      if (length(safe_axes)) {
        default_axis <- safe_axes[[length(safe_axes)]]
        chunk_axis <- resolve_chunk_axis(
          if (isTRUE(chunk_margin_missing)) NULL else chunk_margin,
          length(selected_dims),
          default = default_axis
        )
        if (chunk_axis %in% safe_axes) {
          resolved_chunk <- infer_nd_chunk_size(
            seed = seed,
            requested_dims = selected_dims,
            axis = chunk_axis,
            chunk_size = chunk_size,
            target_bytes = target_bytes
          )
          chunks <- seq_chunk(selected_dims[[chunk_axis]], resolved_chunk)
          total_count <- prod(selected_dims[collapse_axes])

          eval_nd_reduce_chunk <- function(pos) {
            piece <- eval_nd_chunk(pos, chunk_axis = chunk_axis)
            reduce_block_builtin_nd(
              piece$block,
              collapse_axes = collapse_axes,
              type = reduce_info$type,
              na_rm = reduce_info$na.rm
            )
          }

          if (allow_parallel_nd) {
            avail <- suppressWarnings(parallel::detectCores(logical = FALSE))
            default_cores <- if (is.na(avail)) 1L else max(1L, avail - 1L)
            cores <- as.integer(workers %||% default_cores)
            pieces <- parallel::mclapply(
              chunks,
              eval_nd_reduce_chunk,
              mc.cores = max(1L, cores)
            )
          } else {
            pieces <- lapply(chunks, eval_nd_reduce_chunk)
          }

          acc <- NULL
          counts <- NULL
          for (piece in pieces) {
            if (reduce_info$type %in% c("sum", "mean")) {
              acc <- if (is.null(acc)) piece$partial else acc + piece$partial
              if (!is.null(piece$counts)) {
                counts <- if (is.null(counts)) piece$counts else counts + piece$counts
              }
            } else {
              acc <- merge_nd_extrema(
                acc,
                piece$partial,
                type = reduce_info$type,
                na_rm = reduce_info$na.rm
              )
              if (!is.null(piece$counts)) {
                counts <- if (is.null(counts)) piece$counts else counts + piece$counts
              }
            }
          }

          if (identical(reduce_info$type, "mean")) {
            if (reduce_info$na.rm) {
              acc[counts == 0] <- NA_real_
              non_zero <- counts > 0
              acc[non_zero] <- acc[non_zero] / counts[non_zero]
            } else {
              acc <- acc / total_count
            }
          } else if (reduce_info$na.rm && !is.null(counts)) {
            acc[counts == 0] <- NA_real_
          }

          acc <- apply_result_names(acc, out_dimnames, reduce_info)
          return(handle_collect_output(acc, into))
        }
      }
    }

    arr <- pull_seed_nd(seed, indices)
    arr <- apply_ops(arr, plan$ops, rhs_chunks = rhs_chunks)
    if (!is.null(plan$reduce)) {
      arr <- apply_reduce_full(arr, plan$reduce)
    }
    arr <- apply_result_names(arr, out_dimnames, reduce_info)
    return(handle_collect_output(arr, into))
  }

  # ---- 2D path (original) ----------------------------------------------------
  allow_parallel <- isTRUE(parallel) &&
    is.null(into) &&
    is.null(plan$reduce) &&
    identical(chunk_margin, "cols") &&
    !plan$pair_rhs &&
    !is.function(seed$begin) &&
    !is.function(seed$end) &&
    identical(.Platform$OS.type, "unix")

  if (!allow_parallel) {
    if (isTRUE(parallel) && identical(.Platform$OS.type, "windows")) {
      warning("parallel collect() is only enabled on Unix-like platforms; falling back to sequential")
    }
    if (is.function(seed$begin)) seed$begin()
    on.exit({
      if (is.function(seed$end)) seed$end()
    }, add = TRUE)
  }

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
        rhs_mat <- apply_ops(
          rhs_mat,
          rhs_plan$ops,
          chunk_context = list(
            rows = seq_len(length(rhs_rows)),
            cols = seq_len(length(rhs_cols)),
            full_nrow = length(rhs_rows),
            full_ncol = length(rhs_cols)
          )
        )
        rhs_chunks[[idx]] <- rhs_mat
      }
    }
    mat <- apply_ops(mat, plan$ops, rhs_chunks, chunk_context = full_chunk_context)
    res <- apply_reduce_full(mat, plan$reduce)
    res <- apply_result_names(res, out_dimnames, reduce_info)
    return(handle_collect_output(res, into))
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

  rhs_chunks_for <- function(pos, margin = c("cols", "rows")) {
    margin <- match.arg(margin)
    chunks <- vector("list", length(plan$ops))
    for (idx in plan$rhs_indices) {
      ctx <- rhs_contexts[[idx]]
      if (!is.null(ctx)) {
        rhs_block <- if (identical(margin, "cols")) {
          rhs_cols <- ctx$cols[pos]
          pull_seed(ctx$seed, rows = ctx$rows, cols = rhs_cols)
        } else {
          rhs_rows <- ctx$rows[pos]
          pull_seed(ctx$seed, rows = rhs_rows, cols = ctx$cols)
        }
        rhs_block <- apply_ops(
          rhs_block,
          ctx$plan$ops,
          chunk_context = if (identical(margin, "cols")) {
            list(
              rows = seq_len(length(ctx$rows)),
              cols = pos,
              full_nrow = length(ctx$rows),
              full_ncol = length(ctx$cols)
            )
          } else {
            list(
              rows = pos,
              cols = seq_len(length(ctx$cols)),
              full_nrow = length(ctx$rows),
              full_ncol = length(ctx$cols)
            )
          }
        )
        chunks[[idx]] <- rhs_block
        next
      }
      rhs_val <- rhs_precomputed[[idx]]
      if (is.null(rhs_val)) {
        next
      }
      if (is.matrix(rhs_val) && all(dim(rhs_val) == c(n_rows, n_cols))) {
        chunks[[idx]] <- if (identical(margin, "cols")) {
          rhs_val[, pos, drop = FALSE]
        } else {
          rhs_val[pos, , drop = FALSE]
        }
      } else {
        chunks[[idx]] <- rhs_val
      }
    }
    if (!any(vapply(chunks, Negate(is.null), logical(1)))) {
      return(NULL)
    }
    chunks
  }

  if (!is.null(reduce_info) && identical(reduce_info$type, "generic")) {
    block <- pull_seed(seed, rows = rows, cols = cols)
    rhs_chunks <- rhs_chunks_for(seq_len(n_cols))
    block <- apply_ops(block, plan$ops, rhs_chunks, chunk_context = full_chunk_context)
    res <- apply_reduce_full(block, plan$reduce)
    res <- apply_result_names(res, out_dimnames, reduce_info)
    return(handle_collect_output(res, into))
  }

  collect_margin <- if (is.null(reduce_info)) chunk_margin else "cols"
  chunk_size <- infer_chunk_size(
    seed = seed,
    requested_rows = n_rows,
    requested_cols = n_cols,
    chunk_size = chunk_size,
    margin = collect_margin,
    target_bytes = target_bytes
  )
  chunk_extent <- if (identical(collect_margin, "cols")) n_cols else n_rows
  chunks <- seq_chunk(chunk_extent, chunk_size)

  if (is.null(reduce_info)) {
    if (!length(chunks)) {
      block <- pull_seed(seed, rows = rows, cols = cols)
      rhs_chunks <- rhs_chunks_for(seq_len(n_cols), margin = "cols")
      block <- apply_ops(block, plan$ops, rhs_chunks, chunk_context = full_chunk_context)
      block <- apply_result_names(block, out_dimnames)
      return(handle_collect_output(block, into))
    }

    if (!is.null(into) && identical(collect_margin, "rows")) {
      warning("chunk_margin='rows' is not supported with into= writers; using column chunks instead")
      collect_margin <- "cols"
      chunk_size <- infer_chunk_size(
        seed = seed,
        requested_rows = n_rows,
        requested_cols = n_cols,
        chunk_size = chunk_size,
        margin = collect_margin,
        target_bytes = target_bytes
      )
      chunks <- seq_chunk(n_cols, chunk_size)
    }

    eval_chunk <- function(pos) {
      if (identical(collect_margin, "cols")) {
        pull_cols <- cols[pos]
        block <- pull_seed(seed, rows = rows, cols = pull_cols)
        rhs_chunks <- rhs_chunks_for(pos, margin = "cols")
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
        list(block = block, rows = rows, cols = pull_cols, positions = pos)
      } else {
        pull_rows <- rows[pos]
        block <- pull_seed(seed, rows = pull_rows, cols = cols)
        rhs_chunks <- rhs_chunks_for(pos, margin = "rows")
        block <- apply_ops(
          block,
          plan$ops,
          rhs_chunks,
          chunk_context = list(
            rows = pos,
            cols = seq_len(n_cols),
            full_nrow = n_rows,
            full_ncol = n_cols
          )
        )
        list(block = block, rows = pull_rows, cols = cols, positions = pos)
      }
    }

    if (allow_parallel) {
      avail <- suppressWarnings(parallel::detectCores(logical = FALSE))
      default_cores <- if (is.na(avail)) 1L else max(1L, avail - 1L)
      cores <- as.integer(workers %||% default_cores)
      pieces <- parallel::mclapply(chunks, eval_chunk, mc.cores = max(1L, cores))
      result <- matrix(vector(mode = typeof(pieces[[1]]$block), length = n_rows * n_cols), nrow = n_rows, ncol = n_cols)
      for (piece in pieces) {
        result[, piece$positions] <- piece$block
      }
      result <- apply_result_names(result, out_dimnames)
      return(result)
    }

    result <- NULL
    for (pos in chunks) {
      piece <- eval_chunk(pos)
      block <- piece$block
      if (is.null(into)) {
        if (is.null(result)) {
          result <- matrix(vector(mode = typeof(block), length = n_rows * n_cols), nrow = n_rows, ncol = n_cols)
        }
        if (identical(collect_margin, "cols")) {
          result[, piece$positions] <- block
        } else {
          result[piece$positions, ] <- block
        }
      } else {
        assign_chunk(into, block, rows = piece$rows, cols = piece$cols, positions = piece$positions)
      }
    }
    if (is.null(into)) {
      result <- apply_result_names(result, out_dimnames)
      return(result)
    }
    finalize_target(into)
    return(invisible(NULL))
  }

  type <- reduce_info$type
  na_rm <- reduce_info$na.rm

  if (identical(reduce_axes, c(1L, 2L))) {
    acc <- if (type %in% c("sum", "mean")) 0 else NULL
    counts <- if (na_rm || identical(type, "mean")) 0 else NULL
    for (pos in chunks) {
      pull_cols <- cols[pos]
      block <- pull_seed(seed, rows = rows, cols = pull_cols)
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
      if (type %in% c("sum", "mean")) {
        acc <- acc + sum(block, na.rm = na_rm)
        if (!is.null(counts)) {
          counts <- counts + sum(!is.na(block))
        }
      } else if (identical(type, "min")) {
        partial <- suppressWarnings(min(block, na.rm = na_rm))
        if (na_rm && is.infinite(partial)) {
          partial <- NA_real_
        }
        acc <- if (is.null(acc) || is.na(acc)) partial else {
          if (is.na(partial)) acc else min(acc, partial)
        }
        if (!is.null(counts)) {
          counts <- counts + sum(!is.na(block))
        }
      } else if (identical(type, "max")) {
        partial <- suppressWarnings(max(block, na.rm = na_rm))
        if (na_rm && is.infinite(partial)) {
          partial <- NA_real_
        }
        acc <- if (is.null(acc) || is.na(acc)) partial else {
          if (is.na(partial)) acc else max(acc, partial)
        }
        if (!is.null(counts)) {
          counts <- counts + sum(!is.na(block))
        }
      }
    }
    if (identical(type, "mean")) {
      if (!is.null(counts) && na_rm) {
        acc <- if (counts == 0) NA_real_ else acc / counts
      } else {
        # as.numeric() guards against integer overflow of the element count
        # for very large streamed matrices (n_rows * n_cols > .Machine$integer.max).
        acc <- acc / (as.numeric(n_rows) * as.numeric(n_cols))
      }
    } else if (!is.null(counts) && na_rm && counts == 0) {
      acc <- NA_real_
    }
    return(handle_collect_output(acc, into))
  }

  if (identical(reduce_axes, 2L)) {
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
      if (type %in% c("sum", "mean")) {
        partial <- rowSums(block, na.rm = na_rm)
        acc <- acc + partial
        if (!is.null(counts)) {
          counts <- counts + rowSums(!is.na(block))
        }
      } else if (identical(type, "min")) {
        partial <- safe_min(block, "rows", na.rm = na_rm)
        if (is.null(acc)) {
          acc <- partial
        } else {
          acc <- pmin(acc, partial, na.rm = na_rm)
        }
        if (!is.null(counts)) {
          counts <- counts + rowSums(!is.na(block))
        }
      } else if (identical(type, "max")) {
        partial <- safe_max(block, "rows", na.rm = na_rm)
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
      acc <- apply_result_names(acc, out_dimnames, reduce_info)
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
      acc <- apply_result_names(acc, out_dimnames, reduce_info)
      return(handle_collect_output(acc, into))
    }
    if (!is.null(counts) && na_rm) {
      acc[counts == 0] <- NA_real_
    }
    acc <- apply_result_names(acc, out_dimnames, reduce_info)
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
    if (type %in% c("sum", "mean")) {
      partial <- colSums(block, na.rm = na_rm)
      acc[pos] <- acc[pos] + partial
      if (!is.null(counts)) {
        counts[pos] <- counts[pos] + colSums(!is.na(block))
      }
    } else if (identical(type, "min")) {
      partial <- safe_min(block, "cols", na.rm = na_rm)
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
      partial <- safe_max(block, "cols", na.rm = na_rm)
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
  acc <- apply_result_names(acc, out_dimnames, reduce_info)
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

#' Apply a function to streamed matrix blocks
#'
#' Evaluates a `delarr` slice-by-slice, materialising manageable chunks for
#' further processing without realising the full matrix.
#'
#' @param x A `delarr` object.
#' @param margin Dimension along which to chunk (`"cols"` or `"rows"`).
#' @param size Approximate chunk size.
#' @param fn Function applied to each materialised chunk.
#' @param parallel Logical; process chunks in parallel when possible.
#' @param workers Number of worker processes for parallel execution.
#'
#' @return A list of results returned by `fn`.
#'
#' @examples
#' mat <- matrix(1:20, nrow = 4, ncol = 5)
#' darr <- delarr(mat)
#'
#' # Apply function to column chunks
#' col_maxes <- block_apply(darr, margin = "cols", size = 2L, fn = function(block) {
#'   apply(block, 2, max)
#' })
#' unlist(col_maxes)
#'
#' # Apply function to row chunks
#' row_means <- block_apply(darr, margin = "rows", size = 2L, fn = function(block) {
#'   rowMeans(block)
#' })
#' unlist(row_means)
#'
#' @export
block_apply <- function(x, margin = c("cols", "rows"), size = 16384L, fn,
                        parallel = FALSE, workers = NULL) {
  margin <- match.arg(margin)
  if (!is.function(fn)) {
    stop("fn must be a function", call. = FALSE)
  }
  dims <- dim(x)
  total <- if (margin == "cols") dims[2] else dims[1]
  chunks <- seq_chunk(total, size)
  eval_chunk <- function(i) {
    indices <- chunks[[i]]
    slice_arr <- if (margin == "cols") {
      x[, indices, drop = FALSE]
    } else {
      x[indices, , drop = FALSE]
    }
    block <- collect(slice_arr, chunk_size = size)
    fn(block)
  }

  if (isTRUE(parallel) && identical(.Platform$OS.type, "unix")) {
    avail <- suppressWarnings(parallel::detectCores(logical = FALSE))
    default_cores <- if (is.na(avail)) 1L else max(1L, avail - 1L)
    cores <- as.integer(workers %||% default_cores)
    out <- parallel::mclapply(seq_along(chunks), eval_chunk, mc.cores = max(1L, cores))
    return(out)
  }

  out <- vector("list", length(chunks))
  for (i in seq_along(chunks)) {
    out[[i]] <- eval_chunk(i)
  }
  out
}
