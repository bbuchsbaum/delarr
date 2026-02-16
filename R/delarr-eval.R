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
    op$op %in% c("center", "scale", "zscore", "detrend") &&
      identical(op$dim, "rows")
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
    result <- apply(mat, margin, min, na.rm = na_rm)
    # When na.rm=TRUE and all values are NA, min returns Inf - should be NA
    if (na_rm) {
      result[is.infinite(result)] <- NA_real_
    }
    return(result)
  }
  if (identical(fn, base::max)) {
    result <- apply(mat, margin, max, na.rm = na_rm)
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
  chunk_margin <- match.arg(chunk_margin)
  if (isTRUE(optimize)) {
    x <- optimize_delarr(x)
  }

  seed <- x$seed
  plan <- compile_plan(x)
  rows <- plan$rows %||% seq_len(seed$nrow)
  cols <- plan$cols %||% seq_len(seed$ncol)
  n_rows <- length(rows)
  n_cols <- length(cols)

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
        rhs_mat <- apply_ops(rhs_mat, rhs_plan$ops)
        rhs_chunks[[idx]] <- rhs_mat
      }
    }
    mat <- apply_ops(mat, plan$ops, rhs_chunks)
    res <- apply_reduce_full(mat, plan$reduce)
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
        rhs_block <- apply_ops(rhs_block, ctx$plan$ops)
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

  reduce_info <- classify_reduce(plan$reduce)
  if (!is.null(reduce_info) && identical(reduce_info$type, "generic")) {
    block <- pull_seed(seed, rows = rows, cols = cols)
    rhs_chunks <- rhs_chunks_for(seq_len(n_cols))
    block <- apply_ops(block, plan$ops, rhs_chunks)
    res <- apply_reduce_full(block, plan$reduce)
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
        block <- apply_ops(block, plan$ops, rhs_chunks)
        list(block = block, rows = rows, cols = pull_cols, positions = pos)
      } else {
        pull_rows <- rows[pos]
        block <- pull_seed(seed, rows = pull_rows, cols = cols)
        rhs_chunks <- rhs_chunks_for(pos, margin = "rows")
        block <- apply_ops(block, plan$ops, rhs_chunks)
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
