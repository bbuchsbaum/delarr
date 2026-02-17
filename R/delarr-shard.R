# ---- Dependency guard --------------------------------------------------------

check_shard_available <- function() {
  if (!requireNamespace("shard", quietly = TRUE)) {
    stop(
      "The 'shard' package is required for shared-memory parallel collect.\n",
      "Install it with: install.packages('shard')",
      call. = FALSE
    )
  }
}

# ---- delarr_shard: shared-memory backend ------------------------------------

#' Create a delayed matrix backed by shared memory
#'
#' Wraps a numeric matrix into shard's shared memory, returning a `delarr`.
#' The shared ALTREP vector is stored on the seed so that `collect_shard()`
#' can reuse it without re-sharing (zero-copy).
#'
#' @param x A numeric matrix.
#' @param backing Backing type passed to `shard::share()`.
#'
#' @return A `delarr` backed by shared memory.
#' @export
#' @examples
#' if (requireNamespace("shard", quietly = TRUE)) {
#'   mat <- matrix(rnorm(20), 4, 5)
#'   darr <- delarr_shard(mat)
#'   collect(darr)
#' }
delarr_shard <- function(x, backing = "auto") {
  check_shard_available()
  if (!is.matrix(x) || !is.numeric(x)) {
    stop("x must be a numeric matrix", call. = FALSE)
  }
  shared <- shard::share(x, backing = backing, min_bytes = 0, readonly = TRUE)
  seed <- delarr_seed(
    nrow = nrow(x),
    ncol = ncol(x),
    pull = function(rows = NULL, cols = NULL) {
      rows <- rows %||% seq_len(nrow(x))
      cols <- cols %||% seq_len(ncol(x))
      shared[rows, cols, drop = FALSE]
    },
    chunk_hint = list(cols = 4096L),
    dimnames = dimnames(x)
  )
  seed$shared <- shared
  class(seed) <- c("delarr_shard_seed", class(seed))
  new_delarr(seed = seed, ops = list())
}

# ---- shard_writer: writer backed by shard::buffer() -------------------------

#' Shared-memory writer for streaming `collect()`
#'
#' Creates a writer object backed by `shard::buffer()` conforming to delarr's
#' writer protocol. Pass to `collect(x, into = shard_writer(...))` to stream
#' results into shared memory.
#'
#' @param nrow,ncol Dimensions of the output matrix.
#' @param backing Backing type passed to `shard::buffer()`.
#'
#' @return A writer list with `$write()`, `$finalize()`, `$result()`, and
#'   `$close()` methods.
#' @export
#' @examples
#' if (requireNamespace("shard", quietly = TRUE)) {
#'   mat <- matrix(rnorm(20), 4, 5)
#'   darr <- delarr(mat)
#'   w <- shard_writer(4, 5)
#'   collect(darr |> d_map(~ .x * 2), into = w)
#'   w$result()
#'   w$close()
#' }
shard_writer <- function(nrow, ncol, backing = "auto") {
  check_shard_available()
  nrow <- as.integer(nrow)
  ncol <- as.integer(ncol)
  buf <- shard::buffer("double", dim = c(nrow, ncol), backing = backing)
  list(
    write = function(block, rows = NULL, cols = NULL, positions = NULL) {
      if (!is.null(positions)) {
        buf[, positions] <- block
      } else if (!is.null(cols)) {
        buf[, seq_along(cols)] <- block
      } else {
        buf[, seq_len(ncol(block))] <- block
      }
      invisible(NULL)
    },
    finalize = function() invisible(NULL),
    result = function() as.matrix(buf),
    close = function() {
      tryCatch(shard::buffer_close(buf), error = function(e) NULL)
      invisible(NULL)
    }
  )
}

# ---- collect_shard: parallel collect via shard_map --------------------------

#' Parallel collect using shard's shared-memory workers
#'
#' Evaluates a `delarr` pipeline in parallel using `shard::shard_map()`. This
#' gives proper multi-process parallelism with shared-memory I/O, including
#' parallel reductions.
#'
#' Pipelines that require full-matrix evaluation (row-wise center/scale/zscore/
#' detrend), paired RHS delarrs (`d_map2` with two delarrs), or generic
#' (user-supplied) reductions automatically fall back to sequential `collect()`.
#'
#' @param x A `delarr` object.
#' @param workers Number of worker processes. Defaults to
#'   `parallel::detectCores() - 1`.
#' @param chunk_size Column chunk size for sharding. If `NULL`, uses the
#'   seed's `chunk_hint` or a sensible default.
#' @param optimize Logical; run DAG optimizations before evaluation.
#'
#' @return A materialised matrix or vector.
#' @export
#' @examples
#' if (requireNamespace("shard", quietly = TRUE)) {
#'   mat <- matrix(rnorm(100), 10, 10)
#'   darr <- delarr_shard(mat)
#'   result <- collect_shard(darr |> d_map(~ .x^2), workers = 2)
#'   all.equal(result, mat^2)
#' }
collect_shard <- function(x, workers = NULL, chunk_size = NULL,
                          optimize = TRUE) {
  check_shard_available()
  stopifnot(inherits(x, "delarr"))

  if (isTRUE(optimize)) {
    x <- optimize_delarr(x)
  }

  seed <- x$seed
  plan <- compile_plan(x)
  rows <- plan$rows %||% seq_len(seed$nrow)
  cols <- plan$cols %||% seq_len(seed$ncol)
  n_rows <- length(rows)
  n_cols <- length(cols)

  # ---- Fallback conditions --------------------------------------------------
  if (requires_full_eval(plan$ops) || plan$pair_rhs) {
    return(collect(x, optimize = FALSE))
  }

  reduce_info <- classify_reduce(plan$reduce)
  if (!is.null(reduce_info) && identical(reduce_info$type, "generic")) {
    return(collect(x, optimize = FALSE))
  }

  # ---- Prepare shared source ------------------------------------------------
  if (inherits(seed, "delarr_shard_seed") && !is.null(seed$shared)) {
    src_shared <- seed$shared
    # The shared matrix has original indexing; we need to remap if sliced
    src_rows <- rows
    src_cols <- cols
  } else {
    # Materialize the seed slice, then share it
    if (is.function(seed$begin)) seed$begin()
    src_mat <- pull_seed(seed, rows = rows, cols = cols)
    if (is.function(seed$end)) seed$end()
    src_shared <- shard::share(src_mat, min_bytes = 0, readonly = TRUE)
    # After materialization, index space remaps to 1:n
    src_rows <- seq_len(n_rows)
    src_cols <- seq_len(n_cols)
  }
  # For temporarily-created shared objects, deterministically close the
  # underlying segment on exit.  Seed-owned shared objects are left alone.
  owns_shared <- !inherits(seed, "delarr_shard_seed") || is.null(seed$shared)
  on.exit({
    if (owns_shared) {
      tryCatch(
        shard::segment_close(shard::shared_segment(src_shared)),
        error = function(e) NULL
      )
    }
    rm(src_shared)
  }, add = TRUE)

  # ---- Resolve workers and chunk_size ---------------------------------------
  avail <- suppressWarnings(parallel::detectCores(logical = FALSE))
  n_workers <- as.integer(workers %||% max(1L, if (is.na(avail)) 1L else avail - 1L))
  n_workers <- max(1L, n_workers)

  if (is.null(chunk_size)) {
    hint <- seed$chunk_hint
    chunk_size <- if (is.list(hint) && !is.null(hint$cols)) {
      as.integer(hint$cols)
    } else {
      as.integer(max(1L, ceiling(n_cols / (n_workers * 4L))))
    }
  }
  chunk_size <- as.integer(min(chunk_size, n_cols))

  # Build worker closures in clean environments containing only the variables
  # they need.  This prevents serialisation of the full collect_shard() frame
  # (which holds the delarr, seed, plan, etc.) and avoids stack overflows when
  # ops contain closures with deep environment chains.
  make_worker_env <- function(...) {
    e <- new.env(parent = globalenv())
    args <- list(...)
    for (nm in names(args)) e[[nm]] <- args[[nm]]
    e
  }

  ops <- plan$ops

  # ---- Path A: Non-reduction (elementwise) ----------------------------------
  if (is.null(reduce_info)) {
    out_buf <- shard::buffer("double", dim = c(n_rows, n_cols))
    on.exit(tryCatch(shard::buffer_close(out_buf), error = function(e) NULL),
            add = TRUE)

    wenv <- make_worker_env(.rows = src_rows, .cols = src_cols, .ops = ops)
    worker_fn <- function(shard, src, buf) {
      pull_cols <- .cols[shard$idx]
      block <- src[.rows, pull_cols, drop = FALSE]
      block <- delarr:::apply_ops(block, .ops)
      buf[, shard$idx] <- block
      NULL
    }
    environment(worker_fn) <- wenv

    res <- shard::shard_map(
      shard::shards(n_cols, block_size = chunk_size, workers = n_workers),
      fun = worker_fn,
      borrow = list(src = src_shared),
      out = list(buf = out_buf),
      workers = n_workers,
      packages = "delarr",
      max_retries = 1L,
      diagnostics = FALSE
    )

    if (!shard::succeeded(res)) {
      warning("shard_map failed; falling back to sequential collect()")
      return(collect(x, optimize = FALSE))
    }
    return(as.matrix(out_buf))
  }

  # ---- Path B: Row-reduction ------------------------------------------------
  if (identical(reduce_info$dim, "rows")) {
    wenv <- make_worker_env(.rows = src_rows, .cols = src_cols, .ops = ops,
                            .type = reduce_info$type, .na_rm = reduce_info$na.rm)
    worker_fn <- function(shard, src) {
      pull_cols <- .cols[shard$idx]
      block <- src[.rows, pull_cols, drop = FALSE]
      block <- delarr:::apply_ops(block, .ops)
      partial <- switch(.type,
        sum  = rowSums(block, na.rm = .na_rm),
        mean = rowSums(block, na.rm = .na_rm),
        min  = apply(block, 1L, min, na.rm = .na_rm),
        max  = apply(block, 1L, max, na.rm = .na_rm)
      )
      counts <- if (.na_rm || identical(.type, "mean")) {
        rowSums(!is.na(block))
      } else {
        NULL
      }
      list(partial = partial, counts = counts)
    }
    environment(worker_fn) <- wenv

    res <- shard::shard_map(
      shard::shards(n_cols, block_size = chunk_size, workers = n_workers),
      fun = worker_fn,
      borrow = list(src = src_shared),
      workers = n_workers,
      packages = "delarr",
      max_retries = 1L,
      diagnostics = FALSE
    )

    if (!shard::succeeded(res)) {
      warning("shard_map failed; falling back to sequential collect()")
      return(collect(x, optimize = FALSE))
    }

    parts <- shard::results(res)
    return(merge_row_reduction(parts, reduce_info$type, reduce_info$na.rm,
                               n_rows, n_cols))
  }

  # ---- Path C: Column-reduction ---------------------------------------------
  wenv <- make_worker_env(.rows = src_rows, .cols = src_cols, .ops = ops,
                          .type = reduce_info$type, .na_rm = reduce_info$na.rm)
  worker_fn <- function(shard, src) {
    pull_cols <- .cols[shard$idx]
    block <- src[.rows, pull_cols, drop = FALSE]
    block <- delarr:::apply_ops(block, .ops)
    partial <- switch(.type,
      sum  = colSums(block, na.rm = .na_rm),
      mean = colSums(block, na.rm = .na_rm),
      min  = apply(block, 2L, min, na.rm = .na_rm),
      max  = apply(block, 2L, max, na.rm = .na_rm)
    )
    counts <- if (.na_rm || identical(.type, "mean")) {
      colSums(!is.na(block))
    } else {
      NULL
    }
    list(partial = partial, counts = counts, positions = shard$idx)
  }
  environment(worker_fn) <- wenv

  res <- shard::shard_map(
    shard::shards(n_cols, block_size = chunk_size, workers = n_workers),
    fun = worker_fn,
    borrow = list(src = src_shared),
    workers = n_workers,
    packages = "delarr",
    max_retries = 1L,
    diagnostics = FALSE
  )

  if (!shard::succeeded(res)) {
    warning("shard_map failed; falling back to sequential collect()")
    return(collect(x, optimize = FALSE))
  }

  parts <- shard::results(res)
  merge_col_reduction(parts, reduce_info$type, reduce_info$na.rm,
                      n_rows, n_cols)
}

# ---- Merge helpers for reductions -------------------------------------------

merge_row_reduction <- function(parts, type, na_rm, n_rows, n_cols) {
  if (type %in% c("sum", "mean")) {
    acc <- numeric(n_rows)
    counts <- if (na_rm || identical(type, "mean")) numeric(n_rows) else NULL
    for (p in parts) {
      acc <- acc + p$partial
      if (!is.null(counts) && !is.null(p$counts)) {
        counts <- counts + p$counts
      }
    }
    if (identical(type, "mean")) {
      if (!is.null(counts) && na_rm) {
        acc[counts == 0] <- NA_real_
        idx <- counts > 0
        acc[idx] <- acc[idx] / counts[idx]
      } else {
        acc <- acc / n_cols
      }
    } else if (na_rm && !is.null(counts)) {
      acc[counts == 0] <- NA_real_
    }
    return(acc)
  }

  # min / max
  acc <- NULL
  counts <- if (na_rm) numeric(n_rows) else NULL
  for (p in parts) {
    partial <- p$partial
    if (na_rm) {
      # Replace Inf/-Inf from all-NA columns with NA for merging
      partial[is.infinite(partial)] <- NA_real_
    }
    if (is.null(acc)) {
      acc <- partial
    } else if (identical(type, "min")) {
      acc <- pmin(acc, partial, na.rm = na_rm)
    } else {
      acc <- pmax(acc, partial, na.rm = na_rm)
    }
    if (!is.null(counts) && !is.null(p$counts)) {
      counts <- counts + p$counts
    }
  }
  if (!is.null(counts) && na_rm) {
    acc[counts == 0] <- NA_real_
  }
  acc
}

merge_col_reduction <- function(parts, type, na_rm, n_rows, n_cols) {
  if (type %in% c("sum", "mean")) {
    acc <- numeric(n_cols)
    counts <- if (na_rm || identical(type, "mean")) numeric(n_cols) else NULL
    for (p in parts) {
      acc[p$positions] <- acc[p$positions] + p$partial
      if (!is.null(counts) && !is.null(p$counts)) {
        counts[p$positions] <- counts[p$positions] + p$counts
      }
    }
    if (identical(type, "mean")) {
      if (!is.null(counts) && na_rm) {
        acc[counts == 0] <- NA_real_
        idx <- counts > 0
        acc[idx] <- acc[idx] / counts[idx]
      } else {
        acc <- acc / n_rows
      }
    } else if (na_rm && !is.null(counts)) {
      acc[counts == 0] <- NA_real_
    }
    return(acc)
  }

  # min / max
  acc <- rep(NA_real_, n_cols)
  counts <- if (na_rm) numeric(n_cols) else NULL
  for (p in parts) {
    partial <- p$partial
    if (na_rm) {
      partial[is.infinite(partial)] <- NA_real_
    }
    pos <- p$positions
    missing <- is.na(acc[pos])
    if (any(missing)) {
      acc[pos][missing] <- partial[missing]
    }
    if (any(!missing)) {
      if (identical(type, "min")) {
        acc[pos][!missing] <- pmin(acc[pos][!missing], partial[!missing],
                                   na.rm = na_rm)
      } else {
        acc[pos][!missing] <- pmax(acc[pos][!missing], partial[!missing],
                                   na.rm = na_rm)
      }
    }
    if (!is.null(counts) && !is.null(p$counts)) {
      counts[pos] <- counts[pos] + p$counts
    }
  }
  if (!is.null(counts) && na_rm) {
    acc[counts == 0] <- NA_real_
  }
  acc
}

