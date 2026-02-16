#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(devtools)
})

devtools::load_all(".", quiet = TRUE)

bench_elapsed <- function(expr, iters = 3L, warmup = 1L) {
  force(expr)
  for (i in seq_len(warmup)) {
    eval.parent(substitute(expr))
  }
  times <- numeric(iters)
  for (i in seq_len(iters)) {
    t0 <- proc.time()[["elapsed"]]
    eval.parent(substitute(expr))
    times[i] <- proc.time()[["elapsed"]] - t0
  }
  c(
    min = min(times),
    median = stats::median(times),
    mean = mean(times),
    max = max(times)
  )
}

fmt <- function(x) sprintf("%.4f", x)

results <- list()
set.seed(42)

message("Preparing in-memory benchmark data...")
nr <- 2000L
nc <- 1200L
mat <- matrix(rnorm(nr * nc), nrow = nr, ncol = nc)
x_mem <- delarr(mat) |>
  d_map(~ log1p(abs(.x))) |>
  d_where(~ .x > 0.25, fill = 0) |>
  d_map(~ sqrt(.x + 1))

ref <- collect(x_mem, chunk_size = 128L, optimize = FALSE)

message("Benchmarking in-memory collect variants...")
results$memory_opt_false <- bench_elapsed(
  collect(x_mem, chunk_size = 128L, optimize = FALSE)
)
results$memory_opt_true <- bench_elapsed(
  collect(x_mem, chunk_size = 128L, optimize = TRUE)
)
results$memory_target_bytes <- bench_elapsed(
  collect(x_mem, target_bytes = 2e6, optimize = TRUE)
)
results$memory_row_chunk <- bench_elapsed(
  collect(x_mem, chunk_margin = "rows", chunk_size = 128L, optimize = TRUE)
)

if (!identical(.Platform$OS.type, "windows")) {
  results$memory_parallel <- bench_elapsed(
    collect(x_mem, chunk_size = 128L, optimize = TRUE, parallel = TRUE, workers = 2L)
  )
}

stopifnot(
  isTRUE(all.equal(collect(x_mem, chunk_size = 128L, optimize = TRUE), ref)),
  isTRUE(all.equal(collect(x_mem, chunk_margin = "rows", chunk_size = 128L, optimize = TRUE), ref))
)
if (!is.null(results$memory_parallel)) {
  stopifnot(isTRUE(all.equal(collect(x_mem, chunk_size = 128L, optimize = TRUE, parallel = TRUE, workers = 2L), ref)))
}

message("Preparing HDF5 benchmark data...")
h5_path <- tempfile(fileext = ".h5")
on.exit(unlink(h5_path), add = TRUE)
write_hdf5(mat, h5_path, "X", compression = 0L)
x_h5 <- delarr_hdf5(h5_path, "X") |>
  d_map(~ .x * 1.5 + 1) |>
  d_where(~ .x > 1, fill = 1)

results$hdf5_chunk128 <- bench_elapsed(
  collect(x_h5, chunk_size = 128L, optimize = TRUE)
)
results$hdf5_target_bytes <- bench_elapsed(
  collect(x_h5, target_bytes = 2e6, optimize = TRUE)
)

message("Preparing mmap benchmark data...")
mm_path <- tempfile(fileext = ".bin")
on.exit(unlink(mm_path), add = TRUE)
writeBin(as.double(mat), mm_path)

old_mmap_backend <- function(path, nrow, ncol, mode = mmap::real64()) {
  delarr_backend(
    nrow = nrow,
    ncol = ncol,
    pull = function(rows = NULL, cols = NULL) {
      rows <- rows %||% seq_len(nrow)
      cols <- cols %||% seq_len(ncol)
      m <- mmap::mmap(path, mode = mode)
      on.exit(mmap::munmap(m))
      vec <- m[]
      full_mat <- matrix(vec[seq_len(nrow * ncol)], nrow = nrow, ncol = ncol)
      full_mat[rows, cols, drop = FALSE]
    }
  )
}

x_mmap_fast <- delarr_mmap(mm_path, nrow = nr, ncol = nc) |>
  d_map(~ .x * 2 + 1)
x_mmap_slow <- old_mmap_backend(mm_path, nrow = nr, ncol = nc) |>
  d_map(~ .x * 2 + 1)

results$mmap_fast <- bench_elapsed(
  collect(x_mmap_fast, chunk_size = 64L, optimize = TRUE)
)
results$mmap_slow_baseline <- bench_elapsed(
  collect(x_mmap_slow, chunk_size = 64L, optimize = TRUE)
)

out_fast <- collect(x_mmap_fast, chunk_size = 64L)
out_slow <- collect(x_mmap_slow, chunk_size = 64L)
stopifnot(isTRUE(all.equal(out_fast, out_slow)))

rows <- list(
  list(name = "memory_opt_false", timing = results$memory_opt_false),
  list(name = "memory_opt_true", timing = results$memory_opt_true),
  list(name = "memory_target_bytes", timing = results$memory_target_bytes),
  list(name = "memory_row_chunk", timing = results$memory_row_chunk),
  if (!is.null(results$memory_parallel)) list(name = "memory_parallel", timing = results$memory_parallel) else NULL,
  list(name = "hdf5_chunk128", timing = results$hdf5_chunk128),
  list(name = "hdf5_target_bytes", timing = results$hdf5_target_bytes),
  list(name = "mmap_fast", timing = results$mmap_fast),
  list(name = "mmap_slow_baseline", timing = results$mmap_slow_baseline)
)
rows <- Filter(Negate(is.null), rows)

tab <- do.call(
  rbind,
  lapply(rows, function(x) {
    data.frame(
      name = x$name,
      min = fmt(as.numeric(x$timing[["min"]])),
      median = fmt(as.numeric(x$timing[["median"]])),
      mean = fmt(as.numeric(x$timing[["mean"]])),
      max = fmt(as.numeric(x$timing[["max"]])),
      stringsAsFactors = FALSE
    )
  })
)

opt_speedup <- as.numeric(results$memory_opt_false[["median"]]) / as.numeric(results$memory_opt_true[["median"]])
mmap_speedup <- as.numeric(results$mmap_slow_baseline[["median"]]) / as.numeric(results$mmap_fast[["median"]])

lines <- c(
  "# delarr collect() benchmark",
  "",
  sprintf("Date: %s", format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z")),
  "",
  sprintf("Data shape: %d x %d (%.1fM cells)", nr, nc, (nr * nc) / 1e6),
  "",
  "## Timing summary (seconds)",
  "",
  "| benchmark | min | median | mean | max |",
  "|---|---:|---:|---:|---:|"
)

for (i in seq_len(nrow(tab))) {
  lines <- c(
    lines,
    sprintf(
      "| %s | %s | %s | %s | %s |",
      tab$name[i], tab$min[i], tab$median[i], tab$mean[i], tab$max[i]
    )
  )
}

lines <- c(
  lines,
  "",
  "## Key speedups",
  "",
  sprintf("- optimize=TRUE vs optimize=FALSE: %.2fx (median)", opt_speedup),
  sprintf("- mmap fast pull vs naive full-materialize pull: %.2fx (median)", mmap_speedup)
)

out_path <- file.path("notes", "benchmark_collect.md")
writeLines(lines, con = out_path)

cat(paste(lines, collapse = "\n"), "\n")
message("Wrote benchmark report: ", out_path)
