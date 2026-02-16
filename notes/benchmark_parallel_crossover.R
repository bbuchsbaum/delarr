#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(devtools)
})

devtools::load_all(".", quiet = TRUE)

bench_elapsed <- function(expr, iters = 2L, warmup = 1L) {
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
  c(min = min(times), median = stats::median(times), mean = mean(times), max = max(times))
}

fmt <- function(x) sprintf("%.4f", x)

# Progressively larger shapes to identify parallel crossover.
sizes <- list(
  c(2000L, 1200L),
  c(4000L, 2000L),
  c(6000L, 2500L),
  c(8000L, 3000L)
)

rows <- list()
chunk_size <- 256L
workers <- 2L

for (shape in sizes) {
  nr <- shape[[1]]
  nc <- shape[[2]]
  cells <- as.double(nr) * as.double(nc)
  iters <- if (cells <= 8e6) 3L else 2L

  message(sprintf("Benchmarking %dx%d (%0.1fM cells)...", nr, nc, cells / 1e6))
  set.seed(100 + nr + nc)
  mat <- matrix(rnorm(nr * nc), nrow = nr, ncol = nc)
  x <- delarr(mat) |>
    d_map(~ log1p(abs(.x))) |>
    d_where(~ .x > 0.25, fill = 0) |>
    d_map(~ sqrt(.x + 1))

  # correctness gate
  seq_ref <- collect(x, chunk_size = chunk_size, optimize = TRUE, parallel = FALSE)
  par_ref <- collect(x, chunk_size = chunk_size, optimize = TRUE, parallel = TRUE, workers = workers)
  stopifnot(isTRUE(all.equal(seq_ref, par_ref)))

  seq_t <- bench_elapsed(
    collect(x, chunk_size = chunk_size, optimize = TRUE, parallel = FALSE),
    iters = iters,
    warmup = 1L
  )
  par_t <- bench_elapsed(
    collect(x, chunk_size = chunk_size, optimize = TRUE, parallel = TRUE, workers = workers),
    iters = iters,
    warmup = 1L
  )

  speedup <- as.numeric(seq_t[["median"]]) / as.numeric(par_t[["median"]])
  rows[[length(rows) + 1L]] <- data.frame(
    shape = sprintf("%dx%d", nr, nc),
    cells_m = sprintf("%.1f", cells / 1e6),
    seq_median = fmt(as.numeric(seq_t[["median"]])),
    par_median = fmt(as.numeric(par_t[["median"]])),
    speedup = sprintf("%.2fx", speedup),
    winner = if (speedup > 1) "parallel" else "sequential",
    stringsAsFactors = FALSE
  )

  rm(mat, x, seq_ref, par_ref, seq_t, par_t)
  gc()
}

tab <- do.call(rbind, rows)
lines <- c(
  "# delarr parallel crossover benchmark",
  "",
  sprintf("Date: %s", format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z")),
  sprintf("Chunk size: %d columns; workers: %d", chunk_size, workers),
  "",
  "| shape | cells (M) | seq median (s) | par median (s) | speedup (seq/par) | winner |",
  "|---|---:|---:|---:|---:|---|"
)

for (i in seq_len(nrow(tab))) {
  lines <- c(
    lines,
    sprintf(
      "| %s | %s | %s | %s | %s | %s |",
      tab$shape[i], tab$cells_m[i], tab$seq_median[i], tab$par_median[i], tab$speedup[i], tab$winner[i]
    )
  )
}

wins <- tab$winner
if (any(wins == "parallel")) {
  first_parallel <- which(wins == "parallel")[1]
  lines <- c(
    lines,
    "",
    sprintf("Parallel first wins at: **%s**", tab$shape[first_parallel])
  )
} else {
  lines <- c(
    lines,
    "",
    "Parallel did not win in tested ranges; crossover likely at larger workloads or with more workers."
  )
}

out_path <- file.path("notes", "benchmark_parallel_crossover.md")
writeLines(lines, con = out_path)
cat(paste(lines, collapse = "\n"), "\n")
message("Wrote crossover report: ", out_path)
