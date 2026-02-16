random_index <- function(n) {
  kind <- sample(c("positive", "negative", "logical"), 1L)
  if (kind == "positive") {
    size <- sample.int(n, 1L)
    sort(sample.int(n, size = size))
  } else if (kind == "negative") {
    if (n == 1L) {
      return(1L)
    }
    drop_n <- sample.int(n - 1L, 1L)
    -sort(sample.int(n, size = drop_n))
  } else {
    idx <- sample(c(TRUE, FALSE), n, replace = TRUE)
    if (!any(idx)) {
      idx[sample.int(n, 1L)] <- TRUE
    }
    idx
  }
}

test_that("random delayed DAGs match eager evaluation", {
  set.seed(101)
  for (iter in seq_len(40)) {
    nr <- sample(3:6, 1L)
    nc <- sample(3:7, 1L)
    mat <- matrix(rnorm(nr * nc), nr, nc)
    x <- delarr(mat)
    eager <- mat
    steps <- sample(3:6, 1L)

    for (k in seq_len(steps)) {
      op <- sample(c("map", "where", "add_scalar", "mul_scalar", "slice_rows", "slice_cols"), 1L)
      if (op == "map") {
        f <- sample(c("square", "log1p"), 1L)
        if (identical(f, "square")) {
          x <- d_map(x, ~ .x^2)
          eager <- eager^2
        } else {
          x <- d_map(x, ~ log1p(abs(.x)))
          eager <- log1p(abs(eager))
        }
      } else if (op == "where") {
        thr <- runif(1, -0.5, 0.5)
        pred <- local({
          threshold <- thr
          function(.x) .x > threshold
        })
        x <- d_where(x, pred, fill = 0)
        eager[eager <= thr] <- 0
      } else if (op == "add_scalar") {
        s <- runif(1, -2, 2)
        x <- x + s
        eager <- eager + s
      } else if (op == "mul_scalar") {
        s <- runif(1, -2, 2)
        x <- x * s
        eager <- eager * s
      } else if (op == "slice_rows" && nrow(eager) >= 1L) {
        idx <- random_index(nrow(eager))
        x <- x[idx, , drop = FALSE]
        eager <- eager[idx, , drop = FALSE]
      } else if (op == "slice_cols" && ncol(eager) >= 1L) {
        idx <- random_index(ncol(eager))
        x <- x[, idx, drop = FALSE]
        eager <- eager[, idx, drop = FALSE]
      }
    }

    chunk <- sample(1:max(1L, ncol(eager)), 1L)
    expect_equal(collect(x, chunk_size = chunk), eager, tolerance = 1e-10)
  }
})

test_that("random pipelines are invariant to chunk margin and size", {
  set.seed(202)
  for (iter in seq_len(20)) {
    mat <- matrix(rnorm(48), 6, 8)
    x <- delarr(mat) |>
      d_map(~ .x + 1) |>
      d_where(~ .x > -0.25, fill = NA_real_) |>
      d_map(~ ifelse(is.na(.x), 0, .x))
    out_cols <- collect(x, chunk_margin = "cols", chunk_size = sample(1:4, 1L))
    out_rows <- collect(x, chunk_margin = "rows", chunk_size = sample(1:3, 1L))
    expect_equal(out_cols, out_rows, tolerance = 1e-10)
  }
})
