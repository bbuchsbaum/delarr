test_that("delarr() wraps 3D arrays", {
  arr <- array(seq_len(24), dim = c(3, 4, 2))
  darr <- delarr(arr)
  expect_s3_class(darr, "delarr")
  expect_equal(dim(darr), c(3L, 4L, 2L))
})

test_that("delarr() wraps 4D arrays", {
  arr <- array(rnorm(120), dim = c(3, 4, 5, 2))
  darr <- delarr(arr)
  expect_equal(dim(darr), c(3L, 4L, 5L, 2L))
})

test_that("delarr_seed_nd creates valid seeds", {
  arr <- array(1:60, dim = c(3, 4, 5))
  seed <- delarr_seed_nd(
    dims = c(3, 4, 5),
    pull = function(indices) {
      idx <- lapply(seq_along(dim(arr)), function(k) {
        indices[[k]] %||% seq_len(dim(arr)[k])
      })
      do.call("[", c(list(arr), idx, list(drop = FALSE)))
    }
  )
  expect_s3_class(seed, "delarr_seed")
  expect_equal(dim(seed), c(3L, 4L, 5L))
  expect_equal(seed$nrow, 3L)
  expect_equal(seed$ncol, 4L)
})

test_that("pull_seed_nd retrieves correct sub-arrays", {
  arr <- array(seq_len(24), dim = c(2, 3, 4))
  seed <- delarr_seed_nd(
    dims = c(2, 3, 4),
    pull = function(indices) {
      idx <- lapply(seq_along(dim(arr)), function(k) {
        indices[[k]] %||% seq_len(dim(arr)[k])
      })
      do.call("[", c(list(arr), idx, list(drop = FALSE)))
    }
  )
  res <- pull_seed_nd(seed, list(NULL, 1:2, 3:4))
  expect_equal(dim(res), c(2L, 2L, 2L))
  expect_equal(res, arr[, 1:2, 3:4, drop = FALSE])
})

test_that("3D slicing works via [", {
  arr <- array(seq_len(60), dim = c(3, 4, 5))
  darr <- delarr(arr)
  sliced <- darr[1:2, , 3:5]
  expect_equal(dim(sliced), c(2L, 4L, 3L))
})

test_that("3D dimnames are preserved", {
  arr <- array(1:24, dim = c(2, 3, 4),
               dimnames = list(c("r1", "r2"), c("c1", "c2", "c3"), NULL))
  darr <- delarr(arr)
  dn <- dimnames(darr)
  expect_equal(length(dn), 3L)
  expect_equal(dn[[1]], c("r1", "r2"))
  expect_equal(dn[[2]], c("c1", "c2", "c3"))
  expect_null(dn[[3]])

  sliced <- darr[1, 2:3, ]
  dn2 <- dimnames(sliced)
  expect_equal(dn2[[1]], "r1")
  expect_equal(dn2[[2]], c("c2", "c3"))
})

test_that("print works for N-d delarr", {
  arr <- array(1:120, dim = c(3, 4, 5, 2))
  darr <- delarr(arr)
  out <- capture.output(print(darr))
  expect_match(out, "3 x 4 x 5 x 2")
})

test_that("is_nd_seed detects N-d seeds", {
  mat <- matrix(1:12, 3, 4)
  darr2d <- delarr(mat)
  expect_false(delarr:::is_nd_seed(darr2d$seed))

  arr <- array(1:24, dim = c(2, 3, 4))
  darr3d <- delarr(arr)
  expect_true(delarr:::is_nd_seed(darr3d$seed))
})

test_that("elementwise d_map works on 3D delarr (lazy)", {
  arr <- array(1:24, dim = c(2, 3, 4))
  darr <- delarr(arr)
  mapped <- d_map(darr, ~ .x * 2)
  expect_equal(dim(mapped), c(2L, 3L, 4L))
})

test_that("Ops work on 3D delarr (lazy)", {
  arr <- array(1:24, dim = c(2, 3, 4))
  darr <- delarr(arr)
  result <- darr + 10
  expect_equal(dim(result), c(2L, 3L, 4L))
})

test_that("axis utility functions work on 3D arrays", {
  arr <- array(rnorm(60), dim = c(3, 4, 5))

  m1 <- delarr:::axis_means(arr, 1L)
  expect_equal(length(m1), 3L)
  expect_equal(m1, apply(arr, 1, mean))

  m3 <- delarr:::axis_means(arr, 3L)
  expect_equal(length(m3), 5L)
  expect_equal(m3, apply(arr, 3, mean))

  s2 <- delarr:::axis_sums(arr, 2L)
  expect_equal(length(s2), 4L)
  expect_equal(s2, apply(arr, 2, sum))
})

test_that("axis_center works on 3D arrays", {
  arr <- array(rnorm(60), dim = c(3, 4, 5))
  centered <- delarr:::axis_center(arr, 3L)
  expect_equal(dim(centered), dim(arr))
  means_after <- apply(centered, 3, mean)
  expect_true(all(abs(means_after) < 1e-12))
})

test_that("axis_scale works on 3D arrays", {
  arr <- array(rnorm(60), dim = c(3, 4, 5))
  scaled <- delarr:::axis_scale(arr, 2L, center = TRUE, scale = TRUE)
  expect_equal(dim(scaled), dim(arr))
  means_after <- apply(scaled, 2, mean)
  sds_after <- apply(scaled, 2, sd)
  expect_true(all(abs(means_after) < 1e-12))
  expect_true(all(abs(sds_after - 1) < 1e-12))
})

# ---- collect() for N-d arrays ------------------------------------------------

test_that("collect materialises a 3D delarr", {
  arr <- array(seq_len(24), dim = c(2, 3, 4))
  darr <- delarr(arr)
  result <- collect(darr)
  expect_equal(result, arr)
})

test_that("collect with d_map on 3D array", {
  arr <- array(rnorm(60), dim = c(3, 4, 5))
  darr <- delarr(arr)
  result <- collect(darr |> d_map(~ .x^2))
  expect_equal(result, arr^2)
})

test_that("collect with Ops on 3D array", {
  arr <- array(1:24, dim = c(2, 3, 4))
  darr <- delarr(arr)
  result <- collect(darr * 2 + 1)
  expect_equal(result, arr * 2 + 1)
})

test_that("collect with slicing on 3D array", {
  arr <- array(seq_len(60), dim = c(3, 4, 5))
  darr <- delarr(arr)
  result <- collect(darr[1:2, , 3:5])
  expect_equal(result, arr[1:2, , 3:5, drop = FALSE])
})

test_that("collect with d_where on 3D array", {
  arr <- array(as.double(1:24), dim = c(2, 3, 4))
  darr <- delarr(arr)
  result <- collect(d_where(darr, ~ .x > 12, fill = 0))
  expected <- arr
  expected[expected <= 12] <- 0
  expect_equal(result, expected)
})

test_that("collect with d_center on 3D array (axis=3)", {
  arr <- array(rnorm(60), dim = c(3, 4, 5))
  darr <- delarr(arr)
  result <- collect(d_center(darr, dim = "rows"))
  expected <- arr
  row_means <- apply(arr, 1, mean)
  for (i in seq_len(3)) expected[i, , ] <- expected[i, , ] - row_means[i]
  expect_equal(result, expected, tolerance = 1e-12)
})

test_that("collect with d_reduce on 3D array (dim=rows)", {
  arr <- array(as.double(1:24), dim = c(2, 3, 4))
  darr <- delarr(arr)
  result <- collect(d_reduce(darr, sum, dim = "rows"))
  expected <- apply(arr, 1, sum)
  expect_equal(as.numeric(result), expected)
})

test_that("collect with d_reduce on 3D array (dim=cols)", {
  arr <- array(as.double(1:24), dim = c(2, 3, 4))
  darr <- delarr(arr)
  result <- collect(d_reduce(darr, mean, dim = "cols"))
  expected <- apply(arr, 2, mean)
  expect_equal(as.numeric(result), expected)
})

test_that("collect works on 4D arrays (fMRI-like)", {
  arr <- array(rnorm(120), dim = c(3, 4, 5, 2))
  darr <- delarr(arr)
  result <- collect(darr |> d_map(~ .x * 10))
  expect_equal(result, arr * 10)
  expect_equal(dim(result), c(3L, 4L, 5L, 2L))
})

test_that("collect preserves dimnames for 3D arrays", {
  arr <- array(1:24, dim = c(2, 3, 4),
               dimnames = list(c("r1", "r2"), c("c1", "c2", "c3"), NULL))
  darr <- delarr(arr)
  result <- collect(darr)
  expect_equal(dimnames(result)[[1]], c("r1", "r2"))
  expect_equal(dimnames(result)[[2]], c("c1", "c2", "c3"))
  expect_null(dimnames(result)[[3]])
})

test_that("chained ops work on 3D collect", {
  arr <- array(rnorm(60), dim = c(3, 4, 5))
  darr <- delarr(arr)
  result <- collect(darr |> d_map(~ .x + 1) |> d_map(~ .x * 2))
  expect_equal(result, (arr + 1) * 2)
})

test_that("collect chunks N-d arrays along a configurable axis", {
  arr <- array(seq_len(72), dim = c(3, 4, 6))
  pulls <- 0L
  seed <- delarr_seed_nd(
    dims = dim(arr),
    pull = function(indices) {
      pulls <<- pulls + 1L
      idx <- lapply(seq_along(dim(arr)), function(k) {
        indices[[k]] %||% seq_len(dim(arr)[[k]])
      })
      do.call(`[`, c(list(arr), idx, list(drop = FALSE)))
    }
  )

  result <- collect(delarr(seed), chunk_size = 2L, chunk_margin = 3L)
  expect_equal(result, arr)
  expect_equal(pulls, 3L)
})

test_that("built-in N-d reductions stream by chunk along reduced axes", {
  arr <- array(seq_len(72), dim = c(3, 4, 6))
  pulls <- 0L
  seed <- delarr_seed_nd(
    dims = dim(arr),
    pull = function(indices) {
      pulls <<- pulls + 1L
      idx <- lapply(seq_along(dim(arr)), function(k) {
        indices[[k]] %||% seq_len(dim(arr)[[k]])
      })
      do.call(`[`, c(list(arr), idx, list(drop = FALSE)))
    }
  )

  result <- collect(d_reduce(delarr(seed), sum, axis = 3L), chunk_size = 2L, chunk_margin = 3L)
  expect_equal(result, apply(arr, c(1, 2), sum))
  expect_equal(pulls, 3L)
})

test_that("N-d min and max reductions handle all-NA slices across chunks", {
  arr <- array(as.double(seq_len(2 * 3 * 4)), dim = c(2, 3, 4))
  arr[1, 2, ] <- NA_real_
  darr <- delarr(arr)

  min_result <- collect(
    d_reduce(darr, min, axis = 3L, na.rm = TRUE),
    chunk_size = 2L,
    chunk_margin = 3L
  )
  max_result <- collect(
    d_reduce(darr, max, axis = 3L, na.rm = TRUE),
    chunk_size = 2L,
    chunk_margin = 3L
  )

  expected_min <- apply(arr, c(1, 2), function(x) {
    if (all(is.na(x))) NA_real_ else min(x, na.rm = TRUE)
  })
  expected_max <- apply(arr, c(1, 2), function(x) {
    if (all(is.na(x))) NA_real_ else max(x, na.rm = TRUE)
  })

  expect_equal(min_result, expected_min)
  expect_equal(max_result, expected_max)
})

test_that("N-d generic reductions fall back to full evaluation", {
  arr <- array(as.double(c(1:23, NA)), dim = c(2, 3, 4))
  darr <- delarr(arr)

  expect_equal(
    collect(d_reduce(darr, stats::median, axis = c(1L, 2L, 3L))),
    stats::median(as.vector(arr))
  )

  trimmed_mean <- function(x, na.rm = FALSE) mean(x, trim = 0.25, na.rm = na.rm)
  expect_equal(
    collect(d_reduce(darr, trimmed_mean, axis = c(1L, 2L, 3L), na.rm = TRUE)),
    trimmed_mean(as.vector(arr), na.rm = TRUE)
  )
})

test_that("N-d collect can stream chunks into a callback", {
  arr <- array(seq_len(2 * 3 * 4), dim = c(2, 3, 4))
  chunks <- list()

  result <- collect(
    delarr(arr) |> d_map(~ .x * 2),
    into = function(block, indices, axis, positions) {
      chunks[[length(chunks) + 1L]] <<- list(
        block = block,
        indices = indices,
        axis = axis,
        positions = positions
      )
    },
    chunk_size = 2L,
    chunk_margin = 3L
  )

  expect_null(result)
  expect_equal(length(chunks), 2L)
  expect_equal(chunks[[1L]]$axis, 3L)
  expect_equal(chunks[[1L]]$positions, 1:2)
  expect_equal(chunks[[1L]]$block, arr[, , 1:2, drop = FALSE] * 2)
  expect_equal(chunks[[2L]]$block, arr[, , 3:4, drop = FALSE] * 2)
})

test_that("N-d collect rejects writer-style into targets", {
  arr <- array(seq_len(2 * 3 * 4), dim = c(2, 3, 4))
  writer <- list(write = function(...) NULL)

  expect_error(
    collect(delarr(arr), into = writer, chunk_size = 2L, chunk_margin = 3L),
    "Writer-style into targets"
  )
})

# ---- axis-based verb tests ----------------------------------------------------

test_that("d_reduce with axis= on 3D array", {
  arr <- array(as.double(1:60), dim = c(3, 4, 5))
  darr <- delarr(arr)

  # Collapse axis 3 → result should have shape 3 x 4
  result <- collect(d_reduce(darr, sum, axis = 3L))
  expected <- apply(arr, c(1, 2), sum)
  expect_equal(as.numeric(result), as.numeric(expected))
})

test_that("d_reduce with axis= on 4D array", {
  arr <- array(rnorm(120), dim = c(3, 4, 5, 2))
  darr <- delarr(arr)

  # Collapse axis 4 (time) → keep spatial dims
  result <- collect(d_reduce(darr, mean, axis = 4L))
  expected <- apply(arr, c(1, 2, 3), mean)
  expect_equal(result, expected, tolerance = 1e-12)
})

test_that("d_reduce supports multiple axes", {
  arr <- array(as.double(1:120), dim = c(3, 4, 5, 2))
  darr <- delarr(arr)

  result <- collect(d_reduce(darr, sum, axis = c(2L, 4L)))
  expected <- apply(arr, c(1, 3), sum)
  expect_equal(result, expected)
})

test_that("d_reduce supports collapsing all axes", {
  arr <- array(as.double(1:24), dim = c(2, 3, 4))
  darr <- delarr(arr)

  result <- collect(d_reduce(darr, mean, axis = c(1L, 2L, 3L)))
  expect_equal(result, mean(arr))
})

test_that("d_center with axis= on 3D array", {
  arr <- array(rnorm(60), dim = c(3, 4, 5))
  darr <- delarr(arr)

  # Center along axis 3 (each (i,j) slice across dim3 gets mean-subtracted)
  result <- collect(d_center(darr, axis = 3L))
  expect_equal(dim(result), dim(arr))
  # Means along axis 3 should be ~0
  means <- apply(result, 3, mean)
  expect_true(all(abs(means) < 1e-12))
})

test_that("d_zscore with axis= on 3D array", {
  arr <- array(rnorm(60), dim = c(3, 4, 5))
  darr <- delarr(arr)

  result <- collect(d_zscore(darr, axis = 2L))
  expect_equal(dim(result), dim(arr))
  # Means along axis 2 should be ~0, SDs ~1
  means <- apply(result, 2, mean)
  sds <- apply(result, 2, sd)
  expect_true(all(abs(means) < 1e-12))
  expect_true(all(abs(sds - 1) < 1e-12))
})

test_that("d_detrend with axis= on 3D array", {
  # Create array with a linear trend along axis 3
  arr <- array(0, dim = c(2, 3, 10))
  for (k in 1:10) arr[, , k] <- arr[, , k] + k
  arr <- arr + array(rnorm(60, sd = 0.01), dim = c(2, 3, 10))
  darr <- delarr(arr)

  result <- collect(d_detrend(darr, axis = 3L, degree = 1L))
  expect_equal(dim(result), dim(arr))
  # After removing linear trend, values should be near zero
  expect_true(all(abs(result) < 0.5))
})

test_that("print shows axis-based ops", {
  arr <- array(1:24, dim = c(2, 3, 4))
  darr <- delarr(arr) |> d_center(axis = 3L)
  out <- capture.output(print(darr))
  expect_match(out, "axis=3")
})

test_that("print shows multi-axis reduce ops", {
  arr <- array(1:24, dim = c(2, 3, 4))
  darr <- delarr(arr) |> d_reduce(sum, axis = c(1L, 3L))
  out <- capture.output(print(darr))
  expect_match(out, "axis=1,3")
})

test_that("dim reflects axis-based reduce", {
  arr <- array(1:60, dim = c(3, 4, 5))
  darr <- delarr(arr)
  reduced <- d_reduce(darr, sum, axis = 2L)
  d <- dim(reduced)
  expect_equal(d, c(3L, 1L, 5L))
})

test_that("dim and names reflect multi-axis reduce", {
  arr <- array(
    seq_len(60),
    dim = c(3, 4, 5),
    dimnames = list(paste0("r", 1:3), paste0("c", 1:4), paste0("z", 1:5))
  )
  darr <- delarr(arr) |> d_reduce(sum, axis = c(1L, 3L))
  expect_equal(dim(darr), c(1L, 4L, 1L))
  expect_equal(dimnames(darr), list(NULL, paste0("c", 1:4), NULL))
  expect_equal(names(collect(darr)), paste0("c", 1:4))
})

test_that("d_aperm permutes dimensions and dimnames lazily", {
  arr <- array(
    seq_len(24),
    dim = c(2, 3, 4),
    dimnames = list(c("r1", "r2"), c("c1", "c2", "c3"), paste0("t", 1:4))
  )
  darr <- d_aperm(delarr(arr), c(3L, 1L, 2L))
  expect_equal(dim(darr), c(4L, 2L, 3L))
  expect_equal(dimnames(darr), list(paste0("t", 1:4), c("r1", "r2"), c("c1", "c2", "c3")))
  expect_equal(collect(darr), aperm(arr, c(3, 1, 2)))
})

# ---- blocked axis chunking tests ---------------------------------------------

test_that("chunked collect with d_center(axis=3) produces correct results", {
  arr <- array(rnorm(60), dim = c(3, 4, 5))
  darr <- delarr(arr)

  # Full materialization reference
  ref <- collect(d_center(darr, axis = 3L))

  # Force chunking with small chunk_size along axis 3 (the safe axis)
  chunked <- collect(d_center(darr, axis = 3L), chunk_size = 2L, chunk_margin = 3L)
  expect_equal(chunked, ref, tolerance = 1e-12)
})

test_that("chunked collect with d_zscore(axis=2) chunks along axis 2 correctly", {
  arr <- array(rnorm(60), dim = c(3, 4, 5))
  darr <- delarr(arr)

  ref <- collect(d_zscore(darr, axis = 2L))
  chunked <- collect(d_zscore(darr, axis = 2L), chunk_size = 1L, chunk_margin = 2L)
  expect_equal(chunked, ref, tolerance = 1e-12)
})

test_that("blocked_chunk_axes blocks the correct axes", {
  ops <- list(list(op = "center", axis = 3L))
  # For a 3D array, center(axis=3) needs full data on axes 1,2
  # Only axis 3 is safe to chunk
  blocked <- delarr:::blocked_chunk_axes(ops, ndim = 3L)
  expect_equal(sort(blocked), c(1L, 2L))
  expect_false(3L %in% blocked)
})

# ---- utility roundtrip tests -------------------------------------------------

test_that("extract_axis_chunk and assign_axis_chunk roundtrip", {
  arr <- array(seq_len(60), dim = c(3, 4, 5))
  chunk <- delarr:::extract_axis_chunk(arr, 3L, 2:4)
  expect_equal(dim(chunk), c(3L, 4L, 3L))
  expect_equal(chunk, arr[, , 2:4, drop = FALSE])

  out <- array(0L, dim = c(3, 4, 5))
  out <- delarr:::assign_axis_chunk(out, chunk, 3L, 2:4)
  expect_equal(out[, , 2:4], arr[, , 2:4])
  expect_true(all(out[, , 1] == 0L))
  expect_true(all(out[, , 5] == 0L))
})
