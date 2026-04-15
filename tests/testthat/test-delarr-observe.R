test_that("explain infers chunk plans for N-d pipelines and reductions", {
  arr <- array(seq_len(24), dim = c(2, 3, 4))

  info_nd <- explain(delarr(arr) |> d_map(~ .x + 1), target_bytes = 32)
  expect_s3_class(info_nd, "delarr_explain")
  expect_equal(info_nd$input_dim, c(2, 3, 4))
  expect_equal(info_nd$output_dim, c(2, 3, 4))
  expect_equal(info_nd$chunk_margin, 3L)
  expect_equal(info_nd$chunk_size, 2L)
  expect_equal(info_nd$chunk_count, 2L)

  info_reduce <- explain(d_reduce(delarr(arr), sum, axis = 3L), target_bytes = 32)
  expect_true(info_reduce$has_reduce)
  expect_equal(info_reduce$reduce_dim, NULL)
  expect_equal(info_reduce$output_dim, c(2, 3))
  expect_equal(info_reduce$chunk_margin, 3L)
  expect_equal(info_reduce$chunk_size, 2L)
  expect_equal(info_reduce$chunk_count, 2L)
})

test_that("explain falls back to a single full pass for generic N-d reductions", {
  arr <- array(seq_len(24), dim = c(2, 3, 4))

  info <- explain(
    d_reduce(delarr(arr), function(x, na.rm = FALSE) sum(x, na.rm = na.rm), axis = 3L),
    chunk_margin = 3L,
    target_bytes = 32
  )

  expect_true(info$has_reduce)
  expect_null(info$chunk_margin)
  expect_true(is.na(info$chunk_size))
  expect_equal(info$chunk_count, 1L)
})

test_that("print methods emit useful summaries", {
  arr <- array(seq_len(24), dim = c(2, 3, 4))

  info <- explain(d_reduce(delarr(arr), sum, axis = 3L), target_bytes = 32)
  expect_snapshot_output(print(info))

  prof <- profile_collect(delarr(arr), reps = 2L)
  expect_snapshot_output(print(prof))
})

test_that("profile_collect validates reps and reports output size", {
  arr <- array(seq_len(24), dim = c(2, 3, 4))

  expect_error(profile_collect(delarr(arr), reps = 0L), "reps must be >= 1")

  prof <- profile_collect(delarr(arr), reps = 2L)
  expect_s3_class(prof, "delarr_profile")
  expect_equal(length(prof$output_size_bytes), 2L)
  expect_true(all(!is.na(prof$output_size_bytes)))
  expect_equal(prof$output_class, "array")
})
