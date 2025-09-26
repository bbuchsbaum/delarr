# Replacing DelayedArray with delarr in fmridataset

This note captures the concrete tasks needed to drop DelayedArray from
*fmridataset* and switch to `delarr`.

## Adapter layer

1. Introduce `as_delarr()` as the public generic; mirror the existing
   `as_delayed_array()` methods (matrix, nifti, hdf5, study, etc.).
2. Each method calls `delarr::delarr_backend()` with:
   - `nrow = backend_get_dims(backend)$time`
   - `ncol = sum(backend_get_mask(backend))`
   - `pull = function(rows = NULL, cols = NULL) backend_get_data(backend, rows, cols)`
3. For the study backend, port the row-concatenation logic from
   `StudyBackendSeed$extract_array()` into the pull closure so that rows are
   materialised lazily subject-by-subject.

## Call sites

- Update `fmri_series()` to call `as_delarr()` and keep matrix-like slicing.
- Allow `output = "lazy"` as the new spelling while preserving the existing
  `"DelayedMatrix"` alias to keep client code working.
- Replace `DelayedArray::extract_array()` usage with
  `collect(x[rows, cols, drop = FALSE])`.

## Tests

- Swap `expect_s4_class(..., "DelayedArray")` for
  `expect_s3_class(..., "delarr")`.
- Redirect the memory-efficiency test that currently calls
  `DelayedMatrixStats::rowMeans2()` to `delarr::collect(d_reduce(...))` or a
  lightweight wrapper.
- Ensure integration tests rely on `collect()` for materialisation.

## Dependencies

- Remove `DelayedArray` and `S4Vectors` from `DESCRIPTION`.
- Add `delarr` to `Imports`, and conditionally Suggest it in packages that only
  need the lazy infrastructure for tests.

## Documentation

- Update user guides and vignettes to reflect the new lazy type name.
- Mention that `delarr` uses standard matrix subsetting and is fully pipeable.

## Optional enhancements

- If needed, provide a thin wrapper `as_delayed_array()` that forwards to
  `as_delarr()` during the transition period to minimise diff size.
- Schedule a second pass to take advantage of `block_apply()` once delarr gains
  chunk-aware reductions.
