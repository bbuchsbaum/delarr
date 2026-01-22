# Codebase Concerns

**Analysis Date:** 2026-01-22

## Tech Debt

**Duplicate validation in hdf5_writer:**
- Issue: Lines 23-24 and 29-30 in `/Users/bbuchsbaum/code/delarr/R/delarr-writer-hdf5.R` perform identical validation of the `chunk` parameter
- Files: `R/delarr-writer-hdf5.R`
- Impact: Code duplication makes maintenance harder; if validation logic needs to change, it must be updated in two places
- Fix approach: Remove the duplicate check at line 29-30 and keep only the first validation block

**Unimplemented backend interface:**
- Issue: `delarr_mmap()` in `/Users/bbuchsbaum/code/delarr/R/delarr-backends.R` at line 119 is a placeholder stub that immediately errors
- Files: `R/delarr-backends.R`
- Impact: Users expecting mmap support will encounter a hard error at runtime. This is documented in open-questions but blocks any integration work requiring memory-mapped arrays
- Fix approach: Either implement mmap support with appropriate dependencies, or clearly document in package vignettes that users must supply custom backends via `delarr_backend()`

**Unused compression parameter:**
- Issue: `hdf5_writer()` at `/Users/bbuchsbaum/code/delarr/R/delarr-writer-hdf5.R` line 19 accepts a `compression` parameter that is never used (line 14 explicitly notes "currently unused")
- Files: `R/delarr-writer-hdf5.R`
- Impact: API surface confusion; users may pass compression settings that are silently ignored
- Fix approach: Either implement compression support and pass it to `create_dataset()`, or remove the parameter from the function signature and document the constraint

## Known Gaps

**Limited reduce function support:**
- Issue: In `/Users/bbuchsbaum/code/delarr/R/delarr-eval.R` lines 121-137, `apply_reduce_full()` has explicit hardcoded branches for `sum`, `mean`, `min`, and `max`, with a generic fallback
- Files: `R/delarr-eval.R`
- Impact: Generic reduce functions fall back to full materialisation without chunking; users with custom reduction functions cannot leverage streaming benefits. The `classify_reduce()` function at line 140 also defaults unknown functions to "generic"
- Fix approach: Extend the classification system to detect which custom functions support streaming semantics, or provide user-configurable hints via operation metadata

**Detrend semantics uncertainty:**
- Issue: Open questions note (lines 14-15 in `/Users/bbuchsbaum/code/delarr/notes/open-questions.md`) that detrend semantics for fMRI data need confirmation regarding axis behaviour (time along rows vs columns)
- Files: `R/delarr-helpers.R`, `R/delarr-verbs.R`
- Impact: Current implementation in `detrend_matrix()` at `/Users/bbuchsbaum/code/delarr/R/delarr-helpers.R` uses `ncol(mat)` as sequence for row detrending, which may not align with fMRI time-series conventions. Tests in `/Users/bbuchsbaum/code/delarr/tests/testthat/test-core.R` do not validate fMRI-specific semantics
- Fix approach: Conduct fMRI domain analysis, update implementation if needed, and add integration tests with realistic fMRI data

## Fragile Areas

**Complex chunked reduce logic:**
- Files: `R/delarr-eval.R` lines 302-425
- Why fragile: The row and column reduction logic spans 124 lines with deeply nested conditionals handling min/max initialization, count tracking for NAs, and partial accumulation. The patterns are similar but with subtle differences (e.g., `pmin`/`pmax` vs direct assignment). Tests verify correctness but do not cover edge cases like all-NA columns or empty blocks
- Safe modification: Add comprehensive edge-case tests before refactoring; extract min/max accumulation into a separate helper function to reduce duplication
- Test coverage: `/Users/bbuchsbaum/code/delarr/tests/testthat/test-core.R` lines 145-153 test chunked reductions, but missing: all-NA cases, empty blocks, mixed NA patterns, and very large chunk sizes

**Index normalization with negative indices:**
- Files: `R/utils.R` lines 22-33
- Why fragile: `normalize_index()` must handle positive, negative, and logical indices with proper validation. Negative index support via `setdiff()` is correct but depends on strict semantics (no mixing, no zero, no NA)
- Safe modification: Keep tests comprehensive; any change to setdiff logic or boundary handling risks subtle off-by-one errors
- Test coverage: Basic tests exist but no explicit tests for edge cases like `c(-1, -nrow)` or logical vectors of length 1

**HDF5 file handle lifecycle:**
- Files: `R/delarr-backends.R` lines 65-81, `R/delarr-writer-hdf5.R` lines 38-79
- Why fragile: Both `delarr_hdf5()` and `hdf5_writer()` manage HDF5 file handles via environments and callbacks. The lifecycle depends on proper invocation of `begin()` and `end()` lifecycle functions. If `collect()` is interrupted or errors before `end()` is called, file handles may leak
- Safe modification: Ensure all `collect()` paths guarantee `on.exit()` cleanup (currently done at line 193-195 in delarr-eval.R, but test with actual file handle limits)
- Test coverage: `/Users/bbuchsbaum/code/delarr/tests/testthat/test-core.R` lines 236-255 test HDF5 writer but use temporary files; no explicit tests for handle exhaustion or recovery from exceptions mid-stream

**Broadcast dimension logic:**
- Files: `R/delarr-eval.R` lines 46-71 (`broadcast_rhs()`) and `/Users/bbuchsbaum/code/delarr/R/utils.R` lines 36-41 (`seq_chunk()`)
- Why fragile: `broadcast_rhs()` infers dimension intent from vector length; if `ncol != nrow` and vector length equals either, interpretation depends on that condition. Row/column inference is implicit (lines 63-67 test `len == nr` first, then `len == nc`). The `seq_chunk()` helper at line 36 uses `ceiling()` which requires careful off-by-one analysis
- Safe modification: Add explicit tests for edge cases: vectors matching both dimensions (currently errors), zero-length dimensions, and broadcast with operations that fail dimension validation
- Test coverage: Lines 194-203 in test-core.R test broadcasting with scalars and vectors; missing coverage for ambiguous cases and error handling

## Performance Bottlenecks

**Full materialisation for center/scale/zscore on row dimension:**
- Problem: In `/Users/bbuchsbaum/code/delarr/R/delarr-eval.R` line 203, operations like `d_center(..., dim = "rows")` trigger full data materialisation via `requires_full_eval()` check at line 40
- Files: `R/delarr-eval.R`, `R/delarr-verbs.R`
- Cause: Centering by rows requires computing per-row statistics first, which demands seeing all columns for each row. The streaming architecture reads column chunks, making per-row statistics require full data load
- Improvement path: Implement a two-pass streaming approach: first pass computes per-row means/sds in chunks, second pass applies centering with cached statistics. This would add complexity but unlock streaming for this common operation

**Generic reduce function fallback:**
- Problem: User-defined reduce functions (not sum/mean/min/max) at line 154-156 in `delarr-eval.R` cause full materialisation (line 269)
- Files: `R/delarr-eval.R`
- Cause: No way to hint to the engine which custom functions support incremental reduction
- Improvement path: Introduce optional function attributes (e.g., `attr(fn, "streaming") <- TRUE`) to allow users to opt custom functions into chunked reduction; provide helpers for common streaming patterns (e.g., weighted sum, variance)

**Row detrending materialises full matrix:**
- Problem: `d_detrend(..., dim = "rows")` at `/Users/bbuchsbaum/code/delarr/R/delarr-verbs.R` line 111 contributes to `requires_full_eval()` check (implicitly via `center` operation if applied)
- Files: `R/delarr-helpers.R`, `R/delarr-eval.R`
- Cause: Fitting per-row polynomials requires all columns for each row
- Improvement path: Similar to centering—implement incremental per-row statistics gathering across chunks, then a second pass to detrend

## Scaling Limits

**Single-machine memory constraints:**
- Current capacity: Chunk size defaults to 16384 columns (line 171 in `delarr-eval.R`); tested up to ~60 elements (test-core.R line 147: 6×10 matrix)
- Limit: Any operation requiring full matrix materialisation (center/scale on rows, generic reduce) will load the entire dataset into memory. For a 10GB HDF5 file with millions of rows, this is infeasible
- Scaling path: Implement two-pass streaming for row-wise statistics as noted above; implement sparse array support for fMRI masks (noted in open-questions.md line 11)

**HDF5 dataset size limitations:**
- Current capacity: `hdf5_writer()` is tested with small synthetic data (30 elements, line 240 test-core.R)
- Limit: The design assumes dataset dimensions fit in integer range; no explicit validation of HDF5 chunk limit (typically 4GB per chunk)
- Scaling path: Validate chunk specifications against HDF5 limits; add warnings for unreasonably large chunking; consider adding a size estimation helper

## Maintenance & Stability

**Expression DAG optimisation postponed:**
- Issue: Line 12 of `/Users/bbuchsbaum/code/delarr/notes/open-questions.md` notes that "redundant d_map chains" are not collapsed at append time
- Files: `R/delarr-verbs.R`, `R/delarr-eval.R`
- Impact: Users can write `x |> d_map(f) |> d_map(g)` but it will apply both functions sequentially rather than composing. For large pipelines with many maps, this adds overhead
- Current state: Not yet implemented, accepted as future work

**fmridataset integration blocking:**
- Issue: Codebase exists alongside fmridataset migration notes (`notes/fmridataset-integration.md`) but the adapter layer is not yet implemented
- Files: Related code in `R/delarr-backends.R`, `R/delarr-core.R`; integration notes at `/Users/bbuchsbaum/code/delarr/notes/fmridataset-integration.md`
- Impact: fmridataset cannot yet use delarr as a drop-in replacement for DelayedArray; this blocks adoption and real-world testing
- Current state: Blocked pending completion of API stabilisation

## Missing Critical Features

**Sparse array support:**
- Problem: fMRI analysis uses brain masks (sparse binary arrays) frequently; dense matrix operations are wasteful for 3M-element volumetric data with 50k voxels in mask
- Blocks: Memory-efficient analysis of large-scale fMRI datasets
- Noted in: Open-questions.md line 11

**Memory-mapped backend:**
- Problem: `delarr_mmap()` is stubbed but not implemented
- Blocks: Efficient access to large arrays stored on disk without HDF5 dependency
- Noted in: `R/delarr-backends.rs` line 119 and open-questions.md line 6

**Sparse-dense binary operations:**
- Problem: No support for operations like `sparse_mask * dense_array`
- Blocks: Common fMRI operation where dense time-series are masked to voxels
- Noted in: open-questions.md line 11

## Test Coverage Gaps

**HDF5 exception handling:**
- What's not tested: Behaviour when HDF5 file is corrupted, dataset is deleted mid-stream, or file permissions change
- Files: `tests/testthat/test-core.R` line 236-255
- Risk: Silent data loss or corrupt output if HDF5 operations fail partway through
- Priority: Medium (HDF5 errors should propagate loudly, but integration points need verification)

**Broadcasting with edge cases:**
- What's not tested: Vectors that could match both row and column dimensions, incompatible broadcasting shapes, broadcasting with NaN/Inf
- Files: `R/delarr-eval.R` line 46-71
- Risk: Silent dimension mismatches or numeric edge cases in production
- Priority: Medium (existing tests cover happy path)

**Negative index edge cases:**
- What's not tested: All-negative indices, duplicated indices, reverse-order indices
- Files: `R/utils.R` line 5-34
- Risk: Incorrect slicing if edge cases are not properly normalized
- Priority: Low (negative indexing is less common; existing tests cover basic usage)

**Very large chunk sizes:**
- What's not tested: Chunk size larger than matrix, chunk size near integer overflow, non-power-of-2 chunk sizes
- Files: `R/delarr-eval.R` line 160-173, `R/utils.R` line 36-41
- Risk: Edge cases in `seq_chunk()` or chunk inference could cause off-by-one errors
- Priority: Low (defaults are reasonable; explicit testing of boundary conditions would be defensive)

**All-NA reduction:**
- What's not tested: Reducing rows/columns that are entirely NA with `na.rm = TRUE`
- Files: `R/delarr-eval.R` line 302-425
- Risk: NaN results when dividing by zero in mean reduction (line 355: `acc[idx] / counts[idx]` when count is zero)
- Priority: High (mean of all-NA should be NA, not NaN; this can silently corrupt results)

---

*Concerns audit: 2026-01-22*
