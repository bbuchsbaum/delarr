# delarr CRAN Preparation

## What This Is

A lightweight delayed matrix type for R with lazy evaluation, chunked streaming, and HDF5 support. The package provides a tidy-friendly API for fused elementwise transforms, reductions, and streaming materialization without loading full matrices into memory.

This milestone focuses on making delarr production-quality and CRAN-ready: comprehensive testing, sound code, proper documentation, and passing R CMD check.

## Core Value

Lazy matrix operations that stream in chunks to handle datasets larger than memory, with a simple S3-based API that integrates cleanly with R workflows.

## Requirements

### Validated

Existing working functionality (from codebase mapping):

- ✓ Core `delarr` S3 class with lazy operation queuing — existing
- ✓ Slicing with `[` operator preserves laziness — existing
- ✓ Elementwise transforms via `d_map()` and `d_map2()` — existing
- ✓ Row/column reductions via `d_reduce()` with sum/mean/min/max — existing
- ✓ Centering, scaling, z-score normalization via `d_center()`, `d_scale()`, `d_zscore()` — existing
- ✓ Polynomial detrending via `d_detrend()` — existing
- ✓ Masked updates via `d_where()` — existing
- ✓ Chunked streaming materialization via `collect()` — existing
- ✓ In-memory backend via `delarr_mem()` — existing
- ✓ HDF5 backend via `delarr_hdf5()` — existing
- ✓ Custom backend support via `delarr_backend()` — existing
- ✓ HDF5 streaming writer via `hdf5_writer()` — existing
- ✓ Arithmetic and comparison operators — existing
- ✓ Basic test suite with testthat — existing

### Active

CRAN preparation requirements:

- [ ] R CMD check passes with no errors, warnings, or notes
- [ ] All exported functions have complete roxygen2 documentation
- [ ] All examples in documentation run without error
- [ ] Comprehensive test coverage for edge cases (all-NA, empty, boundary conditions)
- [ ] Tech debt cleanup: remove duplicate validation in hdf5_writer
- [ ] Tech debt cleanup: handle unused compression parameter (remove or implement)
- [ ] Tech debt cleanup: handle stub `delarr_mmap()` (remove or document clearly)
- [ ] Fix all-NA reduction bug (should return NA, not NaN)
- [ ] Vignette passes R CMD check (no missing suggests, examples work)
- [ ] DESCRIPTION meets CRAN requirements (proper licensing, maintainer, etc.)
- [ ] NEWS.md documents changes for release

### Out of Scope

Explicitly excluded from this milestone:

- Sparse array support — complexity deferred to future milestone
- Memory-mapped backend implementation — requires additional dependencies
- Two-pass streaming for row-wise operations — performance optimization, not correctness
- DAG optimization (fusing consecutive d_map calls) — performance optimization
- fmridataset integration — blocked until API is stable

## Context

**Codebase state:** Functional package with ~9 R source files, single test file with ~34 test cases, one vignette. Core lazy evaluation and streaming work correctly for happy paths.

**Known issues from codebase mapping:**
- Duplicate validation in `hdf5_writer()` (lines 23-24 and 29-30)
- Unused `compression` parameter in `hdf5_writer()`
- Stub `delarr_mmap()` that throws error
- All-NA reduction returns NaN instead of NA (high priority bug)
- Test coverage gaps: negative indices, broadcasting edge cases, large chunks, HDF5 exceptions

**CRAN requirements:**
- R CMD check must pass on multiple platforms
- All Suggests packages must be handled gracefully (hdf5r can require for full testing)
- Documentation must be complete and examples must run
- No undocumented exports

## Constraints

- **Testing dependency**: hdf5r required for full test suite — OK per user preference
- **R version**: Package requires R >= 4.1 (already specified)
- **No new dependencies**: Keep Imports minimal (only rlang currently)

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Require hdf5r for full testing | User preference, simplifies test logic | — Pending |
| Remove unused compression param | Cleaner than implementing unused feature | — Pending |
| Remove delarr_mmap stub | Cleaner than documenting unimplemented feature | — Pending |

---
*Last updated: 2026-01-22 after initialization*
