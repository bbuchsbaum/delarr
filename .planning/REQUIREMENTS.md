# Requirements: delarr CRAN Preparation

**Defined:** 2026-01-22
**Core Value:** Production-quality delayed matrix package ready for CRAN distribution

## v1 Requirements

Requirements for CRAN submission. Each maps to roadmap phases.

### R CMD Check Compliance

- [ ] **CHECK-01**: R CMD check --as-cran passes with 0 errors
- [ ] **CHECK-02**: R CMD check --as-cran passes with 0 warnings
- [ ] **CHECK-03**: R CMD check --as-cran passes with 0 notes (or explained)
- [ ] **CHECK-04**: All examples complete in <5 seconds each

### Documentation

- [ ] **DOCS-01**: Every exported function has @param for all parameters
- [ ] **DOCS-02**: Every exported function has @return documenting return value
- [ ] **DOCS-03**: Every exported function has runnable @examples
- [ ] **DOCS-04**: No \\dontrun{} shortcuts (use \\donttest{} if needed)
- [ ] **DOCS-05**: All examples with file I/O use tempdir()
- [ ] **DOCS-06**: Vignette builds without errors
- [ ] **DOCS-07**: Spelling passes (spelling::spell_check_package())
- [ ] **DOCS-08**: All URLs valid (urlchecker::url_check())

### Code Correctness

- [ ] **CODE-01**: Fix all-NA reduction bug (mean of all-NA should return NA, not NaN)
- [ ] **CODE-02**: Remove duplicate validation in hdf5_writer() (lines 23-24 vs 29-30)
- [ ] **CODE-03**: Implement compression parameter in hdf5_writer()
- [ ] **CODE-04**: Implement delarr_mmap() memory-mapped backend

### Test Coverage

- [ ] **TEST-01**: Tests pass on R CMD check
- [ ] **TEST-02**: Add tests for all-NA reduction edge case
- [ ] **TEST-03**: Add tests for negative index edge cases
- [ ] **TEST-04**: Add tests for broadcasting edge cases (ambiguous dimensions, NaN/Inf)
- [ ] **TEST-05**: Add tests for chunk size boundary conditions
- [ ] **TEST-06**: HDF5 tests skip gracefully when hdf5r unavailable

### Optional Dependency Handling

- [ ] **DEP-01**: All hdf5r usage wrapped in requireNamespace() checks
- [ ] **DEP-02**: Graceful error messages when HDF5 unavailable
- [ ] **DEP-03**: Examples using HDF5 in \\donttest{} or conditional blocks
- [ ] **DEP-04**: Vignette handles missing hdf5r gracefully

### Platform Testing

- [ ] **PLAT-01**: Pass win-builder R-devel check
- [ ] **PLAT-02**: Pass win-builder R-release check
- [ ] **PLAT-03**: Package works when hdf5r unavailable (core functionality)

### Submission Preparation

- [ ] **SUB-01**: Create NEWS.md with initial release notes
- [ ] **SUB-02**: Create cran-comments.md for submission
- [ ] **SUB-03**: Version bumped to 0.1.0
- [ ] **SUB-04**: Maintainer email is valid and monitored
- [ ] **SUB-05**: DESCRIPTION URL and BugReports fields populated

## v2 Requirements

Deferred to post-CRAN acceptance:

### Performance

- **PERF-01**: Two-pass streaming for row-wise center/scale/zscore
- **PERF-02**: DAG optimization (fuse consecutive d_map calls)
- **PERF-03**: Custom streaming reduce hints

### Features

- **FEAT-01**: Sparse array support
- **FEAT-02**: fmridataset integration adapters

### Documentation

- **DOCS-V2-01**: pkgdown website
- **DOCS-V2-02**: Additional vignettes (performance guide, backend development)

## Out of Scope

Explicitly excluded from this milestone:

| Feature | Reason |
|---------|--------|
| Sparse array support | Complexity deferred to v2 |
| fmridataset integration | Blocked until API stable |
| Two-pass streaming | Performance optimization, not correctness |
| DAG optimization | Performance optimization, not correctness |
| Multiple vignettes | One vignette sufficient for initial release |

## Traceability

| Requirement | Phase | Status |
|-------------|-------|--------|
| CHECK-01 | Phase 1 | Pending |
| CHECK-02 | Phase 1 | Pending |
| CHECK-03 | Phase 1 | Pending |
| CHECK-04 | Phase 1 | Pending |
| DOCS-01 | Phase 1 | Pending |
| DOCS-02 | Phase 1 | Pending |
| DOCS-03 | Phase 1 | Pending |
| DOCS-04 | Phase 1 | Pending |
| DOCS-05 | Phase 1 | Pending |
| DOCS-06 | Phase 1 | Pending |
| DOCS-07 | Phase 1 | Pending |
| DOCS-08 | Phase 1 | Pending |
| CODE-01 | Phase 2 | Pending |
| CODE-02 | Phase 2 | Pending |
| CODE-03 | Phase 2 | Pending |
| CODE-04 | Phase 2 | Pending |
| TEST-01 | Phase 3 | Pending |
| TEST-02 | Phase 3 | Pending |
| TEST-03 | Phase 3 | Pending |
| TEST-04 | Phase 3 | Pending |
| TEST-05 | Phase 3 | Pending |
| TEST-06 | Phase 3 | Pending |
| DEP-01 | Phase 3 | Pending |
| DEP-02 | Phase 3 | Pending |
| DEP-03 | Phase 3 | Pending |
| DEP-04 | Phase 3 | Pending |
| PLAT-01 | Phase 4 | Pending |
| PLAT-02 | Phase 4 | Pending |
| PLAT-03 | Phase 4 | Pending |
| SUB-01 | Phase 5 | Pending |
| SUB-02 | Phase 5 | Pending |
| SUB-03 | Phase 5 | Pending |
| SUB-04 | Phase 5 | Pending |
| SUB-05 | Phase 5 | Pending |

**Coverage:**
- v1 requirements: 30 total
- Mapped to phases: 30
- Unmapped: 0 ✓

---
*Requirements defined: 2026-01-22*
*Last updated: 2026-01-22 after initial definition*
