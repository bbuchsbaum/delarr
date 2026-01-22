# Roadmap: delarr CRAN Preparation

**Created:** 2026-01-22
**Depth:** Quick (4 phases)
**Coverage:** 30/30 requirements mapped

## Overview

Prepare the delarr package for initial CRAN submission by ensuring R CMD check compliance, complete documentation, comprehensive testing, multi-platform validation, and proper submission artifacts. This roadmap delivers a production-quality delayed matrix package ready for CRAN distribution.

## Phases

### Phase 1: Baseline & Documentation

**Goal:** Establish clean R CMD check baseline with complete, runnable documentation

**Dependencies:** None (starting phase)

**Plans:** 6 plans

Plans:
- [x] 01-01-PLAN.md — Fix R CMD check NOTEs (LICENSE stub, .Rbuildignore)
- [x] 01-02-PLAN.md — Add @examples to core functions (delarr, collect, d_map, d_map2, d_reduce, block_apply)
- [x] 01-03-PLAN.md — Add @examples to transformation verbs (d_center, d_scale, d_zscore, d_detrend, d_where) and generics
- [x] 01-04-PLAN.md — Add @examples to HDF5 and backend functions (delarr_hdf5, hdf5_writer, delarr_backend, delarr_mem, delarr_mmap)
- [x] 01-05-PLAN.md — Run spelling check, final R CMD check validation
- [x] 01-06-PLAN.md — Add @examples to delarr_seed() (gap closure)

**Requirements:**
- CHECK-01: R CMD check passes with 0 errors
- CHECK-02: R CMD check passes with 0 warnings
- CHECK-03: R CMD check passes with 0 notes (or explained)
- CHECK-04: All examples complete in <5 seconds each
- DOCS-01: Every exported function has @param for all parameters
- DOCS-02: Every exported function has @return documenting return value
- DOCS-03: Every exported function has runnable @examples
- DOCS-04: No \dontrun{} shortcuts (use \donttest{} if needed)
- DOCS-05: All examples with file I/O use tempdir()
- DOCS-06: Vignette builds without errors
- DOCS-07: Spelling passes (spelling::spell_check_package())
- DOCS-08: All URLs valid (urlchecker::url_check())

**Success Criteria:**
1. Developer can run `devtools::check()` and receive 0 errors, 0 warnings, 0 notes
2. Every exported function has complete roxygen2 documentation (@param, @return, @examples)
3. All documentation examples run successfully in <5 seconds each
4. All file I/O in examples uses tempdir(), no files written to working directory
5. Vignette builds cleanly without errors or warnings
6. Spelling check passes with no typos or unknown words
7. All URLs in documentation are valid and accessible via HTTPS

### Phase 2: Code Quality

**Goal:** Fix bugs, resolve tech debt, and establish comprehensive test coverage

**Dependencies:** Phase 1 (clean baseline required before adding tests)

**Plans:** 5 plans

Plans:
- [x] 02-01-PLAN.md — Fix all-NA reduction bug, add reduction edge case tests
- [x] 02-02-PLAN.md — Clean up hdf5_writer() (remove duplicate validation, implement compression)
- [x] 02-03-PLAN.md — Resolve delarr_mmap() stub (implement or remove)
- [x] 02-04-PLAN.md — Add edge case tests (negative indices, chunk boundaries, broadcasting)
- [x] 02-05-PLAN.md — Final validation (HDF5 test policy, full test suite, R CMD check)

**Requirements:**
- CODE-01: Fix all-NA reduction bug (mean of all-NA should return NA, not NaN)
- CODE-02: Remove duplicate validation in hdf5_writer() (lines 23-24 vs 29-30)
- CODE-03: Implement compression parameter in hdf5_writer()
- CODE-04: Implement delarr_mmap() memory-mapped backend
- TEST-01: Tests pass on R CMD check
- TEST-02: Add tests for all-NA reduction edge case
- TEST-03: Add tests for negative index edge cases
- TEST-04: Add tests for broadcasting edge cases (ambiguous dimensions, NaN/Inf)
- TEST-05: Add tests for chunk size boundary conditions
- TEST-06: HDF5 tests fail (not skip) when hdf5r unavailable with clear error message

**Success Criteria:**
1. All-NA reduction operations return NA (not NaN) for mean/sum/max/min
2. hdf5_writer() has no duplicate validation code
3. compression parameter in hdf5_writer() is either implemented or removed
4. delarr_mmap() is either implemented or removed from exports
5. Test suite covers edge cases: all-NA inputs, negative indices, broadcasting boundaries, chunk size limits
6. All tests pass locally and on R CMD check
7. HDF5 tests fail with clear error message when hdf5r unavailable (provides env var escape hatch)

### Phase 3: Platform Readiness

**Goal:** Make hdf5r and mmap required Imports and validate on Windows via win-builder

**Dependencies:** Phase 2 (code must be correct before platform testing)

**Plans:** 3 plans

Plans:
- [ ] 03-01-PLAN.md — Move hdf5r and mmap to Imports, remove defensive requireNamespace checks
- [ ] 03-02-PLAN.md — Clean up examples and tests (remove conditional wrappers)
- [ ] 03-03-PLAN.md — Win-builder validation (R-devel and R-release)

**Requirements (REVISED per user decision in 03-CONTEXT.md):**
- DEP-01: INVERTED — Remove unnecessary requireNamespace("hdf5r") checks (hdf5r is now required Import)
- DEP-02: INVERTED — Remove graceful degradation code (not needed with required Import)
- DEP-03: SIMPLIFIED — Remove if-requireNamespace wrappers from examples (keep \donttest{} only for genuinely slow examples)
- DEP-04: KEPT AS-IS — Vignette conditional chunks remain as harmless safety net (user preference)
- PLAT-01: Pass win-builder R-devel check
- PLAT-02: Pass win-builder R-release check
- PLAT-03: INVERTED — Verify package works WITH hdf5r and mmap (they're required)

**Success Criteria:**
1. hdf5r and mmap are in DESCRIPTION Imports field (not Suggests)
2. No defensive requireNamespace() checks for hdf5r or mmap in R/ code
3. HDF5 and mmap examples run directly without conditional wrappers
4. Tests no longer need DELARR_SKIP_HDF5 escape hatch
5. Vignette builds successfully (conditional chunks kept as safety net)
6. win-builder checks (R-devel and R-release) pass with 0 errors, 0 warnings
7. All three backends (delarr_hdf5, delarr_mem, delarr_mmap) work on Windows

### Phase 4: Submission

**Goal:** Finalize submission artifacts and submit to CRAN

**Dependencies:** Phase 3 (all quality gates passed)

**Plans:** (created by /gsd:plan-phase)

**Requirements:**
- SUB-01: Create NEWS.md with initial release notes
- SUB-02: Create cran-comments.md for submission
- SUB-03: Version bumped to 0.1.0
- SUB-04: Maintainer email is valid and monitored
- SUB-05: DESCRIPTION URL and BugReports fields populated

**Success Criteria:**
1. NEWS.md documents v0.1.0 features and scope
2. cran-comments.md summarizes test environments and R CMD check results
3. DESCRIPTION version field shows 0.1.0
4. DESCRIPTION Maintainer field has valid, monitored email address
5. DESCRIPTION has URL and BugReports fields with valid GitHub links
6. Package tarball builds successfully via `devtools::build()`
7. Final R CMD check passes cleanly before submission

## Progress

| Phase | Status | Progress | Requirements |
|-------|--------|----------|--------------|
| 1 - Baseline & Documentation | Complete | 100% | 12 requirements |
| 2 - Code Quality | Complete | 100% | 10 requirements |
| 3 - Platform Readiness | Planned | 0% | 7 requirements (revised) |
| 4 - Submission | Pending | 0% | 5 requirements |

**Overall:** 22/30 requirements complete (73%)

## Key Decisions

| Decision | Rationale | Phase |
|----------|-----------|-------|
| Compression parameter implemented | gzip_level 0-9 with default 4L; NULL disables compression | Phase 2 |
| delarr_mmap() implemented | Full mmap package backend for memory-mapped binary files | Phase 2 |
| hdf5r and mmap are required Imports | User decision: package requires both dependencies; no graceful degradation needed | Phase 3 |
| Remove defensive code for required deps | If in Imports, guaranteed present; requireNamespace() checks are redundant | Phase 3 |
| Keep vignette conditionals as safety net | User preference for defensive documentation, even if redundant | Phase 3 |

---
*Roadmap created: 2026-01-22*
*Last updated: 2026-01-22*
