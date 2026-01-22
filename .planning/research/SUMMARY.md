# Project Research Summary

**Project:** delarr - CRAN submission preparation
**Domain:** R package development and CRAN submission
**Researched:** 2026-01-22
**Confidence:** HIGH

## Executive Summary

CRAN submission in 2026 is a well-documented process with mature tooling centered on the r-lib ecosystem (devtools, usethis, roxygen2, testthat). The delarr package is 80% aligned with CRAN requirements but faces specific challenges: optional system dependencies (hdf5r), file I/O operations, and stub functions. Success requires zero ERRORs/WARNINGs from R CMD check, complete documentation with runnable examples, multi-platform testing, and graceful handling of optional dependencies.

The critical path prioritizes documentation completeness and R CMD check compliance (highest rejection reasons), followed by multi-platform testing to catch hdf5r-related issues. Key risks include file I/O in examples writing outside tempdir(), slow-running HDF5 examples, and platform-specific failures when HDF5 libraries are unavailable. These risks are mitigated by wrapping all hdf5r usage in conditional checks, using tempdir() exclusively for examples, and testing on win-builder early.

The recommended phase structure follows CRAN's submission requirements: documentation audit and R CMD check fixes (Phase 1), optional dependency handling for hdf5r (Phase 2), multi-platform testing (Phase 3), and final submission preparation (Phase 4). This order minimizes rework by catching fundamental issues before platform-specific testing.

## Key Findings

### Recommended Stack

CRAN submission relies on a standardized toolchain with specific version requirements. The r-lib ecosystem provides comprehensive automation: devtools (2.4.6) orchestrates development workflows, roxygen2 (7.3.3) generates documentation, testthat (3.3.2) handles testing, and rcmdcheck (1.4.0) validates packages. Code quality tools (lintr, spelling, urlchecker) catch common mistakes before submission. GitHub Actions with r-lib/actions provides free multi-platform CI/CD, while rhub v2 (2.0.1) enables pre-submission testing on CRAN's platform matrix.

**Core technologies:**
- **devtools 2.4.6**: Primary development interface — wraps R CMD check, submission, and release workflows
- **roxygen2 7.3.3**: Documentation generation — keeps code and docs synchronized, already in use by delarr
- **testthat 3.3.2**: Unit testing framework — required for quality signal, already in use by delarr
- **rcmdcheck 1.4.0**: R CMD check wrapper — structured output for automation and CI/CD
- **rhub v2**: Multi-platform testing — replicates CRAN's check environment before submission
- **lintr/spelling/urlchecker**: Code quality gates — catch typos, style violations, broken links

**Installation priority:**
1. Core tools (devtools, usethis) — immediate need for check workflows
2. Code quality (lintr, spelling, urlchecker) — catch issues before R CMD check
3. Multi-platform testing (rhub v2, GitHub Actions) — validate before submission

### Expected Features

CRAN acceptance requires mandatory features (table stakes) and values quality signals (differentiators). Missing any table stakes causes automatic rejection. Differentiators improve acceptance likelihood and maintainer goodwill.

**Must have (table stakes):**
- Pass R CMD check --as-cran with 0 errors, 0 warnings, 0 notes — automatic rejection if fails
- Valid DESCRIPTION file with proper Title/Description formatting — first impression, common rejection reason
- Complete function documentation (@param, @return, @examples) — every exported function requires all three
- Runnable examples (<5 seconds each) — tested daily on CRAN infrastructure
- S3 method registration in NAMESPACE — delarr has this correct already
- Cross-platform compatibility (Linux, Windows, macOS) — test with win-builder
- Secure downloads (HTTPS only) — security policy
- Valid URLs in documentation — checked automatically, use urlchecker
- Proper external dependency handling (hdf5r) — must work when unavailable

**Should have (differentiators):**
- NEWS.md with changelog — documents version history
- cran-comments.md — communicates with CRAN maintainers, explains unavoidable NOTEs
- Comprehensive test suite — demonstrates code quality
- Package vignette — shows real-world usage, delarr already has one
- win-builder and rhub testing — catches platform issues before submission
- Graceful degradation — works when optional dependencies missing

**Defer (v2+):**
- pkgdown website — can be added after CRAN acceptance
- Multiple vignettes — one is sufficient for initial submission
- ORCID IDs in Authors@R — professional metadata but not required
- Reverse dependency checks — only needed if packages already depend on delarr

**Anti-features to avoid:**
- \\dontrun{} for convenience — use \\donttest{} instead
- File I/O outside tempdir() — security violation
- Stub functions that always error — poor API design (delarr has delarr_mmap)
- Unused function parameters — confuses users (delarr has compression in hdf5_writer)

### Architecture Approach

CRAN packages follow strict structural conventions: mandatory files (DESCRIPTION, NAMESPACE), standard directories (R/, man/, tests/, vignettes/), and specific naming patterns. All exported objects require documentation in man/, all S3 methods need registration in NAMESPACE, and all tests must run quickly without external resources. The architecture emphasizes separation of concerns: code in R/, auto-generated docs in man/, tests in tests/testthat/, and long-form docs in vignettes/.

**Major components:**
1. **DESCRIPTION file** — package metadata with Title/Description formatting rules, dependency declarations (Imports vs Suggests), and author/maintainer info
2. **NAMESPACE management** — roxygen2-generated export/import control, S3 method registration, delarr's current NAMESPACE is correct
3. **Documentation (man/)** — every exported function needs @param, @return, @examples that run successfully
4. **Testing structure (tests/)** — testthat 3.x with tests/testthat.R entry point, conditional skipping for optional dependencies
5. **Vignettes** — R Markdown with knitr, must build successfully, delarr already has one

**delarr current status:**
- Good: DESCRIPTION complete, NAMESPACE properly structured, 29 .Rd files, testthat setup, vignette present
- Review needed: Verify all examples use tempdir(), expand test coverage, check hdf5r conditional usage

### Critical Pitfalls

Based on official CRAN policy and common rejection patterns, five pitfalls pose the highest risk to delarr specifically:

1. **File I/O outside tempdir()** — delarr has HDF5 file writers that must use tempdir() in all examples and tests. Automatic rejection if examples write to working directory. Fix: audit all hdf5_writer() and delarr_hdf5() examples to use `tempfile(fileext = ".h5")`.

2. **Examples taking >5 seconds** — HDF5 operations and block processing could be slow with realistic data sizes. Use tiny arrays (10x10) in examples, not production sizes (1000x1000). Use \\donttest{} for slow examples. Check runtime: `system.time(example("function_name"))`.

3. **Platform-specific failures** — hdf5r requires system HDF5 libraries unavailable on some CRAN platforms. Fix: hdf5r must be in Suggests (correct), all usage wrapped in `if (requireNamespace("hdf5r", quietly = TRUE))`, tests use `skip_if_not_installed("hdf5r")`. Test early on win-builder.

4. **Optional dependencies not used conditionally** — every hdf5r call must check availability first. CRAN checks run in minimal environments. Audit delarr_hdf5(), hdf5_writer(), all tests, all examples for unconditional hdf5r usage.

5. **Stub functions that always error** — delarr_mmap() is exported but only throws an error. Decision needed: remove from exports (best), make internal, or document clearly as not implemented. CRAN allows this but it confuses users.

**Additional risks:**
- Unused parameter (compression in hdf5_writer) — remove or document as reserved
- LazyData without data directory — already correct (LazyData: false)
- DESCRIPTION formatting — current Title/Description look clean but should mention 'hdf5r' explicitly

## Implications for Roadmap

Based on research, the submission process follows a strict dependency chain: R CMD check compliance enables multi-platform testing, which enables submission. Documentation completeness is the foundation (highest rejection reason). Optional dependency handling must be correct before platform testing or issues won't be caught.

### Phase 1: R CMD Check Compliance
**Rationale:** R CMD check with 0 errors/0 warnings is the gate to submission. Documentation completeness is the #1 rejection reason. Start here to catch fundamental issues early.

**Delivers:** Clean R CMD check output, complete documentation, fixed fundamental issues.

**Addresses:**
- Complete documentation audit (all @param, @return, @examples)
- Fix any ERRORs or WARNINGs
- Verify examples use tempdir() for file operations
- Check example runtimes (<5 seconds each)
- Run spelling and URL checks

**Avoids:**
- Pitfall 1 (file I/O) — audit all HDF5 examples
- Pitfall 2 (slow examples) — test all examples with system.time()
- Pitfall 11 (missing @return) — audit all exported functions

**Research flag:** SKIP. Standard R CMD check process is well-documented.

### Phase 2: Optional Dependency Handling
**Rationale:** hdf5r is a system dependency that won't be available on all CRAN platforms. Must be handled correctly before multi-platform testing or issues multiply.

**Delivers:** Graceful degradation when hdf5r unavailable, conditional tests, safe examples.

**Addresses:**
- Wrap all hdf5r usage in requireNamespace() checks
- Add skip_if_not_installed("hdf5r") to tests
- Make HDF5 examples conditional or use \\donttest{}
- Add informative error messages when HDF5 features requested but unavailable

**Avoids:**
- Pitfall 4 (platform failures) — prevent cascading failures on Windows/macOS
- Pitfall 8 (optional dependencies) — CRAN checks in minimal environments

**Research flag:** SKIP. Pattern is well-established for optional dependencies.

### Phase 3: API Cleanup
**Rationale:** Stub functions and unused parameters create confusion. Fix before submission to avoid user complaints post-acceptance.

**Delivers:** Clean, consistent API with no stub functions.

**Addresses:**
- Remove or implement delarr_mmap()
- Fix unused compression parameter in hdf5_writer()
- Verify DESCRIPTION mentions 'hdf5r' backend explicitly

**Avoids:**
- Pitfall 12 (stub functions) — poor user experience
- Pitfall 13 (unused parameters) — confusing API

**Research flag:** SKIP. API design decisions, not technical research.

### Phase 4: Multi-Platform Testing
**Rationale:** After documentation and dependencies are correct, validate on CRAN's platform matrix. Catching platform issues late requires rework of previous phases.

**Delivers:** Confidence package works on Windows, macOS, Linux with multiple R versions.

**Addresses:**
- Test on win-builder (R-devel and R-release)
- Setup GitHub Actions with r-lib/actions for continuous testing
- Consider rhub v2 for additional platform combinations
- Address any platform-specific issues

**Avoids:**
- Pitfall 4 (platform failures) — HDF5 libraries may be unavailable
- Pitfall 1 (path issues) — Windows path separator differences

**Research flag:** SKIP. Standard testing process, r-lib/actions provides templates.

### Phase 5: Submission Preparation
**Rationale:** Final checklist items before upload. These depend on all previous phases being complete.

**Delivers:** Submittable package tarball with complete documentation.

**Addresses:**
- Update DESCRIPTION version to 0.1.0
- Create NEWS.md with initial release notes
- Create cran-comments.md with test results
- Add URL/BugReports fields to DESCRIPTION
- Final spell check and URL check
- Review R CMD check output one last time

**Avoids:**
- Pitfall 7 (DESCRIPTION formatting) — verify Title/Description follow conventions
- Pitfall 6 (unexplained NOTEs) — document any unavoidable NOTEs in cran-comments.md

**Research flag:** SKIP. Checklist-driven, no deep research needed.

### Phase Ordering Rationale

- **Documentation first:** Highest rejection reason, enables all other work
- **Dependencies before testing:** Conditional handling must be correct before platform tests or failures cascade
- **API cleanup before submission:** Last chance to fix user-facing issues without version bump
- **Multi-platform testing last:** Validates all previous work, catching platform-specific issues
- **Submission preparation final:** Depends on everything else being complete

This ordering minimizes rework by establishing solid foundations before platform-specific validation.

### Research Flags

**Phases with standard patterns (skip research-phase):**
- **All phases:** CRAN submission is a well-documented process with mature tooling. Official documentation (Writing R Extensions, R Packages book, CRAN policies) covers all scenarios. No deep research needed during execution.

**Potential issues needing validation:**
- **hdf5r behavior on Windows:** Test early on win-builder to catch system library issues
- **HDF5 file locking:** Some platforms have different file locking behavior, test thoroughly
- **Example timing:** Measure all examples to ensure <5 second limit

## Confidence Assessment

| Area | Confidence | Notes |
|------|------------|-------|
| Stack | HIGH | Based on official CRAN docs, r-lib ecosystem is standard, versions verified from CRAN |
| Features | HIGH | Based on official CRAN Repository Policy and submission checklist |
| Architecture | HIGH | Based on Writing R Extensions manual and R Packages book |
| Pitfalls | HIGH | Based on CRAN policies, CRAN Cookbook, and common rejection patterns |

**Overall confidence:** HIGH

All research based on authoritative sources: official CRAN documentation (Repository Policy, submission checklist, Writing R Extensions), R Packages (2e) book by Wickham/Bryan, and CRAN Cookbook for common issues. The r-lib toolchain is industry standard with stable APIs.

### Gaps to Address

**hdf5r system dependency behavior:**
- How it fails on platforms without HDF5 libraries needs empirical testing
- Mitigation: Test on win-builder early in Phase 4
- Consider documenting hdf5lib package as alternative (bundles HDF5 libraries, new Nov 2025)

**Vignette build time:**
- Single vignette should be fine, but verify build time is reasonable
- Mitigation: Run devtools::build_vignettes() and measure time

**Test coverage completeness:**
- Research indicates tests are expected but didn't analyze current coverage
- Mitigation: Run covr::package_coverage() to identify gaps (target >70%)

**Empty src/ directory:**
- Package has empty src/ directory which could trigger NOTE
- Mitigation: Remove empty directory in Phase 3

## Sources

### Primary (HIGH confidence)
- [CRAN Repository Policy](https://cran.r-project.org/web/packages/policies.html) — authoritative submission requirements
- [Writing R Extensions](https://cran.r-project.org/doc/manuals/r-release/R-exts.html) — complete technical manual for R packages
- [CRAN Submission Checklist](https://cran.r-project.org/web/packages/submission_checklist.html) — pre-submission verification
- [R Packages (2e)](https://r-pkgs.org/release.html) — modern package development guide by Wickham/Bryan
- [CRAN Cookbook](https://contributor.r-project.org/cran-cookbook/) — common DESCRIPTION and code issues

### Secondary (MEDIUM-HIGH confidence)
- [devtools on CRAN](https://rdrr.io/cran/devtools/) — version 2.4.6 documentation
- [testthat 3.3.0 release](https://tidyverse.org/blog/2025/11/testthat-3-3-0/) — testing framework updates
- [rhub v2 announcement](https://blog.r-hub.io/2024/04/11/rhub2/) — multi-platform testing migration
- [r-lib/actions repository](https://github.com/r-lib/actions) — GitHub Actions workflows for R packages
- [R-hub blog on workflow automation](https://blog.r-hub.io/2020/04/29/maintenance/) — best practices

### Package-Specific Resources
- [hdf5r on CRAN](https://cran.r-project.org/web/packages/hdf5r/index.html) — optional dependency documentation
- [hdf5lib on CRAN](https://cran.r-project.org/web/packages/hdf5lib/index.html) — bundled HDF5 libraries alternative (Nov 2025)

---
*Research completed: 2026-01-22*
*Ready for roadmap: yes*
