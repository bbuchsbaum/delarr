# Phase 1: Baseline & Documentation - Research

**Researched:** 2026-01-22
**Domain:** R package documentation, CRAN compliance, R CMD check
**Confidence:** HIGH

## Summary

Phase 1 aims to establish a clean R CMD check baseline with complete, runnable documentation for all exported functions. The delarr package currently has 16 exported functions and 10 S3 methods across 29 Rd files. The current R CMD check status shows **0 errors, 0 warnings, and 2 NOTEs**, which is a strong starting position.

The main work involves:
1. Adding `@examples` to all 16 exported functions (currently 0 have examples)
2. Fixing 2 R CMD check NOTEs (`.planning` directory and invalid LICENSE stub)
3. Ensuring all examples use `tempdir()` for file I/O (critical for HDF5 examples)
4. Installing and running spelling and URL checkers (URL check already passes)

**Primary recommendation:** Add runnable `@examples` to all exported functions, prioritizing HDF5 functions which require special care with `tempdir()` usage and conditional execution.

## Standard Stack

The established tools for R package documentation and CRAN compliance:

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| roxygen2 | 7.3.2+ | Function documentation from inline comments | Industry standard for R package documentation, required for @examples |
| devtools | Latest | Package development workflow | Provides `check()`, `document()`, and other essential development tools |
| usethis | Latest | Project setup helpers | Automates .Rbuildignore, LICENSE, and other boilerplate |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| spelling | Latest | Spell check package documentation | DOCS-07 requirement, catches typos in docs |
| urlchecker | Latest | Validate URLs in documentation | DOCS-08 requirement, already installed and passing |
| testthat | 3.1.0+ | Testing framework | Already in Suggests, used for example validation |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| roxygen2 | Manual .Rd files | Manual Rd is error-prone and harder to maintain; roxygen2 is CRAN standard |
| devtools::check() | R CMD check directly | devtools handles .Rbuildignore properly, recommended workflow |

**Installation:**
```bash
# In R console
install.packages(c("spelling", "urlchecker"))
```

## Architecture Patterns

### Recommended Documentation Structure
```
R/
├── delarr-core.R         # delarr(), dim(), print(), as.matrix(), Ops
├── delarr-backends.R     # delarr_backend(), delarr_mem(), delarr_hdf5(), delarr_mmap()
├── delarr-verbs.R        # d_map(), d_map2(), d_reduce(), d_center(), etc.
├── delarr-eval.R         # collect(), block_apply()
├── delarr-writer-hdf5.R  # hdf5_writer()
├── delarr-seed.R         # delarr_seed() (infrastructure)
└── generics.R            # rowMeans2(), colMeans2() (generics)
```

### Pattern 1: Simple Function Examples
**What:** Quick, self-contained examples demonstrating core usage
**When to use:** Most exported functions (delarr(), d_map(), d_reduce(), etc.)
**Example:**
```r
#' @examples
#' # Create a delayed matrix from a regular matrix
#' mat <- matrix(1:12, 3, 4)
#' darr <- delarr(mat)
#' darr
#'
#' # Apply operations lazily
#' result <- darr |> d_map(~ .x * 2) |> collect()
#' result
```

### Pattern 2: Conditional HDF5 Examples
**What:** Examples that only run when hdf5r is available
**When to use:** delarr_hdf5(), hdf5_writer()
**Example:**
```r
#' @examples
#' if (requireNamespace("hdf5r", quietly = TRUE)) {
#'   # Use tempdir() for all file I/O
#'   tf <- tempfile(fileext = ".h5")
#'
#'   # Create test HDF5 file
#'   input <- matrix(1:20, 4, 5)
#'   f <- hdf5r::H5File$new(tf, mode = "w")
#'   f$create_dataset("data", robj = input)
#'   f$close_all()
#'
#'   # Load as delayed array
#'   darr <- delarr_hdf5(tf, "data")
#'   result <- collect(darr)
#'
#'   # Clean up
#'   unlink(tf)
#' }
```
**Source:** [R Packages (2e) - Function documentation](https://r-pkgs.org/man.html)

### Pattern 3: S3 Methods (No Examples Required)
**What:** S3 methods inherit examples from their generics
**When to use:** `[.delarr`, `dim.delarr`, `print.delarr`, etc.
**Note:** S3 methods don't require separate examples; the generic's examples suffice

### Anti-Patterns to Avoid
- **File I/O outside tempdir():** Automatic CRAN rejection. Always use `tempfile()` or `file.path(tempdir(), "filename")`
- **Examples >5 seconds:** Will trigger CRAN warnings. Use tiny test arrays (3x4, 5x6), not production sizes
- **`\dontrun{}` shortcuts:** CRAN requires runnable examples. Use conditional blocks instead
- **Persistent state changes:** Don't change working directory or options without restoration

**Source:** [R-hub blog - Non-standard files/directories](https://blog.r-hub.io/2020/05/20/rbuildignore/)

## Don't Hand-Roll

Problems that look simple but have existing solutions:

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Documentation from comments | Manual .Rd files | roxygen2 | Standard toolchain, reduces errors, easier maintenance |
| Package checking | Manual R CMD check | devtools::check() | Handles .Rbuildignore, provides cleaner output |
| .Rbuildignore management | Manual regex | usethis::use_build_ignore() | Proper escaping, validation |
| Example validation | Manual testing | R CMD check --as-cran | Runs examples in isolated environment |

**Key insight:** R package infrastructure has well-established tooling. Deviating from roxygen2/devtools/usethis workflow creates unnecessary friction with CRAN.

## Common Pitfalls

### Pitfall 1: Hidden Files NOTE
**What goes wrong:** `.planning` directory triggers R CMD check NOTE about hidden files
**Why it happens:** R CMD check warns about any files/directories starting with `.` as they're "most likely included in error"
**How to avoid:** Add `.planning` to .Rbuildignore file
**Warning signs:** NOTE in R CMD check output mentioning "hidden files and directories"
**Source:** [R Packages (2e) - Appendix A](https://r-pkgs.org/R-CMD-check.html)

### Pitfall 2: Invalid LICENSE Stub
**What goes wrong:** "License stub is invalid DCF" NOTE because LICENSE file contains full MIT text
**Why it happens:** CRAN expects MIT LICENSE files to only contain `YEAR: ` and `COPYRIGHT HOLDER: ` fields, not full license text
**How to avoid:** Replace LICENSE file contents with proper DCF stub format
**Warning signs:** NOTE in R CMD check mentioning "License stub is invalid DCF"
**Current state:** LICENSE file has full MIT text instead of stub

### Pitfall 3: HDF5 Examples Without tempdir()
**What goes wrong:** Automatic CRAN rejection if examples write to working directory
**Why it happens:** HDF5 operations naturally create files; easy to forget tempdir()
**How to avoid:** Audit all HDF5 examples; wrap paths with `tempfile(fileext = ".h5")`
**Warning signs:** Files left in package directory after running examples
**Critical functions:** delarr_hdf5(), hdf5_writer() examples must use tempdir()
**Source:** [GitHub Issue - R.utils #107](https://github.com/HenrikBengtsson/R.utils/issues/107)

### Pitfall 4: Missing @examples for Exported Functions
**What goes wrong:** CRAN rejects packages where exported functions lack runnable examples
**Why it happens:** Initial development focuses on code, documentation comes later
**How to avoid:** Add `@examples` to all exported functions before submission
**Warning signs:** "checking for missing documentation entries" NOTE or manual review
**Current state:** 0 out of 16 exported functions have @examples
**Source:** [R Packages (2e) - Releasing to CRAN](https://r-pkgs.org/release.html)

### Pitfall 5: Long-Running Examples
**What goes wrong:** CRAN check times out or produces warnings for slow examples
**Why it happens:** Using production-sized data in examples (e.g., 1000x1000 matrices)
**How to avoid:** Use tiny test arrays (3x4, 5x6 max); examples should complete in <1 second
**Warning signs:** "Examples with CPU time > 2.5 times elapsed time" warning
**Requirement:** All examples must complete in <5 seconds (CHECK-04)

## Code Examples

Verified patterns from documentation best practices:

### Example Template for Core Functions
```r
#' @examples
#' # Create delayed matrix from regular matrix
#' mat <- matrix(rnorm(12), 3, 4)
#' arr <- delarr(mat)
#'
#' # Apply transformations lazily
#' result <- arr |>
#'   d_center(dim = "rows") |>
#'   d_scale(dim = "rows") |>
#'   collect()
#' head(result)
```
**Source:** Vignette delarr-getting-started.Rmd

### Example Template for HDF5 Functions
```r
#' @examples
#' if (requireNamespace("hdf5r", quietly = TRUE)) {
#'   # All file I/O uses tempdir()
#'   tf <- tempfile(fileext = ".h5")
#'
#'   # Create sample HDF5 file
#'   data <- matrix(1:20, 4, 5)
#'   f <- hdf5r::H5File$new(tf, mode = "w")
#'   f$create_dataset("X", robj = data)
#'   f$close_all()
#'
#'   # Use delayed HDF5 backend
#'   arr <- delarr_hdf5(tf, "X")
#'   result <- arr |> d_map(~ .x * 2) |> collect()
#'
#'   # Clean up temp file
#'   unlink(tf)
#' }
```
**Source:** Adapted from vignette HDF5 section

### Example Template for Writer Functions
```r
#' @examples
#' if (requireNamespace("hdf5r", quietly = TRUE)) {
#'   # Input file
#'   tf_in <- tempfile(fileext = ".h5")
#'   input <- matrix(1:20, 4, 5)
#'   f <- hdf5r::H5File$new(tf_in, mode = "w")
#'   f$create_dataset("X", robj = input)
#'   f$close_all()
#'
#'   # Process and stream to output file
#'   arr <- delarr_hdf5(tf_in, "X")
#'   transformed <- arr |> d_center("cols")
#'
#'   tf_out <- tempfile(fileext = ".h5")
#'   writer <- hdf5_writer(tf_out, "result", ncol = ncol(transformed))
#'   collect(transformed, into = writer)
#'
#'   # Verify output
#'   g <- hdf5r::H5File$new(tf_out, mode = "r")
#'   result <- g[["result"]]$read()
#'   g$close_all()
#'
#'   # Clean up
#'   unlink(c(tf_in, tf_out))
#' }
```
**Source:** Adapted from vignette streaming section

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Manual .Rd files | roxygen2 inline docs | ~2013 | Industry standard; reduces doc/code drift |
| `\dontrun{}` for optional deps | Conditional `if (requireNamespace())` | ~2018 | More examples actually run during check |
| Relaxed file I/O | Strict tempdir() enforcement | Ongoing | CRAN auto-rejects packages writing to pwd |
| Lenient timing | <5 second example requirement | ~2020 | Faster checks, better user experience |

**Deprecated/outdated:**
- `\dontrun{}` for examples: CRAN now expects runnable examples; use conditional blocks instead
- Examples without cleanup: Must explicitly `unlink()` temp files in HDF5 examples

## Current State Audit

### R CMD Check Status
**Current:** 0 errors, 0 warnings, 2 NOTEs
- NOTE 1: Hidden files and directories (`.planning`)
- NOTE 2: License stub is invalid DCF

### Documentation Completeness
**Exported Functions:** 16 total
- ✅ All have `@param` documentation for parameters
- ✅ All have `@return` documentation
- ❌ **0 have `@examples`** (16 need examples)

**S3 Methods:** 10 total (don't require separate examples)
- dim.delarr, [.delarr, print.delarr, as.matrix.delarr, Ops.delarr
- rowMeans2.delarr, colMeans2.delarr
- dim.delarr_seed
- (Plus S3 generics rowMeans2, colMeans2)

### Example Status
**Files with examples:** 0/16 exported functions
**File I/O concerns:**
- delarr_hdf5(): Will need tempfile() usage
- hdf5_writer(): Will need tempfile() usage
- Vignette already demonstrates correct tempdir() pattern

### Vignette Status
**Status:** Builds successfully without errors ✅
**HDF5 handling:** Already uses conditional blocks and tempdir() correctly ✅

### Spelling/URL Status
**Spelling:** Not yet run (spelling package needs installation)
**URL check:** ✅ Passes (`urlchecker::url_check()` shows "All URLs are correct!")

## Work Items Identified

### Critical (Blocks CHECK requirements)
1. **Add @examples to all 16 exported functions** (DOCS-03)
   - Priority 1: Core functions (delarr, collect, d_map, d_reduce)
   - Priority 2: HDF5 functions (delarr_hdf5, hdf5_writer) - requires tempdir()
   - Priority 3: Helper verbs (d_center, d_scale, d_zscore, d_detrend, d_where)
   - Priority 4: Backend functions (delarr_backend, delarr_mem, delarr_mmap)
   - Priority 5: Advanced functions (block_apply, d_map2)

2. **Fix LICENSE stub DCF format** (CHECK-03)
   - Replace full MIT text with stub containing only YEAR and COPYRIGHT HOLDER

3. **Add .planning to .Rbuildignore** (CHECK-03)
   - Use `usethis::use_build_ignore(".planning")` or manual regex

### Important (Completes requirements)
4. **Install and run spelling check** (DOCS-07)
   - `install.packages("spelling")`
   - `spelling::spell_check_package()`
   - Add any legitimate words to WORDLIST

5. **Verify example runtimes** (CHECK-04)
   - Run `devtools::check()` with timing
   - Ensure each example completes in <5 seconds
   - Use small test arrays (3x4, 5x6) to keep examples fast

### Validation
6. **Final R CMD check verification** (CHECK-01, CHECK-02, CHECK-03)
   - Run `devtools::check()` to confirm 0 errors, 0 warnings, 0 notes
   - Verify all examples run successfully
   - Confirm no files written outside tempdir()

## Open Questions

1. **delarr_mmap() handling**
   - What we know: Currently exported but not implemented (throws error)
   - What's unclear: Should it have stub example showing error, or be unexported?
   - Recommendation: Add minimal example showing it's not yet implemented; defer implementation decision to Phase 2 (CODE-04)

2. **S3 method examples**
   - What we know: S3 methods don't strictly require separate examples
   - What's unclear: Should internal methods like `[.delarr` have explicit examples?
   - Recommendation: No separate examples needed; generics cover usage

3. **Compression parameter in hdf5_writer()**
   - What we know: Parameter exists but is unused (comment says "currently unused")
   - What's unclear: Document as planned feature or remove?
   - Recommendation: Document current state in example; defer implementation to Phase 2 (CODE-03)

## Sources

### Primary (HIGH confidence)
- R Packages (2e) - Function documentation: https://r-pkgs.org/man.html
- R Packages (2e) - Releasing to CRAN: https://r-pkgs.org/release.html
- R Packages (2e) - R CMD check: https://r-pkgs.org/R-CMD-check.html
- CRAN Repository Policy: https://cran.r-project.org/web/packages/policies.html
- Package source code (verified via Read tool)

### Secondary (MEDIUM confidence)
- R-hub blog - Non-standard files: https://blog.r-hub.io/2020/05/20/rbuildignore/
- GitHub Issue - R.utils #107 tempdir() requirement: https://github.com/HenrikBengtsson/R.utils/issues/107
- roxygen2 documentation: https://roxygen2.r-lib.org/articles/rd.html
- Karl Broman - Getting on CRAN: https://kbroman.org/pkg_primer/pages/cran.html

### Tertiary (LOW confidence)
- None - All findings verified with authoritative sources

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH - roxygen2/devtools are documented R package standards
- Architecture: HIGH - Verified current codebase structure via file reads
- Pitfalls: HIGH - Confirmed via R CMD check output and CRAN documentation
- Work items: HIGH - Based on actual R CMD check results and file audits

**Research date:** 2026-01-22
**Valid until:** 2026-02-22 (30 days - R package standards are stable)
