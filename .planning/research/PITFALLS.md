# CRAN Submission Pitfalls

**Domain:** R package submission to CRAN
**Package:** delarr (delayed array with fused execution)
**Researched:** 2026-01-22
**Overall confidence:** HIGH (based on official CRAN policy, R-packages book, CRAN Cookbook)

## Executive Summary

CRAN rejection is usually caused by automated check failures (ERRORs, WARNINGs, or substantial NOTEs), DESCRIPTION field formatting issues, or policy violations around file I/O, examples, and documentation. The delarr package has specific risk factors: optional system dependencies (hdf5r), file I/O operations (HDF5 writers), stub functions (delarr_mmap), and formula interfaces (rlang::as_function). This document catalogs pitfalls by severity and provides detection/prevention strategies tied to R CMD check.

---

## Critical Pitfalls

These cause automatic rejection or require package rewrites.

### Pitfall 1: Any ERRORs or WARNINGs in R CMD check

**What goes wrong:** CRAN automatically rejects packages with any ERRORs or WARNINGs from `R CMD check --as-cran`.

**Why it happens:** Check failures indicate code that doesn't work, violates R standards, or has portability issues.

**Consequences:** Immediate rejection. No human review.

**Prevention:**
- Run `R CMD check --as-cran` on tarball before submission
- Test on R-devel (not just R-release)
- Check on win-builder for Windows-specific issues
- Use `devtools::check(cran = TRUE)` locally

**Detection:**
```r
devtools::check(cran = TRUE)
# Must show: 0 errors ✓ | 0 warnings ✓ | 0 notes ✓
```

**R CMD check catches:** All ERRORs and WARNINGs

**delarr risk:** MEDIUM. File I/O and optional dependencies could cause platform-specific failures.

**Sources:**
- [CRAN Checklist for submissions](https://cran.r-project.org/web/packages/submission_checklist.html)
- [R Packages (2e) - Releasing to CRAN](https://r-pkgs.org/release.html)

---

### Pitfall 2: File I/O Outside tempdir() in Examples/Tests/Vignettes

**What goes wrong:** Functions write to user's home directory, working directory, or anywhere except R's temp directory.

**Why it happens:** Developers test with local paths and forget CRAN's strict sandboxing requirements.

**Consequences:** Automatic rejection. CRAN policy violation. Security concern.

**Prevention:**
- Use `tempdir()` or `tempfile()` for ALL file operations in examples/tests/vignettes
- Document parameters for file paths (let users specify, default to temp)
- Use `withr::local_tempfile()` and `withr::local_tempdir()` in tests for self-cleaning
- For interactive functions, get explicit user confirmation before writing to non-temp locations

**Detection:**
- Grep codebase for file path literals: `grep -r "~/\|/tmp/\|getwd()" R/ tests/ vignettes/`
- Check if examples specify paths: all should use `tempdir()`

**R CMD check catches:** May not catch all cases. Manual review required.

**delarr risk:** HIGH. Package has HDF5 file writers (`hdf5_writer()`, `delarr_hdf5()`).

**Critical action:** Verify all examples with HDF5 operations use:
```r
# Good
h5_file <- tempfile(fileext = ".h5")

# Bad
h5_file <- "output.h5"  # writes to working directory
```

**Sources:**
- [CRAN Repository Policy](https://cran.r-project.org/web/packages/policies.html)
- [CRAN Cookbook - Code Issues](https://contributor.r-project.org/cran-cookbook/code_issues.html)

---

### Pitfall 3: Examples Taking >5-10 Seconds

**What goes wrong:** Documentation examples run too long during `R CMD check`.

**Why it happens:** Examples demonstrate full analysis workflows instead of minimal API usage.

**Consequences:** Automatic rejection with NOTE: "Examples with CPU time > 5s". CRAN has limited check farm resources.

**Prevention:**
- Keep examples under 5 seconds each (10 seconds is hard limit)
- Use `\donttest{}` for slow examples (won't run on CRAN but will in help)
- Use `\dontrun{}` for examples requiring user setup
- Use small toy datasets in examples
- For delarr: use tiny arrays (10x10, not 1000x1000)

**Detection:**
```r
system.time(example("function_name"))
```

**R CMD check catches:** YES. Issues NOTE for examples >5s.

**delarr risk:** MEDIUM. Block operations and HDF5 I/O could be slow with realistic data sizes.

**Critical action:** Review examples in `d_map()`, `block_apply()`, `hdf5_writer()` - ensure they use minimal test data.

**Sources:**
- [JEFworks - Get your R package on CRAN in 10 steps](https://jef.works/blog/2018/06/18/get-your-package-on-cran-in-10-steps/)
- [CRAN Repository Policy](https://cran.r-project.org/web/packages/policies.html)

---

### Pitfall 4: Platform-Specific Failures (Windows/macOS/Linux)

**What goes wrong:** Package passes checks locally but fails on other operating systems.

**Why it happens:** Path separators, line endings, system dependencies, or platform-specific assumptions.

**Consequences:** Rejection if package doesn't work on at least 2 major platforms.

**Prevention:**
- Test on win-builder (Windows): submit tarball to win-builder.r-project.org
- Use forward slashes `/` for all file paths (R converts automatically)
- Avoid platform-specific system calls
- For system dependencies: handle gracefully if unavailable

**Detection:**
- Upload to win-builder: `devtools::check_win_devel()`
- Check R-hub (deprecated but historical): used to test multiple platforms
- GitHub Actions with multiple OS matrices

**R CMD check catches:** Platform-specific errors only on that platform.

**delarr risk:** HIGH. hdf5r has system library requirements.

**Critical action:**
- Ensure hdf5r is in `Suggests:` (not `Imports:`)
- All hdf5r usage must be conditional: `if (requireNamespace("hdf5r", quietly = TRUE))`
- Document system requirements clearly
- Consider linking to hdf5lib (new as of Nov 2025) for bundled HDF5 library

**Sources:**
- [R-bloggers - Everything about WinBuilder](https://www.r-bloggers.com/2020/03/everything-you-should-know-about-winbuilder/)
- [CRAN Repository Policy](https://cran.r-project.org/web/packages/policies.html)

---

### Pitfall 5: Missing or Incorrect S3 Method Registration

**What goes wrong:** S3 methods not registered in NAMESPACE, causing dispatch failures.

**Why it happens:** Developers export methods manually instead of using proper registration.

**Consequences:** Methods don't get called. R CMD check issues WARNINGs about undocumented exports.

**Prevention:**
- Use roxygen2 `@export` tags - automatically generates `S3method()` directives
- Register with: `S3method(generic, class)` in NAMESPACE
- Don't manually export S3 methods with `export()`
- Ensure method signatures match generic

**Detection:**
```r
# In R/file.R
#' @export
print.delarr <- function(x, ...) { ... }

# Generates in NAMESPACE:
# S3method(print, delarr)
# NOT: export(print.delarr)
```

**R CMD check catches:** YES. WARNINGs for signature incompatibility.

**delarr risk:** LOW. NAMESPACE looks correct (uses S3method directives).

**Current state:** NAMESPACE has proper S3method() entries:
```
S3method(dim, delarr)
S3method(`[`, delarr)
S3method(as.matrix, delarr)
S3method(print, delarr)
S3method(Ops, delarr)
S3method(rowMeans2, delarr)
S3method(colMeans2, delarr)
```

**Sources:**
- [Advanced R - S3](https://adv-r.hadley.nz/s3.html)
- [R Packages (2e) - R CMD check](https://r-pkgs.org/R-CMD-check.html)

---

## Moderate Pitfalls

These cause delays, require explanations, or generate NOTEs needing clarification.

### Pitfall 6: Substantial NOTEs Without Explanation

**What goes wrong:** R CMD check generates NOTEs that aren't explained in submission comments.

**Why it happens:** Some NOTEs are unavoidable (new submission, non-ASCII data) but look concerning without context.

**Consequences:** Delayed review. CRAN maintainers may reject if unclear.

**Prevention:**
- Eliminate all NOTEs if possible
- For unavoidable NOTEs, include explanation in submission form
- Acceptable NOTEs:
  - "New submission"
  - "Non-ASCII data" (if encoding declared as UTF-8)
  - "Specified C++17" (if genuinely needed)

**Detection:**
```r
devtools::check(cran = TRUE)
# Review any NOTEs in output
```

**R CMD check catches:** YES. Lists all NOTEs.

**delarr risk:** LOW. No obvious sources of unavoidable NOTEs.

**Watch for:**
- First submission will trigger "New submission" NOTE (expected)
- If hdf5r unavailable: "Suggests package not available for checking"

**Sources:**
- [R Packages (2e) - Releasing to CRAN](https://r-pkgs.org/release.html)

---

### Pitfall 7: DESCRIPTION Title/Description Field Formatting

**What goes wrong:** Title/Description fields violate CRAN's formatting conventions.

**Why it happens:** Natural language doesn't follow CRAN's strict style rules.

**Consequences:** Rejection with request to reformat. Common friction point.

**Prevention:**

**Title field:**
- Use Title Case (except articles/prepositions): use `tools::toTitleCase()`
- Quote package names: 'shiny', 'ggplot2'
- Quote software/API names: 'HDF5', 'API'
- Don't include package name itself
- Don't start with "A package for..." or "This package..."
- Don't end with period

**Description field:**
- More detailed than Title
- Full sentences with proper punctuation
- Quote package names: 'rlang', 'hdf5r'
- Explain acronyms on first use
- No redundant phrases like "for R" (it's CRAN, obviously R)

**Detection:**
```r
# Current Title:
# "Lazy Delayed Arrays with Fused Execution"
# GOOD: Title case, descriptive, no violations

# Current Description:
# "Provides a lightweight delayed array abstraction with tidy-friendly
#  verbs, expression fusion, and pluggable storage backends."
# CHECK: Should mention 'rlang' or 'hdf5r' if they're key features
```

**R CMD check catches:** NO. Manual CRAN review only.

**delarr risk:** LOW. Current DESCRIPTION looks clean.

**Suggested enhancement for Description:**
"Provides a lightweight delayed array abstraction with tidy-friendly verbs,
expression fusion using 'rlang', and pluggable storage backends including
in-memory and 'HDF5' formats via 'hdf5r'."

**Sources:**
- [CRAN Cookbook - DESCRIPTION File Issues](https://contributor.r-project.org/cran-cookbook/description_issues.html)
- [R Packages (2e) - DESCRIPTION](https://r-pkgs.org/description.html)

---

### Pitfall 8: Optional Dependencies Not Used Conditionally

**What goes wrong:** Package uses `Suggests:` dependencies without checking availability first.

**Why it happens:** Dependencies work locally but fail on minimal CRAN check environments.

**Consequences:** Test/example failures when suggested packages unavailable.

**Prevention:**
- Always check before using suggested packages:
```r
if (!requireNamespace("hdf5r", quietly = TRUE)) {
  stop("Package 'hdf5r' needed for this function. Please install it.",
       call. = FALSE)
}
```
- Use environment variable to skip tests conditionally:
```r
skip_if_not_installed("hdf5r")  # in testthat
```
- Functions with optional features should degrade gracefully

**Detection:**
```r
# Set environment to test without Suggests
Sys.setenv("_R_CHECK_DEPENDS_ONLY_" = "true")
devtools::check()
```

**R CMD check catches:** Sometimes. Depends on check environment.

**delarr risk:** HIGH. Package has hdf5r in Suggests (system dependency).

**Critical action:** Audit all hdf5r usage:
- `delarr_hdf5()`: must check for hdf5r
- `hdf5_writer()`: must check for hdf5r
- Tests using HDF5: must skip if hdf5r unavailable
- Examples: either use `\donttest{}` or conditional execution

**Sources:**
- [R Packages (2e) - Dependencies in Practice](https://r-pkgs.org/dependencies-in-practice.html)
- [CRAN Repository Policy](https://cran.r-project.org/web/packages/policies.html)

---

### Pitfall 9: Undefined Global Variables (Non-Standard Evaluation)

**What goes wrong:** R CMD check reports "no visible binding for global variable" NOTEs.

**Why it happens:** Functions use NSE (data-masking) where variable names are treated as bare symbols, not strings.

**Consequences:** Generates NOTEs. Not a rejection reason but creates friction.

**Prevention:**
- Use `.data` pronoun from rlang: `.data$column_name`
- Use `{{ var }}` (curly-curly) for function arguments
- Declare global variables: `utils::globalVariables(c("var1", "var2"))`
- Better: avoid NSE in package functions (use standard evaluation)

**Detection:**
R CMD check will list all undefined globals in output.

**R CMD check catches:** YES. Issues NOTE for each undefined global.

**delarr risk:** MEDIUM. Package uses rlang and formula interface.

**Critical action:** Review `d_map()`, `d_reduce()` - if they use formula syntax, verify they don't trigger global variable NOTEs.

**Example prevention:**
```r
# If using rlang::as_function() with formulas:
# User provides: ~ .x + 1
# This is fine - the function handles the formula scope internally
# No global variable issues expected
```

**Sources:**
- [R Packages (2e) - R CMD check](https://r-pkgs.org/R-CMD-check.html)

---

### Pitfall 10: LazyData Without Data Directory

**What goes wrong:** DESCRIPTION specifies `LazyData: true` but package has no `data/` directory.

**Why it happens:** Old package templates included LazyData by default.

**Consequences:** NOTE from R CMD check (since March 2021). `R CMD build` strips the field automatically.

**Prevention:**
- If package has no `data/` directory: set `LazyData: false` or omit the field entirely
- If package HAS `data/` with .rda files: keep `LazyData: true`

**Detection:**
```r
# Check for data/ directory
dir.exists("data")

# If FALSE and LazyData: true in DESCRIPTION, change to false
```

**R CMD check catches:** YES. Issues NOTE: "'LazyData' is specified without a 'data' directory"

**delarr risk:** NONE. DESCRIPTION already has `LazyData: false` (correct).

**Sources:**
- [R-devel NEWS - March 26, 2021](https://developer.r-project.org/blosxom.cgi/R-devel/2021/03/26)
- [GitHub - usethis LazyData fix](https://github.com/r-lib/usethis/pull/1404)

---

## Minor Pitfalls

These cause annoyance but are easily fixable.

### Pitfall 11: Missing @return Documentation

**What goes wrong:** Exported functions lack `@return` roxygen2 tag.

**Why it happens:** Developers focus on `@param` but forget return value.

**Consequences:** Incomplete documentation. May trigger NOTE or human review comment.

**Prevention:**
- Every exported function MUST have `@return` tag
- If function is called for side effects, document that: `@return NULL, invisibly`
- Be specific: describe type, dimensions, class

**Detection:**
```r
# Grep for exported functions without @return
# Manual review of R/*.R files
```

**R CMD check catches:** May generate NOTE for incomplete documentation.

**delarr risk:** LOW. Standard issue, easily audited.

**Sources:**
- [R Packages (2e) - Function documentation](https://r-pkgs.org/man.html)

---

### Pitfall 12: Stub Functions That Always Error

**What goes wrong:** Exported function exists only to throw an error (not implemented).

**Why it happens:** Placeholder for future features or platform-specific functionality.

**Consequences:** Confusion for users. Technically allowed but poor practice.

**Prevention:**
- If feature truly not available: don't export the function at all
- If platform-specific: check platform and provide helpful message
- If future feature: keep internal until implemented

**Detection:**
- Grep for functions that only call `stop()` or `rlang::abort()`

**R CMD check catches:** NO. This is an API design issue, not a check violation.

**delarr risk:** MEDIUM. Known issue: `delarr_mmap()` is stub function that just errors.

**Critical action:** Decide on `delarr_mmap()`:
1. Remove from exports (best option if not implemented)
2. Make internal (.delarr_mmap)
3. Implement basic functionality
4. Document clearly: "@details Memory-mapped backend not yet implemented. Will error."

**Sources:**
- General R package design principles

---

### Pitfall 13: Unused Function Parameters

**What goes wrong:** Function has parameter that's never used in function body.

**Why it happens:** API design evolution, placeholder for S3 method compatibility, or copy-paste.

**Consequences:** R CMD check may issue NOTE (context-dependent). Confuses users.

**Prevention:**
- Remove unused parameters if not needed for S3 dispatch
- For S3 methods: match generic signature even if not using all params
- Document why parameter exists: `@param unused Reserved for future use`
- Use `_ =` prefix for intentionally unused params (R 4.0+)

**Detection:**
Manual code review or use codetools:
```r
codetools::checkUsage(fun_name)
```

**R CMD check catches:** May issue NOTE depending on context.

**delarr risk:** HIGH. Known issue: `compression` parameter in `hdf5_writer()` is unused.

**Critical action:** Fix `hdf5_writer()`:
1. Remove `compression` parameter if not implemented
2. Implement compression functionality
3. Document as reserved: `@param compression Reserved for future use`

**Sources:**
- [R CMD check documentation](https://cran.r-project.org/doc/manuals/r-patched/packages/tools/refman/tools.html)

---

### Pitfall 14: Vignette Builder Configuration

**What goes wrong:** VignetteBuilder specified but dependencies missing, or pandoc unavailable.

**Why it happens:** knitr + rmarkdown + pandoc chain has many points of failure.

**Consequences:** Vignette building fails during R CMD check.

**Prevention:**
- If using R Markdown vignettes: add both knitr AND rmarkdown to Suggests
- Specify `VignetteBuilder: knitr` in DESCRIPTION
- Ensure pandoc available on build machine
- Test with: `devtools::build_vignettes()`

**Detection:**
```r
# Check vignette builds
devtools::build_vignettes()

# Check pandoc
rmarkdown::find_pandoc()
```

**R CMD check catches:** YES. ERROR if vignette build fails.

**delarr risk:** LOW. DESCRIPTION has correct setup:
```
Suggests: knitr, rmarkdown
VignetteBuilder: knitr
```

**Sources:**
- [R Packages (2e) - Vignettes](https://r-pkgs.org/vignettes.html)

---

### Pitfall 15: Environment State Management / Global Variables

**What goes wrong:** Package modifies global environment or global state without permission.

**Why it happens:** Convenience functions use `assign(..., envir = .GlobalEnv)` or modify options.

**Consequences:** CRAN policy violation. Potential rejection.

**Prevention:**
- Never assign to global environment in package code
- Use package-internal environments for state:
```r
pkg_env <- new.env(parent = emptyenv())
```
- For options: use `on.exit()` or withr to restore state
- For interactive use: require explicit user confirmation

**Detection:**
```r
# Grep for problematic patterns
grep -r "<<-\|assign.*globalenv\|.GlobalEnv" R/
```

**R CMD check catches:** May issue NOTE: "CRAN checks: assignments to the global environment"

**delarr risk:** MEDIUM. Package uses "Environment-based state management" (per project context).

**Critical action:** Audit environment usage:
- If using environments internally: OK
- If assigning to `.GlobalEnv`: VIOLATION, must fix
- If modifying options: must restore on exit

**Sources:**
- [GitHub - CRAN checks: assignments to global environment](https://github.com/floybix/latticist/issues/13)

---

## HDF5-Specific Pitfalls

### Pitfall 16: System Dependencies Not Available on CRAN Check Machines

**What goes wrong:** Package requires HDF5 C libraries not available on CRAN infrastructure.

**Why it happens:** hdf5r requires system-level HDF5 installation.

**Consequences:** Package checks fail on some platforms.

**Prevention:**
- Make hdf5r optional (already in `Suggests:` - correct)
- Consider using hdf5lib package (new Nov 2025): provides bundled HDF5 library
- Document system requirements clearly in README and DESCRIPTION
- Add SystemRequirements field if needed: `SystemRequirements: HDF5 (optional)`
- Gracefully degrade when HDF5 unavailable

**Detection:**
Test on fresh system without HDF5 libraries installed.

**R CMD check catches:** Only if hdf5r installation attempted and fails.

**delarr risk:** MEDIUM. Optional dependency reduces risk but needs careful handling.

**Sources:**
- [R-bloggers - November 2025 Top CRAN Packages](https://www.r-bloggers.com/2026/01/november-2025-top-40-new-cran-packages/)
- [CRAN hdf5lib package](https://cran.r-project.org/web/packages/hdf5lib/index.html)

---

## Phase-Specific Warnings

| Milestone Topic | Likely Pitfall | Mitigation |
|-----------------|---------------|------------|
| Fix R CMD check issues | Undefined globals from rlang | Use `.data$` pronoun or globalVariables() |
| DESCRIPTION polish | Title/Description formatting | Follow CRAN Cookbook guidelines |
| Examples cleanup | File I/O to working directory | Change all to tempdir() |
| Examples performance | HDF5 operations >5 seconds | Use tiny test arrays |
| Optional dependency handling | hdf5r usage without checks | Wrap all HDF5 code in requireNamespace() |
| S3 method audit | Already correct | Just verify, low risk |
| Test coverage | Tests use HDF5 without skip | Add skip_if_not_installed("hdf5r") |
| Vignette audit | HDF5 examples fail without library | Use conditional chunks or \donttest{} |
| Win-builder testing | Platform-specific HDF5 issues | Test early, may need platform-specific code |
| Stub function cleanup | delarr_mmap() always errors | Remove from exports or implement |
| Unused parameter cleanup | hdf5_writer(compression = ) | Remove or implement |

---

## Pre-Submission Checklist

Before submitting to CRAN, verify:

- [ ] `devtools::check(cran = TRUE)` passes: 0 errors, 0 warnings, 0 notes
- [ ] All examples use `tempdir()` for file I/O
- [ ] All examples run in <5 seconds each
- [ ] Tested on win-builder (devel and release)
- [ ] hdf5r used conditionally in all functions/tests/examples
- [ ] S3 methods properly registered in NAMESPACE
- [ ] All exported functions have @return documentation
- [ ] DESCRIPTION Title and Description follow CRAN conventions
- [ ] No assignments to global environment
- [ ] Stub function (delarr_mmap) handled appropriately
- [ ] Unused parameter (compression) removed or documented
- [ ] Vignettes build successfully
- [ ] LazyData: false (correct for no-data packages)
- [ ] LICENSE file present (MIT + file LICENSE)

---

## Sources

### Official CRAN Resources (HIGH confidence)
- [CRAN Repository Policy](https://cran.r-project.org/web/packages/policies.html)
- [CRAN Checklist for submissions](https://cran.r-project.org/web/packages/submission_checklist.html)
- [CRAN Cookbook - DESCRIPTION File Issues](https://contributor.r-project.org/cran-cookbook/description_issues.html)
- [CRAN Cookbook - Code Issues](https://contributor.r-project.org/cran-cookbook/code_issues.html)

### R Packages Book (HIGH confidence)
- [R Packages (2e) - Releasing to CRAN](https://r-pkgs.org/release.html)
- [R Packages (2e) - R CMD check](https://r-pkgs.org/R-CMD-check.html)
- [R Packages (2e) - DESCRIPTION](https://r-pkgs.org/description.html)
- [R Packages (2e) - Dependencies in Practice](https://r-pkgs.org/dependencies-in-practice.html)
- [R Packages (2e) - Vignettes](https://r-pkgs.org/vignettes.html)

### Community Resources (MEDIUM confidence)
- [R-bloggers - Everything about WinBuilder](https://www.r-bloggers.com/2020/03/everything-you-should-know-about-winbuilder/)
- [R-hub blog - Win-Builder](https://blog.r-hub.io/2020/04/01/win-builder/)
- [JEFworks - Get your R package on CRAN in 10 steps](https://jef.works/blog/2018/06/18/get-your-package-on-cran-in-10-steps/)

### Technical References (HIGH confidence)
- [Advanced R - S3](https://adv-r.hadley.nz/s3.html)
- [rlang documentation](https://cran.r-project.org/web/packages/rlang/rlang.pdf)
- [CRAN hdf5lib package](https://cran.r-project.org/web/packages/hdf5lib/index.html)
