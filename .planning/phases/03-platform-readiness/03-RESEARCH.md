# Phase 3: Platform Readiness - Research

**Researched:** 2026-01-22
**Domain:** R package CRAN submission and cross-platform compatibility
**Confidence:** HIGH

## Summary

Phase 3 focuses on ensuring the delarr package works reliably across Windows, macOS, and Linux platforms, with particular attention to Windows compatibility testing via win-builder. Based on user decisions, hdf5r and mmap are **required Imports** (not optional Suggests), which fundamentally changes the implementation approach from typical optional dependency patterns.

The key insight is that CRAN packages with system dependencies (like hdf5r's dependency on HDF5 libraries) handle their own platform-specific bundling. On Windows, hdf5r includes pre-compiled HDF5 binaries in the CRAN distribution, eliminating the need for manual system library installation. Similarly, mmap provides Windows support via the MapViewOfFile API rather than POSIX mmap.

This phase involves three main activities:
1. **Code cleanup:** Remove defensive `requireNamespace()` checks for hdf5r and mmap since they're guaranteed present
2. **Example simplification:** Remove `\donttest{}` blocks that were conditionally checking for dependencies
3. **Win-builder validation:** Test on Windows R-release and R-devel to ensure all three backends work correctly

**Primary recommendation:** Trust the dependency model—if hdf5r and mmap are in Imports, they're guaranteed available. Clean up defensive code, validate on win-builder, and rely on CRAN's cross-platform infrastructure.

## Standard Stack

### Core Platform Testing Tools

| Tool | Version | Purpose | Why Standard |
|------|---------|---------|--------------|
| win-builder | Current | Windows package testing service | Official CRAN infrastructure for Windows validation |
| devtools | ≥2.4 | R package development tools | Provides `check_win_devel()` and `check_win_release()` |
| usethis | ≥2.1.6 | R package automation | Simplifies GitHub Actions workflow setup |
| rhub v2 | ≥2.0 | Multi-platform testing | GitHub Actions-based testing on 30+ platforms |
| rcmdcheck | Current | R CMD check wrapper | Used by GitHub Actions and rhub for checks |

### Supporting Tools

| Tool | Version | Purpose | When to Use |
|------|---------|---------|-------------|
| urlchecker | Current | Validate URLs in documentation | Pre-submission checks (CRAN enforces redirects) |
| goodpractice | Current | Static analysis of package quality | Optional pre-CRAN validation |
| pkgndep | Current | Dependency weight analysis | If concerned about 20+ imports NOTE |

### Platform Testing Options

| Service | Use Case | Access Method | Cost |
|---------|----------|---------------|------|
| win-builder | Windows validation (required for CRAN) | `devtools::check_win_devel()` | Free |
| GitHub Actions | Continuous multi-platform testing | `usethis::use_github_action("check-standard")` | Free for public repos |
| rhub v2 | Specialized platforms (ASAN, Valgrind, etc.) | `rhub::rhub_check()` | Free with GitHub Actions minutes |

**Installation:**

```r
# Development tools
install.packages(c("devtools", "usethis", "rcmdcheck", "urlchecker"))

# For rhub v2 (GitHub Actions-based)
install.packages("rhub")
```

## Architecture Patterns

### Dependency Management Pattern: Imports vs Suggests

**Decision context:** User chose to make hdf5r and mmap **required Imports**, not optional Suggests.

#### Required Imports Pattern

**What:** Packages in the `Imports` field of DESCRIPTION are guaranteed present at package load time.

**DESCRIPTION setup:**
```r
Imports:
    hdf5r,
    mmap,
    rlang
```

**Code pattern (NO defensive checks needed):**
```r
# Source: https://r-pkgs.org/dependencies-in-practice.html
# When package is in Imports, just use it directly:

create_hdf5_backend <- function(path, dims) {
  # NO requireNamespace() check needed
  h5file <- hdf5r::H5File$new(path, mode = "w")
  h5file$create_dataset("data", dims = dims)
  # ...
}
```

**Key principle:** If it's in Imports, it's present. No `if (!requireNamespace(...))` needed.

#### Optional Suggests Pattern (for reference)

**What:** Pattern used when packages are truly optional—NOT applicable to delarr since hdf5r/mmap are required.

**Code pattern (for matrixStats, which IS in Suggests):**
```r
# Source: https://r-pkgs.org/dependencies-in-practice.html
# Pattern 1: Required suggested package (error if missing)
compute_stats <- function(x) {
  if (!requireNamespace("matrixStats", quietly = TRUE)) {
    stop(
      'Package "matrixStats" must be installed to use this function.',
      call. = FALSE
    )
  }
  matrixStats::colMeans2(x)
}

# Pattern 2: Optional with fallback
compute_stats <- function(x) {
  if (requireNamespace("matrixStats", quietly = TRUE)) {
    matrixStats::colMeans2(x)  # Fast path
  } else {
    apply(x, 2, mean)  # Slower fallback
  }
}
```

### Win-builder Testing Pattern

**Workflow:**

```r
# Step 1: Test locally first
devtools::check(remote = TRUE, manual = TRUE)

# Step 2: Test on Windows R-devel
devtools::check_win_devel()
# Wait ~30 minutes for email with results

# Step 3: Test on Windows R-release
devtools::check_win_release()
# Wait ~30 minutes for email

# Step 4: Review results
# Check for ERRORs (must fix), WARNINGs (must fix), NOTEs (minimize)
```

**What win-builder tests:**
- Windows Server 2022 environment
- Pre-installed CRAN and Bioconductor packages
- Rtools for compilation
- Same infrastructure CRAN maintainers use

**Source:** https://win-builder.r-project.org/

### GitHub Actions Multi-Platform Pattern

**Setup (one-time):**
```bash
# In R console:
usethis::use_github_action("check-standard")
```

**What it does:**
- Creates `.github/workflows/R-CMD-check.yaml`
- Tests on: Linux (R-release, R-devel, R-oldrel), macOS (R-release), Windows (R-release)
- Runs on every push and pull request
- Results visible in GitHub Actions tab

**Source:** https://github.com/r-lib/actions

**When to use:**
- Continuous validation during development
- Catch platform issues early
- Public visibility of package health
- CRAN mirrors this testing anyway

### Example Documentation Pattern

**CRAN requirement:** Examples must run in < 5 seconds per .Rd file.

**Source:** https://github.com/microsoft/LightGBM/issues/2988

#### Pattern 1: Quick Examples (preferred)

```r
#' Create HDF5 backend
#'
#' @examples
#' # Quick in-memory demonstration
#' temp_file <- tempfile(fileext = ".h5")
#' backend <- create_hdf5_backend(temp_file, c(10, 10))
#' unlink(temp_file)
#'
#' @export
```

**Why preferred:** Runs in < 5 seconds, automatically tested by CRAN.

#### Pattern 2: Slow Examples (use \donttest)

```r
#' @examples
#' \donttest{
#' # Larger example that takes > 5 seconds
#' big_file <- tempfile(fileext = ".h5")
#' backend <- create_hdf5_backend(big_file, c(1000, 1000))
#' # ... slow operations ...
#' unlink(big_file)
#' }
```

**Important:** As of R 4.0.0, `\donttest{}` examples ARE run by `R CMD check --as-cran`. Only use for genuinely slow examples (> 5 sec), not for conditional dependencies.

**Source:** https://forum.posit.co/t/r-cmd-check-r-4-0-0-now-runs-donttest-how-to-proceed-with-long-running-examples/64493

#### Anti-Pattern: \dontrun for Available Dependencies

```r
# DON'T DO THIS when package is in Imports:
#' @examples
#' \dontrun{
#' if (requireNamespace("hdf5r", quietly = TRUE)) {
#'   # ... hdf5r example ...
#' }
#' }
```

**Why it's bad:**
- hdf5r is in Imports, so it's always available
- `\dontrun{}` means "example can't run" (wrong message)
- CRAN will request unwrapping if examples work

**Source:** https://blog.r-hub.io/2020/01/27/examples/

### Vignette Conditional Code Pattern

**User preference:** Keep conditional chunks as "harmless safety net" even though not strictly necessary.

**Pattern:**
```r
# In vignette setup chunk
knitr::opts_chunk$set(
  collapse = TRUE,
  comment = "#>",
  eval = requireNamespace("hdf5r", quietly = TRUE)
)
```

**Why it's harmless:**
- Vignettes are built in an environment with all Imports available
- Check is redundant but doesn't hurt
- User preference for defensive coding in documentation

**Alternative pattern (if concerned about vignette build failures):**
```r
# Check once at top
has_hdf5 <- requireNamespace("hdf5r", quietly = TRUE)

# Use in specific chunks
```{r, eval=has_hdf5}
# HDF5-specific examples
```
```

**Source:** https://r-pkgs.org/vignettes.html

### System Requirements Pattern

**For packages with external system dependencies (like HDF5):**

DESCRIPTION field:
```
SystemRequirements: C++11
```

**Key insights:**
- hdf5r handles HDF5 bundling itself on Windows (via CRAN binaries)
- On Linux/macOS, users must install system HDF5 libraries
- This is standard CRAN practice for system library dependencies
- Don't try to bundle or download system libraries yourself

**Windows specifics:**
- CRAN Windows binaries include pre-compiled HDF5 libraries
- Source: https://github.com/mannau/h5-libwin (hdf5r uses this)
- No manual installation needed for Windows users

**Source:** https://cran.r-project.org/web/packages/policies.html

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Windows testing without Windows | VM or dual-boot setup | win-builder via `devtools::check_win_devel()` | Official CRAN infrastructure, 30-min results |
| Multi-platform CI | Custom scripts | `usethis::use_github_action("check-standard")` | Maintained by r-lib, tests 6+ OS/R combinations |
| Conditional dependency checks | Custom `tryCatch()` wrappers | `requireNamespace()` with `quietly = TRUE` | Standard R idiom, proper namespace checking |
| URL validation | Manual checking | `urlchecker::url_check()` | CRAN enforces 301 redirects, automates detection |
| System library bundling | Download in `configure` script | Trust upstream package (hdf5r) | hdf5r already bundles for Windows, Linux/macOS use system package managers |

**Key insight:** CRAN submission has a well-established ecosystem. Don't reinvent win-builder, don't bundle system libraries that upstream packages handle, don't create custom dependency checking when standard patterns exist.

## Common Pitfalls

### Pitfall 1: Over-Defensive Code for Imports

**What goes wrong:** Adding `requireNamespace()` checks for packages listed in Imports.

**Example:**
```r
# DESCRIPTION has: Imports: hdf5r
# Code unnecessarily checks:
if (!requireNamespace("hdf5r", quietly = TRUE)) {
  stop("hdf5r required")
}
```

**Why it happens:** Confusion between Imports (guaranteed) and Suggests (optional).

**How to avoid:**
- Imports = always available, use directly
- Suggests = might be absent, use `requireNamespace()`
- Review DESCRIPTION and remove checks for Imports

**Warning signs:**
- `requireNamespace()` for packages in Imports field
- Conditional logic around core functionality
- Error messages about "missing" required imports

**Source:** https://r-pkgs.org/dependencies-in-practice.html

### Pitfall 2: Assuming \donttest Skips Examples

**What goes wrong:** Wrapping examples in `\donttest{}` assuming CRAN won't run them.

**Why it happens:** Old behavior (pre-R 4.0) where `\donttest` was sometimes skipped.

**How to avoid:** As of R 4.0.0, `R CMD check --as-cran` DOES run `\donttest{}` examples. Only use for genuinely slow examples (> 5 seconds), not for conditional dependencies.

**Warning signs:**
- Many examples wrapped in `\donttest{}`
- Examples checking for available packages that are in Imports
- CRAN submission notes about example coverage

**Source:** https://forum.posit.co/t/r-cmd-check-r-4-0-0-now-runs-donttest-how-to-proceed-with-long-running-examples/64493

### Pitfall 3: Not Testing on Win-builder Before Submission

**What goes wrong:** Submitting to CRAN without win-builder validation, discovering Windows-specific failures after submission.

**Why it happens:** Developers on macOS/Linux forget Windows exists.

**How to avoid:**
1. Run `devtools::check_win_devel()` before CRAN submission
2. Run `devtools::check_win_release()` for current R version
3. Fix ALL ERRORs and WARNINGs
4. Document unavoidable NOTEs in cran-comments.md

**Warning signs:**
- Package works locally but fails CRAN checks
- Windows-specific path issues
- Encoding problems with non-ASCII characters
- Missing Rtools dependencies

**Source:** https://r-pkgs.org/release.html

### Pitfall 4: Assuming System Libraries Are Available

**What goes wrong:** Code assumes system libraries (like HDF5) are installed, but they're not on CRAN's Windows check machines.

**Why it happens:** Misunderstanding how CRAN handles system dependencies.

**How to avoid:**
- For Windows: Trust CRAN binary builds (hdf5r bundles HDF5)
- For Linux/macOS: SystemRequirements field documents what users need
- Don't try to download/bundle system libraries yourself
- Upstream packages (hdf5r, RcppGSL, etc.) handle this

**Warning signs:**
- Custom configure scripts downloading libraries
- Package installs locally but fails on win-builder
- Linking errors on Windows

**CRAN policy:** "Where a package wishes to make use of a library not written solely for the package, the package installation should first look to see if it is already installed and if so is of a suitable version."

**Source:** https://cran.r-project.org/web/packages/policies.html

### Pitfall 5: Executable Files or Hidden Files

**What goes wrong:** Including binary executables or hidden files (names starting with `.`) in package.

**Why it happens:** Development artifacts accidentally included in package build.

**How to avoid:**
- Use `.Rbuildignore` to exclude development files
- Never include platform-specific binaries
- Check build contents: `devtools::check()` will NOTE hidden files

**Warning signs:**
- NOTE about hidden files/directories
- NOTE about non-portable file names
- Package rejected for security reasons

**R CMD check looks for:**
- Files starting with `.` (hidden on Unix)
- Executable permissions
- Platform-specific file names

**Source:** https://kbroman.org/pkg_primer/pages/check.html

### Pitfall 6: Example Timing Exceeds 5 Seconds

**What goes wrong:** Examples take too long, CRAN requests simplification.

**Why it happens:** Examples demonstrate realistic use cases, which are often slow.

**How to avoid:**
- Use small toy data in examples (10x10 matrices, not 1000x1000)
- Wrap genuinely slow examples in `\donttest{}`
- Create faster examples that demonstrate the same API
- Monitor example timing in `R CMD check` output

**Warning signs:**
- NOTE about examples taking > 5 seconds per .Rd file
- CRAN maintainers request smaller examples
- Examples timing out on slower CRAN check machines

**Source:** https://github.com/microsoft/LightGBM/issues/2988

## Code Examples

### Example 1: Removing Defensive requireNamespace() Check

**Before (defensive code assuming optional dependency):**
```r
# Source: Current delarr code in R/delarr-backends.R:101
create_hdf5_backend <- function(path, dims, ...) {
  if (!requireNamespace("hdf5r", quietly = TRUE)) {
    stop("Package 'hdf5r' required for HDF5 backend", call. = FALSE)
  }
  # Implementation
}
```

**After (trusting Imports):**
```r
# hdf5r is in Imports, guaranteed available
create_hdf5_backend <- function(path, dims, ...) {
  # Just use it directly
  h5file <- hdf5r::H5File$new(path, mode = "w")
  # Implementation
}
```

**Why:** DESCRIPTION lists `Imports: hdf5r`, so requireNamespace() check is redundant.

### Example 2: Cleaning Up Examples

**Before (over-defensive):**
```r
#' @examples
#' \donttest{
#' if (requireNamespace("hdf5r", quietly = TRUE)) {
#'   tmp <- tempfile(fileext = ".h5")
#'   backend <- create_hdf5_backend(tmp, c(10, 10))
#'   unlink(tmp)
#' }
#' }
```

**After (direct, fast example):**
```r
#' @examples
#' # Quick example with cleanup
#' tmp <- tempfile(fileext = ".h5")
#' backend <- create_hdf5_backend(tmp, c(10, 10))
#' unlink(tmp)
```

**Why:**
- hdf5r is guaranteed present (Imports)
- Small dims (10x10) runs fast (< 5 sec)
- No `\donttest{}` needed for fast examples
- No conditional check needed for required import

### Example 3: Win-builder Submission Workflow

```r
# Source: https://devtools.r-lib.org/reference/check_win.html

# 1. Check locally first
devtools::check(remote = TRUE, manual = TRUE)
# Must pass with 0 ERRORs, 0 WARNINGs

# 2. Check on Windows R-devel
devtools::check_win_devel()
# Wait for email (~30 min)
# Review results at provided URL

# 3. Check on Windows R-release
devtools::check_win_release()
# Wait for email (~30 min)
# Review results

# 4. If NOTEs exist, document them
# Create/update cran-comments.md:
#
# ## R CMD check results
#
# 0 errors | 0 warnings | 1 note
#
# * checking CRAN incoming feasibility ... NOTE
#   New submission
#
# This is a new release.
```

### Example 4: GitHub Actions Setup

```r
# Source: https://usethis.r-lib.org/reference/github_actions.html

# One-time setup
usethis::use_github_action("check-standard")

# This creates .github/workflows/R-CMD-check.yaml
# Commit and push to GitHub

# Results appear in GitHub Actions tab
# Tests on:
# - Linux: R-release, R-devel, R-oldrel
# - macOS: R-release
# - Windows: R-release
```

### Example 5: Handling matrixStats (Actual Optional Suggest)

**Pattern for TRULY optional package (matrixStats is in Suggests):**

```r
# Source: Current delarr code in R/utils.R:44
compute_col_means <- function(x) {
  # matrixStats is optional, provides performance boost
  if (requireNamespace("matrixStats", quietly = TRUE)) {
    matrixStats::colMeans2(x)  # Fast C implementation
  } else {
    apply(x, 2, mean)  # Base R fallback
  }
}
```

**Why this is correct:**
- matrixStats IS in Suggests (not Imports)
- Fallback to base R maintains functionality
- Performance enhancement, not core requirement

**Contrast with hdf5r/mmap:**
- Those are in Imports = no fallback needed
- Core functionality requires them
- Different pattern entirely

### Example 6: Vignette Conditional Setup (User Preference)

```r
# Source: https://r-pkgs.org/vignettes.html
# In vignette .Rmd file

# Setup chunk
knitr::opts_chunk$set(
  collapse = TRUE,
  comment = "#>",
  eval = TRUE  # hdf5r is in Imports, always available
)

# User prefers keeping this as safety net (harmless):
has_hdf5 <- requireNamespace("hdf5r", quietly = TRUE)

# Then in specific chunks where extra caution desired:
```{r, eval=has_hdf5}
# HDF5 examples
```

# Note: This is redundant but user preference for defensive documentation
```

## State of the Art

### Evolution of R Package Testing

| Old Approach | Current Approach (2026) | When Changed | Impact |
|--------------|-------------------------|--------------|--------|
| R-hub v1 (centralized VMs) | R-hub v2 (GitHub Actions) | 2024 | Faster, more accessible, uses GitHub infrastructure |
| `\donttest{}` skipped by CRAN | `\donttest{}` run by `--as-cran` | R 4.0.0 (2020) | Examples must actually work, not just document |
| Manual FTP upload to win-builder | `devtools::check_win_*()` | devtools 2.0+ | Automated workflow, email results |
| Custom CI setups | `usethis::use_github_action()` | usethis 2.1.6+ (2022) | Standardized multi-platform testing |
| requireNamespace() for everything | Import for required, Suggests for optional | Long-standing best practice | Clearer dependency model |

**Deprecated/outdated:**
- **R-hub v1 (web interface):** Replaced by R-hub v2 with GitHub Actions (2024)
- **Not testing `\donttest{}`:** R 4.0.0+ runs these in CRAN checks
- **20+ Imports without justification:** CRAN now issues NOTE, encourages Suggests
- **Platform-specific file paths:** Use `file.path()`, works across OS
- **Manual win-builder FTP uploads:** Use devtools functions instead

**Current state (2026):**
- GitHub Actions is the standard CI platform for R packages
- rhub v2 uses GitHub Actions infrastructure (30+ platforms)
- Win-builder remains the canonical Windows testing service
- R uses UTF-8 on Windows (as of R 4.2.0, 2022)
- CRAN check flavors test ~12 OS/R version combinations

**Source:** https://blog.r-hub.io/2024/04/11/rhub2/

## Open Questions

### 1. Platform-Specific Test Failures

**What we know:**
- hdf5r bundles HDF5 on Windows (CRAN binaries)
- mmap uses MapViewOfFile on Windows, mmap on Unix
- Both packages are on CRAN, implying Windows support exists

**What's unclear:**
- Whether delarr's specific usage patterns work on Windows
- If there are path separator issues in HDF5 file handling
- Performance characteristics of mmap on Windows vs Unix

**Recommendation:**
- Run win-builder checks early in phase (don't wait until end)
- If failures occur, they'll be specific to delarr code, not hdf5r/mmap
- Test all three backends explicitly: delarr_hdf5, delarr_mem, delarr_mmap

### 2. GitHub Actions Setup Timing

**What we know:**
- `usethis::use_github_action("check-standard")` sets up multi-platform CI
- This is recommended but not required for CRAN submission
- Win-builder is sufficient for CRAN validation

**What's unclear:**
- Whether to set up GitHub Actions in this phase or later
- If continuous CI is worth the setup time for this phase

**Recommendation:**
- GitHub Actions is "nice to have" for ongoing development
- Win-builder is "must have" for CRAN submission
- Prioritize win-builder; add GitHub Actions if time permits
- Can always add GitHub Actions post-CRAN acceptance

### 3. Example Timing on Slow Machines

**What we know:**
- CRAN requires < 5 seconds per .Rd file
- HDF5 file creation/deletion might be slow on network filesystems
- tempfile() should be fast but depends on system

**What's unclear:**
- Whether current examples (after cleanup) stay under 5 seconds on CRAN check machines
- If HDF5 examples need smaller dimensions or `\donttest{}`

**Recommendation:**
- Start with unwrapped examples using small dims (10x10)
- Monitor timing in win-builder results
- Add `\donttest{}` only if CRAN feedback requests it
- Prefer smaller toy examples over wrapping in `\donttest{}`

## Sources

### Primary (HIGH confidence)

Official R documentation and CRAN resources:
- [CRAN Repository Policy](https://cran.r-project.org/web/packages/policies.html) - Official CRAN requirements
- [CRAN Submission Checklist](https://cran.r-project.org/web/packages/submission_checklist.html) - Required checks before submission
- [Win-builder service](https://win-builder.r-project.org/) - Official Windows testing service
- [R Packages (2e) - Chapter 22: Releasing to CRAN](https://r-pkgs.org/release.html) - Authoritative guide by Hadley Wickham & Jennifer Bryan
- [R Packages (2e) - Chapter 11: Dependencies in Practice](https://r-pkgs.org/dependencies-in-practice.html) - Imports vs Suggests patterns
- [R Packages (2e) - Chapter 17: Vignettes](https://r-pkgs.org/vignettes.html) - Vignette best practices

R package documentation:
- [devtools::check_win_devel()](https://devtools.r-lib.org/reference/check_win.html) - Win-builder wrapper function
- [usethis::use_github_action()](https://usethis.r-lib.org/reference/github_actions.html) - GitHub Actions setup
- [r-lib/actions repository](https://github.com/r-lib/actions) - Standard R package CI workflows

Package-specific information:
- [hdf5r on CRAN](https://cran.r-project.org/package=hdf5r) - Confirms Windows support with bundled libraries
- [hdf5r GitHub README](https://github.com/hhoeflin/hdf5r) - Documents Windows HDF5 bundling
- [mmap on CRAN](https://cran.r-project.org/package=mmap) - Confirms Windows MapViewOfFile support

### Secondary (MEDIUM confidence)

Community resources and blog posts:
- [R-hub v2 announcement](https://blog.r-hub.io/2024/04/11/rhub2/) - Explains GitHub Actions transition
- [R-hub v2 documentation](https://r-hub.github.io/rhub/) - Usage guide for rhub package
- [R-hub blog: Code examples in manuals](https://blog.r-hub.io/2020/01/27/examples/) - Example best practices
- [StatnMap: Debugging on Win-builder](https://statnmap.com/2021-04-11-how-to-debug-on-win-builder-before-sending-to-cran/) - Practical win-builder guide
- [CRAN Cookbook - General Issues](http://contributor.r-project.org/cran-cookbook/general_issues.html) - Common CRAN pitfalls
- [ThinkR: Prepare for CRAN](https://github.com/ThinkR-open/prepare-for-cran) - Community checklist

### Tertiary (LOW confidence - community discussions)

Package-specific issues and discussions:
- [LightGBM Issue #2988](https://github.com/microsoft/LightGBM/issues/2988) - Example timing requirements
- [Posit Community: R 4.0.0 \donttest discussion](https://forum.posit.co/t/r-cmd-check-r-4-0-0-now-runs-donttest-how-to-proceed-with-long-running-examples/64493) - Behavior change in R 4.0
- R-package-devel mailing list discussions - Various timing/example questions

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH - All tools are officially maintained by CRAN, Posit, or r-lib
- Architecture patterns: HIGH - Based on official R Packages book and CRAN policies
- Pitfalls: HIGH - Documented in official CRAN policy and R Packages book
- hdf5r/mmap Windows support: MEDIUM - Confirmed via CRAN presence and package docs, not personally verified
- Platform-specific test outcomes: LOW - Won't know until win-builder runs

**Research date:** 2026-01-22
**Valid until:** 90 days (stable ecosystem; CRAN policies change slowly)

**Key assumptions:**
1. hdf5r's Windows HDF5 bundling continues to work as documented
2. mmap's Windows MapViewOfFile implementation is functional
3. CRAN policies remain stable (they evolve gradually)
4. Win-builder continues to be primary Windows testing service

**Validation needed:**
- Actual win-builder runs to confirm Windows compatibility
- Example timing measurements on CRAN check infrastructure
- Verification that all three backends (hdf5, mmap, mem) work on Windows
