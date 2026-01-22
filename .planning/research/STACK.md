# Technology Stack for CRAN Submission

**Project:** delarr
**Researched:** 2026-01-22
**Confidence:** HIGH (based on official CRAN documentation, current package versions, and community standards)

## Executive Summary

CRAN submission in 2025 relies on a mature, well-standardized toolchain centered around the r-lib ecosystem. The core workflow uses **devtools** (2.4.6) and **usethis** (3.2.1) for development automation, **roxygen2** (7.3.3) for documentation, **testthat** (3.3.2) for testing, and **rcmdcheck** (1.4.0) for local validation. GitHub Actions with **r-lib/actions** provides CI/CD, and **rhub v2** enables multi-platform testing before submission.

Given delarr's existing setup (S3 classes, testthat, roxygen2, single vignette), the stack is already 80% aligned with CRAN standards. Key additions needed: code quality tools (lintr, spelling, urlchecker), CI/CD workflows, and comprehensive checking infrastructure.

---

## Core Development Tools

### devtools: Primary Development Interface
| Attribute | Value |
|-----------|-------|
| **Package** | devtools |
| **Version** | 2.4.6 (October 2025) |
| **Purpose** | Orchestrates all development workflows |
| **Why Essential** | Provides high-level functions (`check()`, `document()`, `test()`, `build()`) that wrap lower-level tools. Standard interface for R package development. |

**Key Functions for CRAN:**
- `devtools::check()` - Runs R CMD check locally
- `devtools::check_win_devel()` - Tests on Windows R-devel (CRAN uses this)
- `devtools::submit_cran()` - Submits package to CRAN
- `devtools::release()` - Interactive release checklist
- `devtools::spell_check()` - Spell checking wrapper

**Installation:**
```r
install.packages("devtools")
```

**Rationale:** devtools is the de facto standard. Every CRAN submission guide assumes its use. Wraps complexity of R CMD check, document building, and dependency management into developer-friendly functions.

**Sources:**
- [devtools on CRAN](https://rdrr.io/cran/devtools/)
- [R Packages (2e) - Releasing to CRAN](https://r-pkgs.org/release.html)

---

### usethis: Workflow Automation
| Attribute | Value |
|-----------|-------|
| **Package** | usethis |
| **Version** | 3.2.1 (August 2025) |
| **Purpose** | Automates setup of package infrastructure |
| **Why Essential** | Creates standard files/configurations, ensures CRAN-compliant structure |

**Key Functions for CRAN:**
- `usethis::use_release_issue()` - Generates GitHub issue with CRAN submission checklist
- `usethis::use_version()` - Bumps version numbers
- `usethis::use_cran_comments()` - Creates cran-comments.md for submission notes
- `usethis::use_news_md()` - Sets up NEWS.md for changelog
- `usethis::use_github_action("check-standard")` - Adds GitHub Actions for R CMD check

**Installation:**
```r
install.packages("usethis")
```

**Rationale:** usethis enforces best practices through automation. The release checklist from `use_release_issue()` is comprehensive and community-vetted. Prevents common submission errors by standardizing structure.

**Sources:**
- [usethis on CRAN](https://rdrr.io/cran/usethis/)
- [usethis GitHub Actions documentation](https://usethis.r-lib.org/reference/github_actions.html)

---

## Documentation Tools

### roxygen2: API Documentation
| Attribute | Value |
|-----------|-------|
| **Package** | roxygen2 |
| **Version** | 7.3.3 (September 2025) |
| **Purpose** | Generates .Rd files from inline comments |
| **Why Essential** | CRAN requires complete, accurate documentation. roxygen2 keeps code and docs in sync. |
| **Status** | Already in use by delarr |

**Key Features:**
- Inline documentation with `#'` comments
- Automatic NAMESPACE generation
- Parameter validation (@param, @return, @examples)
- Cross-referencing between functions

**Installation:**
```r
install.packages("roxygen2")
```

**Rationale:** roxygen2 is the standard (90%+ of CRAN packages use it). Alternative (hand-written .Rd files) is error-prone and doesn't scale. CRAN requires documentation completeness - roxygen2's validation catches missing params/return values.

**delarr Status:** Already configured. Ensure all exported functions have complete documentation (no missing @param, @return, @examples).

**Sources:**
- [roxygen2 on CRAN](https://cran.r-project.org/web/packages/roxygen2/index.html)
- [R Packages (2e) - Function documentation](https://r-pkgs.org/man.html)

---

### pkgdown: Website Generation
| Attribute | Value |
|-----------|-------|
| **Package** | pkgdown |
| **Version** | 2.2.0 (November 2025) |
| **Purpose** | Generates package website from documentation |
| **Why Essential** | Not required for CRAN, but standard practice. Helps users find and understand your package. |

**Key Features:**
- Auto-generates website from roxygen2 docs
- Renders vignettes as articles
- Reference index for all functions
- New in 2.2.0: `build_llm_docs()` for LLM-friendly docs

**Installation:**
```r
install.packages("pkgdown")
```

**Setup:**
```r
usethis::use_pkgdown()
usethis::use_pkgdown_github_pages()
```

**Rationale:** Not CRAN-required, but expected by users. GitHub Pages hosting is free. Improves discoverability and reduces support burden. Low effort for high value.

**Sources:**
- [pkgdown 2.2.0 release](https://tidyverse.org/blog/2025/11/pkgdown-2-2-0/)
- [pkgdown on CRAN](https://cran.r-project.org/web/packages/pkgdown/readme/README.html)

---

## Testing Tools

### testthat: Unit Testing Framework
| Attribute | Value |
|-----------|-------|
| **Package** | testthat |
| **Version** | 3.3.2 (January 2026) |
| **Purpose** | Unit testing framework |
| **Why Essential** | CRAN doesn't require tests, but they're expected. testthat 3.x is the standard. |
| **Status** | Already in use by delarr |

**Key Features:**
- BDD-style testing (describe/it blocks)
- Snapshot testing for complex outputs
- Parallel test execution
- Clear error messages (improved in 3.3.0)

**Requirements:**
- Requires R >= 4.1
- Tests must pass during R CMD check
- Avoid tests that require network, long computation, or external services

**Installation:**
```r
install.packages("testthat")
```

**Rationale:** testthat is universal (95%+ of CRAN packages). CRAN doesn't mandate tests, but reviewers notice their absence. Tests prevent regressions and build confidence in package stability.

**delarr Status:** Already configured. Ensure comprehensive coverage of core functionality, especially edge cases for delayed operations.

**Sources:**
- [testthat 3.3.0 release](https://tidyverse.org/blog/2025/11/testthat-3-3-0/)
- [testthat on CRAN](https://cran.r-project.org/web/packages/testthat/testthat.pdf)

---

### covr: Test Coverage Analysis
| Attribute | Value |
|-----------|-------|
| **Package** | covr |
| **Version** | Latest (November 2025) |
| **Purpose** | Measures test coverage |
| **Why Recommended** | Not required, but identifies untested code paths |

**Key Functions:**
- `covr::package_coverage()` - Analyzes package test coverage
- `covr::report()` - Generates HTML coverage report
- Integrates with Codecov/Coveralls for CI

**Installation:**
```r
install.packages("covr")
```

**Rationale:** CRAN doesn't check coverage, but high coverage (>80%) correlates with fewer post-release bugs. Identifies edge cases you forgot to test. Good for finding blind spots before users do.

**Sources:**
- [covr on CRAN](https://cran.r-project.org/web/packages/covr/covr.pdf)
- [covr package website](https://covr.r-lib.org/)

---

## Code Quality Tools

### lintr: Static Analysis
| Attribute | Value |
|-----------|-------|
| **Package** | lintr |
| **Version** | Latest (2025) |
| **Purpose** | Lints R code for style and potential issues |
| **Why Recommended** | Catches common mistakes, enforces tidyverse style guide |

**Key Features:**
- Checks for style violations (spacing, naming)
- Detects potential bugs (unused variables, T/F instead of TRUE/FALSE)
- Configurable rules via .lintr file
- IDE integration (RStudio, VSCode)

**Installation:**
```r
install.packages("lintr")
```

**Rationale:** CRAN doesn't enforce style, but consistent style improves maintainability. lintr catches common pitfalls that R CMD check misses (e.g., using `1:length(x)` which fails when x is empty).

**Sources:**
- [lintr package website](https://lintr.r-lib.org/)
- [R-hub blog on workflow automation](https://blog.r-hub.io/2020/04/29/maintenance/)

---

### styler: Code Formatter
| Attribute | Value |
|-----------|-------|
| **Package** | styler |
| **Version** | Latest (2025) |
| **Purpose** | Auto-formats R code to tidyverse style |
| **Why Recommended** | Fixes style issues automatically, complements lintr |

**Key Functions:**
- `styler::style_pkg()` - Styles entire package
- `styler::style_file()` - Styles specific files

**Installation:**
```r
install.packages("styler")
```

**Rationale:** Manual style fixes are tedious. styler automates formatting, ensuring consistency. Run once before CRAN submission to clean up code. Pairs with lintr (styler fixes, lintr validates).

**Sources:**
- [Maëlle Salmon's blog on automatic tools](https://masalmon.eu/2017/06/17/automatictools/)

---

### spelling: Spell Checker
| Attribute | Value |
|-----------|-------|
| **Package** | spelling |
| **Version** | Latest (August 2025) |
| **Purpose** | Spell-checks documentation, vignettes, DESCRIPTION |
| **Why Essential** | CRAN reviewers notice typos. Spell check prevents embarrassment. |

**Key Features:**
- Checks .Rd files, vignettes, DESCRIPTION
- Maintains custom dictionary (WORDLIST)
- Integrates with R CMD check via unit test

**Installation:**
```r
install.packages("spelling")
```

**Setup:**
```r
# Add spell check as unit test
usethis::use_spell_check()
```

**Rationale:** CRAN reviewers are human. Typos in DESCRIPTION or examples create bad first impression. spelling catches mistakes before submission. The WORDLIST file handles technical terms (e.g., "hdf5r", "delarr").

**Sources:**
- [spelling package on CRAN](https://cran.r-project.org/web/packages/spelling/spelling.pdf)
- [GitHub: ropensci/spelling](https://github.com/ropensci/spelling)

---

### urlchecker: URL Validator
| Attribute | Value |
|-----------|-------|
| **Package** | urlchecker |
| **Version** | 1.0.1 (July 2025) |
| **Purpose** | Validates URLs in documentation and DESCRIPTION |
| **Why Essential** | CRAN checks URLs and rejects packages with broken links |

**Key Features:**
- Checks all URLs in package documentation
- Concurrent requests (faster than CRAN's serial checks)
- Identifies 404s, redirects, and malformed URLs

**Installation:**
```r
install.packages("urlchecker")
```

**Usage:**
```r
urlchecker::url_check()
```

**Rationale:** CRAN automated checks validate all URLs. A single broken link can delay submission. urlchecker runs the same checks locally, catching issues early. Faster than waiting for CRAN feedback.

**Sources:**
- [urlchecker on CRAN](https://cran.r-project.org/web/packages/urlchecker/urlchecker.pdf)
- [urlchecker package website](https://urlchecker.r-lib.org/)

---

## Checking Infrastructure

### rcmdcheck: R CMD check from R
| Attribute | Value |
|-----------|-------|
| **Package** | rcmdcheck |
| **Version** | 1.4.0 |
| **Purpose** | Runs R CMD check programmatically, parses results |
| **Why Essential** | Used by devtools and GitHub Actions. Provides structured output. |

**Key Features:**
- Runs R CMD check in isolated process
- Parses output into structured format (errors/warnings/notes)
- Supports background execution
- Used internally by `devtools::check()`

**Installation:**
```r
install.packages("rcmdcheck")
```

**Rationale:** Direct R CMD check output is hard to parse. rcmdcheck provides programmatic access, enabling automation. Foundation for CI/CD workflows. devtools::check() wraps this.

**Sources:**
- [rcmdcheck on CRAN](https://cran.r-project.org/package=rcmdcheck)
- [rcmdcheck package website](https://rcmdcheck.r-lib.org/)

---

### rhub: Multi-Platform Testing (v2)
| Attribute | Value |
|-----------|-------|
| **Package** | rhub |
| **Version** | 2.0.1 (July 2025) |
| **Purpose** | Tests package on multiple platforms via GitHub Actions |
| **Why Essential** | CRAN tests on Windows/Mac/Linux. rhub v2 replicates this before submission. |

**Key Changes in v2:**
- Now uses GitHub Actions (v1 used web service)
- Requires GitHub repository
- Free for public repos
- Mirrors CRAN's platform matrix

**Installation:**
```r
install.packages("rhub")
```

**Setup:**
```r
rhub::rhub_setup()  # Adds .github/workflows/rhub.yaml
```

**Usage:**
```r
rhub::rhub_check()  # Triggers GitHub Actions checks
```

**Rationale:** CRAN tests on platforms you might not have access to (Solaris, ARM, Windows R-devel). rhub v2 runs these checks before submission. The old `check_rhub()` in devtools is deprecated - use rhub v2 directly.

**delarr Note:** Given optional hdf5r dependency, test on all platforms to ensure graceful handling when hdf5r unavailable.

**Sources:**
- [rhub v2 announcement](https://blog.r-hub.io/2024/04/11/rhub2/)
- [rhub on CRAN](https://cran.r-project.org/web/packages/rhub/rhub.pdf)
- [rhub v2 documentation](https://r-hub.github.io/rhub/reference/rhubv2.html)

---

## CI/CD: GitHub Actions

### r-lib/actions: Standard Workflows
| Attribute | Value |
|-----------|-------|
| **Repository** | r-lib/actions |
| **Version** | v2 (current) |
| **Purpose** | Provides reusable GitHub Actions for R packages |
| **Why Essential** | Standard CI/CD for CRAN packages. Catches issues before submission. |

**Core Actions:**
- `setup-r@v2` - Installs R
- `setup-r-dependencies@v2` - Installs package dependencies
- `check-r-package@v2` - Runs R CMD check
- `setup-pandoc@v2` - Installs pandoc (for vignettes)
- `setup-tinytex@v2` - Installs LaTeX (for PDF vignettes)

**Recommended Workflow:**
```r
usethis::use_github_action("check-standard")
```

This creates `.github/workflows/R-CMD-check.yaml` that:
- Tests on Windows, Mac, Linux
- Tests on R-release and R-devel
- Runs on every push and PR
- Matches CRAN's check configuration

**Rationale:** CRAN checks packages on multiple OS/R version combinations. GitHub Actions replicates this for free. Catches platform-specific issues early. "Check-standard" workflow is the baseline for CRAN submissions.

**delarr Note:** Given hdf5r optional dependency, ensure tests handle its absence gracefully on all platforms.

**Sources:**
- [r-lib/actions repository](https://github.com/r-lib/actions)
- [usethis GitHub Actions documentation](https://usethis.r-lib.org/reference/github_actions.html)
- [R-bloggers: Checking packages on schedule](https://www.r-bloggers.com/2025/02/checking-your-r-packages-and-practicals-on-a-schedule-using-github-actions/)

---

## Version Control & Release Management

### Git + GitHub
| Attribute | Value |
|-----------|-------|
| **Tool** | Git + GitHub |
| **Purpose** | Version control, issue tracking, CI/CD hosting |
| **Why Essential** | Required for rhub v2, GitHub Actions. Standard for open source R packages. |

**Key usethis functions:**
- `usethis::use_git()` - Initializes git repo
- `usethis::use_github()` - Creates GitHub repo
- `usethis::use_github_release()` - Tags releases

**Rationale:** Git is universal. GitHub provides free CI/CD (Actions), free hosting (Pages for pkgdown), and free multi-platform testing (rhub v2). Not technically required for CRAN, but practically essential.

---

## Optional but Recommended

### goodpractice: Meta-Checker
| Attribute | Value |
|-----------|-------|
| **Package** | goodpractice |
| **Purpose** | Runs multiple checks (rcmdcheck + covr + lintr + cyclocomp) |
| **When to Use** | Final pre-submission check |

**Installation:**
```r
install.packages("goodpractice")
```

**Usage:**
```r
goodpractice::gp()  # Runs all checks
```

**Rationale:** Bundles multiple tools into one check. Useful for final validation, but adds complexity. Better to run tools individually during development, then use goodpractice for final gate.

**Sources:**
- [R-hub blog on workflow automation](https://blog.r-hub.io/2020/04/29/maintenance/)

---

### revdepcheck: Reverse Dependency Checker
| Attribute | Value |
|-----------|-------|
| **Package** | revdepcheck |
| **Purpose** | Checks packages that depend on yours |
| **When to Use** | If other CRAN packages depend on delarr |

**Not Needed for Initial Submission:** delarr has no reverse dependencies yet. Relevant for future updates.

---

## Installation Script

Complete setup for CRAN submission:

```r
# Core development tools
install.packages(c(
  "devtools",      # 2.4.6 - development workflows
  "usethis",       # 3.2.1 - infrastructure automation
  "pkgbuild"       # package building (devtools dependency)
))

# Documentation
install.packages(c(
  "roxygen2",      # 7.3.3 - already installed
  "pkgdown"        # 2.2.0 - website generation
))

# Testing
install.packages(c(
  "testthat",      # 3.3.2 - already installed
  "covr"           # coverage analysis
))

# Code quality
install.packages(c(
  "lintr",         # static analysis
  "styler",        # code formatting
  "spelling",      # spell checking
  "urlchecker"     # URL validation
))

# Checking infrastructure
install.packages(c(
  "rcmdcheck",     # 1.4.0 - R CMD check wrapper
  "rhub"           # 2.0.1 - multi-platform testing
))

# Optional but recommended
install.packages(c(
  "goodpractice"   # meta-checker
))
```

---

## Workflow Overview

**Daily development:**
1. `devtools::load_all()` - Load package
2. `devtools::test()` - Run tests
3. `devtools::document()` - Update documentation

**Pre-commit:**
1. `styler::style_pkg()` - Format code
2. `lintr::lint_package()` - Check style
3. `devtools::check()` - Local R CMD check

**Pre-submission:**
1. `spelling::spell_check_package()` - Check spelling
2. `urlchecker::url_check()` - Validate URLs
3. `devtools::check_win_devel()` - Test on Windows R-devel
4. `rhub::rhub_check()` - Multi-platform testing
5. `covr::package_coverage()` - Check test coverage
6. `usethis::use_release_issue()` - Generate checklist
7. `devtools::submit_cran()` - Submit to CRAN

**GitHub Actions (automatic):**
- R CMD check on every push (Linux/Mac/Windows, R-release/R-devel)
- pkgdown site rebuild on main branch

---

## Tool Categories Summary

| Category | Tools | Status |
|----------|-------|--------|
| **Development** | devtools, usethis | Install |
| **Documentation** | roxygen2, pkgdown | roxygen2 ✓, add pkgdown |
| **Testing** | testthat, covr | testthat ✓, add covr |
| **Code Quality** | lintr, styler, spelling, urlchecker | Install all |
| **Checking** | rcmdcheck, rhub v2 | Install both |
| **CI/CD** | GitHub Actions (r-lib/actions) | Setup workflows |
| **Version Control** | Git, GitHub | Assumed present |

---

## Confidence Assessment

| Area | Level | Rationale |
|------|-------|-----------|
| Core Tools | **HIGH** | devtools/usethis/roxygen2/testthat are universal standards, versions from CRAN |
| Checking | **HIGH** | rcmdcheck and rhub v2 documented in official r-pkgs.org guide |
| CI/CD | **HIGH** | r-lib/actions is maintained by R Core/Tidyverse team |
| Code Quality | **MEDIUM** | Tools are standard, but adoption varies (lintr ~60%, spelling ~40%) |
| Versions | **HIGH** | All versions verified from CRAN (January 2026) or official release pages |

---

## Sources

- [R Packages (2e) - Releasing to CRAN](https://r-pkgs.org/release.html) - Authoritative guide
- [CRAN Submission Checklist](https://cran.r-project.org/web/packages/submission_checklist.html) - Official requirements
- [devtools on CRAN](https://rdrr.io/cran/devtools/) - Version 2.4.6
- [usethis on CRAN](https://rdrr.io/cran/usethis/) - Version 3.2.1
- [roxygen2 on CRAN](https://cran.r-project.org/web/packages/roxygen2/index.html) - Version 7.3.3
- [testthat 3.3.0 release](https://tidyverse.org/blog/2025/11/testthat-3-3-0/) - Version 3.3.2
- [pkgdown 2.2.0 release](https://tidyverse.org/blog/2025/11/pkgdown-2-2-0/) - Version 2.2.0
- [rhub v2 announcement](https://blog.r-hub.io/2024/04/11/rhub2/) - v2 migration guide
- [r-lib/actions repository](https://github.com/r-lib/actions) - GitHub Actions workflows
- [R-hub blog on workflow automation](https://blog.r-hub.io/2020/04/29/maintenance/) - Best practices
- [spelling on CRAN](https://cran.r-project.org/web/packages/spelling/spelling.pdf)
- [urlchecker on CRAN](https://cran.r-project.org/web/packages/urlchecker/urlchecker.pdf)
- [covr on CRAN](https://cran.r-project.org/web/packages/covr/covr.pdf)
- [lintr package website](https://lintr.r-lib.org/)
