## Submission

This is a new submission of delarr (version 0.1.0).

delarr provides a lightweight delayed-array abstraction with lazy, fused
execution and a tidy-friendly API. It has a single hard dependency (rlang).
All heavier functionality is optional and used conditionally via Suggests:
HDF5-backed arrays (hdf5r), memory-mapped arrays (mmap), and zero-copy
parallel execution (shard). The package degrades gracefully and passes
R CMD check when these Suggests are not installed.

## R CMD check results

Local `R CMD check --as-cran` (macOS Sonoma, R 4.5.1): **0 errors | 0 warnings | 1 note**

* checking CRAN incoming feasibility ... NOTE
  New submission

win-builder (Windows, R 4.6.1 ucrt, 2026-06-24): **0 errors | 0 warnings | 1 note**

* checking CRAN incoming feasibility ... NOTE
  New submission
  Possibly misspelled words in DESCRIPTION: Bioconductor's, HDF, backends
  (false positives; HDF is in `inst/WORDLIST`)

## Test environments

* local: macOS Sonoma, R 4.5.1 — 0 errors, 0 warnings, 1 note (New submission)
* win-builder: Windows Server 2022, R 4.6.1 (release) — 0 errors, 0 warnings, 1 note; binary built (`delarr_0.1.0.zip`)
* win-builder: R-devel — submitted 2026-06-24; second email may still be pending
* GitHub Actions (R-CMD-check): Ubuntu, macOS, Windows — R release and devel (pending CI run after push)
* rhub: workflow file created locally (`.github/workflows/rhub.yaml`); push to GitHub then run `rhub::rhub_check()`

## Reverse dependencies

There are no reverse dependencies on CRAN yet.

## Notes for CRAN

* Suggested-package examples (`hdf5r`, `mmap`, `shard`) are wrapped in
  `requireNamespace()` guards so examples pass when Suggests are absent.
* `cran-comments.md` is listed in `.Rbuildignore` and is not part of the
  source tarball.
