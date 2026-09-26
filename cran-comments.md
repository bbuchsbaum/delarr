## Submission

This is an update of delarr from 0.1.0 to 0.2.0.

The release adds reconstructible provider seeds (`delarr_provider_seed()`,
`delarr_provider_pull()`) so storage packages can keep plain, serializable
descriptors in lazy plans, plus a `Math` group generic. It also fixes unary
`-`/`+` on delayed arrays and tightens provider validation: descriptors
carrying functions, environments, or external pointers (including inside
attributes or language objects) are rejected, and fractional, negative,
non-finite, or out-of-range dimensions now error instead of being silently
truncated.

delarr has a single hard dependency (rlang). HDF5 (hdf5r), memory-mapped
(mmap), and shared-memory parallel (shard) backends remain optional via
Suggests and are used only behind `requireNamespace()` guards.

## R CMD check results

Local `R CMD check --as-cran` (macOS Sonoma 14.3, R 4.5.1), tarball SHA-256
`801722ba292acedb9e181c40549fbe6997d1eca2911f5e93186840ca1fabd569`:
**0 errors | 0 warnings | 1 note**

* checking HTML version of manual ... NOTE
  Skipping checking HTML validation: 'tidy' doesn't look like recent enough
  HTML Tidy.

  This note reflects the local toolchain (outdated HTML Tidy), not the package.

CRAN incoming feasibility (remote checks enabled): OK.

The same check with hdf5r, mmap, matrixStats, and shard made unavailable
(`_R_CHECK_FORCE_SUGGESTS_=false`) gives the same result; the 29 affected tests
skip cleanly.

## Test environments

* local: macOS Sonoma 14.3, R 4.5.1 — 0 errors, 0 warnings, 1 note (local HTML Tidy)
* win-builder (release, devel): not yet run for 0.2.0
* GitHub Actions (Ubuntu, macOS, Windows; release and devel): not yet run for 0.2.0

## Reverse dependencies

There are no reverse dependencies on CRAN. The downstream package fmridataset
(not on CRAN) was tested against this candidate: 931 assertions across its
provider, ArraySource, and serialization tests, with no failures.
