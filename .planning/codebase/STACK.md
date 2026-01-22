# Technology Stack

**Analysis Date:** 2026-01-22

## Languages

**Primary:**
- R (>= 4.1) - Core language for entire package

**Build & Documentation:**
- R Markdown - Vignettes and documentation (VignetteBuilder: knitr)
- Roxygen2 - Documentation generation (version 7.3.2.9000)

## Runtime

**Environment:**
- R 4.1+ (specified in DESCRIPTION)
- Platform agnostic (no platform-specific dependencies detected)

**Package Manager:**
- Base R package system (DESCRIPTION/NAMESPACE)
- Lockfile: Not detected (no renv.lock or equivalent)

## Frameworks

**Core Framework:**
- Base R S3 object system - Used for class definition (`delarr` S3 class with 6 methods)

**Testing:**
- testthat (>= 3.1.0) - Test framework
- Config: `Config/testthat/edition: 3` in DESCRIPTION

**Build/Dev:**
- knitr - Vignette rendering for documentation
- roxygen2 (7.3.2.9000) - Documentation generation from inline comments
- rmarkdown - Markdown rendering for vignettes

## Key Dependencies

**Critical (Imports):**
- `rlang` - Used in `d_map()` and `d_map2()` for formula-to-function conversion via `rlang::as_function()`
  - Location: `R/delarr-verbs.R` lines 10, 24

**Optional (Suggests):**
- `matrixStats` - Performance optimization for mean/sd calculations
  - Used: `R/utils.R` lines 44-48, 56-61 for `rowMeans2()`, `colMeans2()`, `rowSds()`, `colSds()`
  - Conditionally loaded with `requireNamespace()` - falls back to base R if unavailable
  - Exported: `rowMeans2()` and `colMeans2()` methods available in `NAMESPACE`

- `hdf5r` - HDF5 file I/O support
  - Used: `R/delarr-backends.R` for `delarr_hdf5()` function (lines 52-107)
  - Used: `R/delarr-writer-hdf5.R` for `hdf5_writer()` function (lines 19-81)
  - Conditionally required with runtime check: `requireNamespace("hdf5r", quietly = TRUE)`
  - Provides `H5File` class for reading/writing HDF5 datasets

## Configuration

**Package Metadata:**
- `DESCRIPTION` - Standard R package metadata
- `NAMESPACE` - Exports definition (15 functions exported, 6 S3 methods)
- `R/` directory - 9 R source files containing implementation

**Build Configuration:**
- Roxygen: `list(markdown = TRUE)` - Enables markdown in roxygen comments
- Encoding: UTF-8
- LazyData: false - Data not lazy-loaded

**No External Configuration Files Detected:**
- No `.Rprofile`, `.Renviron`, or configuration secrets expected
- No build system configuration (Make, CMake, etc.)

## Platform Requirements

**Development:**
- R >= 4.1 installed
- Optional: HDF5 system libraries for `hdf5r` (C-level HDF5 bindings)
- Optional: matrixStats package for performance
- Test execution: `pkgload::load_all()` and `testthat::test_dir()`

**Production/Usage:**
- R >= 4.1 runtime
- rlang package installed
- Optional: hdf5r if using `delarr_hdf5()` or `hdf5_writer()`
- Optional: matrixStats if available for performance optimization

**Development Tools:**
- pkgload - For loading package during development (`pkgload::load_all(".")`)
- devtools - Mentioned in README for installation

## Dependency Tree

```
delarr (root)
├── Required:
│   └── rlang
├── Optional (Suggests):
│   ├── matrixStats (fallback available)
│   ├── hdf5r (gracefully skipped if missing)
│   ├── testthat (testing only)
│   ├── knitr (vignettes only)
│   ├── rmarkdown (vignettes only)
│   └── roxygen2 (documentation generation only)
└── Base R Standard Library:
    ├── stats (poly, lm.fit functions)
    └── Base functions (sweep, apply, etc.)
```

---

*Stack analysis: 2026-01-22*
