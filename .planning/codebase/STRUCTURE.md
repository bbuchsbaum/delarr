# Codebase Structure

**Analysis Date:** 2026-01-22

## Directory Layout

```
delarr/
├── R/                      # R source code (core implementation)
│   ├── delarr-core.R       # delarr class, constructors, S3 methods
│   ├── delarr-seed.R       # Storage backend abstraction
│   ├── delarr-backends.R   # Concrete backends (mem, HDF5, custom)
│   ├── delarr-eval.R       # Lazy evaluation & materialization (collect, compile_plan)
│   ├── delarr-verbs.R      # User API verbs (d_map, d_reduce, etc.)
│   ├── delarr-helpers.R    # Helper functions (detrending, masking)
│   ├── delarr-writer-hdf5.R # HDF5 streaming output
│   ├── generics.R          # S3 generic definitions (rowMeans2, colMeans2)
│   └── utils.R             # Utilities (indexing, chunking, scaling)
├── tests/                  # Test suite
│   ├── testthat.R          # Test runner setup
│   └── testthat/
│       └── test-core.R     # Core functionality tests
├── man/                    # Generated roxygen2 documentation
├── vignettes/              # Long-form documentation
│   └── delarr-getting-started.Rmd
├── notes/                  # Development notes and design docs
├── DESCRIPTION             # Package metadata
├── NAMESPACE               # Package exports
├── LICENSE                 # MIT license
└── README.md               # User documentation
```

## Directory Purposes

**R/:**
- Purpose: All R source code implementing the delarr package
- Contains: Class definitions, operations, backends, evaluation logic, user verbs
- Key files: `delarr-core.R` (primary entry point), `delarr-eval.R` (materialization logic)

**tests/testthat/:**
- Purpose: Unit and integration tests
- Contains: Core functionality tests via `testthat` framework
- Key files: `test-core.R` (comprehensive test suite)

**man/:**
- Purpose: Roxygen2-generated documentation files (.Rd format)
- Contains: Auto-generated function reference documentation
- Key files: Correspond 1:1 to exported functions and generics

**vignettes/:**
- Purpose: Long-form user documentation and tutorials
- Contains: Markdown/Rmd files demonstrating package usage
- Key files: `delarr-getting-started.Rmd` (introductory guide)

**notes/:**
- Purpose: Development documentation and design decisions
- Contains: Ad-hoc notes and architectural discussions
- Generated: Not part of package distribution

## Key File Locations

**Entry Points:**
- `R/delarr-core.R`: Main user-facing `delarr()` constructor and S3 class definition
- `R/delarr-eval.R`: `collect()` function for materialization; primary execution entry point

**Configuration:**
- `DESCRIPTION`: Package metadata, dependencies, version
- `NAMESPACE`: Explicit exports and S3 method registrations
- `.Rbuildignore`: Build-time exclusions

**Core Logic:**
- `R/delarr-core.R`: Class definition, slicing, dimension computation, operator overloading
- `R/delarr-eval.R`: Compilation and streaming evaluation (250+ lines)
- `R/delarr-verbs.R`: User-facing transformation pipeline verbs
- `R/delarr-backends.R`: Backend implementation factories

**Testing:**
- `tests/testthat/test-core.R`: Comprehensive test coverage for core functionality
- `tests/testthat.R`: Test discovery and runner

**Documentation:**
- `README.md`: User guide with quick examples
- `vignettes/delarr-getting-started.Rmd`: Tutorial walkthrough

## Naming Conventions

**Files:**
- Hyphenated names: `delarr-core.R`, `delarr-verbs.R` (feature grouping by domain)
- Test files: `test-*.R` (testthat convention)
- No file-per-function; logical grouping by concern

**Directories:**
- Standard R package structure: `R/`, `tests/`, `man/`, `vignettes/`
- Lowercase: `R/`, `tests/` (R packaging convention)

**Functions:**
- User-facing: Lowercase with underscores: `delarr()`, `d_map()`, `collect()`
- User verbs: `d_` prefix for deferred operations: `d_map`, `d_reduce`, `d_center`, `d_scale`, `d_zscore`, `d_detrend`, `d_where`
- Internal: Same naming, no special prefix; rely on non-export in NAMESPACE
- Backend constructors: `delarr_*` prefix: `delarr_mem()`, `delarr_hdf5()`, `delarr_backend()`
- Generics: Exported with suffix `2` to mirror matrixStats: `rowMeans2`, `colMeans2`

**Variables:**
- Camelcase for parameters: `nrow`, `ncol`, `na.rm`
- Lowercase with underscores for internal: `current_rows`, `reduce_op`, `chunk_size`
- Single letters for iterators: `i`, `j`, `x`, `y` (matrix operation context)

**Classes:**
- S3 only; class name in lowercase: `delarr`, `delarr_seed`

## Where to Add New Code

**New Feature (e.g., d_newop):**
- Implementation: Add function to `R/delarr-verbs.R` following the pattern of existing verbs (lines 8-12 show minimal template)
- Operation execution: Add case in `apply_ops()` switch statement in `R/delarr-eval.R` lines 79-109
- Helper function (if needed): Add to `R/delarr-helpers.R`
- Tests: Add test case to `tests/testthat/test-core.R`
- Export: Add to NAMESPACE via roxygen `@export` tag

**New Backend:**
- Factory function: Add to `R/delarr-backends.R` following pattern of `delarr_hdf5()` (lines 51-108)
- Seed creation: Use `delarr_seed()` with custom `pull` function
- Lifecycle management: Add `begin` and `end` hooks if resource initialization needed
- Tests: Add backend construction and materialization tests to `test-core.R`
- Export: Add to NAMESPACE via `@export`

**New Output Writer (like hdf5_writer):**
- Implementation: Create new file `R/delarr-writer-*.R` or add to existing writer file
- Interface: Return list with `write(block, rows, cols, positions)` and `finalize()` methods (see `R/delarr-writer-hdf5.R` lines 62-80)
- Integration: `collect(..., into = your_writer)` automatically routes chunks to writer's `write()` method
- Tests: Test streaming output with `block_apply()` and direct `collect()` calls

**Utility Function:**
- Pure utilities (indexing, chunking): Add to `R/utils.R`
- Matrix operation helpers: Add to `R/delarr-helpers.R`
- Keep utilities focused; avoid interdependencies

**New Generic:**
- Definition: Add to `R/generics.R` with roxygen docstring
- Default method: Implement `*.default` or provide informative error
- Export: Tag with `@export` in roxygen

## Special Directories

**man/:**
- Purpose: Roxygen2-generated documentation
- Generated: Yes, via `roxygen2::roxygenise()`
- Committed: Yes, included in git for CRAN compatibility
- Do not edit directly; modify roxygen comments in `R/` files instead

**vignettes/:**
- Purpose: Long-form tutorials and user guides
- Generated: Partially; .Rmd source files are committed, .html outputs are auto-generated
- Committed: .Rmd source files committed; .html generated at build time
- Build: `knitr::knit()` via `VignetteBuilder: knitr` in DESCRIPTION

**notes/:**
- Purpose: Developer reference and design history
- Generated: No, handwritten
- Committed: Yes
- Not part of package distribution (listed in .Rbuildignore)

**.planning/codebase/:**
- Purpose: GSD mapping and planning documents
- Generated: Manually by GSD orchestration tools
- Committed: Yes
- Not part of CRAN distribution (listed in .Rbuildignore)

---

*Structure analysis: 2026-01-22*
