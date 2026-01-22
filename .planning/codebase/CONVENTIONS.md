# Coding Conventions

**Analysis Date:** 2026-01-22

## Naming Patterns

**Files:**
- Descriptive names with hyphens: `delarr-core.R`, `delarr-verbs.R`, `delarr-helpers.R`
- Functionality grouped by purpose within files
- Example: `delarr-core.R` contains core constructors and S3 methods

**Functions:**
- snake_case for all user-facing and internal functions
- Examples: `delarr()`, `d_map()`, `d_reduce()`, `delarr_seed()`, `normalize_index()`
- Public functions prefixed with `d_` for delayed operations: `d_map`, `d_map2`, `d_reduce`, `d_center`, `d_scale`, `d_zscore`, `d_detrend`, `d_where`
- Backend constructors prefixed with `delarr_`: `delarr_seed()`, `delarr_backend()`, `delarr_mem()`, `delarr_hdf5()`, `delarr_mmap()`
- Helper functions without prefix when internal: `normalize_index()`, `seq_chunk()`, `safe_mean()`
- S3 methods follow pattern `function.class`: `dim.delarr()`, `print.delarr()`, `[.delarr()`, `as.matrix.delarr()`, `Ops.delarr()`

**Variables:**
- snake_case throughout: `current_rows`, `rhs_indices`, `chunk_size`, `na.rm`
- Matrix variables use `mat` or `x`, vectors use single letter or descriptive name
- Environment variables use camelCase with clear purpose: `tracker$pulls`
- Single letter in anonymous functions acceptable: `.x`, `.y` (tidyverse convention)

**Types/Classes:**
- S3 classes use lowercase with hyphens if multi-word: `delarr`, `delarr_seed`
- Exported classes documented with roxygen
- Class registration via `structure(..., class = "className")`

## Code Style

**Formatting:**
- Roxygen2 with markdown enabled for documentation
- Standard R indentation (2 spaces observed in functions)
- Function definitions on single line when possible
- Long parameter lists wrap to multiple lines with one parameter per line

**Linting:**
- No explicit linting configuration detected
- Code follows R standard style conventions
- Documentation enforced through roxygen2 (`RoxygenNote: 7.3.2.9000`)

## Documentation

**Roxygen Documentation:**
- All exported functions have `#'` documentation blocks
- Pattern includes `@param`, `@return`, `@export` tags
- `@keywords internal` marks internal functions
- Examples shown in documentation where helpful
- Markdown enabled in DESCRIPTION: `Roxygen: list(markdown = TRUE)`

**Comments:**
- Minimal inline comments; code is self-documenting
- Roxygen blocks provide comprehensive parameter and return documentation
- Internal helper functions documented with roxygen even if not exported

## Error Handling

**Pattern - stopifnot():**
Used for precondition checks in user-facing functions:
```R
stopifnot(inherits(x, "delarr"))
```
Example locations: `delarr-core.R` line 184, `delarr-verbs.R` line 9

**Pattern - stop() with call.=FALSE:**
Used for validation errors with custom messages:
```R
stop("Unsupported input for delarr()", call. = FALSE)
```
Provides clean error messages without function call context.
Examples: `delarr-core.R` line 32, `delarr-seed.R` line 20, `utils.R` line 11

**Pattern - requireNamespace():**
Optional dependencies checked gracefully:
```R
if (!requireNamespace("matrixStats", quietly = TRUE)) {
  # fallback implementation
}
```
Examples: `utils.R` lines 44-54 (matrixStats), `delarr-backends.R` lines 52-54 (hdf5r)

**Pattern - tryCatch():**
Used for graceful error recovery in file operations:
```R
chunk_dims <- tryCatch(dset$chunk_dims, error = function(e) NULL)
```
Example: `delarr-backends.R` line 59

## Special Operators

**NULL coalescing operator `%||%`:**
Defined in `utils.R` as standard idiom for NULL replacement:
```R
`%||%` <- function(x, y) {
  if (is.null(x)) y else x
}
```
Used throughout for safe NULL handling: `delarr-core.R` line 24, test file line 47

**Pipe operator `|>`:**
Native R pipe used in newer code patterns:
```R
delarr(mat) |> d_center("rows") |> d_reduce(mean, "rows")
```
Seen in tests at `test-core.R` line 164

## Function Design

**Size:** Functions range 5-50 lines, with larger functions decomposed into helpers

**Parameters:**
- Required parameters first
- Optional parameters with sensible defaults
- Formula support via `rlang::as_function()` for `d_map()`, `d_map2()`, `d_where()`
- Dimension selection via `match.arg()` for consistency

**Return Values:**
- Functions return modified objects or new instances
- S3 methods return appropriately typed results
- Constructors return objects with explicit class assignment via `structure()`
- Operations deferred to maintain lazy evaluation

## Module Design

**Exports:**
- Controlled via NAMESPACE file
- All user-facing functions explicitly exported
- S3 method registrations via `S3method()` directives
- 26 items exported total

**File Organization:**
- `delarr-core.R`: Constructor `delarr()`, `new_delarr()`, S3 methods for core operations
- `delarr-verbs.R`: User-facing lazy operations (`d_map`, `d_reduce`, `d_center`, etc.)
- `delarr-seed.R`: Seed abstraction and `delarr_seed()` constructor
- `delarr-backends.R`: Backend constructors (`delarr_mem`, `delarr_hdf5`, etc.)
- `delarr-eval.R`: Execution engine and compilation logic
- `utils.R`: Shared utilities for indexing, mean/SD calculation, matrix operations
- `delarr-helpers.R`: Specialized transformations (detrending, masking)
- `generics.R`: Generic function definitions
- `delarr-writer-hdf5.R`: HDF5 output functionality

## Control Flow Patterns

**Slice normalization:**
`normalize_index()` in `utils.R` handles logical, positive, and negative indices uniformly

**Operation queuing:**
All deferred operations appended to `x$ops` list via `add_op()` helper
Enables operation fusion and lazy evaluation

**Lazy evaluation:**
Operations stored as named lists with operation type (`op$op`) and parameters
Applied only during materialization via `apply_ops()`

---

*Convention analysis: 2026-01-22*
