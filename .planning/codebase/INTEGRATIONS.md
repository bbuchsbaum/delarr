# External Integrations

**Analysis Date:** 2026-01-22

## APIs & External Services

**No external APIs detected.**

The package does not integrate with third-party web APIs, cloud services, or remote data sources. It operates entirely on local data and files.

## Data Storage

**File Storage:**
- **HDF5 Files** - Optional high-performance storage backend
  - SDK/Client: `hdf5r` R package (optional)
  - Access: `delarr_hdf5(path, dataset)` - reads from HDF5 files (`R/delarr-backends.R` lines 51-107)
  - Write: `hdf5_writer(path, dataset, ncol, chunk)` - streams results to HDF5 files (`R/delarr-writer-hdf5.R` lines 19-81)
  - Implementation: Uses `hdf5r::H5File$new()` for file operations, `hdf5r::H5File$create_dataset()` for writing
  - Connection: File paths passed as strings; no connection pooling
  - Lifecycle: Files explicitly opened/closed with `begin()` and `end()` callbacks

- **Local Filesystem** - Default in-memory and filesystem-based storage
  - Backend: `delarr_mem()` for in-memory matrices (`R/delarr-backends.R` lines 35-40)
  - Backend: `delarr_backend()` for custom pull functions (`R/delarr-backends.R` lines 15-27)
  - No database; operates on matrices and arrays

**In-Memory Storage:**
- S3 objects with lazy operation queues
- Chunked streaming: Column-based chunks with configurable sizes
- Default chunk hint for HDF5: `list(cols = chunk_dims[[2]])` per dataset

**Caching:**
- Not applicable - no caching layer present
- Lazy evaluation via operation fusion (`R/delarr-eval.R`)

## Authentication & Identity

**Auth Provider:**
- Not applicable - no authentication mechanism

**File Access Control:**
- HDF5 mode defaults to read-only (`"r"`) for data access
- Write mode (`"a"`) used only for `hdf5_writer()` output
- No user/role-based access control

## Monitoring & Observability

**Error Tracking:**
- Not detected - no error tracking service integration

**Logging:**
- Not detected - no logging framework integration
- Base R error handling via `stop()` for validation
- No debug logging or metrics collection

## CI/CD & Deployment

**Hosting:**
- Not applicable - R package distributed via CRAN or GitHub repository

**CI Pipeline:**
- Not detected - no CI configuration files (no .github/workflows, gitlab-ci.yml, etc.)
- Package supports standard R CMD CHECK workflow

**Build Process:**
- Roxygen2-based documentation generation
- Standard R package build: `R CMD INSTALL .`

## Environment Configuration

**Configuration Source:**
- File paths passed at runtime as function arguments
- No environment variables detected in codebase
- No `.env` or configuration files required

**HDF5 Configuration Example:**
```r
# File path specified directly
lzy <- delarr_hdf5("input.h5", "X")

# Writer configuration
collect(into = hdf5_writer(
  path = "output.h5",
  dataset = "X_zscore",
  ncol = ncol(lzy),
  chunk = c(128L, 4096L)
))
```

## Webhooks & Callbacks

**Incoming:**
- Not applicable - package does not receive webhooks

**Outgoing:**
- Optional lifecycle callbacks available for custom backends:
  - `begin()` - Invoked before streaming begins (`R/delarr-backends.R`)
  - `end()` - Invoked after streaming completes (`R/delarr-backends.R`)
  - Used by `delarr_hdf5()` to manage file handle lifecycle
  - Location: `R/delarr-backends.R` lines 65-81

**Example Callback Implementation:**
```r
# From delarr_hdf5()
begin <- function() {
  if (!is.null(state$file)) return(invisible(NULL))
  state$file <- hdf5r::H5File$new(path, mode = "r")
  state$dset <- state$file[[dataset]]
  invisible(NULL)
}

end <- function() {
  if (!is.null(state$file)) {
    state$file$close_all()
    state$file <- NULL
    state$dset <- NULL
  }
  invisible(NULL)
}
```

## Integration Points with Other Packages

**fmridataset (mentioned in README):**
- `delarr_backend()` mirrors contract of existing DelayedArray seeds
- Purpose: Replacement pathway for S4 seeds with S3 `delarr` objects
- Implementation: Users can create `as_delarr()` conversion methods
- Status: Integration not yet implemented in delarr codebase; documented as future direction

**matrixStats (Optional Performance Integration):**
- Conditionally used for fast row/column statistics
- Fallback to base R if unavailable
- Implementation: `R/utils.R` lines 43-67 with `requireNamespace()` checks

## Data Flow Patterns

**Read Path (HDF5):**
```
delarr_hdf5(path, dataset)
  → H5File$new(path, mode = "r")
  → Extract dims and chunk_dims
  → Create pull() closure that calls dset[rows, cols]
  → Return delarr object
  → Data fetched on demand during collect()
```

**Write Path (HDF5):**
```
hdf5_writer(path, dataset, ncol, chunk)
  → Returns writer object with write() and finalize() methods
  → collect(into = writer)
    → writer$write(block, rows, cols, positions) for each chunk
    → H5File$create_dataset() on first block
    → env$dset[rows, positions] <- block for each chunk
    → writer$finalize() calls file$close_all()
```

**Compute Path (All Backends):**
```
delarr(source)
  → Define seed with pull() function
  → Add operations to DAG (d_map, d_scale, etc.)
  → collect() triggers evaluation:
    → Compile operation plan (compile_plan)
    → Stream data in column chunks (default 4096L cols)
    → Apply fused operations (apply_ops)
    → Write output via writer or return materialized matrix
```

## Deployment Model

**Distribution:**
- R package deployed via:
  - CRAN (when published)
  - GitHub repository (current: development)
  - Local installation: `devtools::install()` or `pkgload::load_all()`

**No External Services Required:**
- Package is self-contained except for optional `hdf5r`
- Works offline after installation
- No remote API calls or cloud dependencies

---

*Integration audit: 2026-01-22*
