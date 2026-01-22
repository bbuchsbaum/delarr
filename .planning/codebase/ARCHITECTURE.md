# Architecture

**Analysis Date:** 2026-01-22

## Pattern Overview

**Overall:** Lazy evaluation pipeline with deferred operation composition and streaming chunk-based materialization.

**Key Characteristics:**
- Operation fusion: Operations are queued as a list and composed without execution until `collect()` is called
- Streaming architecture: Data is pulled in configurable column chunks to avoid materializing full matrices in memory
- Pluggable backends: Abstract storage access via `delarr_seed` interface with concrete implementations for in-memory, HDF5, and custom sources
- S3 object model: Single primary class `delarr` with S3 methods for standard matrix operations and custom verbs

## Layers

**Seed Layer (Storage Backend):**
- Purpose: Abstract data source access and provide configurable streaming hints
- Location: `R/delarr-seed.R`, `R/delarr-backends.R`
- Contains: `delarr_seed` constructor, concrete backends (`delarr_mem`, `delarr_hdf5`, `delarr_backend`)
- Depends on: R base functions, optional `hdf5r` for HDF5 support
- Used by: Core `delarr` class during materialization

**Core DAG Layer (Operation Queue):**
- Purpose: Wrap seeds in delayed matrix containers and manage operation composition
- Location: `R/delarr-core.R`
- Contains: `delarr` constructor, `new_delarr`, slicing (`[.delarr`), dimension computation, arithmetic operators
- Depends on: Seed layer, utility helpers
- Used by: Verb layer and evaluation layer

**Verb Layer (User API):**
- Purpose: Provide tidy-friendly operations that queue transformations without executing
- Location: `R/delarr-verbs.R`
- Contains: `d_map`, `d_map2`, `d_reduce`, `d_center`, `d_scale`, `d_zscore`, `d_detrend`, `d_where`, `rowMeans2.delarr`, `colMeans2.delarr`
- Depends on: Core layer, `rlang::as_function` for formula support
- Used by: End users building pipelines

**Evaluation/Materialization Layer:**
- Purpose: Compile queued operations, handle streaming chunk logic, optimize execution paths
- Location: `R/delarr-eval.R`
- Contains: `collect`, `compile_plan`, `apply_ops`, `block_apply`, chunk helpers, reduction accumulators
- Depends on: All layers above, seed access functions
- Used by: `collect` is the primary user entry point for materialization

**Helper/Utility Layer:**
- Purpose: Provide common matrix operations and infrastructure utilities
- Location: `R/delarr-helpers.R`, `R/utils.R`
- Contains: Statistical operations (`safe_center`, `safe_scale_matrix`, `detrend_matrix`, `where_mask`), indexing (`normalize_index`), chunking (`seq_chunk`)
- Depends on: Base R, optional `matrixStats` for performance
- Used by: Evaluation layer for operation implementation

**Writer Layer (Output Targets):**
- Purpose: Enable streaming results to external storage (HDF5, callbacks)
- Location: `R/delarr-writer-hdf5.R`
- Contains: `hdf5_writer` factory function
- Depends on: `hdf5r` package
- Used by: `collect()` via the `into` parameter

**Generic Layer (Extension Points):**
- Purpose: Define extensible generics for delayed matrix operations
- Location: `R/generics.R`
- Contains: `rowMeans2`, `colMeans2` S3 generics
- Depends on: Base R S3 system
- Used by: Package extensions and method definition

## Data Flow

**Pipeline Construction:**

1. User creates a `delarr` via `delarr(matrix)`, `delarr_mem(matrix)`, `delarr_hdf5(path, dataset)`, or `delarr_backend(...)`
2. User applies verbs (`d_center()`, `d_map()`, `d_reduce()`, etc.) which call `add_op(x, op)`
3. Each verb returns a new `delarr` with the operation appended to the ops list
4. Operations accumulate without execution; the object remains lazy

**Materialization Flow:**

1. User calls `collect(x)` or `as.matrix(x)` on the lazy `delarr`
2. `collect()` calls `compile_plan(x)` which:
   - Merges consecutive slice operations into unified row/column index sets
   - Extracts reduce operations for special handling
   - Tracks any RHS delarr objects in binary operations
3. `collect()` determines execution strategy based on operation types:
   - If operation requires full matrix (row-wise center/scale/zscore), loads entire matrix at once
   - Otherwise, streams column chunks in a loop
4. For each chunk (or full matrix):
   - Pull chunk from seed via `pull_seed(seed, rows, cols)`
   - Apply queued operations via `apply_ops(chunk, ops)`
   - If reduce present, accumulate results
   - If `into` writer supplied, stream chunk to writer; otherwise accumulate in result matrix
5. Return materialized result or write invisibly to `into` target

**Reduction Handling:**

- Reductions are classified as sum/mean/min/max (optimized) or generic
- Generic reductions require full matrix evaluation
- Sum and mean reductions accumulate across chunks with optional NA count tracking
- Min/max maintain partial extrema across chunks
- NA handling via `na.rm` flag affects both accumulator initialization and final results

**Binary Operation Streaming:**

- When RHS is a `delarr`, the evaluation layer determines if RHS chunks can be matched to LHS chunks
- If RHS has identical dimensions and slicing, it streams alongside LHS
- Otherwise, full RHS is materialized once

## Key Abstractions

**delarr (Delayed Array):**
- Purpose: Represents a lazily evaluated matrix with a queued operation pipeline
- Examples: `R/delarr-core.R` lines 12-33
- Pattern: S3 object with `list(seed = delarr_seed, ops = list(...))`
- Contract: Supports matrix subsetting `[`, dimensions `dim()`, `dimnames()`, arithmetic/comparison operators, conversion via `as.matrix()`

**delarr_seed (Storage Backend):**
- Purpose: Encapsulates data access for a materialized source
- Examples: `R/delarr-seed.R` lines 17-40, `R/delarr-backends.R` lines 15-26
- Pattern: List with required fields `nrow`, `ncol`, `pull` function; optional `chunk_hint`, `dimnames`, `begin`, `end` lifecycle hooks
- Contract: `pull(rows = NULL, cols = NULL)` returns a base matrix; dimension fields are static

**Operation (DAG Node):**
- Purpose: Represents a single deferred transformation
- Examples: `R/delarr-core.R` line 63-68 (slice), `R/delarr-verbs.R` lines 8-12 (map)
- Pattern: Named list with `op` field identifying the type and additional fields for parameters
- Contract: Operations are immutable; new delarr objects are created via `append(ops, list(new_op))`

**Plan (Compilation Result):**
- Purpose: Optimized representation of the operation pipeline for execution
- Examples: `R/delarr-eval.R` lines 1-37
- Pattern: List containing `rows`, `cols` (unified index sets), `ops` (remaining operations), `reduce` (reduction info), `rhs_indices` (binary op positions)
- Contract: Produces execution strategy decisions in `collect()`

**Chunk (Materialized Block):**
- Purpose: In-memory matrix slice for processing
- Examples: `R/delarr-eval.R` line 280
- Pattern: Base R matrix, typically a column-contiguous subset
- Contract: All streaming operations work on chunks; chunk size is configurable via `infer_chunk_size()`

## Entry Points

**Package-level exports:**
- `delarr()`: `R/delarr-core.R` line 12 - Main constructor, wraps matrices or seeds
- `collect()`: `R/delarr-eval.R` line 190 - Materializes lazy pipeline
- Verbs (`d_map`, `d_center`, etc.): `R/delarr-verbs.R` - User-facing API

**Backend entry points:**
- `delarr_mem()`: `R/delarr-backends.R` line 35 - In-memory backend
- `delarr_hdf5()`: `R/delarr-backends.R` line 51 - HDF5 backend
- `delarr_backend()`: `R/delarr-backends.R` line 15 - Custom pull function backend
- `hdf5_writer()`: `R/delarr-writer-hdf5.R` line 19 - Output streaming target

**S3 methods:**
- `[.delarr`: `R/delarr-core.R` line 62 - Slicing
- `dim.delarr`: `R/delarr-core.R` line 81 - Dimension query (includes operation effects)
- `print.delarr`: `R/delarr-core.R` line 119 - Display
- `as.matrix.delarr`: `R/delarr-core.R` line 156 - Force materialization
- `Ops.delarr`: `R/delarr-core.R` line 169 - Arithmetic/comparison operators

## Error Handling

**Strategy:** Eager validation during operation queueing and execution; errors throw with `call. = FALSE` to suppress internal stack.

**Patterns:**

- **Construction validation:** `delarr()` checks input type (matrix or seed); seeds validate nrow/ncol/pull function signature
- **Index validation:** `normalize_index()` in `R/utils.R` catches negative indexing errors, bounds errors, NA values, mixed positive/negative indices
- **Operation validation:** Verbs check input class via `stopifnot(inherits(x, "delarr"))` before proceeding
- **Materialization errors:** `collect()` validates chunk conformance, binary op conformability, pull function return types
- **Dimension mismatches:** Binary operations via `broadcast_rhs()` validate conformability early
- **Seed contract violations:** `pull_seed()` enforces that backends return matrices

## Cross-Cutting Concerns

**Logging:** None; debug output via standard R `cat()` calls in `print.delarr()` for operation inspection

**Validation:** Multi-level: construction-time (seeds, inputs), lazy evaluation (type checks on verbs), materialization (operation execution, dimension checks)

**Authentication:** Not applicable; package is in-process R computation

**Memory Management:**
- Relies on R garbage collection for temporary chunk matrices
- HDF5 writer uses `on.exit` cleanup handlers for file handles
- Seeds with lifecycle hooks (`begin`, `end`) manage resource initialization/shutdown

**Dimension Tracking:** `dim.delarr()` simulates operation effects without materialization, tracking slice and reduce dimension changes

**Missing Value Handling:** Controlled via `na.rm` flags on reduction and centering/scaling operations; propagates through chunk processing and accumulation logic

---

*Architecture analysis: 2026-01-22*
