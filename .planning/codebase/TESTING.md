# Testing Patterns

**Analysis Date:** 2026-01-22

## Test Framework

**Runner:**
- testthat 3.1.0+
- Config file: `tests/testthat.R`
- Configuration in DESCRIPTION: `Config/testthat/edition: 3`

**Assertion Library:**
- testthat's built-in expectations: `expect_*()` family

**Run Commands:**
```bash
devtools::test()              # Run all tests
devtools::test(filter = "core")  # Run specific test file
devtools::test_coverage()     # Generate coverage report
```

## Test File Organization

**Location:**
- Tests co-located in dedicated directory: `tests/testthat/`
- Test runner: `tests/testthat.R` invokes `test_check("delarr")`

**Naming:**
- Test files prefixed with `test-`: `test-core.R`
- All tests currently in single file `test-core.R`

**Structure:**
```
delarr/
├── tests/
│   ├── testthat.R                 # Test runner
│   └── testthat/
│       └── test-core.R            # All tests (256 lines)
```

## Test Structure

**Suite Organization:**
```R
test_that("delarr constructs from matrix", {
  mat <- matrix(1:9, 3, 3)
  x <- delarr(mat)
  expect_s3_class(x, "delarr")
  expect_equal(dim(x), c(3, 3))
  expect_equal(collect(x), mat)
})
```

**Patterns:**
- Each `test_that()` block focuses on single behavior
- Setup within test block (no shared fixtures)
- Immediate assertions after operations
- Descriptive test names explain what is tested

## Test Types

**Unit Tests:**
- Core functionality: construction, slicing, arithmetic operations
- Examples at `test-core.R` lines 1-34 (constructor, slicing, fusion)
- Lazy evaluation verification: `test_that("delarr constructs from matrix")`

**Integration Tests:**
- Cross-component interactions: binary ops between delarr instances
- Chunked evaluation: `test_that("collect streams in column chunks")`
- State machine testing: operations sequence correctly through pipeline
- Example: `test_that("emap2 with delarr RHS pulls matching chunks")` lines 36-57

**End-to-End Tests:**
- HDF5 backend with streaming: `test_that("HDF5 writer streams results to disk")` lines 236-255
- Requires conditional skipping: `skip_if_not_installed("hdf5r")`
- Tests full pipeline: file read → transformation → file write

## Test Coverage

**Current Test Counts:**
- Constructor patterns: 1 test
- Slicing and dimension handling: 5 tests
- Operations (map, reduce, center, scale, zscore, detrend): 12 tests
- Binary operations and broadcasting: 6 tests
- Reductions with NA handling: 4 tests
- Masking (d_where): 2 tests
- Block iteration: 1 test
- Printing: 2 tests
- HDF5 integration: 1 test
- Total: ~34 unique test cases

**High-coverage areas:**
- Lazy operation queueing
- Dimension computation
- NA handling across operations
- Broadcasting in binary operations
- Chunked evaluation

**Test coverage gaps:**
- Negative/logical indexing in slicing (mentioned but not comprehensive)
- Memory backend (`delarr_mem`) limited coverage
- Error conditions not extensively tested
- Edge cases (empty matrices, single row/column)
- Dimension names propagation

## Common Patterns

**Assertion Pattern - Equality:**
```R
test_that("slicing is deferred and collected correctly", {
  mat <- matrix(1:16, 4, 4)
  x <- delarr(mat)
  y <- x[2:3, 1:2]
  expect_equal(dim(y), c(2, 2))
  expect_equal(collect(y), mat[2:3, 1:2])
})
```
Uses `expect_equal()` for numeric comparisons against base R equivalents.

**Assertion Pattern - Class/Type:**
```R
test_that("delarr constructs from matrix", {
  mat <- matrix(1:9, 3, 3)
  x <- delarr(mat)
  expect_s3_class(x, "delarr")
})
```
Verifies S3 class with `expect_s3_class()`.

**Assertion Pattern - Type Checking:**
```R
test_that("comparisons return logical matrices", {
  set.seed(3)
  mat <- matrix(rnorm(15), 5, 3)
  x <- delarr(mat)
  out <- collect(x > 0)
  expect_type(out, "logical")
  expect_identical(out, mat > 0)
})
```
Uses `expect_type()` for atomic types, `expect_identical()` for exact equality.

**Assertion Pattern - Near-Equality:**
```R
test_that("center and scale operate along requested dimension", {
  set.seed(1)
  mat <- matrix(rnorm(20), 5, 4)
  x <- delarr(mat)
  centered <- collect(d_center(x, dim = "rows"))
  expect_true(all(abs(rowMeans(centered)) < 1e-8))
})
```
Uses tolerance-based comparison for floating-point operations.

**Setup Pattern - Seed Tracking:**
```R
test_that("emap2 with delarr RHS pulls matching chunks", {
  set.seed(11)
  lhs <- matrix(rnorm(20), 4, 5)
  rhs <- matrix(rnorm(20), 4, 5)
  tracker <- new.env(parent = emptyenv())
  tracker$pulls <- 0L
  seed_rhs <- delarr_seed(
    nrow = nrow(rhs),
    ncol = ncol(rhs),
    pull = function(rows = NULL, cols = NULL) {
      tracker$pulls <- tracker$pulls + 1L
      rows <- rows %||% seq_len(nrow(rhs))
      cols <- cols %||% seq_len(ncol(rhs))
      rhs[rows, cols, drop = FALSE]
    }
  )
  x <- delarr(lhs)
  y <- delarr(seed_rhs)
  chunk <- 2L
  expect_equal(collect(x + y, chunk_size = chunk), lhs + rhs)
  expect_equal(tracker$pulls, ceiling(ncol(lhs) / chunk))
})
```
Uses environment-based tracking to verify pull behavior without modifying code.

**Teardown Pattern - Temporary Files:**
```R
test_that("HDF5 writer streams results to disk", {
  skip_if_not_installed("hdf5r")
  path <- tempfile(fileext = ".h5")
  on.exit(unlink(path), add = TRUE)
  # ... test code ...
  file$close_all()
})
```
Uses `on.exit()` for resource cleanup regardless of test pass/fail.

**Async Testing:**
Not applicable - all operations are synchronous.

**String Matching Pattern:**
```R
test_that("print summarises pipeline", {
  mat <- matrix(1:6, 3, 2)
  x <- delarr(mat) |> d_center("rows") |> d_reduce(mean, "rows")
  out <- paste(capture.output(print(x)), collapse = "\n")
  expect_match(out, "<delarr> 1 x 2", fixed = TRUE)
  expect_match(out, "center(rows)", fixed = TRUE)
})
```
Uses `capture.output()` and `expect_match()` for text output testing.

**Seed Removal Pattern:**
All tests set seed before random operations:
```R
set.seed(5)
mat <- matrix(rnorm(25), 5, 5)
```
Ensures reproducibility across test runs.

## Missing Test Coverage Areas

**Critical gaps:**
1. Error handling in `normalize_index()` - negative/logical indices tested implicitly but not explicitly
2. Boundary conditions - empty matrices, single-row/column cases
3. Large matrix operations - current tests use small matrices
4. Memory backend comprehensive coverage
5. Operation combinations not fully tested (e.g., multiple consecutive reductions)
6. Dimension name propagation through operations
7. Formula parsing edge cases in `d_map`, `d_map2`, `d_where`

---

*Testing analysis: 2026-01-22*
