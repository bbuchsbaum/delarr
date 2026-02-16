# Getting Started with delarr

## Overview

`delarr` is a lightweight delayed matrix type for R designed for
datasets that are too large to fit comfortably in memory. Rather than
loading an entire matrix at once, `delarr` keeps data on disk (or in any
external storage) and streams it through your analysis pipeline in
manageable chunks. The package keeps its surface area deliberately
small—one S3 class plus a handful of verbs—while still supporting fused
transformations, streaming reductions, and pluggable backends.
Operations are lazy: they build up a pipeline of work that only executes
when you call
[`collect()`](https://bbuchsbaum.github.io/delarr/reference/collect.md).
This lets you chain multiple transformations without intermediate
allocations. This vignette walks through the essentials: creating
delayed arrays, building lazy pipelines, streaming to and from HDF5
files, and writing custom backends for your own storage formats.

## Installation

`delarr` is under active development. Install the latest development
version by cloning the repository and loading it with `pkgload` or
`devtools`:

``` r
# install.packages("pkgload")
pkgload::load_all("/path/to/delarr")
```

Once installed, load the package:

``` r
library(delarr)
```

## A first lazy pipeline

The core constructor
[`delarr()`](https://bbuchsbaum.github.io/delarr/reference/delarr.md)
wraps an existing matrix (or a seed backend). All transformation
verbs—[`d_map()`](https://bbuchsbaum.github.io/delarr/reference/d_map.md),
[`d_center()`](https://bbuchsbaum.github.io/delarr/reference/d_center.md),
[`d_scale()`](https://bbuchsbaum.github.io/delarr/reference/d_scale.md),
etc.—return a new `delarr` without actually computing anything. The
operations are recorded and fused together, then executed in a single
pass when you call
[`collect()`](https://bbuchsbaum.github.io/delarr/reference/collect.md).

This lazy evaluation has two major benefits:

1.  **Memory efficiency**: Intermediate results are never materialised.
    A chain of ten transformations uses the same memory as one.
2.  **Streaming**: When backed by on-disk storage, data flows through
    the pipeline in chunks, so you can process datasets larger than RAM.

``` r
set.seed(1)
mat <- matrix(rnorm(20), 5, 4)
arr <- delarr(mat)

result <- arr |>
  d_center(dim = "rows", na.rm = FALSE) |>
  d_map(~ .x * 0.5) |>
  d_reduce(mean, dim = "rows") |>
  collect()

result
#> [1] -1.561251e-17  6.938894e-18  0.000000e+00  1.387779e-17  0.000000e+00
```

The pipeline above:

1.  **`d_center(dim = "rows")`** — subtracts each row’s mean (operates
    across columns within each row)
2.  **`d_map(~ .x * 0.5)`** — multiplies every element by 0.5
    (elementwise, no row/column orientation)
3.  **`d_reduce(mean, dim = "rows")`** — computes the mean of each row,
    collapsing to a single column

## Broadcasting and binary operations

Binary operations
([`d_map2()`](https://bbuchsbaum.github.io/delarr/reference/d_map2.md)
or arithmetic operators) remain lazy and support broadcasting of scalars
and row/column vectors. When you add a vector to a matrix, `delarr`
automatically determines whether it should broadcast across rows or
columns based on the vector length. This makes common operations like
subtracting row means or scaling by column standard deviations simple
and memory-efficient—the bias/scale vectors are applied on-the-fly as
each chunk streams through.

``` r
row_bias <- rnorm(nrow(mat))

delarr(mat) |>
  (`+`)(row_bias) |>
  (`*`)(1.5) |>
  d_reduce(mean, dim = "cols") |>
  collect(chunk_size = 2)
#> [1] 0.3157507 0.3245493 0.1790303 0.8111963
```

## Streaming to HDF5

HDF5 is a popular format for storing large numerical arrays on disk. The
[`delarr_hdf5()`](https://bbuchsbaum.github.io/delarr/reference/delarr_hdf5.md)
function creates a delayed array backed by an HDF5 dataset, opening the
file only when
[`collect()`](https://bbuchsbaum.github.io/delarr/reference/collect.md)
executes and closing it immediately after.

To write results back to disk without loading the full matrix into
memory, pair your pipeline with
[`hdf5_writer()`](https://bbuchsbaum.github.io/delarr/reference/hdf5_writer.md).
This creates a streaming sink that receives data chunk-by-chunk. The
`compression` parameter controls gzip compression level (0-9, with 4 as
a good default balance of speed and size).

``` r
# Create test data in an HDF5 file
tf_in <- tempfile(fileext = ".h5")
input <- matrix(runif(30), 5, 6)
write_hdf5(input, tf_in, "X")

# Load lazily, transform, and stream to a new file
X <- delarr_hdf5(tf_in, "X")
centred <- X |> d_center("cols")

tf_out <- tempfile(fileext = ".h5")
writer <- hdf5_writer(tf_out, "X_centered", ncol = ncol(centred))
collect(centred, into = writer)

# Verify the result
read_hdf5(tf_out, "X_centered")
#>             [,1]         [,2]        [,3]        [,4]         [,5]
#> [1,]  0.05913912 -0.301175319  0.38311389 -0.23447141 -0.127991653
#> [2,]  0.44272897 -0.084369772 -0.23615866 -0.01394294  0.372375760
#> [3,]  0.01961660  0.117992784 -0.07069631  0.27382248 -0.120381101
#> [4,] -0.17368323  0.261363598 -0.19736736 -0.40824127 -0.133289660
#> [5,] -0.34780146  0.006188708  0.12110843  0.38283314  0.009286654
#>               [,6]
#> [1,]  0.1153051266
#> [2,]  0.0874462614
#> [3,] -0.3869036657
#> [4,]  0.0004274896
#> [5,]  0.1837247880

# Clean up
unlink(c(tf_in, tf_out))
```

## Custom backends with `delarr_seed()`

The real power of `delarr` comes from its pluggable backend system. Any
storage layer—databases, cloud storage, memory-mapped files, or custom
binary formats—can become a `delarr` backend by implementing a simple
contract:

1.  **Dimensions**: Know the total number of rows and columns

2.  **Pull function**: Given row and column indices, return the
    corresponding submatrix

The
[`delarr_seed()`](https://bbuchsbaum.github.io/delarr/reference/delarr_seed.md)
function wraps these two pieces into a seed object that
[`delarr()`](https://bbuchsbaum.github.io/delarr/reference/delarr.md)
understands. The pull function is called lazily during
[`collect()`](https://bbuchsbaum.github.io/delarr/reference/collect.md),
receiving only the indices needed for the current chunk.

``` r
random_backend <- list(
  pull = function(rows = NULL, cols = NULL) {
    rows <- if (is.null(rows)) seq_len(100) else rows
    cols <- if (is.null(cols)) seq_len(50) else cols
    matrix(rnorm(length(rows) * length(cols)), length(rows), length(cols))
  }
)

seed <- delarr_seed(
  nrow = 100,
  ncol = 50,
  pull = function(rows, cols) random_backend$pull(rows, cols)
)

rand_arr <- delarr(seed)
rand_arr |>
  d_map(~ .x^2) |>
  d_reduce(mean, dim = "cols") |>
  collect()
#>  [1] 0.7233715 1.1830402 0.9029285 1.0262304 1.3119029 1.0274333 1.2002124
#>  [8] 1.2593355 1.0879169 1.0533032 1.0211869 1.0731659 1.0520222 0.9345241
#> [15] 1.0942047 1.0145886 1.3023160 1.0513637 1.2534753 1.1267768 1.1987305
#> [22] 1.0589216 0.9288278 0.9502513 1.1073139 0.7505165 1.0521073 1.1741118
#> [29] 1.0906651 1.3899871 1.0702889 1.1941535 1.1789617 0.9774898 1.0885509
#> [36] 0.9973437 1.2122340 1.1195303 0.9233984 0.8966970 0.8562727 1.0275834
#> [43] 1.2038752 0.7201439 1.1070323 0.8734731 0.9997249 1.0055618 1.0938935
#> [50] 0.8017712
```

## Available verbs

`delarr` provides a focused set of transformation verbs: -
**[`d_map()`](https://bbuchsbaum.github.io/delarr/reference/d_map.md)**
/
**[`d_map2()`](https://bbuchsbaum.github.io/delarr/reference/d_map2.md)**:
Elementwise transformations using formula or function syntax -
**[`d_center()`](https://bbuchsbaum.github.io/delarr/reference/d_center.md)**
/
**[`d_scale()`](https://bbuchsbaum.github.io/delarr/reference/d_scale.md)**
/
**[`d_zscore()`](https://bbuchsbaum.github.io/delarr/reference/d_zscore.md)**:
Common preprocessing operations with row or column orientation -
**[`d_detrend()`](https://bbuchsbaum.github.io/delarr/reference/d_detrend.md)**:
Remove linear trends from rows or columns -
**[`d_where()`](https://bbuchsbaum.github.io/delarr/reference/d_where.md)**:
Masked updates—apply changes only where a condition holds -
**[`d_reduce()`](https://bbuchsbaum.github.io/delarr/reference/d_reduce.md)**:
Row-wise or column-wise reductions (sum, mean, min, max, or custom
functions)

All verbs return a new `delarr`, so you can chain them freely. The
operations fuse together and execute in a single streaming pass.

## How delarr stays efficient

`delarr` uses several strategies to minimise memory usage and maximise
throughput: **Operation fusion.** When you chain multiple
transformations, `delarr` doesn’t create intermediate matrices. A
pipeline like `d_center() |> d_map(~ .x * 2) |> d_scale()` records three
operations but executes them in a single pass. Each element is centred,
scaled, and transformed together before moving to the next chunk.

**Chunked streaming.** Data flows through the pipeline in column chunks
(you control the size via `chunk_size` in
[`collect()`](https://bbuchsbaum.github.io/delarr/reference/collect.md)).
Only one chunk lives in memory at a time, so a 10 GB matrix can be
processed with just a few hundred megabytes of RAM.

**Lazy file access.** On-disk backends like
[`delarr_hdf5()`](https://bbuchsbaum.github.io/delarr/reference/delarr_hdf5.md)
and
[`delarr_mmap()`](https://bbuchsbaum.github.io/delarr/reference/delarr_mmap.md)
open files only during
[`collect()`](https://bbuchsbaum.github.io/delarr/reference/collect.md)
and read only the columns needed for the current chunk. The file handle
is closed immediately after, avoiding resource leaks.

**Minimal dispatch overhead.** `delarr` uses simple S3 classes and
closures rather than S4 machinery. The core execution loop is a tight
`for` over column chunks with direct function calls—no method lookup per
element.

**Streaming reductions.** Operations like `d_reduce(mean, dim = "cols")`
compute running accumulators as chunks flow through, never materialising
the full matrix. The final result requires only O(ncol) or O(nrow)
memory, not O(nrow × ncol).

## When to use delarr

`delarr` is a good fit when:

- Your data is larger than available RAM
- You need to apply the same pipeline to many datasets
- You want streaming I/O without writing explicit chunking loops
- You prefer a simple, pipe-friendly API over S4 complexity

For sparse matrices, GPU acceleration, or parallel distributed
computing, consider specialised packages. `delarr` focuses on doing one
thing well: efficient lazy operations on dense, out-of-core matrices.
