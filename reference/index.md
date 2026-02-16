# Package index

## Core

- [`delarr()`](https://bbuchsbaum.github.io/delarr/reference/delarr.md)
  : Create a delayed matrix

- [`delarr_seed()`](https://bbuchsbaum.github.io/delarr/reference/delarr_seed.md)
  :

  Construct a seed backend for `delarr`

- [`collect()`](https://bbuchsbaum.github.io/delarr/reference/collect.md)
  : Materialise a delayed matrix

- [`block_apply()`](https://bbuchsbaum.github.io/delarr/reference/block_apply.md)
  : Apply a function to streamed matrix blocks

## Backends

- [`delarr_mem()`](https://bbuchsbaum.github.io/delarr/reference/delarr_mem.md)
  : Create a delayed matrix from an in-memory matrix
- [`delarr_hdf5()`](https://bbuchsbaum.github.io/delarr/reference/delarr_hdf5.md)
  : Create a delayed matrix sourced from an HDF5 dataset
- [`delarr_mmap()`](https://bbuchsbaum.github.io/delarr/reference/delarr_mmap.md)
  : Create a delayed matrix from a memory-mapped file
- [`delarr_backend()`](https://bbuchsbaum.github.io/delarr/reference/delarr_backend.md)
  : Wrap a custom backend as a delayed matrix

## Verbs

- [`d_map()`](https://bbuchsbaum.github.io/delarr/reference/d_map.md) :
  Apply an elementwise transformation lazily
- [`d_map2()`](https://bbuchsbaum.github.io/delarr/reference/d_map2.md)
  : Apply a binary elementwise transformation lazily
- [`d_reduce()`](https://bbuchsbaum.github.io/delarr/reference/d_reduce.md)
  : Reduce along rows or columns lazily
- [`d_reduce_many()`](https://bbuchsbaum.github.io/delarr/reference/d_reduce_many.md)
  : Run multiple reductions and collect results
- [`d_center()`](https://bbuchsbaum.github.io/delarr/reference/d_center.md)
  : Center a delayed matrix along rows or columns
- [`d_scale()`](https://bbuchsbaum.github.io/delarr/reference/d_scale.md)
  : Scale a delayed matrix along rows or columns
- [`d_zscore()`](https://bbuchsbaum.github.io/delarr/reference/d_zscore.md)
  : Z-score a delayed matrix
- [`d_detrend()`](https://bbuchsbaum.github.io/delarr/reference/d_detrend.md)
  : Detrend a delayed matrix
- [`d_where()`](https://bbuchsbaum.github.io/delarr/reference/d_where.md)
  : Apply a boolean mask to a delayed matrix
- [`d_transpose()`](https://bbuchsbaum.github.io/delarr/reference/d_transpose.md)
  : Transpose a delayed matrix
- [`d_matmul()`](https://bbuchsbaum.github.io/delarr/reference/d_matmul.md)
  : Delayed matrix multiplication

## Summaries

- [`rowMeans2()`](https://bbuchsbaum.github.io/delarr/reference/rowMeans2.md)
  : Row means for delayed matrices
- [`colMeans2()`](https://bbuchsbaum.github.io/delarr/reference/colMeans2.md)
  : Column means for delayed matrices

## HDF5 I/O

- [`write_hdf5()`](https://bbuchsbaum.github.io/delarr/reference/write_hdf5.md)
  : Write a matrix to an HDF5 file

- [`read_hdf5()`](https://bbuchsbaum.github.io/delarr/reference/read_hdf5.md)
  : Read a matrix from an HDF5 file

- [`hdf5_writer()`](https://bbuchsbaum.github.io/delarr/reference/hdf5_writer.md)
  :

  HDF5 writer for streaming
  [`collect()`](https://bbuchsbaum.github.io/delarr/reference/collect.md)

## Utilities

- [`optimize_delarr()`](https://bbuchsbaum.github.io/delarr/reference/optimize_delarr.md)
  : Optimize a delayed pipeline

- [`explain()`](https://bbuchsbaum.github.io/delarr/reference/explain.md)
  : Explain a delayed execution plan

- [`profile_collect()`](https://bbuchsbaum.github.io/delarr/reference/profile_collect.md)
  :

  Profile
  [`collect()`](https://bbuchsbaum.github.io/delarr/reference/collect.md)
  runtime

## S3 Methods

- [`Ops(`*`<delarr>`*`)`](https://bbuchsbaum.github.io/delarr/reference/Ops.delarr.md)
  :

  Arithmetic and comparison operators for `delarr`

- [`as.matrix(`*`<delarr>`*`)`](https://bbuchsbaum.github.io/delarr/reference/as.matrix.delarr.md)
  : Materialise a delayed matrix as a base matrix

- [`colMeans2(`*`<delarr>`*`)`](https://bbuchsbaum.github.io/delarr/reference/colMeans2.delarr.md)
  : Column means for a delayed matrix

- [`dim(`*`<delarr>`*`)`](https://bbuchsbaum.github.io/delarr/reference/dim.delarr.md)
  : Dimensions of a delayed matrix

- [`dim(`*`<delarr_seed>`*`)`](https://bbuchsbaum.github.io/delarr/reference/dim.delarr_seed.md)
  :

  Dimensions for a `delarr_seed`

- [`dimnames(`*`<delarr>`*`)`](https://bbuchsbaum.github.io/delarr/reference/dimnames.delarr.md)
  : Dimension names for a delayed matrix

- [`print(`*`<delarr>`*`)`](https://bbuchsbaum.github.io/delarr/reference/print.delarr.md)
  : Pretty-print a delayed matrix

- [`rowMeans2(`*`<delarr>`*`)`](https://bbuchsbaum.github.io/delarr/reference/rowMeans2.delarr.md)
  : Row means for a delayed matrix

- [`` `[`( ``*`<delarr>`*`)`](https://bbuchsbaum.github.io/delarr/reference/sub-.delarr.md)
  : Subset a delayed matrix
