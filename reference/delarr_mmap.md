# Create a delayed matrix from a memory-mapped file

Uses the mmap package to lazily read slices from a binary file on
demand. The file must contain raw numeric data in column-major order
(R's default).

## Usage

``` r
delarr_mmap(path, nrow, ncol, mode = NULL)
```

## Arguments

- path:

  Path to the binary file containing matrix data.

- nrow:

  Number of rows in the matrix.

- ncol:

  Number of columns in the matrix.

- mode:

  mmap mode object specifying data type. Default is double().

## Value

A `delarr` that streams data from the memory-mapped file.

## Note

This backend supports 2D matrices only. For N-d arrays, use
[`delarr_hdf5()`](https://bbuchsbaum.github.io/delarr/reference/delarr_hdf5.md)
or wrap an in-memory array with
[`delarr()`](https://bbuchsbaum.github.io/delarr/reference/delarr.md).

## Examples

``` r
if (requireNamespace("mmap", quietly = TRUE)) {
  # Create a binary file with matrix data
  mat <- matrix(1:20, nrow = 4, ncol = 5)
  tf <- tempfile()
  writeBin(as.double(mat), tf)

  # Load as delayed array
  darr <- delarr_mmap(tf, nrow = 4, ncol = 5)
  darr

  # Apply operations and collect
  result <- darr |> d_map(~ .x * 2) |> collect()
  result

  # Clean up
  unlink(tf)
}
```
