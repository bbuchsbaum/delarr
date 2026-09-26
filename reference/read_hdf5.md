# Read a matrix from an HDF5 file

Simple convenience function to read a matrix from an HDF5 dataset. For
lazy/streaming access, use
[`delarr_hdf5()`](https://bbuchsbaum.github.io/delarr/reference/delarr_hdf5.md)
instead.

## Usage

``` r
read_hdf5(path, dataset)
```

## Arguments

- path:

  Path to the HDF5 file.

- dataset:

  Name of the dataset to read.

## Value

The matrix stored in the dataset.

## Examples

``` r
if (requireNamespace("hdf5r", quietly = TRUE)) {
  # Write and read back
  mat <- matrix(1:20, nrow = 4, ncol = 5)
  tf <- tempfile(fileext = ".h5")
  write_hdf5(mat, tf, "X")
  read_hdf5(tf, "X")

  # Clean up
  unlink(tf)
}
```
