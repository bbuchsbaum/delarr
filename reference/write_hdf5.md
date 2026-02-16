# Write a matrix to an HDF5 file

Simple convenience function to write a matrix to an HDF5 dataset. For
streaming writes during
[`collect()`](https://bbuchsbaum.github.io/delarr/reference/collect.md),
use
[`hdf5_writer()`](https://bbuchsbaum.github.io/delarr/reference/hdf5_writer.md)
instead.

## Usage

``` r
write_hdf5(x, path, dataset, compression = 4L)
```

## Arguments

- x:

  A matrix to write.

- path:

  Path to the HDF5 file. Created if it doesn't exist.

- dataset:

  Name of the dataset to create.

- compression:

  Gzip compression level (0-9), or NULL for no compression.

## Value

The path to the HDF5 file (invisibly).

## Examples

``` r
# Write a matrix to HDF5
mat <- matrix(1:20, nrow = 4, ncol = 5)
tf <- tempfile(fileext = ".h5")
write_hdf5(mat, tf, "X")

# Read it back as a delarr
darr <- delarr_hdf5(tf, "X")
collect(darr)
#>      [,1] [,2] [,3] [,4] [,5]
#> [1,]    1    5    9   13   17
#> [2,]    2    6   10   14   18
#> [3,]    3    7   11   15   19
#> [4,]    4    8   12   16   20

# Clean up
unlink(tf)
```
