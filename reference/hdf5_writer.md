# HDF5 writer for streaming `collect()`

Creates or extends an HDF5 dataset so that `collect(x, into = writer)`
can stream column blocks directly to disk without materialising the full
matrix in memory.

## Usage

``` r
hdf5_writer(path, dataset, ncol, chunk = c(128L, 4096L), compression = 4L)
```

## Arguments

- path:

  Path to the HDF5 file. The file is created if it does not exist.

- dataset:

  Name of the dataset to create or update.

- ncol:

  Total number of columns that will be written. The writer uses this to
  size the target dataset up-front.

- chunk:

  Integer vector of length two giving the chunk size `(rows, cols)` for
  the target dataset (optional).

- compression:

  Gzip compression level (0-9). Use 0 for no compression, higher values
  for better compression at cost of speed. Default is 4. Use NULL to
  disable compression entirely.

## Value

A writer object with `$write()` and `$finalize()` methods understood by
[`collect()`](https://bbuchsbaum.github.io/delarr/reference/collect.md).

## Examples

``` r
# Create source data in a temp HDF5 file
tf_in <- tempfile(fileext = ".h5")
data <- matrix(1:20, nrow = 4, ncol = 5)
f <- hdf5r::H5File$new(tf_in, mode = "w")
f$create_dataset("X", robj = data)
f$close_all()

# Load, transform, and stream to output file
darr <- delarr_hdf5(tf_in, "X")
transformed <- darr |> d_center(dim = "cols")

tf_out <- tempfile(fileext = ".h5")
writer <- hdf5_writer(tf_out, "result", ncol = ncol(transformed), compression = 4L)
collect(transformed, into = writer)

# Verify output
g <- hdf5r::H5File$new(tf_out, mode = "r")
result <- g[["result"]]$read()
g$close_all()
result
#>      [,1] [,2] [,3] [,4] [,5]
#> [1,] -1.5 -1.5 -1.5 -1.5 -1.5
#> [2,] -0.5 -0.5 -0.5 -0.5 -0.5
#> [3,]  0.5  0.5  0.5  0.5  0.5
#> [4,]  1.5  1.5  1.5  1.5  1.5

# Clean up
unlink(c(tf_in, tf_out))
```
