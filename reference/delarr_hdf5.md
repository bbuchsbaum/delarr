# Create a delayed array sourced from an HDF5 dataset

Uses `hdf5r` to lazily read slices from disk on demand.

## Usage

``` r
delarr_hdf5(path, dataset)
```

## Arguments

- path:

  Path to the HDF5 file.

- dataset:

  Name of the dataset within the file.

## Value

A `delarr` that streams data from the HDF5 dataset.

## Examples

``` r
if (requireNamespace("hdf5r", quietly = TRUE)) {
  # Create a temporary HDF5 file
  tf <- tempfile(fileext = ".h5")
  data <- matrix(1:20, nrow = 4, ncol = 5)

  # Write test data
  f <- hdf5r::H5File$new(tf, mode = "w")
  f$create_dataset("X", robj = data)
  f$close_all()

  # Load as delayed array
  darr <- delarr_hdf5(tf, "X")
  darr

  # Apply operations and collect
  result <- darr |> d_map(~ .x * 2) |> collect()
  result

  # Clean up
  unlink(tf)
}
```
