# Create a delayed matrix sourced from an HDF5 dataset

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
#> <delarr> 4 x 5 lazy

# Apply operations and collect
result <- darr |> d_map(~ .x * 2) |> collect()
result
#>      [,1] [,2] [,3] [,4] [,5]
#> [1,]    2   10   18   26   34
#> [2,]    4   12   20   28   36
#> [3,]    6   14   22   30   38
#> [4,]    8   16   24   32   40

# Clean up
unlink(tf)
```
