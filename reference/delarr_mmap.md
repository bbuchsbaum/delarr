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
# Create a binary file with matrix data
mat <- matrix(1:20, nrow = 4, ncol = 5)
tf <- tempfile()
writeBin(as.double(mat), tf)

# Load as delayed array
darr <- delarr_mmap(tf, nrow = 4, ncol = 5)
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
