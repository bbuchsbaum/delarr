# Wrap a custom backend as a delayed matrix

Provides a convenience helper that turns a user-supplied slice function
into a ready-to-use `delarr` object.

## Usage

``` r
delarr_backend(
  nrow,
  ncol,
  pull,
  chunk_hint = NULL,
  dimnames = NULL,
  begin = NULL,
  end = NULL
)
```

## Arguments

- nrow, ncol:

  Dimensions of the logical matrix.

- pull:

  Function of `rows` and `cols` returning a base matrix slice.

- chunk_hint:

  Optional preferred chunking metadata.

- dimnames:

  Optional dimnames to expose lazily.

- begin:

  Optional function invoked before streaming.

- end:

  Optional function invoked after streaming.

## Value

A `delarr` backed by the provided pull function.

## Examples

``` r
# Create a custom backend from a pull function
data <- matrix(1:20, nrow = 4, ncol = 5)

darr <- delarr_backend(
  nrow = 4,
  ncol = 5,
  pull = function(rows = NULL, cols = NULL) {
    rows <- rows %||% seq_len(4)
    cols <- cols %||% seq_len(5)
    data[rows, cols, drop = FALSE]
  }
)
darr
#> <delarr> 4 x 5 lazy

# Use like any delarr
result <- darr |> d_map(~ .x * 2) |> collect()
result
#>      [,1] [,2] [,3] [,4] [,5]
#> [1,]    2   10   18   26   34
#> [2,]    4   12   20   28   36
#> [3,]    6   14   22   30   38
#> [4,]    8   16   24   32   40
```
