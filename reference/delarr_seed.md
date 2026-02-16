# Construct a seed backend for `delarr`

Seeds encapsulate storage access for delayed matrices. They define
matrix dimensions and a `pull()` function that returns materialised
slices.

## Usage

``` r
delarr_seed(
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

  Number of rows and columns.

- pull:

  A function accepting `rows` and `cols` indices and returning a base
  matrix slice.

- chunk_hint:

  Optional list describing preferred chunk sizes (e.g.
  `list(cols = 4096L)`).

- dimnames:

  Optional list of dimnames to expose lazily.

- begin:

  Optional function invoked before streaming begins.

- end:

  Optional function invoked after streaming completes.

## Value

An object of class `delarr_seed`.

## Examples

``` r
# Create a custom seed with a pull function
data <- matrix(1:12, nrow = 3, ncol = 4)

seed <- delarr_seed(
  nrow = 3,
  ncol = 4,
  pull = function(rows = NULL, cols = NULL) {
    rows <- rows %||% seq_len(3)
    cols <- cols %||% seq_len(4)
    data[rows, cols, drop = FALSE]
  }
)
seed
#> $nrow
#> [1] 3
#> 
#> $ncol
#> [1] 4
#> 
#> $pull
#> function (rows = NULL, cols = NULL) 
#> {
#>     rows <- rows %||% seq_len(3)
#>     cols <- cols %||% seq_len(4)
#>     data[rows, cols, drop = FALSE]
#> }
#> <environment: 0x55b74e53feb0>
#> 
#> $chunk_hint
#> NULL
#> 
#> $dimnames
#> NULL
#> 
#> $begin
#> NULL
#> 
#> $end
#> NULL
#> 
#> attr(,"class")
#> [1] "delarr_seed"

# Wrap in delarr() to use with lazy operations
darr <- delarr(seed)
result <- darr |> d_map(~ .x * 2) |> collect()
result
#>      [,1] [,2] [,3] [,4]
#> [1,]    2    8   14   20
#> [2,]    4   10   16   22
#> [3,]    6   12   18   24
```
