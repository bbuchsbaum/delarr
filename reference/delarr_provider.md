# Create a delayed array from a reconstructible provider

Create a delayed array from a reconstructible provider

## Usage

``` r
delarr_provider(provider, dims, chunk_hint = NULL, dimnames = NULL)
```

## Arguments

- provider:

  A serializable provider descriptor containing no functions,
  environments, or external pointers.

- dims:

  Integer vector of logical dimensions, with length at least two.

- chunk_hint:

  Optional list of preferred chunk sizes.

- dimnames:

  Optional list of dimension names.

## Value

A lazy `delarr` backed by the provider descriptor.
