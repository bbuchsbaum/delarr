# Pull an array slice from a reconstructible provider

Storage packages implement this generic for a serializable provider
class. The provider descriptor remains inside the lazy plan; live file
handles and closures are created, used, and closed inside the method at
execution time.

## Usage

``` r
delarr_provider_pull(provider, indices, ...)
```

## Arguments

- provider:

  A reconstructible provider descriptor.

- indices:

  A list containing one integer selector per array dimension; `NULL`
  selects an entire dimension.

- ...:

  Provider-specific arguments.

## Value

A matrix or array with dimensions matching `indices`.
