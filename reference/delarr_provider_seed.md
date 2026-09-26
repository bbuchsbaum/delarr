# Construct a reconstructible provider seed

Unlike
[`delarr_seed()`](https://bbuchsbaum.github.io/delarr/reference/delarr_seed.md),
this seed stores no pull, begin, or end closures. It stores a plain
provider descriptor and dispatches reads through
[`delarr_provider_pull()`](https://bbuchsbaum.github.io/delarr/reference/delarr_provider_pull.md)
only when the plan executes. This makes untouched provider-backed plans
safe to serialize and reconstruct in another process.

## Usage

``` r
delarr_provider_seed(provider, dims, chunk_hint = NULL, dimnames = NULL)
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

A `delarr_provider_seed` inheriting from `delarr_seed`.
