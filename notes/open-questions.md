# Open Questions and Next Steps

- **Chunk-aware execution**: finish polishing the streaming runner (including
  writer support) and benchmark memory savings versus eager collect.
- **Backends beyond HDF5**: decide whether to ship optional helpers (e.g., mmap)
  or leave them to external packages to avoid extra dependencies.
- **Writer targets**: design a small writer interface (e.g. `hdf5_writer()`)
  so `collect(into = ...)` can stream output to disk without a full in-memory
  copy.
- **Sparse support**: decide whether to lean on `Matrix` for column-sparse
  blocks or introduce a thin COO adapter compatible with existing fMRI masks.
- **Expression DAG validation**: learn when to collapse redundant `d_map`
  chains at append time instead of waiting for `collect()` to do all the work.
- **detrend semantics**: confirm the intended axis behaviour for fMRI data
  (time along rows versus columns) and adjust helper implementations.
