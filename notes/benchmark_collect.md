# delarr collect() benchmark

Date: 2026-02-16 00:12:47 EST

Data shape: 2000 x 1200 (2.4M cells)

## Timing summary (seconds)

| benchmark | min | median | mean | max |
|---|---:|---:|---:|---:|
| memory_opt_false | 0.0390 | 0.0400 | 0.0507 | 0.0730 |
| memory_opt_true | 0.0360 | 0.0380 | 0.0463 | 0.0650 |
| memory_target_bytes | 0.0370 | 0.0410 | 0.0500 | 0.0720 |
| memory_row_chunk | 0.0460 | 0.0470 | 0.0473 | 0.0490 |
| memory_parallel | 0.0760 | 0.0820 | 0.0940 | 0.1240 |
| hdf5_chunk128 | 0.0610 | 0.0610 | 0.0613 | 0.0620 |
| hdf5_target_bytes | 0.0650 | 0.0670 | 0.0673 | 0.0700 |
| mmap_fast | 0.0360 | 0.0370 | 0.0457 | 0.0640 |
| mmap_slow_baseline | 0.3020 | 0.3180 | 0.3127 | 0.3180 |

## Key speedups

- optimize=TRUE vs optimize=FALSE: 1.05x (median)
- mmap fast pull vs naive full-materialize pull: 8.59x (median)
