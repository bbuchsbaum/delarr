# delarr parallel crossover benchmark

Date: 2026-02-16 00:15:40 EST
Chunk size: 256 columns; workers: 2

| shape | cells (M) | seq median (s) | par median (s) | speedup (seq/par) | winner |
|---|---:|---:|---:|---:|---|
| 2000x1200 | 2.4 | 0.0660 | 0.0780 | 0.85x | sequential |
| 4000x2000 | 8.0 | 0.1410 | 0.1920 | 0.73x | sequential |
| 6000x2500 | 15.0 | 0.2515 | 0.3445 | 0.73x | sequential |
| 8000x3000 | 24.0 | 0.4445 | 0.4680 | 0.95x | sequential |

Parallel did not win in tested ranges; crossover likely at larger workloads or with more workers.

## Additional Stress Check

- Shape: `10000x3000` (30.0M cells)
- Chunk size: `256`
- Workers: `4`
- Sequential median: `0.5315s`
- Parallel median: `0.4575s`
- Speedup (seq/par): `1.16x`

This indicates crossover appears at larger sizes with more workers.
