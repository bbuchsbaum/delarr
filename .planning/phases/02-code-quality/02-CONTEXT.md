# Phase 2: Code Quality - Context

**Gathered:** 2026-01-22
**Status:** Ready for planning

<domain>
## Phase Boundary

Fix bugs, resolve tech debt, and establish comprehensive test coverage. This phase addresses the 4 known code issues (CODE-01 through CODE-04) and adds edge case tests (TEST-01 through TEST-06). No new features or API changes beyond what's needed to fix issues.

</domain>

<decisions>
## Implementation Decisions

### Tech Debt Approach
- Review hdf5_writer() holistically when fixing duplicate validation — not just minimal fix
- Implement the compression parameter (make it functional, gzip or similar)
- Quick attempt at delarr_mmap() (~30 min) — if not working cleanly, remove from exports
- Claude's discretion on small issues found along the way (<5 min fixes OK, note larger issues for later)

### NA Handling Behavior
- All-NA reductions return NA (not NaN) — follow R convention
- Consistent across ALL reduction operations: mean, sum, max, min, sd, var
- No operation-specific exceptions — uniform behavior

### Test Strategy
- HDF5 tests should FAIL (not skip) when hdf5r unavailable — it's a real dependency for full suite
- Comprehensive edge case coverage for negative indices, broadcasting, chunk boundaries
- Property-based or exhaustive testing, not just documented issue cases

### Claude's Discretion
- Exact compression implementation (gzip level, chunk settings)
- Test organization and file structure
- Fix-now vs note-for-later decisions on discovered issues

</decisions>

<specifics>
## Specific Ideas

- "Let's be sensible" — pragmatic approach to stub functions
- If delarr_mmap can work, keep it; if not, remove cleanly
- Match R conventions for NA handling

</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope

</deferred>

---

*Phase: 02-code-quality*
*Context gathered: 2026-01-22*
