# Phase 3: Platform Readiness - Context

**Gathered:** 2026-01-22
**Status:** Ready for planning

<domain>
## Phase Boundary

Ensure the package works across platforms (Windows, macOS, Linux) and handles dependencies correctly. All backends (HDF5, memory, mmap) should work reliably. Win-builder checks (R-release and R-devel) must pass.

</domain>

<decisions>
## Implementation Decisions

### Dependency Strategy
- **hdf5r is a required Import** (not optional Suggests)
- **mmap is a required Import** (not optional Suggests)
- Package requires both dependencies to install — no graceful degradation needed
- hdf5r handles its own HDF5 system library bundling on Windows via CRAN

### Code Cleanup
- Remove all `requireNamespace("hdf5r")` checks — hdf5r guaranteed present
- Remove all `requireNamespace("mmap")` checks — mmap guaranteed present
- Clean up defensive code that assumed optional dependencies

### Examples & Documentation
- Remove `\donttest{}` blocks that were conditional on hdf5r availability
- Use `\donttest{}` only for genuinely slow examples (>5 seconds)
- Keep vignette conditional chunks as harmless safety net (user preference)

### Win-builder Strategy
- Run both R-release and R-devel checks before CRAN submission
- Trust that hdf5r (being on CRAN) handles Windows HDF5 bundling correctly
- No special Windows handling needed beyond what hdf5r provides

### Platform Testing
- Explicitly verify all three backends (delarr_hdf5, delarr_mem, delarr_mmap) work on win-builder
- Existing test suite covers backends; verify they pass on Windows

### Claude's Discretion
- How to handle CRAN platform-specific issues (SystemRequirements field, platform exclusions) if they arise
- Exact cleanup of defensive code patterns

</decisions>

<specifics>
## Specific Ideas

- "hdf5r is on CRAN, therefore importing it should be fine" — user's reasoning for requiring hdf5r
- hdf5r bundles HDF5 libraries on Windows, so win-builder should work without special setup

</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope

</deferred>

---

*Phase: 03-platform-readiness*
*Context gathered: 2026-01-22*
