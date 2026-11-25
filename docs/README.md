# Technical documentation

Start with the [project overview](../README.md) for the architecture, implemented
capabilities, reproducible checks and current limitations. The detailed records
below are retained in their original Spanish to preserve their evidence and
provenance.

## Suggested reading paths

| Goal | Read |
|---|---|
| Understand the system | [Overview and architecture](../README.md#system-architecture), then [demo observers](phase-c/OBSERVERS.md) |
| Reproduce the experimental baseline | [Test commands](../README.md#requirements-and-reproducible-tests), [validation](VALIDATION.md) and [sanitized results](../evidence/publication/results.json) |
| Review data and isolation risks | [Security](../SECURITY.md) and [development limitations](../README.md#development-status-and-limitations) |
| Audit source lineage | [Provenance](PROVENANCE.md), [per-file hashes](provenance/source-files.json), [inventory](../FILES.txt) and [manifest](../SHA256SUMS) |
| Evaluate reuse | [Rights](../README.md#rights-attribution-and-reuse) and [third-party dependencies](../THIRD_PARTY.md) |
| Explore the synthetic dashboard | [Harness](../tests/run_dashboard_demo.py), [projection](../src/engine/dashboard.py) and [manual renderer](../tools/render_test_dashboard.py) |
| Diagnose observer initialization under Wine/Bottles | [Runtime paths, initialization reasons and transport boundaries](phase-c/WINE-BOTTLES-RUNTIME-2026-11-25.md) |

See the [presentation update record](PRESENTATION.md) for the documentation-only scope and manifest boundaries.

## Version and evidence boundaries

The [`v0.1.0-dev` tag](https://github.com/h377ll/vidar-mt5-monitor/tree/v0.1.0-dev)
preserves the initial public code, documentation and validation baseline.
Presentation updates on the default branch do not change that tag or establish
new functional validation. Historical check counts and source hashes refer to
the original publication described in each record.

The project is experimental. Synthetic tests are not evidence of production
readiness, real-position management, profitability or validated autonomous
execution. There is no order executor in this distribution.
