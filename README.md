# mrfpipeline

Operator executable that will orchestrate CMS Transparency in Coverage
discovery, TOC and MRF processing, warehouse ingestion, and additive plan
attachment.

This repository is requirements-first. Stories 01–13 in
[`requirements/`](requirements/) define version 1. [`requirements/DESIGN.md`](requirements/DESIGN.md)
records product decisions that must stay consistent across those stories.

The current command surface specified by Story 01 is:

```text
mrfpipeline migrate
mrfpipeline work
mrfpipeline discover --payer uhc --collection-month <YYYY-MM> --limit <count>
```

Those commands are placeholders until later stories replace them. Story 13
adds `reconcile` and `retry` and replaces this README with operational
documentation.

## River UI (optional, not part of this binary)

The pipeline does not embed a dashboard. River's open-source UI can run as a
separate process against the same PostgreSQL database to inspect queues and
pause them:

```text
export DATABASE_URL=<same URL as the worker>
export RIVER_SCHEMA=mrfpipeline_river
riverui
```

Pause stops fetching **new** jobs on that queue. It does not cancel an already
running download, parse, or ingest. To let the consumer drain remaining
snapshots, pause `toc_download`, `toc_parse`, `toc_import`, `mrf_download`, and
`mrf_parse`, and leave `consumer` running.

Do not cancel, retry, or delete jobs from the UI. Those actions fight
PostgreSQL domain state. Use `mrfpipeline retry` after the worker is stopped.
