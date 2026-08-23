# mrfpipeline

Operator executable that will orchestrate CMS Transparency in Coverage
discovery, TOC and MRF processing, warehouse ingestion, and additive plan
attachment.

This repository is requirements-first. Stories 01–13 in
[`requirements/`](requirements/) define version 1. [`requirements/DESIGN.md`](requirements/DESIGN.md)
records product decisions that must stay consistent across those stories.

The current command surface is:

```text
mrfpipeline migrate
mrfpipeline work
mrfpipeline discover --payer uhc --collection-month <YYYY-MM> --limit <count>
```

`migrate` applies application and River schemas. `discover` enqueues one
bounded UHC discovery run. `work` currently consumes the discovery, TOC
download, TOC parse, TOC import, MRF download, MRF parse, and consumer
ingest queues.
Story 13 adds `reconcile` and `retry` and replaces this README with
operational documentation.

## River UI (optional, not part of this binary)

River's open-source UI can run as a separate process against the same
PostgreSQL database to inspect queues and pause them:

```text
export DATABASE_URL=<same URL as the worker>
export RIVER_SCHEMA=mrfpipeline_river
riverui
```

Pause stops fetching new jobs. It does not cancel in-flight work. Do not
cancel, retry, or delete jobs from the UI.
