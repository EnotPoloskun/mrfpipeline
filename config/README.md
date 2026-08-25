# Local pipeline inputs

This directory contains only the shape of the local configuration mount. The
accepted provider catalog and the service selector are operator-supplied
inputs; neither is committed here.

Before starting `control` or `consumer`, provide a real catalog directory
containing the catalog's `manifest.json` and its catalog files. Point
`MRFPIPELINE_PROVIDER_CATALOG_DIR` at that directory in `.env` or in the
shell environment. A catalog copied from an example is not a valid production
catalog.

Provide a selector CSV through `MRFPIPELINE_SERVICES_FILE`. It must be a
two-column CSV with this exact header:

```csv
billing_code_type,billing_code
CPT,99213
```

The sample row is only a format example. Replace it with the operator's real
service set. The pipeline passes this file directly to `mrfparser`; it does
not add a header, reinterpret rows, or rewrite the file.

The supplied `services3.csv` from the local test environment must therefore
include the header above before it is used for a live parser run.
