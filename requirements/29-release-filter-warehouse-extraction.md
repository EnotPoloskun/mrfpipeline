# Story 29: Release filter warehouse extraction

## Status

Proposed.

## User story

As a pipeline operator, I want one read-only, reproducible extraction over an
exact prospective publication generation so the filter catalog is derived from
the same warehouse facts that the public query service will later aggregate.

## Context

Story 28 defines PostgreSQL destinations but does not read the warehouse. The
pipeline currently invokes `mrfconsumer` for publication validation and does
not run DuckDB in a production worker. Filter extraction is a new explicit
operator operation, not a worker stage and not a change to consumer ingestion.

A representative local 184 MB warehouse contains about 11.7 million facts,
111,000 canonical plans, and expensive global list expansion. Global modifier
and place-of-service discovery is not an HTTP path. This story performs the
scan once for an exact publication candidate, returns compact typed rows, and
leaves the warehouse unchanged.

## Dependencies

- Story 27 exact durable publication generations and output identity;
- Story 28 PostgreSQL filter semantics and row contracts;
- exact `mrfconsumer 2.0.0` warehouse and DuckDB view semantics; and
- the pipeline's existing warehouse recognition, provider-catalog identity,
  path normalization, redaction, and activation preflight rules.

## Goal

Add one focused `internal/filtercatalog` extraction boundary that:

1. receives a validated nonempty exact candidate output relation;
2. recognizes the exact consumer `2.0.0` warehouse and pinned provider catalog;
3. invokes a pinned DuckDB 1.5.5 CLI in one read-only in-memory session;
4. reads only fixed warehouse datasets through checked-in SQL;
5. applies the candidate relation to every snapshot-scoped query;
6. emits typed compact billing, option, network, and provider filter rows in
   deterministic order; and
7. returns sanitized failures without modifying Parquet, PostgreSQL, or release
   state.

This story adds no pipeline command and writes no catalog table. Story 30 owns
persistence and CLI behavior.

## Product decisions

- Keep `mrfconsumer` unchanged. Filter catalogs are a web-serving concern owned
  by the pipeline/query boundary, not part of warehouse publication.
- Scan the warehouse once per candidate catalog build. Do not add incremental
  per-ingestion filter datasets.
- Use an external pinned DuckDB CLI, not a CGO Go driver and not a hand-written
  Go join over duplicated private Parquet schemas.
- Keep the pipeline binary `CGO_ENABLED=0`.
- DuckDB is invoked only by the explicit filter-build operation. Control, MRF,
  and consumer River workers never start DuckDB.
- Use fixed concrete SQL artifacts. Do not add a SQL builder, generic query
  engine, view registry, ORM, plugin, or storage abstraction.
- Execute no DuckDB extension installation/loading, network request, ATTACH,
  persistent database write, warehouse write, or PostgreSQL extension.
- Extract plans from existing pipeline PostgreSQL in Story 30, not from
  warehouse plan Parquet in this story.
- Every fact-derived value uses the standard population:

```sql
billing_code_type IS NOT NULL
AND billing_code IS NOT NULL
AND negotiated_type = 'negotiated'
AND negotiated_rate IS NOT NULL
```

- Do not apply `expiration_date`. The product has no expiration filter and the
  catalog represents payer-published observations in the release.
- Counts are negotiated-price-observation weighted. A fact contributes at most
  once to one distinct list value even if malformed source input repeats that
  value within the list.

## DuckDB runtime contract

### Version and packaging

The only supported production CLI version is exact DuckDB `v1.5.5`.

Update the multi-stage Dockerfile to install the official DuckDB CLI in the
runtime image at:

```text
/usr/local/bin/duckdb
```

Requirements:

- support Linux `amd64` and `arm64` through Docker `TARGETARCH` mapping;
- select only the corresponding official release archive;
- pin one committed SHA-256 checksum per supported archive;
- download over HTTPS during image build only;
- verify the checksum before extraction;
- verify the extracted executable reports version token `v1.5.5`;
- copy no package manager cache, archive, credential, SSH key, database URL,
  warehouse path, provider path, or service selector into the final image; and
- retain the existing BuildKit SSH mount only for private Go module download.

The runtime image may add only packages needed to install/copy the verified
CLI. DuckDB makes no runtime download.

For a host-built operator binary, extraction resolves `duckdb` through
`exec.LookPath` and rejects a missing executable or any version other than
`v1.5.5` before creating catalog state. Do not add another environment
variable or silently accept a different version.

### Process behavior

The extractor starts exactly one DuckDB extraction process for one build and
uses `:memory:`. One separate `duckdb --version` probe before catalog-state
creation is permitted and required for host execution; it is not an extraction
process. The extractor supplies checked-in SQL over stdin and streams one
compact JSON object per line from stdout. SQL emits datasets in the fixed order
documented below, JSON null represents every nullable field, and each physical
line is one
complete object, and successful process EOF after the last ordered row is the
only end marker. Empty stdout, a blank/non-JSON line, a JSON array, trailing
content, or EOF inside an object is invalid protocol. The extractor closes
stdin, consumes stdout and stderr concurrently,
waits for process exit, and propagates context cancellation to the complete
process tree.

- stdout is reserved for the exact extraction protocol;
- stderr is captured only to classify success/failure and is never copied into
  a public error, database failure code, CLI JSON, or application log;
- nonzero exit, malformed protocol, unexpected row type, duplicate row, or
  premature EOF fails the complete extraction;
- cancellation returns the existing sanitized cancellation behavior; and
- no retry occurs inside the extractor. The operator reruns Story 30's
  idempotent build command.

Do not invoke a shell. Use `exec.CommandContext` with an argument vector.

## Candidate output relation

Input is a nonempty slice containing exactly:

```text
payer_id
collection_month
output_id
mrf_snapshot_id
```

Requirements:

- all rows have one exact payer and collection month matching the catalog;
- output IDs and month already satisfy pipeline lexical contracts;
- output ID is exactly `mrf-<snapshot-id>`;
- snapshot IDs and output IDs are unique; and
- the caller supplies or verifies the Story 28 output fingerprint.

The extractor never sorts the release package's numeric target slice in place.
It copies candidate rows, sorts that copy by UTF-8/ASCII `output_id`, calculates
the fingerprint from the copy, and uses the same order for the selected-output
protocol. Numeric snapshot ordering is unrelated to fingerprint order.

The extractor rechecks structural invariants before starting DuckDB. It does
not query release status or choose outputs. Story 30 owns candidate selection.

The SQL session creates one temporary `selected_outputs` relation from those
validated values. Literal encoding is a small dedicated function that accepts
only already validated payer/month/output values and doubles SQL apostrophes.
There is no caller-provided SQL fragment, column, operator, order, path, or
filter expression.

Every snapshot-scoped query begins from or semi-joins this relation. A query
that starts from a relationship view lacking payer/month joins by exact
`output_id`. The extractor never selects every output sharing the payer/month.

## Warehouse recognition and SQL path safety

Before DuckDB:

- normalize the configured local warehouse path through existing pipeline path
  rules;
- use the existing pipeline `InspectWarehouse`/activation-preflight boundaries
  to recognize exact consumer warehouse `2.0.0`, provider-catalog identity,
  candidate snapshots, and positive plan parts;
- convert provider catalog release month from exact `YYYY-MM` text to the
  PostgreSQL-compatible first-of-month date `YYYY-MM-01`; and
- reject a missing, malformed, unsupported, symlinked, or inconsistent
  publication with existing sanitized warehouse/publication failures.

The pre-DuckDB inspection is intentionally not a second full provider-catalog
row/relationship validator. `mrfconsumer` remains unchanged and exports no
filter-specific or new validation API; the pipeline does not duplicate its
private validator. Fixed DuckDB reads validate the schemas and relationships
required by extraction. A read/schema/lexical/semantic violation fails the
complete build through the fixed Story 29 failure classification rather than
weakening or rewriting the value.

Render the checked-in SQL token for the warehouse root using the consumer's
established DuckDB glob-literal rules:

1. convert the absolute cleaned path to slash-separated form;
2. encode original `*`, `?`, `[`, and `]` path characters as DuckDB literal
   bracket expressions exactly once;
3. double SQL apostrophes; and
4. replace only the exact fixed warehouse-root token.

Do not reject a valid local path merely because it contains an apostrophe or
glob metacharacter. Tests must use such a path.

## Fixed read-only views

The SQL artifact exposes only the columns required from these exact consumer
`2.0.0` datasets:

```text
snapshots/.../rate_facts/*.parquet
snapshots/.../rate_provider_groups/*.parquet
snapshots/.../provider_group_memberships/*.parquet
provider_catalog/providers/*.parquet
provider_catalog/provider_taxonomies/*.parquet
```

All `read_parquet` calls use:

```text
hive_partitioning = false
union_by_name = false
```

Plans come from PostgreSQL later. `ingestions`, snapshot `network_names`, and
`provider_groups` are not needed for fact membership because
`rate_facts.network_names` is authoritative for standard network filtering.

The SQL contains no `COPY`, `INSERT`, `UPDATE`, `DELETE`, persistent table,
ATTACH, INSTALL, LOAD, secret, URL, or extension operation. Temporary in-memory
views/tables are allowed only for selected outputs and fixed extraction work.

## Exact extraction datasets

### `summary`

Emit exactly one first protocol row:

```text
dataset = summary
standard_fact_count
```

`standard_fact_count` is the nonnegative count of candidate facts satisfying
the exact standard population. It must be positive because an empty standard
catalog is invalid. The row has no labels, rates, or output identifiers.

Every output protocol row starts with a fixed `dataset` discriminator. Dataset
rows are sorted by their complete logical primary key. The Go decoder rejects
unknown or out-of-order datasets and duplicate keys.

Before protocol encoding, SQL/extractor validation requires every emitted text
field to satisfy its corresponding Story 28 nonempty/CR/LF constraint. A
hostile warehouse value containing CR or LF, or any other non-normalizable
lexical violation, fails the complete extraction with
`filter_catalog_value_invalid`; it is never omitted, trimmed, rewritten, or
deferred to PostgreSQL insertion.

### `billing_codes`

One row per exact `(billing_code_type, billing_code)` in the candidate standard
population:

```text
dataset = billing_codes
billing_code_type
billing_code
billing_code_type_version nullable
warehouse_service_name nullable
warehouse_service_description nullable
observation_count
unmodified_observation_count
```

`unmodified_observation_count` counts facts where
`billing_code_modifiers IS NULL OR len(billing_code_modifiers) = 0`.

For version, service name, and description independently:

1. reject the complete extraction if any non-null candidate label contains CR
   or LF; this lexical validation precedes emptiness selection;
2. trim outer ASCII whitespace for candidate-label emptiness only, using
   exactly space, tab, CR, LF, vertical tab, and form feed;
3. ignore null or empty candidate labels;
4. count facts carrying each remaining exact stored label;
5. choose the greatest count; and
6. break ties by UTF-8 byte lexical order (`COLLATE "C"` semantics).

Store the chosen exact label, not the trimmed or lowercased candidate. If no
usable value exists, emit null. These are source-provided warehouse labels, not
external CPT/HCPCS definitions.

### `code_filter_values`

One row per code/kind/value with positive count:

```text
dataset = code_filter_values
billing_code_type
billing_code
filter_kind
filter_value
display_label
observation_count
```

Kinds and derivation:

- `modifier`: each distinct nonempty element of `billing_code_modifiers`;
- `place_of_service`: each distinct nonempty element of `service_codes`;
- `billing_class`: one nonempty scalar `billing_class`;
- `setting`: one nonempty scalar `setting`; and
- `negotiation_arrangement`: one nonempty scalar
  `negotiation_arrangement`.

A fact with list `["95", "95"]` contributes one observation to modifier `95`.
A fact with distinct `["95", "25"]` contributes one to each. Null/empty lists
do not create a value row. No synthetic no-modifier/all/has-any rows are
emitted.

Display label is the exact value except:

```text
place_of_service / CSTM-00 → Broad or unspecified place of service
```

Do not parse malformed composite service-code strings into invented values.
Preserve each stored list element as one exact identity.

### `output_code_networks`

One row per candidate output/code/network:

```text
dataset = output_code_networks
output_id
billing_code_type
billing_code
network_name
observation_count
```

Derive each distinct nonempty `rate_facts.network_names` element. A fact counts
once for one network value. Retain output identity so Story 30 can constrain
network options through selected plan outputs without materializing a
plan/network cross-product.

### `provider_filter_values`

Release-reachable provider choices use this path:

```text
selected output
  → standard-population all_rate_facts row
  → all_rate_provider_groups for the same output/rate_group_id
  → all_provider_group_memberships
  → providers / provider_taxonomies
```

Provider groups reachable only from percentage/derived/null-rate facts do not
contribute a filter value. The provider list is still not billing-code-scoped:
any code in the standard population can establish reachability. All taxonomy
and geography predicates for one emitted relationship apply to one membership
NPI. Do not combine one NPI's taxonomy with another NPI's location.

Rows:

```text
dataset = provider_filter_values
filter_kind       taxonomy | state | city
parent_value      empty except city uses exact state
filter_value
provider_count
```

Definitions:

- taxonomy: distinct reachable NPIs per exact nonempty taxonomy code;
- state: distinct reachable NPIs per exact nonempty state;
- city: distinct reachable NPIs per exact `(state, city)` pair, requiring the
  stored city to equal Go `strings.ToLower(city)`;
- a non-lowercase stored city fails the complete extraction with
  `filter_catalog_value_invalid`; never lowercase, merge, or preserve it as a
  second case-sensitive identity;
- omit a city when state is null/empty because the UI cascades state → city;
- omit null/empty taxonomy/state/city values; and
- do not read or emit postal code or provider name.

This discovery is release-scoped, not billing-code-scoped. A provider option
may yield zero observations for a later selected billing code; final DuckDB
queries remain authoritative.

## Extraction result

The Go result is one concrete typed aggregate containing:

```text
warehouse schema version
provider catalog schema version
provider catalog release-month date
candidate output fingerprint
standard fact count
[]BillingCode
[]CodeFilterValue
[]OutputCodeNetwork
[]ProviderFilterValue
```

It contains no generic `map[string]any`, SQL row abstraction, Arrow table,
DuckDB handle, path, raw stderr, or warehouse fact. Slices may contain all
compact extracted rows and must be deterministically sorted before return.

The extractor validates before return:

- exactly one summary row exists and standard fact count is positive;
- at least one billing code;
- all counts positive/nonnegative according to Story 28;
- every code-filter/network code exists in billing codes;
- every network output exists in selected outputs;
- unique logical keys;
- every emitted text value satisfies the exact Story 28 lexical constraint;
- required city parent semantics;
- exact candidate fingerprint unchanged; and
- context cancellation at fixed phase boundaries.

A release with published outputs but zero standard negotiated non-null-rate
facts cannot produce a usable public catalog and fails extraction. Do not mark
an empty catalog ready.

## Resource and concurrency behavior

- Run one filter extraction per pipeline process.
- Story 30 serializes builds for one payer/month; this extractor adds no global
  worker pool or background queue.
- DuckDB may use its ordinary internal parallelism. Do not expose thread,
  memory, spill, row-group, or SQL tuning flags in v1.
- Stream stdout decoding and PostgreSQL population boundaries in Story 30 must
  avoid retaining raw JSON output plus decoded copies. The compact typed result
  itself may remain in memory for initial implementation; Story 32 measures it.
- Do not log per-fact progress or values.
- Cancellation terminates the DuckDB process and removes private temporary
  protocol files, if any.

If real acceptance demonstrates unacceptable peak memory, a later measured
story may stream directly into PostgreSQL staging or split fixed queries. Do
not preemptively add a generic spill framework.

## Failure classification and observability

Add fixed filter-catalog failure codes sufficient to distinguish:

```text
filter_catalog_config_invalid
filter_catalog_warehouse_invalid
filter_catalog_duckdb_unavailable
filter_catalog_duckdb_version_invalid
filter_catalog_query_failed
filter_catalog_value_invalid
filter_catalog_protocol_invalid
filter_catalog_cancelled
```

The eight listed lowercase strings are the exact externally observable failure
codes. Go constant names may follow existing `jobs.Failure...` style, but their
serialized values may not differ. Each public branch maps to one code. Errors
and logs never include:

- warehouse/provider/artifact paths;
- SQL text;
- DuckDB stderr;
- payer/output/source URLs beyond already safe operator result fields;
- plan/provider/service raw rows; or
- database credentials.

Successful extraction reports only compact row counts and elapsed wall time to
Story 30. It does not report rates, labels, network names, taxonomy values,
NPIs, or locations.

## Required tests

### SQL semantics

A consumer-owned-style synthetic exact `2.0.0` warehouse must prove:

- selected outputs include two generations and exclude another output sharing
  payer/month;
- percentage/derived/null-rate rows never enter any catalog count;
- expiration dates do not filter rows;
- billing counts and no-modifier counts are exact;
- one exact summary row reports the hand-computed standard fact count;
- repeated modifier/POS elements count a fact once;
- `CSTM-00` label is exact;
- scalar null/empty context values are omitted;
- output/code/network rows retain output scope;
- same canonical network in two outputs remains two output rows;
- taxonomy/state/city counts use distinct reachable NPIs;
- taxonomy and geography do not cross-match different members; and
- ZIP is never emitted.
- non-lowercase stored city fails rather than being normalized or preserved;
- provider catalog release month becomes exact first-of-month date;
- extraction relies on existing shallow warehouse identity/preflight plus
  required DuckDB schema/semantic reads, without a new consumer API or copied
  full provider validator;

### Labels

Prove null/empty candidates are ignored, most-frequent exact label wins, lexical
tie-break is deterministic, and no-label emits null independently for version,
name, and description.

### Runtime and protocol

- Missing/wrong DuckDB version fails before SQL.
- Valid SQL handles warehouse roots containing apostrophe plus `*?[]`.
- Nonzero exit, hostile stderr, malformed/truncated/duplicate/out-of-order JSON,
  unknown dataset, and impossible counts return only fixed sanitized errors.
- Context cancellation terminates the process and leaves no child running.
- Repeated extraction returns byte-for-byte equivalent typed rows.
- Warehouse file metadata and checksums are unchanged before/after extraction.

### Packaging

- Linux `amd64` and `arm64` archive selection is explicit.
- Checksums and version token are pinned.
- Final image contains executable `/usr/local/bin/duckdb` and reports exact
  `v1.5.5`.
- Existing `CGO_ENABLED=0` pipeline build remains.
- No secret, SSH key, local path, or source archive remains in runtime layers.

## Acceptance criteria

- One exact candidate output relation produces complete deterministic compact
  filter rows without changing consumer or warehouse contracts.
- Every snapshot-scoped query applies the exact relation.
- Every count has the agreed observation/provider meaning.
- Plans are not read or fabricated by DuckDB.
- DuckDB is pinned, offline at runtime, read-only, and absent from River worker
  paths.
- Failure/cancellation is sanitized and leaves no database/release mutation.
- No generic query/storage abstraction is introduced.

## Non-goals

- Creating or updating PostgreSQL filter rows.
- Adding `filters build/status` CLI commands.
- Changing activation or publication generations.
- Modifying `mrfconsumer`, its warehouse schema, or its DuckDB artifacts.
- Importing external CPT/HCPCS definitions.
- Producing ZIP/NPI/expiration filters.
- Precomputing medians, percentiles, provider counts by arbitrary selected
  combinations, distributions, or dashboard tiles.
- Implementing HTTP, HTML, JavaScript, a query API, or web-process switching.
- Adding S3, remote warehouse reads, a DuckDB server, or persistent DuckDB DB.

## Documentation

Describe the proposed pinned DuckDB extraction boundary in active/planned
README and design sections without claiming current worker roles execute it.
Keep historical Stories 01–13 material explicitly historical.

## Implementation notes

Expected implementation shape:

```text
internal/filtercatalog/extract.go
internal/filtercatalog/rows.go
internal/filtercatalog/sql/extract.sql.tmpl
internal/filtercatalog/*_test.go
Dockerfile
```

Use explicit structs and fixed SQL. Reuse existing path normalization,
warehouse inspection, and redaction helpers where their contracts already
match. Do not move consumer internals into the pipeline, export consumer row
models, or introduce interfaces with only one implementation.
