# Selective CI for Integration Tests

The selective CI system runs only the integration tests relevant to the connectors changed in a PR, instead of running all 8 integration test batches every time.

## How it works

The `scripts/selective_ci_checks.py` script analyzes the list of changed files in a PR and decides which integration tests to run:

1. **If any file outside `source/{connector}/` changed** (e.g. `api/`, `emitter/`, `setup.py`, `metadata-models/`), it runs the full test suite — these are core changes that could affect any connector.

2. **If only connector source files changed**, it builds a targeted test matrix:
   - Each changed connector's integration tests are included.
   - If a connector imports from another connector (e.g. `bigquery_v2` imports from `sql/`), changing `sql/` also triggers `bigquery_v2` tests. Dependencies are resolved automatically by parsing Python imports with `ast`.

3. **Test-only or script-only changes** skip integration tests entirely (the source hasn't changed, so results would be identical).

## Connector discovery

Connectors are discovered automatically by **convention**: if `source/{name}/` exists and `tests/integration/{name}/` exists, the connector is registered with `test_path: tests/integration/{name}/`. No configuration needed.

For the ~37 connectors where source and test directory names match (powerbi, bigquery_v2, snowflake, dbt, kafka, etc.), this just works.

## When you need `connector.yaml`

A `connector.yaml` file in the source directory is only required for exceptions:

### Non-matching directory names

If the test directory name differs from the source directory name:

```yaml
# src/datahub/ingestion/source/redshift/connector.yaml
test_path: tests/integration/redshift-usage/
```

### Extra source paths

If a connector's source spans multiple directories:

```yaml
# src/datahub/ingestion/source/powerbi/connector.yaml
extra_source_paths:
  - src/datahub/ingestion/source/powerbi_report_server/
```

### Extra test paths

If a connector's tests span multiple directories:

```yaml
# src/datahub/ingestion/source/looker/connector.yaml
extra_test_paths:
  - tests/integration/lookml/
```

### Shared bases

If a source directory is a shared base class used by many connectors (like `sql/`), it needs to declare the test directories for connectors whose source lives under it:

```yaml
# src/datahub/ingestion/source/sql/connector.yaml
is_shared_base: true
test_paths:
  - tests/integration/clickhouse/
  - tests/integration/mysql/
  - tests/integration/postgres/
  # ... etc
```

Connectors that have their own source directory (like `bigquery_v2/`) and import from `sql/` are handled automatically via import analysis — they don't need to be listed here.

## Adding a new connector

### If your test directory name matches your source directory name

Nothing to do. Create `source/myconnector/` and `tests/integration/myconnector/`, and the selective CI system will discover it automatically.

### If your test directory name differs

Create a `connector.yaml` in your source directory:

```yaml
# src/datahub/ingestion/source/myconnector/connector.yaml
test_path: tests/integration/my-connector-tests/
```

### If your connector imports from a shared base

Nothing to do. The import analysis will detect this automatically. When the shared base changes, your connector's tests will be triggered.

## Running the script locally

```bash
# See what would run for a given set of changes
cd metadata-ingestion
python scripts/selective_ci_checks.py --dry-run --base-ref master

# Validate all connector.yaml mappings
python scripts/selective_ci_checks.py --validate

# Force full suite (used by the run-all-tests label)
python scripts/selective_ci_checks.py --force-all
```

## How import analysis works

The `build_import_graph()` function:

1. Scans every `.py` file in each connector's source directory
2. Parses `import` and `from ... import` statements using Python's `ast` module
3. Maps imported modules back to known connector source directories
4. Builds a dependency graph: `connector_a → {connector_b, connector_c}`

When `connector_b` changes, the graph is consulted to find all connectors that import from it, and their tests are added to the matrix.

This catches non-obvious dependencies like `dbt` importing `sql/sql_types.py` — something a human maintaining a manual dependency list would likely miss.

## Safe defaults

- **Unknown files** under `source/` that don't belong to any known connector trigger the full suite.
- **Non-connector files** (api/, emitter/, setup.py, metadata-models/) always trigger the full suite.
- The `run-all-tests` label on a PR forces the full suite regardless of changes.
- If the detection script crashes, the gate job fails the PR — it never silently passes.

## `connector.yaml` reference

| Field                | Required              | Description                                                                           |
| -------------------- | --------------------- | ------------------------------------------------------------------------------------- |
| `test_path`          | No                    | Override the default test directory (default: `tests/integration/{source_dir_name}/`) |
| `extra_source_paths` | No                    | Additional source directories covered by this connector's tests                       |
| `extra_test_paths`   | No                    | Additional test directories for this connector                                        |
| `is_shared_base`     | No                    | Mark as a shared base class (like `sql/`)                                             |
| `test_paths`         | Only for shared bases | List of test directories to expand when this base changes                             |
