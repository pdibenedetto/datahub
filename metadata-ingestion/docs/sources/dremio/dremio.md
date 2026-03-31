### Configuration for Dremio Cloud

```yaml
source:
  type: dremio
  config:
    authentication_method: PAT
    password: <your_api_token>
    is_dremio_cloud: true
    dremio_cloud_project_id: <project_id>

    include_query_lineage: true

    # Emit lineage and properties as PATCH to preserve manual UI edits.
    incremental_lineage: true
    incremental_properties: false

    # Enable checkpointing for stale entity removal and time-window deduplication.
    stateful_ingestion:
      enabled: true
    enable_stateful_time_window: true

    # Optional: map Dremio sources to external platforms for cross-source lineage
    source_mappings:
      - platform: s3
        source_name: samples

    # Optional: restrict ingestion to specific schemas or datasets
    schema_pattern:
      allow:
        - "<source_name>.<table_name>"

sink:
  # Define your sink configuration here
```

### Required Permissions

For Dremio Cloud instances, configure the following project-level privileges for your user account:

```sql
-- Required: Access to containers and their metadata
GRANT READ METADATA ON <project_name> TO USER <username>

-- Required: Access to system tables for dataset metadata
GRANT SELECT ON <project_name> TO USER <username>

-- Optional: For data profiling (only if profiling is enabled)
GRANT SELECT ON SOURCE <source_name> TO USER <username>
GRANT SELECT ON SPACE <space_name> TO USER <username>

-- Optional: For query lineage (only if include_query_lineage: true)
GRANT VIEW JOB HISTORY ON <project_name> TO USER <username>
```

**Notes**:

- In Dremio Cloud, replace `SYSTEM` with your project name when granting privileges
- `READ METADATA` is required for container discovery via API calls
- `SELECT ON <project_name>` is required for accessing system tables
