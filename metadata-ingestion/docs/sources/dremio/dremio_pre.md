### Setup

This integration extracts metadata directly from Dremio using both REST API calls and SQL queries.

**Prerequisites:**

- A running Dremio instance with API access enabled
- A user account with appropriate privileges (detailed below)
- Network connectivity from DataHub to your Dremio instance

#### Steps to Get the Required Information

1. **Generate a Personal Access Token**:

   - Log in to your Dremio instance
   - Navigate to your user profile (top-right corner)
   - Select **Generate API Token** to create a Personal Access Token for programmatic access
   - Save the token securely as it will only be displayed once

2. **Configure Required Permissions**:

   Your user account needs specific Dremio privileges for the DataHub connector to function properly:

   **Container Access (Required for container discovery)**

   ```sql
   -- Grant access to view and use sources and spaces
   GRANT READ METADATA ON SOURCE <source_name> TO USER <username>
   GRANT READ METADATA ON SPACE <space_name> TO USER <username>

   -- Or grant at system level for all containers
   GRANT READ METADATA ON SYSTEM TO USER <username>
   ```

   **System Tables Access (Required for metadata extraction)**

   ```sql
   -- Grant access to system tables for dataset metadata
   GRANT SELECT ON SYSTEM TO USER <username>
   ```

   **Dataset Access (Optional - only needed for data profiling)**

   ```sql
   -- For data profiling (only if profiling is enabled)
   GRANT SELECT ON SOURCE <source_name> TO USER <username>
   GRANT SELECT ON SPACE <space_name> TO USER <username>
   ```

   **Query Lineage Access (Optional - only if include_query_lineage: true)**

   ```sql
   -- Required for query lineage extraction
   GRANT VIEW JOB HISTORY ON SYSTEM TO USER <username>
   ```

   **What each permission does:**

   - `READ METADATA`: Access to view containers and their metadata via API calls
   - `SELECT ON SYSTEM`: Access to system tables (`SYS.*`, `INFORMATION_SCHEMA.*`) for dataset metadata
   - `SELECT ON SOURCE/SPACE`: Read actual data for profiling (optional)
   - `VIEW JOB HISTORY`: Access query history tables for lineage (optional)

3. **Verify External Data Source Access**:

   If your Dremio instance connects to external data sources (AWS S3, databases, etc.), ensure that:

   - Dremio has proper credentials configured for those sources
   - The DataHub user can access datasets from those external sources
   - Network connectivity exists between Dremio and external sources

### Stateful Ingestion

Enabling `stateful_ingestion` unlocks three incremental capabilities that reduce API load on repeated runs:

| Feature                   | Config key                                | What it does                                                                                                                                                          |
| ------------------------- | ----------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Stale entity removal      | `stateful_ingestion.enabled: true`        | Removes entities from DataHub that no longer exist in Dremio                                                                                                          |
| Time-window deduplication | `enable_stateful_time_window: true`       | Advances the query lineage start time to the previous run's end time, so `SYS.JOBS_RECENT` is never re-processed                                                      |
| Incremental profiling     | `profiling.profile_if_updated_since_days` | Skips re-profiling tables that DataHub profiled within the configured window (compared against last-profiled time, since Dremio has no table modification timestamps) |

```yaml
stateful_ingestion:
  enabled: true

enable_stateful_time_window: true # only process new job history each run

profiling:
  enabled: true
  profile_if_updated_since_days: 1 # re-profile at most once per day
```

### Protecting UI Edits Between Runs

By default, each ingestion run overwrites the full `DatasetProperties` aspect, which resets any
descriptions or custom properties you have edited in the DataHub UI. Set
`incremental_properties: true` to emit properties as PATCH operations instead, so only the
fields the connector knows about are updated and your manual edits are preserved.

This is particularly useful for Dremio, which often acts as a semantic layer on top of raw
sources that teams re-document in DataHub.

```yaml
incremental_properties: true
```

Similarly, `incremental_lineage: true` (the default) emits lineage as PATCH operations, so
manually-curated lineage edges added in the DataHub UI are not removed on the next run.
