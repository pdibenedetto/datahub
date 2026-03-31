# Cascade Observability Extension Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend the existing `CascadeOperationContext` in `DeleteEntityService.deleteReferencesTo()` to cover all three deletion phases (files, search refs, graph refs), and propagate cascade operation IDs onto MCPs for downstream Kafka correlation.

**Architecture:** The cascade context created at line 136 is moved to the top of the live (non-dry-run) path, wrapping all three phases. The `cascade` parameter is threaded through `deleteFileReferences()` and `deleteSearchReferences()` via method signature changes. MCPs generated in `updateAspect()` and `deleteSearchReferences` get `SystemMetadata` stamped with the cascade operation ID.

**Tech Stack:** Java 17, Spring, Micrometer (MetricUtils), SLF4J MDC, TestNG + Mockito

---

### Task 1: Restructure `deleteReferencesTo()` — lift cascade context

**Files:**

- Modify: `metadata-service/services/src/main/java/com/linkedin/metadata/entity/DeleteEntityService.java:43,85-184`

- [ ] **Step 1: Add `SystemMetadata` import**

Add to the imports block (after line 42, the `MetadataChangeProposal` import):

```java
import com.linkedin.mxe.SystemMetadata;
```

- [ ] **Step 2: Restructure `deleteReferencesTo()` method**

Replace lines 85-185 (the entire `deleteReferencesTo` method) with:

```java
  public DeleteReferencesResponse deleteReferencesTo(
      @Nonnull OperationContext opContext, final Urn urn, final boolean dryRun) {
    // TODO: update DeleteReferencesResponse to have searchAspects and provide more helpful comment
    // in CLI
    final DeleteReferencesResponse result = new DeleteReferencesResponse();

    if (dryRun) {
      return deleteReferencesToDryRun(opContext, urn, result);
    }

    try (CascadeOperationContext cascade =
        CascadeOperationContext.begin(_metricUtils, "deleteReferencesTo", urn, -1)) {

      // Phase 1: Delete file references (S3 objects + file entity soft-delete)
      int totalFileCount = deleteFileReferences(opContext, urn, false, cascade);

      // Phase 2: Delete search-based references (forms, structured properties)
      int totalSearchAssetCount = deleteSearchReferences(opContext, urn, false, cascade);

      // Phase 3: Delete graph-based references (scroll all incoming relationships)
      RelatedEntitiesScrollResult scrollResult =
          _graphService.scrollRelatedEntities(
              opContext,
              null,
              newFilter("urn", urn.toString()),
              null,
              EMPTY_FILTER,
              ImmutableSet.of(),
              newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING),
              Edge.EDGE_SORT_CRITERION,
              null,
              SCROLL_KEEP_ALIVE,
              BATCH_SIZE,
              null,
              null);

      final List<RelatedAspect> relatedAspects =
          scrollResult.getEntities().stream()
              .flatMap(
                  relatedEntity ->
                      getRelatedAspectStream(
                          opContext,
                          urn,
                          UrnUtils.getUrn(relatedEntity.getUrn()),
                          relatedEntity.getRelationshipType()))
              .limit(10)
              .collect(Collectors.toList());

      result.setRelatedAspects(new RelatedAspectArray(relatedAspects));
      result.setTotal(scrollResult.getNumResults() + totalSearchAssetCount + totalFileCount);

      int totalProcessed = 0;
      do {
        if (!scrollResult.getEntities().isEmpty()) {
          log.info(
              "Processing batch of {} references (total processed: {}, total: {})",
              scrollResult.getEntities().size(),
              totalProcessed,
              scrollResult.getNumResults());
          scrollResult
              .getEntities()
              .forEach(
                  entity -> {
                    deleteReference(opContext, urn, entity, cascade);
                    cascade.recordEntityProcessed();
                  });
          totalProcessed += scrollResult.getEntities().size();
        }

        String nextScrollId = scrollResult.getScrollId();
        if (nextScrollId == null) {
          break;
        }

        scrollResult =
            _graphService.scrollRelatedEntities(
                opContext,
                null,
                newFilter("urn", urn.toString()),
                null,
                EMPTY_FILTER,
                ImmutableSet.of(),
                newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING),
                Edge.EDGE_SORT_CRITERION,
                nextScrollId,
                SCROLL_KEEP_ALIVE,
                BATCH_SIZE,
                null,
                null);
      } while (true);
      log.info("Reference cleanup complete for {}: {} references processed", urn, totalProcessed);
    }

    return result;
  }

  /**
   * Dry-run path: collects counts and a preview of related aspects without creating a cascade
   * context or performing any mutations.
   */
  private DeleteReferencesResponse deleteReferencesToDryRun(
      @Nonnull OperationContext opContext, final Urn urn, final DeleteReferencesResponse result) {
    int totalFileCount = deleteFileReferences(opContext, urn, true, null);
    int totalSearchAssetCount = deleteSearchReferences(opContext, urn, true, null);

    RelatedEntitiesScrollResult scrollResult =
        _graphService.scrollRelatedEntities(
            opContext,
            null,
            newFilter("urn", urn.toString()),
            null,
            EMPTY_FILTER,
            ImmutableSet.of(),
            newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING),
            Edge.EDGE_SORT_CRITERION,
            null,
            SCROLL_KEEP_ALIVE,
            BATCH_SIZE,
            null,
            null);

    final List<RelatedAspect> relatedAspects =
        scrollResult.getEntities().stream()
            .flatMap(
                relatedEntity ->
                    getRelatedAspectStream(
                        opContext,
                        urn,
                        UrnUtils.getUrn(relatedEntity.getUrn()),
                        relatedEntity.getRelationshipType()))
            .limit(10)
            .collect(Collectors.toList());

    result.setRelatedAspects(new RelatedAspectArray(relatedAspects));
    result.setTotal(scrollResult.getNumResults() + totalSearchAssetCount + totalFileCount);
    return result;
  }
```

- [ ] **Step 3: Verify compilation**

Run:

```bash
./gradlew :metadata-service:services:compileJava 2>&1 | tail -5
```

This will fail because `deleteFileReferences` and `deleteSearchReferences` don't accept the `cascade` parameter yet. That's expected — we fix it in Task 2 and Task 3.

- [ ] **Step 4: Commit work-in-progress (do NOT commit yet — wait until Task 3 is done)**

We'll commit after all method signatures are updated so the code compiles.

---

### Task 2: Add cascade context to `deleteFileReferences()`

**Files:**

- Modify: `metadata-service/services/src/main/java/com/linkedin/metadata/entity/DeleteEntityService.java:794-832`

- [ ] **Step 1: Update `deleteFileReferences` signature and body**

Replace the existing `deleteFileReferences` method (lines 794-832) with:

```java
  private int deleteFileReferences(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn deletedUrn,
      final boolean dryRun,
      @Nullable final CascadeOperationContext cascade) {

    Filter filter = DeleteEntityUtils.getFilterForFileDeletion(deletedUrn);
    List<String> entityNames = ImmutableList.of(Constants.DATAHUB_FILE_ENTITY_NAME);

    int totalFileCount = 0;
    String scrollId = null;

    do {
      AssetScrollResult result =
          scrollForAssets(
              opContext, new AssetScrollResult(), filter, entityNames, scrollId, dryRun);

      totalFileCount += result.totalAssetCount;
      scrollId = dryRun ? null : result.scrollId;

      if (!dryRun) {
        result.assets.forEach(
            fileUrn -> {
              try {
                deleteFileAndS3Object(opContext, fileUrn, deletedUrn);
                if (cascade != null) {
                  cascade.recordEntityProcessed();
                }
              } catch (Exception e) {
                log.error(
                    "Failed to process file deletion for urn: {} referenced by deleted entity: {}",
                    fileUrn,
                    deletedUrn,
                    e);
                if (cascade != null) {
                  cascade.recordError("file_delete_failed");
                }
              }
            });
      }
    } while (scrollId != null);

    if (totalFileCount > 0) {
      log.info("Processed {} file(s) referencing deleted entity: {}", totalFileCount, deletedUrn);
    }

    return totalFileCount;
  }
```

---

### Task 3: Add cascade context to `deleteSearchReferences()` and `deleteSearchReferencesForAsset()`

**Files:**

- Modify: `metadata-service/services/src/main/java/com/linkedin/metadata/entity/DeleteEntityService.java:589-705`

- [ ] **Step 1: Update `deleteSearchReferences` signature and body**

Replace the existing `deleteSearchReferences` method (lines 589-611) with:

```java
  private int deleteSearchReferences(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn deletedUrn,
      final boolean dryRun,
      @Nullable final CascadeOperationContext cascade) {
    int totalAssetCount = 0;
    String scrollId = null;
    do {
      AssetScrollResult assetScrollResult =
          getAssetsReferencingUrn(opContext, deletedUrn, scrollId, dryRun);
      List<Urn> assetsReferencingUrn = assetScrollResult.assets;
      totalAssetCount += assetScrollResult.totalAssetCount;
      // if it's a dry run, exit early and stop looping over assets
      scrollId = dryRun ? null : assetScrollResult.scrollId;
      if (!dryRun) {
        assetsReferencingUrn.forEach(
            assetUrn -> {
              List<MetadataChangeProposal> mcps =
                  deleteSearchReferencesForAsset(opContext, assetUrn, deletedUrn, cascade);
              mcps.forEach(
                  mcp -> {
                    if (cascade != null) {
                      if (mcp.getSystemMetadata() == null) {
                        mcp.setSystemMetadata(new SystemMetadata());
                      }
                      cascade.attachToSystemMetadata(mcp.getSystemMetadata());
                    }
                    _entityService.ingestProposal(opContext, mcp, createAuditStamp(), true);
                  });
              if (cascade != null) {
                cascade.recordEntityProcessed();
              }
            });
      }
    } while (scrollId != null);
    return totalAssetCount;
  }
```

- [ ] **Step 2: Update `deleteSearchReferencesForAsset` signature and body**

Replace the existing `deleteSearchReferencesForAsset` method (lines 677-705) with:

```java
  private List<MetadataChangeProposal> deleteSearchReferencesForAsset(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn assetUrn,
      @Nonnull final Urn deletedUrn,
      @Nullable final CascadeOperationContext cascade) {
    // delete entities that should be deleted first
    if (shouldDeleteAssetReferencingUrn(assetUrn, deletedUrn)) {
      _entityService.deleteUrn(opContext, assetUrn);
    }

    List<MetadataChangeProposal> mcps = new ArrayList<>();
    List<String> aspectsToUpdate = getAspectsToUpdate(deletedUrn, assetUrn);
    aspectsToUpdate.forEach(
        aspectName -> {
          try {
            MetadataChangeProposal mcp =
                updateAspectForSearchReference(opContext, assetUrn, deletedUrn, aspectName);
            if (mcp != null) {
              mcps.add(mcp);
            }
          } catch (Exception e) {
            log.error(
                String.format(
                    "Error trying to update aspect %s for asset %s when deleting %s",
                    aspectName, assetUrn, deletedUrn),
                e);
            if (cascade != null) {
              cascade.recordError("search_ref_update_failed");
            }
          }
        });
    return mcps;
  }
```

- [ ] **Step 3: Verify compilation**

Run:

```bash
./gradlew :metadata-service:services:compileJava 2>&1 | tail -5
```

Expected: `BUILD SUCCESSFUL`

- [ ] **Step 4: Run spotless formatting**

Run:

```bash
./gradlew spotlessApply 2>&1 | tail -5
```

Expected: `BUILD SUCCESSFUL`

- [ ] **Step 5: Commit tasks 1-3**

```bash
git add metadata-service/services/src/main/java/com/linkedin/metadata/entity/DeleteEntityService.java
git commit -m "refactor(observability): extend cascade context to cover all deleteReferencesTo phases

Move CascadeOperationContext.begin() to wrap all three deletion phases
(files, search refs, graph refs). Thread cascade parameter through
deleteFileReferences() and deleteSearchReferences() for entity tracking
and error recording. Attach cascade operation ID to MCPs generated in
deleteSearchReferences for downstream Kafka correlation.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: Propagate cascade ID in `updateAspect()`

**Files:**

- Modify: `metadata-service/services/src/main/java/com/linkedin/metadata/entity/DeleteEntityService.java:416-450`

- [ ] **Step 1: Add cascade ID propagation to `updateAspect`**

In the `updateAspect` method, add two lines after the `proposal.setAspect(...)` call (line 428) and before the `auditStamp` creation (line 430):

```java
    // Attach cascade operation ID for cross-service correlation via Kafka
    proposal.setSystemMetadata(new SystemMetadata());
    cascade.attachToSystemMetadata(proposal.getSystemMetadata());
```

The full method after the change:

```java
  private void updateAspect(
      @Nonnull OperationContext opContext,
      Urn urn,
      String aspectName,
      RecordTemplate prevAspect,
      RecordTemplate newAspect,
      CascadeOperationContext cascade) {
    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(urn);
    proposal.setChangeType(ChangeType.UPSERT);
    proposal.setEntityType(urn.getEntityType());
    proposal.setAspectName(aspectName);
    proposal.setAspect(GenericRecordUtils.serializeAspect(newAspect));

    // Attach cascade operation ID for cross-service correlation via Kafka
    proposal.setSystemMetadata(new SystemMetadata());
    cascade.attachToSystemMetadata(proposal.getSystemMetadata());

    final AuditStamp auditStamp =
        new AuditStamp()
            .setActor(UrnUtils.getUrn(Constants.SYSTEM_ACTOR))
            .setTime(System.currentTimeMillis());
    final IngestResult ingestProposalResult =
        _entityService.ingestProposal(opContext, proposal, auditStamp, false);

    if (ingestProposalResult != null && !ingestProposalResult.isSqlCommitted()) {
      log.error(
          "Failed to ingest aspect with references removed. Before {}, after: {}, please check MCP processor"
              + " logs for more information",
          prevAspect,
          newAspect);
      handleError(
          new DeleteEntityServiceError(
              "Failed to ingest new aspect",
              DeleteEntityServiceErrorReason.MCP_PROCESSOR_FAILED,
              ImmutableMap.of("proposal", proposal)),
          cascade);
    }
  }
```

- [ ] **Step 2: Verify compilation**

Run:

```bash
./gradlew :metadata-service:services:compileJava 2>&1 | tail -5
```

Expected: `BUILD SUCCESSFUL`

- [ ] **Step 3: Run spotless formatting**

Run:

```bash
./gradlew spotlessApply 2>&1 | tail -5
```

- [ ] **Step 4: Commit**

```bash
git add metadata-service/services/src/main/java/com/linkedin/metadata/entity/DeleteEntityService.java
git commit -m "feat(observability): propagate cascade operation ID on MCPs in updateAspect

Stamp SystemMetadata with cascadeOperationId on MCPs generated by
updateAspect() during graph-ref deletion. Enables downstream Kafka
consumers to correlate these MCPs back to the triggering delete
operation via the shared cascade operation ID.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

### Task 5: Run existing tests and verify

**Files:**

- Test: `metadata-io/src/test/java/com/linkedin/metadata/entity/DeleteEntityServiceTest.java`

- [ ] **Step 1: Run DeleteEntityService tests**

Run:

```bash
./gradlew :metadata-io:test --tests "com.linkedin.metadata.entity.DeleteEntityServiceTest" 2>&1 | tail -15
```

Expected: All 10 existing tests pass. The constructor at test line 104 passes `null` for `MetricUtils`, so `_metricUtils` is null. `CascadeOperationContext.begin(null, ...)` is safe (metrics become no-ops). The dry-run tests exercise the new `deleteReferencesToDryRun` path with `cascade=null`. The live tests exercise the full cascade path with null `MetricUtils`.

- [ ] **Step 2: Run CascadeOperationContext tests**

Run:

```bash
./gradlew :metadata-utils:test --tests "com.linkedin.metadata.utils.metrics.CascadeOperationContextTest" 2>&1 | tail -10
```

Expected: All 14 tests pass (unchanged).

- [ ] **Step 3: Run full metadata-service compilation**

Run:

```bash
./gradlew :metadata-service:services:compileJava :metadata-service:factories:compileJava :metadata-io:compileJava :metadata-io:compileTestJava 2>&1 | tail -10
```

Expected: `BUILD SUCCESSFUL` — verifies no downstream compilation issues from the signature changes.

- [ ] **Step 4: Commit (no changes expected — verification only)**

No commit needed unless spotless made formatting changes. If it did:

```bash
git add -A && git commit -m "style: spotless formatting

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```
