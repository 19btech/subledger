# AGENTS.md — Dataloader Codebase Guidance

## Quick Facts
- **Type**: Spring Boot 3.4.2 microservice (Java 21)
- **Architecture**: Streaming model execution engine with Pulsar messaging
- **DB**: MongoDB (multi-tenant, tenant-scoped queries)
- **Port**: 8081
- **Key Pattern**: Per-tenant execution locks + event-driven batch processing

---

## 🏗 Architecture Overview

### Core Data Flow
```
REST API (/api/dataloader/model/execute)
    ↓
ModelController.executeModel() / executeDslModel()
    ↓
ModelExecutionService.prepare*Execution()
    ↓
ExcelModelService.generateEventAndDispatch(callback)
    ↓ (page-by-page callback)
ModelExecutionService.dispatch*Batch()
    ↓
PulsarTemplate → ModelExecutionProducer / PythonModelExecutionProducer
    ↓
Pulsar Topic (fyntrac-model-execution / fyntrac-python-model-execution)
```

**Memory Model**: Only ONE page of Set<String> instrumentIds lives in heap at a time.  
**Scaling**: Handles millions of instruments without accumulation (streaming + virtual thread batches).

### Two Model Execution Paths
1. **Excel Path** (`/execute`): Synchronous dispatch via `ModelExecutionProducer`
   - Uses `postModelExecutionMessage()` → stores instruments in Memcached with hash key
   - Logs to `ModelExecutionBatchLog` (logType=`EXECUTION_BATCH`, modelType=`EXCEL`)

2. **Python/DSL Path** (`/execute/dsl`): Async dispatch via `PythonModelExecutionProducer`
   - Direct instrument list in message (`Records.PythonModelExecutionMessageRecord`)
   - Same batch logging pattern (modelType=`PYTHON`)

### Key State Objects
- **Streaming State** (fields: `streamJobId`, `streamTenant`, `streamPostingDate`, `streamPageCounter`)
  - Initialized by `prepare*Execution()`, persisted across callback invocations
  - NOT ThreadLocal — survives the HTTP request boundary
  - Finalized by `finalize*Execution()` (writes EXECUTION_SUMMARY log)

- **Execution Lock** (per-tenant `ReentrantLock`)
  - Enforces single active execution per tenant (returns HTTP 409 if conflict)
  - Released in `finalize*Execution()` or exception handler
  - Use `tryAcquireExecutionLock(tenant)` → `releaseExecutionLock(tenant)` pattern

---

## 🔑 Critical Patterns

### Multi-Tenancy Enforcement
- **All queries** must be wrapped: `TenantContextHolder.runWithTenant(tenant, () -> { ... })`
- Tenant ID source: request header (filter extracts and stores in ThreadLocal via TenantContextHolder)
- **Violation Impact**: Data leakage across tenants or silent failures

### Batch Logging & Progress Tracking
Repository method naming convention:
```java
batchLogRepository.countByTenantIdAndPostingDateAndLogType(tenant, postingDate, "EXECUTION_BATCH")
batchLogRepository.countByTenantIdAndPostingDateAndLogTypeAndStatus(tenant, postingDate, "EXECUTION_BATCH", "SUCCESS")
batchLogRepository.findByTenantIdAndPostingDateAndLogType(tenant, postingDate, "EXECUTION_BATCH")
```
- **logType**: `"EXECUTION_BATCH"` (per-page) or `"EXECUTION_SUMMARY"` (final aggregate)
- **status**: `"SUCCESS"`, `"FAILED"`, `"PARTIAL_SUCCESS"`
- UI polls `/execution-progress` every few seconds to show completion %

### Data Cleanup Before Re-Execution
Method: `ModelExecutionService.cleanupDataForPostingDate(postingDate)`
- Deletes via repository pattern: `repository.deleteByPostingDate(postingDate)`
- Affected collections: TransactionActivity, InstrumentAttribute, Event, GeneralLedgerEnteryStage, AttributeLevelLtd, InstrumentLevelLtd, MetricLevelLtd
- **Currently commented out in controller** (see line 256) — enable if re-execution cleanup needed

### Caching Instrument Counts (Across HTTP Requests)
In-memory cache key format: `"tenantId:postingDate"` → count
- Cold cache hit queries: `eventRepository.countDistinctInstrumentsByPostingDate(postingDate)`
- Populated by `ModelExecutionService.prepareExcelExecution()` and `preparePythonExecution()`
- Used to compute `totalExpectedBatches = ceil(distinctInstruments / pageSize)`
- Seeded atomically via `ConcurrentHashMap.computeIfAbsent()` — DB hit only once per key

---

## 🛠 Developer Workflows

### Build
```bash
cd /home/uabbas/Workspace/subledger/subledger/dataloader

# Standard build
./gradlew clean build

# Build skipping tests
./gradlew clean build -Dtest.skip=true

# Run specific test with debug
./gradlew test -DdebugTest
```

### Run Locally
```bash
# Set required environment variables
export GIT_USER=<github_username>
export GIT_KEY=<github_token>
export ZITADEL_ISSUER_URI=https://fyntrac-auth-mlmc7i.us1.zitadel.cloud
export ZITADEL_PROJECT_ID=368586804800002714

# Start service (port 8081)
./gradlew bootRun
```

### Docker Build (Multi-Platform)
```bash
# Builds for linux/amd64 and linux/arm64
./gradlew dockerPushImage

# Custom platform
./gradlew dockerBuildImage -Pplatform=linux/amd64
```

### Debug Model Execution Locally
1. Set breakpoint in `ModelExecutionService.dispatchExcelBatch()` or `dispatchPythonBatch()`
2. Call `POST /api/dataloader/model/execute` with date JSON
3. Virtual thread callbacks will hit your breakpoint

---

## 📊 Key Dependencies & Integration Points

### Imported from `commons` (com.fyntrac.common)
- **Entities**: `Model`, `ModelConfig`, `Event`, `ModelExecutionBatchLog`, `ExecutionState`, `InstrumentAttribute`
- **Repositories**: `EventRepository`, `ModelExecutionBatchLogRepository`, `TransactionActivityRepository`
- **Services**: `ExcelModelService` (generates events + callbacks), `InstrumentAttributeService`, `ModelService`
- **Utils**: `TenantContextHolder`, `DateUtil`, `StringUtil`, `RecordFactory`
- **DTOs**: `Records.*Record` (message envelope types)

### Pulsar Topics (from application.properties)
| Topic | Producer | Consumer | Message Type |
|-------|----------|----------|--------------|
| `fyntrac-model-execution` | ModelExecutionProducer | (external) | ModelExecutionMessageRecord |
| `fyntrac-python-model-execution` | PythonModelExecutionProducer | (external) | PythonModelExecutionMessageRecord |
| `fyntrac-book-gl-staging` | GeneralLedgerMessageProducer | EventHistoryResultConsumer | GeneralLedgerMessageRecord |
| `fyntrac-aggregate-execution` | (local producer) | ExecuteAggregationConsumer | ExecuteAggregationMessageRecord |

### MongoDB Collections (Tenant-Scoped)
- **Event**: Raw events before processing (indexed: tenantId, postingDate, status)
- **ModelExecutionBatchLog**: Execution progress/debugging (compound index: tenantId, postingDate, logType, status)
- **TransactionActivity, InstrumentAttribute, GeneralLedgerEnteryStage, AttributeLevelLtd, InstrumentLevelLtd, MetricLevelLtd**: Derived data (all deleted by `cleanupDataForPostingDate`)

---

## 🚩 Common Pitfalls & Edge Cases

1. **Forgetting TenantContextHolder.runWithTenant()** → queries silently fail or return empty results
2. **Modifying instrumentCountCache without invalidation** → stale progress percentages
3. **Not releasing execution lock on exception** → tenant permanently locked out (wrap in try-finally)
4. **Assuming pageSize == batch size** → they're independent (pageSize from config, batch from callback)
5. **Concurrent calls to prepare*/finalize*** → set fields will race (not thread-safe by design — expect single HTTP thread per execution)
6. **Batch log queries without logType filter** → includes EXECUTION_SUMMARY; must filter for EXECUTION_BATCH when monitoring progress

---

## 📝 Configuration Points
- **`fyntrac.chunk.size`**: Page size for instrument fetching + batch size (**check application-dev.properties**)
- **`spring.pulsar.producer.topic-execute-***`**: Pulsar topic names (configurable per environment)
- **OAuth2 issuer/audience**: Zitadel JWT validation (environment variables in application.properties)

---

## 🔍 Files to Know
- **ModelExecutionService.java** (~855 lines): Core execution orchestration, locking, caching, progress endpoint
- **ModelController.java**: REST API endpoints for upload, execute, progress polling
- **ExcelModelService** (imported from commons): Generates events + invokes callback batches
- **ModelExecutionBatchLog**: Entity for execution telemetry (queryable for real-time UI)
- **build.gradle**: Gradle config (note: multi-platform Docker buildx setup, Pulsar version pinning)


