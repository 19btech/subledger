# Dataloader Scaling Strategy — k3s Load Balancing + Multi-Million-Record File Loads

Goal: move the fyntrac microservices (currently run via `fyntrac-data/.../run-fyntrac-docker-images.sh` +
`fyntrac-docker-compose.yml`) onto Kubernetes — k3s locally on Fedora first, portable to AWS (self-managed
k3s on EC2 or EKS) later — and specifically make `dataloader` able to ingest a single CSV file with
millions of rows in a bounded, horizontally-scalable way.

Status: **design finalized, nothing implemented yet.** Implement in the stage order below — each stage is
independently shippable and Stage 0 changes the measured baseline the later stages are designed against.

---

## 0. Baseline reality check (source: `AccountingRuleController`, `FileUploadService`,
`TransactionsDataLoadConfig`, `TransactionActivityDataLoadConfig`, `application.properties`)

| # | Finding | Where | Impact |
|---|---------|-------|--------|
| 1 | No `spring.servlet.multipart.max-file-size` / `max-request-size` set | `application.properties` | Spring Boot defaults (1MB file / 10MB request) reject a multi-million-row CSV with `413` before any app code runs. |
| 2 | `handleFileUpload` calls `fileUploadService.uploadFiles(...)` synchronously on the request thread | `AccountingRuleController` | HTTP thread blocks until unzip → CSV convert → validate → **all** Batch jobs finish → Mongo writes complete. Multi-minute blocking call, one pod, one thread. |
| 3 | Batch commit chunk = 10 | `TransactionsDataLoadConfig`, `TransactionActivityDataLoadConfig` | 1M rows ⇒ 100,000 separate Mongo round trips via `MongoItemWriter`. Dominant cost today. |
| 4 | Uploaded/converted files staged under `user.home` (`~/tenants/<tenant>/...`, `~/output/tenants/<tenant>/...`) | `FileUploadService` | Pod-local ephemeral disk. Self-consistent today (one request stays on one pod start-to-finish) but blocks any future cross-pod chunk processing, and progress is lost if the pod restarts mid-job. |
| 5 | `ActivityUploadService.uploadActivity` runs `instrumentAttributeUploadJob` then, only if it completed, `transactionActivityUploadJob` — both share one `Batch` entity (`batchId`, `activityCount`) that GL/reporting key off of | `ActivityUploadService` | Real cross-file ordering dependency to preserve in any chunked/parallel design. |

None of #1–#4 are Kubernetes problems — they reproduce identically under plain Docker. k3s (and later an AWS
ALB) makes #2 worse because the ingress/load balancer in front has its own idle/response timeouts (Traefik's
transport keep-alive locally; ALB defaults to 60s idle) that a multi-minute synchronous request now has to
survive.

**Load-balancing nuance driving the whole design:** a k8s `Service` round-robins *new connections* across
replicas. That parallelizes N different users uploading N different files. It does **nothing** for a single
huge file — that request is pinned to whichever pod accepted it for the entire synchronous pipeline above.
Speeding up *one* file requires explicitly fanning its work across pods (Stage 2), not just adding replicas.

---

## Stage 0 — Fix the real bottleneck (code only, ship first, no k8s required)

1. **Multipart limits** — set generous limits with disk spillover so Tomcat streams to a temp file instead
   of buffering in memory:
   ```properties
   spring.servlet.multipart.max-file-size=1GB
   spring.servlet.multipart.max-request-size=1GB
   spring.servlet.multipart.file-size-threshold=10MB
   ```
2. **Batch chunk size** — raise `chunk(10, ...)` to `chunk(1000, ...)` (start here, tune from measured
   throughput) in `TransactionsDataLoadConfig`, `TransactionActivityDataLoadConfig`, and the other
   `MongoItemWriterBuilder`-based configs handling high-row-count rules. Consider switching those
   `MongoItemWriter`s to unordered bulk writes (`BulkOperations.BulkMode.UNORDERED`, as already used in
   `ReplayStateComputationTasklet`) since independent row inserts don't need ordering guarantees.
3. **Async upload endpoint** — `AccountingRuleController.handleFileUpload`/`handleFileUploadOverwrite`
   should hand off to an `@Async` executor and return `202 Accepted` + `uploadId` immediately; add a
   `GET /api/dataloader/upload/status/{uploadId}` endpoint (backed by the existing `ActivityLog`/`Batch`
   records) for the client to poll. Removes the "ingress killed my connection" failure mode entirely and
   is a prerequisite for Stage 2's async chunk orchestration anyway.

Re-measure single-pod throughput after Stage 0 before deciding how aggressively Stage 2 is needed.

---

## Stage 1 — k3s baseline + shared storage

- **Local distro:** k3s as a native systemd service on Fedora (not a VM/Docker-in-Docker) — ships with
  Traefik ingress and a built-in `LoadBalancer` implementation (ServiceLB) out of the box, and is
  CNCF-conformant so manifests port unchanged to EKS or self-managed k3s on EC2.
- **Manifest tooling:** Kustomize `base/` + `overlays/{local,aws}` — one Deployment/Service pair per
  fyntrac service, environment differences (storage class, ingress class, image registry creds, secret
  source) isolated to small overlay patches.
- **Critical change vs. today's compose setup:** stop staging files under pod-local `user.home`. Any pod
  in the Deployment must be able to read any uploaded/chunked file, both because Stage 2 splits work across
  pods and because a pod can be rescheduled mid-job.
  - Local k3s: a `ReadWriteMany` PVC via `nfs-subdir-external-provisioner` (simplest to stand up on a
    single Fedora box), or a small in-cluster MinIO deployment exposing an S3-compatible API.
  - AWS: MinIO's S3 API maps directly onto a real S3 bucket — prefer the **MinIO/S3-API path over NFS**
    now specifically so the file-storage code (once written against the S3 API) doesn't change when you
    move to AWS; only the endpoint/credentials change via the `aws` overlay.
- Run `dataloader` with 2–3 replicas behind its Service so concurrent uploads from different users actually
  parallelize (the case plain load balancing helps with today).
- `mongodb`/`pulsar` stay single-instance for the local dev cluster initially (StatefulSet + PVC each);
  clustering them is a later concern, not blocking this work.

---

## Stage 2 — Parallelize a single file across pods (the "slice the file" mechanism)

**Pull-based, not static assignment.** Do not hand chunk N to pod N directly — that requires addressing
individual pods (fights the Service abstraction, needs a `StatefulSet`), doesn't adapt to uneven pod load
(straggler problem), and doesn't cooperate with Stage 3 autoscaling. Instead, publish chunk jobs to a Pulsar
topic on a **shared subscription**; every `dataloader` replica is a consumer on that same subscription, so
Pulsar hands each chunk to whichever consumer is next available, rebalances automatically as replicas
scale, and redelivers a chunk if the consumer holding it dies before acking.

### 2.1 Splitting

- Runs on the pod that received the upload, after today's existing Excel→CSV conversion and upfront
  `validatePostingDateInFile` checks (unchanged — those still validate the whole file once, cheaply, before
  any chunking work happens).
- Must split on **CSV record boundaries via `CSVParser`** (already used in `TransactionsDataLoadConfig`/
  `BatchCommonConfig` for header parsing), not raw `BufferedReader.readLine()` — the existing tokenizer
  configuration (`DelimitedLineTokenizer.setQuoteCharacter('"')`) means a field can legally contain an
  embedded newline; a naive line-count split would corrupt such a row.
- Each chunk file gets the original header row re-written at the top, so the existing
  `FlatFileItemReader`/`DelimitedLineTokenizer` config in `TransactionsDataLoadConfig`/
  `TransactionActivityDataLoadConfig` works completely unmodified per chunk — Stage 2 changes *what file
  path* a job parameter points at, not the reader/processor/writer beans.
- Chunk files write to the Stage 1 shared storage (S3-API bucket), not pod-local disk.
- **Sizing:** target chunk count ≈ 3–4× the number of pods expected to be handling the job (e.g. 20–30
  chunks even for an 8-pod ceiling), not 1:1 — so a pod that finishes early pulls another chunk instead of
  the whole job waiting on one large straggler chunk. ~50k rows/chunk is a reasonable starting point for a
  1M-row file; tune from Stage 0's re-measured single-chunk throughput.

### 2.2 Queueing

New topic, following the existing `spring.pulsar.producer.topic-*` naming convention already in
`application.properties`:
```properties
spring.pulsar.producer.topic-dataloader-chunk-job=fyntrac-dataloader-chunk-job
spring.pulsar.consumer.chunk-job.subscription.name=fyntrac-dataloader-chunk-consumers
spring.pulsar.consumer.chunk-job.subscription.type=Shared
```
New message record alongside the existing ones in `com.fyntrac.common.dto.record.Records`:
```java
record ChunkJobMessageRecord(
    long uploadId,
    long batchId,        // ties back to the existing Batch entity
    String rule,          // AccountingRules name — TRANSACTIONACTIVITY, INSTRUMENTATTRIBUTE, etc.
    String chunkFilePath, // shared-storage path
    int chunkIndex,
    int totalChunks
) {}
```

### 2.3 Consuming

Each `dataloader` replica runs a Pulsar listener on the shared subscription. On receiving a
`ChunkJobMessageRecord`, it builds `JobParameters` exactly as `ActivityUploadService.createInstrumentAttributeJob`/
`createTransactionActivityJob` already do, substituting `chunkFilePath` for `filePath`, and calls
`jobLauncher.run(...)` on the **same existing Job beans** (`instrumentAttributeUploadJob`,
`transactionActivityUploadJob`, `transactionsUploadJob`, …) — zero changes to the Batch step/processor/writer
configs themselves. Only ack the Pulsar message after `JobExecution.getStatus() == COMPLETED`; let it
redeliver on failure/pod death.

### 2.4 Completion tracking and ordering (extends the existing `Batch` entity)

`ActivityUploadService` already creates one `Batch` row per upload (`batchId`, `batchStatus`,
`activityCount`) that GL/reporting read to know when a batch's activity is fully posted. Extend it rather
than add a parallel mechanism:
- Add `totalChunks` / `completedChunks` (per rule) to `Batch`, set when chunks are published.
- Each chunk consumer does an atomic `$inc completedChunks` on ack; when `completedChunks == totalChunks`
  for a rule, that rule is done.
- **Preserve the existing cross-rule ordering**: don't publish `TRANSACTIONACTIVITY` chunks until every
  `INSTRUMENTATTRIBUTE` chunk for that `batchId` reports complete — mirrors today's sequential
  `instrumentAttributeUploadJob` → `transactionActivityUploadJob` dependency in `uploadActivity`.
- `batchStatus` flips to `PENDING → COMPLETED` only once the last rule's chunks all complete, which is what
  today's post-processing (reversal/replay jobs launched from
  `InstrumentAttributeJobCompletionListener`/`TransactionActivityJobCompletionListener`) should gate on —
  those still run once per upload, not once per chunk.

### 2.5 Data integrity guarantees (no loss, no silent duplication)

`TransactionActivity`/`InstrumentAttribute` have **no unique/business-key index** — `id` is a bare
Mongo `@Id`, auto-generated on every `save()`. `MongoItemWriter` therefore has zero built-in
duplicate protection today: the same row written twice becomes two indistinguishable documents.
"No data loss" and "no silent duplication" don't fall out of careful splitting alone — they need to
be engineered explicitly.

**Loss from a buggy split.** Split with a single `CSVParser` pass — one iterator, every record
written to exactly one chunk file, never re-read or skipped. Before publishing anything, re-open and
independently re-count every written chunk file's data rows and compare the sum against the row
count from the read pass; a mismatch aborts the whole upload (publish zero Pulsar messages, mark
`FAILED`, leave the original file untouched) rather than proceeding on a possibly-corrupt split.
Chunks are written and verified **before** any message is published — never interleaved — so a
mid-split crash (OOM, disk full) leaves nothing queued and the source file intact to retry from
scratch, instead of a partial, silently-incomplete load.

**Loss from discarding the source too early.** The original uploaded file stays on shared storage
until `Batch.completedChunks == totalChunks` for every rule in that upload — i.e. fully confirmed
done. If anything fails mid-flight, the source is still there to re-split and retry.

**Duplication from at-least-once delivery.** Pulsar's shared subscription redelivers a chunk if its
consumer crashes after finishing the write but before acking — which would blindly re-insert every
row in that chunk, since nothing rejects the duplicate at the data layer. Layered mitigation:
1. Ack only after both the Batch step reports `COMPLETED` *and* the `completedChunks` increment on
   `Batch` is durably recorded — shrinks the crash window, doesn't eliminate it.
2. Each consumer checks whether its `chunkIndex` is already marked complete for that `batchId`
   *before* running the Batch job, not just before acking — makes redelivery a safe no-op even after
   the window above.
3. Belt-and-suspenders reconciliation when a `Batch` completes: compare its expected total row count
   (summed from chunk metadata) against `db.TransactionActivity.count({batchId})`. A mismatch flags
   for manual review instead of silently shipping duplicated GL data.

Note: this is distinct from today's existing validation-skip behavior (`.faultTolerant().skip(...)`
on `ItemValidationException`, logged via `ValidationLoggingListener`) — a row failing business
validation and being deliberately skipped is expected, unchanged by chunking, and already visible in
the activity/validation logs. The guarantees above are about the *mechanical* split/queue/write path
never losing or duplicating a row that should have reached (or been skipped by) that same validation
step exactly once.

---

## Stage 3 — Autoscale to the burst, not the steady state

- Add an HPA, or KEDA with the [Pulsar scaler](https://keda.sh/docs/latest/scalers/apache-pulsar/) watching
  backlog on `fyntrac-dataloader-chunk-job`'s subscription, so `dataloader` replicas scale (e.g. 1 → 8) only
  while chunk jobs are queued, and back down once the backlog drains.
- This is the actual "load balancing" payoff for the million-row-file use case — distributing chunk *work*
  across an elastic pod count, not distributing HTTP *requests*.

---

## AWS portability — what changes vs. what doesn't

| Layer | Local (k3s/Fedora) | AWS | Changes needed |
|---|---|---|---|
| Cluster | k3s systemd service | Self-managed k3s on EC2, or EKS | Only the `aws` Kustomize overlay (ingress class, storage class, node sizing) |
| Ingress/LB | Traefik + k3s ServiceLB | AWS Load Balancer Controller → ALB/NLB | Overlay-only |
| File storage | MinIO (S3 API) in-cluster | Real S3 bucket | Endpoint + credentials only, if chunker/consumer coded against the S3 API from the start |
| Block storage (Mongo/Pulsar PVCs) | `local-path` | EBS CSI | Storage class only |
| Messaging (Pulsar topic/shared-subscription chunking) | unchanged | unchanged | none — this is the piece designed to be infra-agnostic |
| Batch Job/Step configs | unchanged | unchanged | none — Stage 2 deliberately reuses them as-is |

---

## Implementation order

1. Stage 0 (code, ship first, re-measure baseline).
2. Stage 1 (k3s + shared storage), in parallel with or right after Stage 0.
3. Stage 2 (chunking + Pulsar fan-out + `Batch` completion tracking) — the largest piece, several days of
   work touching `FileUploadService`, `ActivityUploadService`, a new chunker utility, and a new Pulsar
   consumer.
4. Stage 3 (HPA/KEDA) — small, layers on top of Stage 2 once chunk-topic backlog exists to scale on.
