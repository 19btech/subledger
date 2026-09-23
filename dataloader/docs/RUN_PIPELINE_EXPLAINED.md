# How a DSL model run works, end to end

Describes the code as it stands on `branch-0.0.4` after the 2026-09-23 scaling work. For what changed
and the measurements behind it, see `EVENT_GENERATION_SCALING_PLAN.md`; for the multi-pod design,
`TARGET_DESIGN_DISTRIBUTED_RUN.md`.

Measurements are tenant TNT004 / Hearst_GHK (~69k instruments), on the dev box.

---

## The stages

| stage | who runs it | what it produces |
|---|---|---|
| 1. Guard + pre-process | one dataloader pod (the run's owner) | a run record, and a clean slate if this date ran before |
| 2. Event generation | the owning pod, alone | `EventHistory` documents |
| 3. Model execution | the dsl-model pods, in parallel (`fyntrac-py-model`) | `TransactionActivity` rows |
| 4a. Attribute + instrument roll-up | the owning pod, per batch, concurrently | `AttributeLevelLtd`, `InstrumentLevelLtd` |
| 4b. GL sync | gl service, per batch | `GeneralLedgerEnteryStage`, `GeneralLedgerAccountBalanceStage` |
| 5. Metric roll-up | the owning pod, **once per posting date** | `MetricLevelLtd` |
| 6. End of day | the owning pod | carry-forward rows, run date advanced |

In code: `AbstractExecutionWorkflow.executeWorkflow` → `DslExecutionWorkflow`'s `preProcess`,
`generateAndProcessEvents` (stages 2–4), `postProcess` (stage 5) and `performEOD` (stage 6).

---

## 1 — Guard and pre-process

The request lands on whichever dataloader pod the Service picked. That pod owns the run to the end.

- **Closed period.** If the accounting period for this posting date has `status = 1`, the run is
  rejected. Closed books do not change.
- **Replay.** If `postingDate <= ExecutionState.executionDate`, this date has run before; earlier
  results are re-aggregated/re-booked and cleaned so a re-run cannot double-count.

## 2 — Event generation

An *event* is a question to ask the model about one instrument. Generation walks every active
instrument and, for each active `EventConfiguration`, decides whether that instrument has something to
answer for on this date (`ExcelModelService.generateEventAndDispatch`).

- **Paging.** A keyset cursor over `InstrumentAttribute` ordered by `instrumentId`; pages never split
  an instrument. Pages are processed **one after another**; within a page up to 50 instruments run at
  once on virtual threads.
- **Reference-table configs** (`triggerSource = reference_table`, e.g. `SSP_RULE → Accounting_Policy`)
  are written **once per (eventId, postingDate)**, before paging starts, under the sentinel
  instrumentId `__SHARED_REFERENCE__` (`Event.SHARED_REFERENCE_INSTRUMENT_ID`, fixed `_id`, so a
  re-run overwrites). Anything that loads an instrument's events must also load these
  (`EventRepository.findEventsForInstrument`); anything that counts or pages instruments must exclude
  the sentinel.
- **Custom-table configs** (`ON_CUSTOM_DATA_TRIGGER`, e.g. PSDLogs, Billing_Schedule) are **prefetched
  per page**: one `instrumentId $in [page]` query per mapping, grouped by (instrumentId, attributeId),
  instead of one query per sub-instrument. (`attributeId` here is the sub-instrument id.)
- Events are saved per page — first removing that page's instruments' events for the date, so a
  regenerated page replaces rather than duplicates — and the page's instruments are split into
  batches of up to 500 and dispatched, while paging continues.

Throughput depends heavily on the pod's CPU: at 4 CPUs (with `-XX:ActiveProcessorCount=4`, see the
plan doc's JVM finding) ~800–980 instruments/s; at the old 1 CPU ~280.

## 3 — Model execution on the workers

Each batch is published to `fyntrac-python-model-execution` with a `correlationId`. The dsl-model pods
consume it on a **Shared** subscription with `receiver_queue_size = 1`, so a batch goes only to a free
pod. `FYNTRAC_BATCH_MAX_CONCURRENT_DISPATCH` (16) caps how many batches are in flight; each slot is
held until that batch's stage 4 is done.

A worker (`fyntrac-py-model/app/pulsar/manager.py`) handling a batch:

1. reserves a **unique job id** (`epoch_ms × 1000 + n`, claimed in `PythonJobIdReservation`) — it
   becomes every `TransactionActivity.batchId` of the batch and is what stages 4a/4b/5 select by;
2. loads the tenant's active model;
3. reads `EventHistory` for the batch's instruments **plus the shared reference events**;
4. transforms them into one data row per instrument (`data_transformer.transform`);
5. looks up each instrument's active `InstrumentAttribute` (one query per instrument);
6. runs the model per instrument across **4 worker processes**; each gets a fresh namespace, while the
   compiled template, parsed DSL expressions and date normalisation are cached per process;
7. writes the batch's `TransactionActivity` in one `insert_many` (zero amounts discarded) and flips
   `EventHistory` status with one `update_many` per status;
8. publishes a completion, which the owning pod picks up through Memcached.

About 3/4 of a batch is the model code itself.

## 4a — Attribute and instrument roll-up (per batch, concurrent)

Two Spring Batch jobs read the batch's `TransactionActivity` (by `batchId`) and accumulate:

| collection | keyed by | scope |
|---|---|---|
| `AttributeLevelLtd` | metric + instrument + attribute + period + postingDate | one instrument |
| `InstrumentLevelLtd` | metric + instrument + period + postingDate | one instrument |

Every row belongs to one instrument and an instrument is in exactly one batch, so batches never touch
the same row and these run concurrently.

## 4b — GL sync (per batch)

The owning pod publishes `{tenantId, jobId}` to `fyntrac-book-gl-staging`; the gl service stages that
job's `TransactionActivity`. This and the progress counters (`incrementCompletedBatches` /
`incrementFailedBatches`, atomic `$inc` on the run record) are the only things still behind the
per-run `ReentrantLock`. GL staging finishes asynchronously, ~90–110 s after the last batch on Hearst.

## 5 — Metric roll-up (once per posting date)

`MetricLevelLtd` is keyed by **metric + period + postingDate — no instrument**: one row per metric for
the whole tenant. It used to be accumulated per batch under the lock (~240 serialized job launches per
Hearst date). It is now computed **once**, in `postProcess`, by `MetricLevelRollupService`:

1. one aggregation over this run's `TransactionActivity`: `postingDate` + `batchId $in` the run's job
   ids, `$sum` of the Decimal128 amounts per transaction name (exact);
2. transaction → metrics via the same map as before (`AggregationService.loadIntoCache`);
3. per metric: add to an existing (metric, postingDate) row, else start from the previous posting
   date's ending balance.

Only batches that reached the old metric step contribute. A failure is recorded and the run ends
`PARTIAL_SUCCESS`. Takes ~0.1 s per Hearst posting date.

The run's job ids are recorded in MongoDB as each batch finishes (`ExecutionRunBatch`: `_id` = job
id, `runId`, `postingDate`) and read back by run id, so the roll-up does not depend on the pod that
owns the run.

## 6 — End of day

`performEOD` runs **post-aggregation**, the carry-forward: for each level (attribute → instrument →
metric) it finds every combination with exactly one row since the previous posting date — a balance
last time, nothing this time — and writes today's row with `activity = 0` and the balance unchanged.
It must follow stage 5, or every metric looks inactive and gets a bogus zero row.

Then `ExecutionState.executionDate` is advanced and the run is marked `COMPLETED`, or
`PARTIAL_SUCCESS` if any batch (or the roll-up) failed.

---

## Where the time goes now

Per Hearst posting date on the dev box, event generation is done in ~70 s; the rest of the ~5 min is
model execution, with the dsl-model workers fully busy and ready batches queueing for them. Earlier
bottlenecks — reference-table duplication, `strptime` in the model runtime, per-sub-instrument
custom-table queries, a CPU-starved dataloader JVM, the per-batch metric lock — are gone; the plan doc
lists each with its measured effect.

## Known gaps

- **No resume.** A run is owned by one pod; if it restarts, the run stops at its last status.
- **`TransactionActivity` growth** across posting dates.
- **Which `InstrumentAttribute` row** the worker uses when an instrument has one per sub-instrument
  (step 3.5 takes whichever MongoDB returns first).
- **`collect_all` over activity events** sees the whole batch, not just the instrument (plan doc,
  open decisions).
