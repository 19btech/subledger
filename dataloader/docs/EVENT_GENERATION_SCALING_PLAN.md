# Model runs at 8M instruments — plan, and what has been done

Goal: make a single model run over ~8 million instruments finish in a sane amount of time.

Status (2026-09-23): **the single-pod work is done and committed** on `branch-0.0.4` (not pushed).
Hearst P4 went from **69 min to ~17 min** with outputs identical to the baseline. What remains for
8M is scale-out across machines — see [What is next](#what-is-next).

Read `RUN_PIPELINE_EXPLAINED.md` for how a run works; `TARGET_DESIGN_DISTRIBUTED_RUN.md` for the
multi-pod design. Diagrams live in `../../docs/diagrams/`.

---

## How every change was verified

Each change below went through the same gate before it was committed:

1. The five DSL test cases (`run_dsl_tests.sh`: Revenue, SBO, IFRS9 Stage 3, SBO replay M1/M2) pass.
2. TNT004 is restored from the after-P3 backup and Hearst **P4** is re-run.
3. The result is compared, document for document, against the 2026-09-22 P4 baseline:
   `TransactionActivity`, `InstrumentLevelLtd`, `AttributeLevelLtd`, `MetricLevelLtd`,
   `GeneralLedgerEnteryStage`, `GeneralLedgerAccountBalanceStage`. Only run-specific ids are excluded
   (`_id`, `batchId`, `sourceId`, and `instrumentAttributeVersionId`, which the upload assigns).
   From the custom-table prefetch (row 5) on, P4's `EventHistory` is also compared event type by
   event type.

"Identical" below means that gate passed. The tooling is in `~/backups/fyntrac/tools/` on the dev
box (`compare_backups.py`, `compare_eventhistory.py`, `profile_batch.py`).

**Harness gotcha:** GL booking is asynchronous (the gl service, via Pulsar) and can still be writing
after the P4 test passes. A backup taken immediately missed 4,034 GL rows written 9 s later.
`run_hearst_parts.sh` now waits for the GL collections to stop growing before it exports.

## What was done, in order

All numbers are Hearst P4 (two posting dates, ~69k instruments), measured on the dev box.

| # | change | where | effect | commit |
|---|---|---|---|---|
| 1 | **Reference-table events written once per posting date** (sentinel instrumentId `__SHARED_REFERENCE__`) instead of one copy per instrument | dataloader `ExcelModelService`; worker EventHistory query | SSP_RULE: 1 doc per date instead of ~68k. `EventHistory` for P4's two dates 10.68 → 8.57 GB; a full P1–P4 run projects to ~2.3 GB | `adb11d1`, `1580490` |
| 2 | **Measure** (the old "Step 0") | — | Model execution, not event generation, was the wall: 4 workers saturated, sum of batch time / 4 = wall time | — |
| 3 | **Date normalisation cached**, `lookup()` normalises its keys once per call | worker `dsl_functions.py` | 75% of model time was `strptime` failing on non-date keys. Per instrument 156 → 44.8 ms; per date 30–34 → 12–13 min | `4e9d303` |
| 4 | **DSL expressions and compiled templates cached** (fresh namespace per instrument kept) | worker `dsl_functions.py`, `model_runner.py` | Per instrument 44.8 → 17.5 ms; per date 9.1–9.7 min | `4e9d303` |
| 5 | **Custom-table rows prefetched once per page** (one `$in` query per mapping instead of one per sub-instrument, plus an uncached definition lookup each time) | dataloader `ExcelModelService` | Event generation 122 → 283 instruments/s; per date 6.0 min | `adb11d1` |
| 6 | **Unique job ids** — `epoch_ms × 1000 + n`, claimed in `PythonJobIdReservation` | worker `manager.py` | **Correctness bug:** two pods starting a batch in the same millisecond shared an id, so each batch's LTD roll-up and GL booking also took the other's transactions (doubled LTD activity, duplicate GL rows). Surfaced once batches got fast | `1580490` |
| 7 | **Dispatch slots 8 → 16** | `k8s/base/dataloader.yaml` | A slot is held through the batch's LTD aggregation; at 8 only 3.0–3.4 of 4 workers were busy, at 16 3.9+ | `696f743` |
| 8 | **Dataloader JVM sized to its pod**: 4 CPUs + `-XX:ActiveProcessorCount=4` | `k8s/base/dataloader.yaml` | See [the JVM finding](#the-jvm-finding). Throttled 41% → 0.5% of periods; event generation → 790–980 instruments/s; per date 5.1–5.3 min | `696f743` |
| 9 | **Worker writes batched**: one status `update_many` per status and one `insert_many` per batch | worker `manager.py` | Save phase 0.56 → 0.22 s per batch; end-to-end within the dev box's run-to-run noise | `1580490` |
| 10 | **gateway / gl / reporting JVMs sized** (`-Xmx512m`, `ActiveProcessorCount` = CPU limit; gl 2 CPUs) | `k8s/base/{gateway,gl,reporting}.yaml` | Heaps had been allowed 8.3 GB inside 1 GiB pods; gl throttling 160 s → 4.5 s per P4 | `2f989c3` |
| 11 | **Metric roll-up once per posting date** instead of a locked Spring Batch job per batch | dataloader `MetricLevelRollupService`, `DslExecutionWorkflow` | ~240 locked launches per date → one 80–100 ms pass; per date 4.9–5.0 min. Removes the last single-JVM dependency in the batch path | `4eae0a7` |

### The JVM finding

The Java services run OpenJDK **21 GA (21+35)**, which does not detect the container
(`jcmd VM.info` → "container information not found", "CPU: total 20"). It sizes GC and JIT threads
and the virtual-thread carrier pool for all 20 host CPUs, and — for services without an explicit
`-Xmx` — the heap for the 32 GB host. Under a 1-CPU limit that ~20-way JVM spent 41% of scheduling
periods throttled. The fix is in each manifest (`-XX:ActiveProcessorCount` equal to `limits.cpu`,
explicit `-Xmx`). The durable fix is a current 21.0.x base image, which detects the container.

## Where it stands

After all of the above, on the dev box, per posting date:

- event generation finishes in **~70 s**;
- the four dsl-model pods (4 worker processes each) are **the only bottleneck** — fully busy, with
  ready batches queueing for them;
- a ~300-instrument model batch takes ~3.5 s: model code ~77%, per-instrument attribute lookups
  ~10%, writes ~6%.

**The dev box is now the limit of what can be measured here.** It is a laptop (i7-13700H: 6 P-cores
with hyper-threading + 8 E-cores), so the 16 worker processes share hyper-thread siblings and E-cores,
the clock moves with temperature, and single runs vary by ±10–20%. In-cluster model cost is ~35 ms
per instrument against 17.5 ms in a single-process profile.

### Rough 8M projection (dev-box rates — directional only)

| stage | rate on the dev box | at 8M instruments, one posting date |
|---|---|---|
| event generation (one dataloader pod, 4 CPUs) | ~800–980 instruments/s | **~2.5 h** |
| model execution (16 worker processes) | ~35 ms/instrument/process | **~5 h** |
| metric roll-up | one aggregation | seconds |

Neither remaining stage can go much further on one machine. Both have to scale out across nodes.

## What is next

1. **Measure on production-like hardware.** Per-instrument model CPU cost there sizes the worker
   fleet for 8M. The dev-box numbers above are directional.
2. **Scale model execution across nodes.** Already queue-driven (Shared subscription, one batch per
   pod) — adding dsl-model replicas on more nodes scales it without code change. Keep
   `FYNTRAC_BATCH_MAX_CONCURRENT_DISPATCH` at roughly 2–4× the replica count.
3. **Distribute event generation** — `TARGET_DESIGN_DISTRIBUTED_RUN.md`. Change 11 (metric roll-up) was
   its prerequisite. Its remaining correctness requirements are listed there with their current status.
4. **Overlap pages within one pod** (cheaper interim step): today page N+1 is generated only after
   page N is saved.
5. **Upgrade the Java base image** to a current 21.0.x so no service depends on the manifest flags.

## Open decisions (need an owner, not code)

- **`collect_all` scope.** The worker hands every instrument the whole batch's rows, so a template's
  `collect_all` over *activity* events sees every instrument in the batch — results can depend on how
  instruments are batched. Hearst only uses it on reference data. Narrowing to the instrument's own
  rows (plus reference rows) cuts ~45% of model CPU but changes semantics for any model that relies
  on it.
- **Which `InstrumentAttribute` row the worker uses.** `_fetch_instrument_attributes` does
  `find_one({instrumentId, endDate: null})`, assuming one active row per instrument. Hearst has one per
  sub-instrument, so it takes whichever MongoDB returns first — and that row supplies each
  transaction's `instrumentAttributeVersionId` and `attributes`.

## Not addressed

- **`TransactionActivity` growth** across posting dates (916k rows for 69k instruments after P4).
- **No resume.** A run is still owned by one dataloader pod; if it restarts, the run stops.
- **GL settle time** — gl takes ~90–110 s after a run to finish staging; not CPU (gl is no longer
  throttled). Likely serial message processing in gl. Correctness is unaffected.
- **Single-replica MongoDB**, and the `reporting` pod's history of restarts (not investigated).
