# Target design — distributing a model run across dataloader pods

Written 2026-09-23, updated the same day after the single-pod work landed. Supersedes the
range-based sketch; this is the version to build — **not started**. The prerequisite (metric roll-up
out of the per-batch path) is done; see [Status](#status-2026-09-23).

Read `RUN_PIPELINE_EXPLAINED.md` first — it describes how the run works today and why the two
bottlenecks are what they are. `EVENT_GENERATION_SCALING_PLAN.md` holds the sequencing.
Diagram: `../../docs/diagrams/event-generation-split-proposed.mmd`.

---

## Shape

Two-tier fan-out. One pod plans the run; every pod works it; the model workers stay as they are.

```
coordinator pod
  └─ publishes ~N chunk messages, Shared subscription, no prefetch
       └─ any free dataloader pod takes a chunk and, for its instruments only:
            1. generates events
            2. dispatches to the dsl-model workers and waits
            3. runs attribute-level + instrument-level roll-up
            4. publishes GL staging
            5. atomically increments the run's done-counter
  └─ the pod whose increment closes out the last chunk runs the tail:
            6. metric-level roll-up   (once, whole run)
            7. EOD carry-forward      (attribute → instrument → metric)
            8. advance ExecutionState, set final status
```

**The unit of work is a chunk of instruments (~500–2,000), not one instrument.** An instrument is
the right *partition key* — it is what makes steps 1–3 safe to run anywhere — but it is the wrong
*message granularity*. See "Why chunks, not single instruments" below.

## Status (2026-09-23)

What already exists on `branch-0.0.4`, and what this design still needs:

| requirement | status |
|---|---|
| Metric roll-up out of the per-chunk path | **Done** (`4eae0a7`): `MetricLevelRollupService` runs once in `postProcess` on the owning pod. In this design it moves to the finisher pod unchanged. |
| Job ids unique across pods | **Done** (`1580490`). This was a live bug even with one dataloader pod: dsl-model pods starting batches in the same millisecond shared an id, and stage 4 then processed both batches' transactions under it. |
| Run's job ids persisted, not in memory | **Not done.** The roll-up selects `batchId $in` the run's job ids, held today in `DslExecutionWorkflow.metricRollupJobIds` on the owning pod. The finisher must be able to read them — e.g. tag the `PythonJobIdReservation` doc (or the batch log) with the run id. |
| Progress counters atomic | **Not done.** `incrementCompletedBatches` is still read-modify-write on the whole `ExecutionInstance`, guarded by the per-run in-process lock. |
| Event writes idempotent | **Partly.** Shared reference-table events use a fixed `_id` (upsert). Per-instrument events are still inserted with fresh ids. |
| Carry-forward writes idempotent | **Not done.** |
| JVM sized to the pod | **Done** for the dataloader (4 CPUs, `ActiveProcessorCount=4`); every pod added here needs the same, or the image upgraded — see the plan doc's JVM finding. |

## Why this is safe

Everything a worker pod does in steps 1–3 is scoped to instruments only it holds:

| written in a chunk | keyed by | collides across chunks? |
|---|---|---|
| `EventHistory` | instrument + event + date | no |
| `TransactionActivity` | instrument + job | no |
| `AttributeLevelLtd` | metric + instrument + attribute + period + date | no |
| `InstrumentLevelLtd` | metric + instrument + period + date | no |

An instrument appears in exactly one chunk, so no two pods ever touch the same row. The one
`EventHistory` exception is the shared reference-table event (instrumentId `__SHARED_REFERENCE__`):
the coordinator writes it once, before publishing chunks — it has a fixed `_id`, so a retry
overwrites rather than duplicates. `MetricLevelLtd`
is the one exception — keyed by metric + period + date with **no instrument** — which is precisely
why it is not in the per-chunk path.

## The tail — metric roll-up and EOD

### Why it moves, and what it becomes

Per chunk, metric roll-up is an **accumulation**: read the current metric row, add this chunk's
share, write it back. That is why it needed a lock (until `4eae0a7`), and why the balances being
`BigDecimal` persisted as strings matters — there is no atomic `$inc` to fall back on.

Run once at the end it is a **computation**: read everything, compute the total, write it. Nothing
accumulates across anything. This is what `MetricLevelRollupService` does today, on the owning pod.
Three consequences, all good:

- the lock has no reason to exist — it is deleted, not distributed;
- the job becomes **idempotent**, which matters because a redelivered "run finished" message would
  otherwise corrupt the totals;
- ~16,000 Spring Batch job launches per posting date at 8M collapse to one.

### The order is forced by the data

`metricLevelPostAggregationReader` reads `MetricLevelLtd` and carries forward anything with exactly
one row since the previous posting date. If carry-forward runs before the roll-up, **every metric
looks like it had no activity today** and gets a bogus zero-activity row — and then the roll-up
lands and there are two rows for the same metric and date.

So the tail is a fixed sequence, not a set of independent jobs:

1. **metric roll-up** — one pass, writes `MetricLevelLtd` for this posting date
2. **EOD post-aggregation** — carry-forward, attribute → instrument → metric, in that order
3. **advance** `ExecutionState.executionDate`
4. **final status** — `COMPLETED`, or `PARTIAL_SUCCESS` if any chunk failed

Attribute- and instrument-level roll-up do **not** move. They stay on the worker pod, per chunk, in
parallel — they are already collision-free and they are the part whose cost scales with instrument
count.

### Who decides the run is finished

Not the coordinator polling. That reintroduces the single pod the design exists to remove, and if it
dies the run hangs forever.

Let the counter elect the finisher. On finishing its chunk each pod makes one atomic increment that
returns the new value:

```
r = findAndModify({_id: runId}, {$inc: {chunksCompleted: 1}}, returnNew: true)
if r.chunksCompleted == r.totalChunks:
    → this pod runs the tail
```

Whichever pod happens to close out the last chunk runs it. No polling, no liveness dependency on any
particular pod, and it costs one round trip the pod is already making.

Two guards:

- **Failed chunks increment too.** This matches today, where a failed batch still increments
  `completedBatches` and the run reaches EOD as `PARTIAL_SUCCESS`. Otherwise one bad chunk hangs the
  whole run.
- **Compare-and-set a `finalized` flag** before running the tail. A redelivered chunk can push the
  counter past `totalChunks`, and exactly one pod must win that race.

## Correctness requirements — non-negotiable

Each of these is a silent-corruption bug if skipped. No exception, no log line, just wrong numbers.

Status of each is in the table above.

1. **Progress counters must be atomic.** `incrementCompletedBatches` currently reads the whole
   `ExecutionInstance`, adds one and saves it back. Two pods doing that lose counts — and this
   design makes the counter load-bearing, since it is what triggers the tail.
2. **Event writes must be upserts.** A pod dying mid-chunk gets that chunk redelivered to another
   pod. Keyed on instrument + event + date.
3. **Metric roll-up must be out of the per-chunk path.** Not "with a distributed lock" — out. It is
   a prerequisite of this design, not a companion change.
4. **Carry-forward must be an upsert** on its natural key, for the same redelivery reason as (2).
5. **The run's job ids must be readable by the finisher.** The metric roll-up selects the run's
   transactions by `batchId $in` those ids; if the finisher cannot see every chunk's ids, their
   transactions silently drop out of `MetricLevelLtd`.
6. **Job ids must be unique across every pod** — already the case; keep it that way. Stage 4 and GL
   select transactions by job id, so a shared id makes two batches' work collide.

## Why chunks, not single instruments

Per-instrument messages look cleaner and cost far more. At 8M instruments:

| cost | chunk of 1,000 | one instrument |
|---|---|---|
| Pulsar messages | ~8,000 | ~8,000,000 (×2 hops) |
| Spring Batch job launches (attribute + instrument roll-up) | ~16,000 | **~16,000,000** |
| model process pool start | once per chunk | every message |
| worker `EventHistory` read + batched writes | once per chunk | once per message |

The job launches are the decisive one. Each writes `JobExecution`, `StepExecution` and parameter
rows to the job repository before any work happens — tens of milliseconds. 16M of those is worse
than what we have today.

The second is per-message overhead on the worker: every message pays process-pool start-up, the
batch reads and the batched writes, which only amortise over many instruments.

And a model pod handed one instrument has nothing to spread across its four worker processes; it
would have to buffer messages to keep them fed, which is batching reinvented at the wrong layer.

## Tuning

- **Subscription:** `Shared` (a work queue — exactly one consumer per message), **not** broadcast.
  With `receiver_queue_size = 1`, matching the model workers, so a chunk goes only to a free pod.
- **Re-tune `max-concurrent-dispatch`.** It is per-pod (16 today, because a slot is also held through
  the batch's attribute/instrument roll-up). With N dataloader pods the model workers see N×16 in
  flight. Size it against the dsl-model replica count, or this recreates the thundering-herd timeouts
  that semaphore was added to fix.
- **Chunk size** starts at the current `fyntrac.batch.dispatch-batch-size` (500) — it is already the
  unit the model workers are tuned around.

## Open questions

- **~~Can metric roll-up read `InstrumentLevelLtd` instead of `TransactionActivity`?~~** Settled in
  practice: it reads the run's `TransactionActivity` with one `$group` (index `{postingDate,
  batchId}`), ~0.1 s per Hearst posting date, and reproduces the old per-batch results exactly.
  Reading `InstrumentLevelLtd` instead remains possible but is not needed for speed.
- **What plans the chunks?** Computing chunk boundaries over 8M instruments without scanning the
  collection twice is unsolved here.
- **Coordinator failure before publishing.** If the planning pod dies mid-publish, some chunks never
  exist and the counter never reaches its total. Needs a timeout or a resumable plan step.

## What this does not solve

- `TransactionActivity` growth across posting dates within a run.
- Single-replica mongodb holding the run's output.
- Model execution capacity — that scales separately, by adding dsl-model replicas on more nodes.
  (The reference-table duplication this line used to list is fixed.)
