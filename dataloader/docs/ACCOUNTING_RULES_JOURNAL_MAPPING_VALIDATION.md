# Accounting Rules & Journal Mapping — Data Validation Checks

Source of truth: `fyntrac-web` (`/home/uabbas/Workspace/fyntrac-web`), pages:

- **Accounting Rules** (`src/app/rules/page.jsx`) — tabs: *Transactions*, *Attributes*, *Balances* (Aggregation). Validation-log type: `ACCOUNTING_RULES`.
- **Journal Mapping** (`src/app/accounting/page.jsx`) — tabs: *Account Type*, *Subledger Mapping*, *Chart of Accounts*. Validation-log type: `JOURNAL_MAPPING`.

Each tab's "Add/Edit" dialog (`src/app/component/add-*.jsx`) performs client-side validation before calling the corresponding `dataloaderApi` REST endpoint. This document:

1. Catalogues every rule enforced by the web app, per entity.
2. Maps each rule to the matching backend validator in `dataloader` (`src/main/java/com/reserv/dataloader/validation/*.java`), used by both the single-record REST `/add` path and the bulk Spring Batch file-upload path.
3. Records the architectural gaps found while auditing the backend (REST-vs-batch inconsistencies, a mislabeled flag, dead-code duplicate checks) so they're not silently lost.
4. Lists the concrete backend changes made to close the frontend-parity gaps.

All REST endpoints below are relative to the `/api/dataloader` base path (e.g. `/transaction/add` in the frontend's `dataloaderApi` client resolves to `/api/dataloader/transaction/add`).

---

## 1. Transactions (Accounting Rules → Transactions)

Frontend: `add-transaction.jsx` · Backend: `TransactionValidator.java` · Controller: `TransactionController` · Endpoint: `POST /transaction/add`

| # | Rule | Frontend behavior | Backend behavior |
|---|------|--------------------|-------------------|
| 1 | **Required** | Transaction Name cannot be blank (Save button disabled). | `ERR_REQ_01` if null/blank. |
| 2 | **Character set** | `^[a-zA-Z0-9_ ]+$` — letters, digits, underscore, **single spaces allowed**. | Same charset, applied once the double/edge-space checks below pass. |
| 3 | **No double spaces** | Reject if name contains `"  "`. Message: *"Double spacing is not allowed."* | ✅ Fixed — now flagged explicitly regardless of edge-space state (`ERR_SPC_01`, message *"Transaction name contains double spaces."*). |
| 4 | **No leading/trailing spaces** | Reject if `name.trim() !== name`. Message: *"Leading or trailing spaces are not allowed."* | `ERR_SPC_02`. |
| 5 | **Special characters rejected** | Anything outside the regex above → *"Special characters are not allowed."* | `ERR_FMT_01`. |
| 6 | **A single internal space is allowed** | `"Loan Payment"` is valid. | ✅ Fixed — previously ANY space (even a single well-placed one) triggered `ERR_SPC_01`, which was stricter than the frontend. Now only double-spaces/edge-spaces are rejected; a lone internal space is accepted. |
| 7 | **Uniqueness** | Server-enforced; frontend surfaces `409`/duplicate response as *"Duplicate transaction name found."* | `ERR_DUP_01` — case-insensitive match against existing transaction names (excluding self on edit). |
| 8 | **Flags: Reportable (`exclusive`) / Journal (`isGL`) — warn-then-confirm** | Both off → warning, non-blocking. Journal-only off → warning, forces `true` on confirm. Reportable-only off → warning, forces `true` on confirm. | `WRN_LOGIC_01` warning when both false (non-blocking) — matches. Single-flag "default to true" is a UI confirmation nudge; the batch/file path already defaults a truly *empty* cell to `true` with `WRN_DEF_01`. |
| 9 | **Replayable** | Simple boolean toggle, no format validation. | Boolean coercion only. |
| 10 | **Rename cascade** | On edit, if the name changes, the frontend re-POSTs every `Aggregation` and `SubledgerMapping` row that referenced the old name. | Not a validator concern — cascade lives client-side; noted here for completeness. |

### Backend architecture note — `exclusive` vs `isReplayable` mislabeling

`Transactions` has three `int` flags: `exclusive`, `isGL`, `isReplayable`. The batch loader's own CSV header-alias table (`TransactionsDataLoadConfig.java`) proves the legacy header **`REPORTABLE` maps to `exclusive`**, and **`REPLAYABLE` maps to `isReplayable`** — yet `TransactionValidator.validate()` (both overloads) validates the `isReplayable` field but labels its comments/messages "reportable" (*"Empty reportable flag. Defaulting to true (1)."*, *"Invalid boolean value for reportable."*, *"Both journal and reportable are false."*), while the actual `exclusive`/"Reportable" field receives no validation messages at all — it's silently coerced from `-1` to `0` with no warning/error.

**Fix applied:** relabeled the `isReplayable` block's messages/comments to say "replayable" (matching its true meaning — governs whether a transaction's activity can be replayed/reversed, per `TransactionActivityReversalService`), and added the missing `WRN_DEF_01`/`ERR_BOOL_01` handling for `exclusive` (the real "Reportable" flag) so it gets the same empty-defaults-to-true / invalid-boolean treatment the frontend implies for "Reportable".

---

## 2. Attributes (Accounting Rules → Attributes)

Frontend: `add-attribute.jsx` · Backend: `AttributesValidator.java` · Controller: `AttributeController` · Endpoint: `POST /attribute/add`

| # | Rule | Frontend | Backend |
|---|------|----------|---------|
| 1 | **Required** | Attribute Name cannot be blank. | `ERR_REQ_01`. |
| 2 | **Character set** | `^[a-zA-Z0-9_]+$` — **no spaces at all** (unlike Transaction name). | `ERR_FMT_01` / `ERR_SPC_01` — any space rejected. |
| 3 | **Uniqueness** | Server-enforced; controller pre-checks via case-insensitive Mongo regex. | `ERR_DUP_01` against existing attribute names (excluding self). |
| 4 | **User Field auto-assignment** | Read-only, auto-computed as `USERFIELD{n+1}`. | Informational — not a validation rule. |
| 5 | **Data Type** | Must pick one of `STRING`, `NUMBER`, `DATE`, `BOOLEAN` from a fixed picker. | `ERR_LIST_01` if raw value isn't in `DataType`'s allowed set. |
| 6 | **Nullable** | Simple boolean toggle. | Accepts Yes/Y/True/1 and No/N/False/0 synonyms; `ERR_LIST_02` on invalid. |
| 7 | **Cross-field: Reclassable requires Versionable** | Blocking error *"Reclassable requires Versionable to be enabled."* | `ERR_LOGIC_02` — same rule, already implemented. |

No backend changes were required for Attributes — the validator already matches the frontend rule set.

---

## 3. Balances / Aggregation (Accounting Rules → Balances)

Frontend: `add-aggregation.jsx` · Backend: `AggregationValidator.java` · Controller: `AggregationController` · Endpoint: `POST /aggregation/add`

| # | Rule | Frontend | Backend |
|---|------|----------|---------|
| 1 | **Required: transactions** | At least one Transaction Name selected. | `ERR_REQ_01` if missing. |
| 2 | **Required: metric name** | Cannot be blank. | `ERR_REQ_01`. |
| 3 | **Metric character set** | `^[a-zA-Z0-9_]+$` — no spaces. | `ERR_SPC_01` / `ERR_FMT_01`. |
| 4 | **Transaction reference integrity** | Picker populated from `/transaction/get/transactions`. | `ERR_REF_01` if not found. |
| 5 | **Uniqueness (transaction + metric)** | Dialog only submits new (`toCreate`) transaction/metric pairs. | `ERR_DUP_01` — composite `METRIC:TRANSACTION` key check (excluding self). |
| 6 | **Removing a linked transaction from a metric** | Explicitly blocked client-side — no backend delete-by-edit endpoint exists. | Out of scope for the validator. |

No rule gaps found in `AggregationValidator` itself. One data-consistency note carried into the gap list below: the batch path uppercases `transactionName`/`metricName` before saving, the REST `/add` path does not — flagged, not fixed (would change stored casing for existing REST-created records; left to a follow-up decision rather than silently altering data casing).

---

## 4. Account Type (Journal Mapping → Account Type)

Frontend: `add-account-type.jsx` · Backend: `AccountTypesValidator.java` · Controller: `AccountTypeController` · Endpoint: `POST /accounttype/add`

| # | Rule | Frontend | Backend |
|---|------|----------|---------|
| 1 | **Required** | Account Subtype cannot be blank. | `ERR_REQ_01`. |
| 2 | **Character set** | `^[a-zA-Z0-9_ ]+$` — spaces allowed internally. | Same charset. |
| 3 | **No double spaces** | *"Double spacing is not allowed."* | ✅ Fixed — added explicit double-space check (`ERR_SPC_01`), independent of the trim check. |
| 4 | **No leading/trailing spaces** | *"Leading or trailing spaces are not allowed."* | `ERR_SPC_02`. |
| 5 | **Account Type enum** | Must pick one of `BALANCESHEET`, `INCOMESTATEMENT`, `CLEARING`. | Enforced by Jackson enum binding at the REST boundary + validator's null check. |
| 6 | **Uniqueness** | Backend-enforced; frontend surfaces duplicate as *"...must be unique."* | `ERR_DUP_01` — already checked against the **full DB** (the REST controller preloads all existing `AccountTypes` rows before validating), not just in-file state. |
| 7 | **Cross-field: one subtype → one type** | N/A per-record in the dialog (subtype is being defined here), but file uploads must not map the same subtype to two different types. | `ERR_LOGIC_03` — already implemented. |
| 8 | **Rename cascade** | On edit, if the subtype changes, the frontend re-POSTs referencing `SubledgerMapping`/`ChartOfAccount` rows. | Client-side concern — noted for completeness. |

### Backend architecture note — batch vs. REST duplicate detection differ

`AccountTypesItemProcessor` (batch/file path) only tracks `seenSubTypes` **within the current file** (reset `@BeforeStep`) — it never checks the database. `AccountTypeController` (REST path) *does* preload the full DB before validating. Net effect: uploading a CSV containing a subtype that already exists in the DB (but appears only once in the file) is **not** flagged as a duplicate by batch, while submitting that same single record via REST **is** flagged. This is a real behavioral inconsistency between the two ingestion paths; recorded in the gap list below since fixing it means changing batch semantics (a broader decision than this frontend-parity pass), not a frontend/backend mismatch.

There is also an unused/dead `AccountTypeItemProcessor` (singular "Type") class that performs a plain pass-through copy with **no validation at all**; it sits alongside the real, validated `AccountTypesItemProcessor` (plural "Types"). Left untouched here but flagged as dead-code risk.

---

## 5. Subledger Mapping (Journal Mapping → Subledger Mapping)

Frontend: `add-subledger-mapping.jsx` · Backend: `SubledgerMappingValidator.java` · Controller: `SubledgerMappingController` · Endpoint: `POST /subledgermapping/add`

| # | Rule | Frontend | Backend |
|---|------|----------|---------|
| 1 | **Required fields** | Transaction Name, Criteria (Sign), Entry Type, Account Subtype all required. | `ERR_REQ_01` per field. |
| 2 | **Transaction reference integrity** | From `/transaction/get/transactions`. | `ERR_REF_02` if not found. |
| 3 | **Account Subtype reference integrity** | From `/accounttype/get/subtypes`. | `ERR_REF_02` if not found. |
| 4 | **Sign enum** | `AMOUNT > 0` → `POSITIVE`, `AMOUNT < 0` → `NEGATIVE`. | `ERR_LIST_01` for invalid raw sign. |
| 5 | **Entry Type enum** | `DEBIT` / `CREDIT`. | `ERR_LIST_01` for invalid raw entry type. |
| 6 | **Rule 1 — Entry-type-already-mapped** | Same `transactionName + sign` already has the **same entryType** mapped (different subtype) → blocked. | `ERR_LOGIC_05` (in-file/in-session state) **+** the controller's own DB-level check. |
| 7 | **Rule 2 — Debit/Credit share same subtype** | Same `transactionName + sign`, Debit and Credit must **not** point to the same `accountSubType` → blocked. | ✅ Added — new DB-aware check in `SubledgerMappingController` (mirrors Rule 3's existing pattern) plus the in-session composite check in the validator. |
| 8 | **Rule 3 — Exact duplicate** | Same `transactionName + sign + entryType + accountSubType` already exists → blocked. | `ERR_DUP_02` — already checked against the DB by the controller (separate inline check), in addition to the validator's in-session composite key. |
| 9 | **Ambiguous sign** | Not explicitly checked client-side. | `ERR_LOGIC_04` — in-session only (see architecture note). |
| 10 | **Auto-create opposite entry** | On a new (non-edit) save, the frontend automatically submits a mirrored second record (sign + entry type flipped, same subtype). | Informational — affects row count per Add action, not a validator rule. |

### Backend architecture notes

- **Shared-singleton validator state.** `SubledgerMappingValidator` is a plain `@Component` (not request- or step-scoped) holding mutable `ConcurrentHashMap`s used for in-session composite checks (Rules 1/3/9 above). Both the REST controller and the batch item processor call `clearState()` before use — for REST, this means the composite in-memory checks can never actually compare against sibling records (state is wiped per request), so `SubledgerMappingController` implements Rule 3 (exact duplicate) and now Rule 2 (Debit/Credit subtype clash) **directly against the database** instead. This is architecturally redundant with the validator but was the existing pattern already established for Rule 3, so Rule 2 was added the same way for consistency and to avoid touching the shared-state class's thread-safety story in this pass.
- For batch/file uploads, the validator's shared state genuinely does its intended job: comparing rows *within the same file* via Rules 1/3/9, since one processor instance processes the whole file sequentially.
- Because Rule 2 (Debit/Credit sharing a subtype) needs to compare **across sign values** for the same transaction, it was added to the batch/file path too, inside `SubledgerMappingItemProcessor`'s in-memory tracking, so file uploads get the same protection as the REST dialog.

---

## 6. Chart of Accounts (Journal Mapping → Chart of Accounts)

Frontend: `add-chart-of-account.jsx` · Backend: `ChartOfAccountValidator.java` · Controller: `ChartOfAccountController` · Endpoint: `POST /chartofaccount/add`

| # | Rule | Frontend | Backend |
|---|------|----------|---------|
| 1 | **Required** | Account Number, Account Name, Account Subtype all required. | `ERR_REQ_01` per field. |
| 2 | **Text field format (Account Number / Account Name / string attributes)** | Shared `validateTextField`: no double spaces, no leading/trailing spaces, charset `^[a-zA-Z0-9_ ]+$`. | Backend patterns are intentionally a richer superset (Account Number additionally allows `_-.`, Account Name additionally allows `()[]&',/`) — kept as-is; this is stricter/more permissive by design, not a gap. |
| 3 | **Account Subtype reference integrity** | Picked from `/accounttype/get/subtypes`. | `ERR_REF_02` if not in preloaded valid-subtypes set. |
| 4 | **Custom attribute per-type validation** | Metadata-driven: `String` → text-field rule; `Number` → must parse as a number; `Date` → must parse as a date; `Boolean` → must be `"true"`/`"false"`. | ✅ Added — `ChartOfAccountValidator` now looks up each dynamic attribute's `dataType` (via the existing `AttributesRepository`) and applies matching Number/Date/Boolean checks, not just whitespace (`ERR_SPC_03`). |
| 5 | **Rule A — Exact duplicate** | Account Number + Name + Subtype + every custom attribute value all match an existing row → blocked. | ✅ Added — DB-level composite check in `ChartOfAccountController` (mirrors the Subledger Mapping controller's pattern), since the validator's own in-memory `seenAccountNumbers`/`seenAccountNames` sets are re-created empty on every REST call and can never catch true duplicates (see architecture note). |
| 6 | **Rule B — Subtype + attribute-set uniqueness** | Same Account Subtype + identical custom-attribute values already exists under a **different** Account Number or Name → blocked. | ✅ Added — same DB-level check, alongside Rule A. |

### Backend architecture note — REST-path duplicate detection was dead code

`ChartOfAccountValidator` is `@StepScope`, so it can't be `@Autowired` into an HTTP-request-scoped controller method. `ChartOfAccountController` works around this by manually `new`-ing a fresh validator instance **per REST request** (`new ChartOfAccountValidator(accountTypesRepository); validator.init();`). Because the validator's `seenAccountNumbers`/`seenAccountNames` in-memory sets start empty on every such instantiation, its duplicate checks (`ERR_DUP_01` on Account Number/Name) can **never fire** on the REST `/add` path — there's nothing previously "seen" to compare against for a lone record. The batch/file path is unaffected (there, the validator genuinely is a per-step Spring bean processing every row in the file sequentially, so its in-memory dedup works as intended).

**Fix applied:** rather than try to make the `@StepScope` validator carry cross-request state (which would reintroduce the same singleton-sharing risk flagged for `SubledgerMappingValidator`), Rule A/B duplicate detection was added as an explicit DB-backed check in `ChartOfAccountController`, following the same pattern already used by `SubledgerMappingController` for its own duplicate rules — fetch all existing records, compare the composite signature (account number, name, subtype, and every custom attribute value), exclude self on edit.

---

## Error-code catalogue in use

Shared `com.fyntrac.common.enums.ErrorCode` values referenced above. `getCode()` returns the enum symbol itself (e.g. `"ERR_REQ_01"`); `getName()` returns the human-readable label (e.g. `"MANDATORY_FIELD"`) — note the naming is the reverse of what the accessor names suggest.

| Code | `getName()` label | Meaning |
|------|--------------------|---------|
| `ERR_REQ_01` | `MANDATORY_FIELD` | Required field missing/empty |
| `ERR_SPC_01` | `NO_WHITESPACE` | Contains disallowed (internal/double) spaces |
| `ERR_SPC_02` | `TRIM_WHITESPACE` | Leading/trailing spaces |
| `ERR_SPC_03` | `COL_VAL_WHITESPACE` | Leading/trailing spaces on a dynamic attribute value |
| `ERR_FMT_01` | `ALPHANUM_UNDERSCORE` | Contains disallowed special characters |
| `ERR_DUP_01` | `DUPLICATE_VALUE` | Duplicate value (single-field) |
| `ERR_DUP_02` | `DUPLICATE_RULE` | Duplicate composite-key value |
| `ERR_REF_01` / `ERR_REF_02` | `REF_NOT_FOUND` / `REF_CONFIG_MISSING` | Referenced value not found in reference/config data |
| `ERR_LIST_01` / `ERR_LIST_02` | `INVALID_ALLOWED_VAL` / `INVALID_YES_NO` | Value not in allowed enum/list |
| `ERR_LOGIC_02` | `LOGIC_DEPENDENCY` | Attribute: reclassable without versionable |
| `ERR_LOGIC_03` | `LOGIC_MULTI_MAP` | Account type: one subtype mapped to two types |
| `ERR_LOGIC_04` | `LOGIC_AMBIGUOUS` | Subledger mapping: ambiguous sign |
| `ERR_LOGIC_05` | `LOGIC_ENTRY_CONFLICT` | Subledger mapping: entry-type conflict |
| `ERR_BOOL_01` | `INVALID_BOOLEAN` | Invalid boolean value |
| `WRN_DEF_01` | `MISSING_FLAG_DEFAULT` | Empty flag defaulted (warning, non-blocking) |
| `WRN_LOGIC_01` | `NULL_ACTION_WARNING` | Both reportable and journal false (warning, non-blocking) |

---

## Backend changes made in this pass

Applied in `com.reserv.dataloader.validation.*` and the corresponding controllers:

1. **`TransactionValidator`** — allow a single internal space in the transaction name (previously any space triggered an error); added an explicit double-space check independent of the "contains space" branch; relabeled the `isReplayable` block's messages from "reportable" to "replayable"; added the missing empty/invalid-boolean handling for `exclusive` (the actual "Reportable" field).
2. **`AccountTypesValidator`** — added an explicit double-space check for `accountSubType`.
3. **`SubledgerMappingController` / `SubledgerMappingItemProcessor`** — added Rule 2 (Debit and Credit for the same transaction+sign must not share an account subtype), DB-backed for REST and in-file for batch.
4. **`ChartOfAccountValidator`** — added datatype-specific validation (Number/Date/Boolean) for dynamic custom attributes, matching the frontend's per-datatype checks.
5. **`ChartOfAccountController`** — added Rule A (full-record duplicate across Account Number + Name + Subtype + all custom attributes) and Rule B (Subtype + attribute-set combination must map to a unique Account Number/Name), checked against the database, since the validator's own in-memory de-duping is structurally unable to run cross-request on the REST path.

## Known gaps intentionally left open (flagged, not fixed in this pass)

These surfaced during the backend audit but are either UI-only affordances that don't need a server-side mirror, or would change existing data/behavior in ways that deserve a standalone decision rather than a silent fix bundled into a frontend-parity pass:

- **Aggregation casing mismatch** — batch uppercases `transactionName`/`metricName` before saving; REST `/add` does not. Fixing this would change the casing of newly-created REST records; left open pending a decision on whether existing REST-created (mixed-case) rows should also be normalized.
- **AccountTypes batch-vs-REST duplicate detection** — batch only checks in-file, REST checks the full DB. Aligning batch to also check the DB is a bigger behavioral change to the file-upload pipeline (performance/preloading implications for large files) and was left as a follow-up.
- **Invalid enum values via REST JSON** (`sign`, `entryType`, `accountType`) fail at Jackson deserialization with a generic 400, not the validator's structured `ERR_LIST_01` — would require a custom deserializer or `@JsonCreator` fallback on each enum; out of scope for a validator-only pass.
- **`SubledgerMappingValidator` shared mutable state** — a plain singleton `@Component` with `ConcurrentHashMap` fields, reset per call via `clearState()`. Under concurrent REST requests this is a theoretical race condition. Not touched here since fixing it properly (request/step scoping or removing the shared state) is a structural change beyond validation-rule parity.
- **Dead `AccountTypeItemProcessor`** (singular "Type") — an unused, unvalidated pass-through class sitting alongside the real `AccountTypesItemProcessor`. Left in place; flagged for a future cleanup pass.
- **`Attributes` REST error response shape** — `AttributeController` joins validation errors into a single string instead of returning the `List<ValidationError>` JSON array every other entity's controller returns. Not changed here to avoid an undocumented breaking change to that endpoint's response contract; flagged for the frontend/backend teams to align on deliberately.
