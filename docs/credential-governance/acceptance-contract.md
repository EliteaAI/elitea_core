# Acceptance contract

Part of [credential governance](README.md). Everything here is observable: a mode, a code, an audit event, a
counter, a count of statements. Reviews and acceptance runs check against this file. For what the codes mean in
the model, see [credential-profiles.md](credential-profiles.md) and [identity-boundaries.md](identity-boundaries.md).

## The governance switch

One platform-wide setting with three modes. There is no per-project mode in slice 1.

| Mode | Behaviour |
|---|---|
| `off` | **Exactly today's behaviour.** Nothing is verified, bound, decided or sealed. No identity-verifier call, no audit method, no credential facts, no secret store, no database read and no Vault module is touched on the dispatch path. The worker receives the same payload as before this feature. |
| `audit_only` | The full decision is taken for each person's interactive run, as `enforce` would take it, and recorded. **Nothing is refused.** An event that `enforce` would have refused is written as `decision=allow, would_deny=true` with the refusal code in `reason_code`; one that would pass is `would_deny=false`. No context row is written and the run goes out as a legacy run. A built-in Elitea tools toolkit that still authenticates with a personal token is recorded as `LEGACY_PAT_INTERNAL_MCP`. A person's schedule run is rehearsed against its mandate only (one `would_deny` row, see below). |
| `enforce` | Runs are refused unless identity, binding and every credential decision pass. Admitted runs carry a governed envelope and reference-only payloads. |

Audit-only does **not** evaluate: webhooks, legacy cron triggers, runs under a mandate beyond the existence, owner,
state and operation of the version's mandate, system-user runs, and child dispatches. A person's schedule run
(entry point `schedule`) is checked against the version's mandate: if enforce would refuse it, one
`decision=allow, would_deny=true` event is written with `reason_code` `MANDATE_REQUIRED`, `MANDATE_INACTIVE`,
`MANDATE_EXPIRED` or `DENIED_OPERATION` and the caller `schedule:entity:<id>`. The run goes ahead. The evaluation runs after the task is started, in the same request, so it adds no latency
before the model call; `admission_duration_seconds{mode}` still measures it.

"Off is exactly today's behaviour" is pinned by a test: for a stored `off` row, no row, a missing table,
`MAX_MODE=off` and the deprecated `ENABLED=false`, the real dispatch wrapper sends the worker the payloads in a
golden snapshot captured before the feature (chat, predict, pipeline, test tool), and calls nothing else
(`tests/integration/test_7120_governance_off_invariant.py`).

### Deployment and stored settings

The deployment keeps three environment variables, read from the process environment only (never from plugin
configuration, and not declared in the admin schema):

| Variable | Meaning |
|---|---|
| `ELITEA_CREDENTIAL_GOVERNANCE_MAX_MODE` | The ceiling: `off`, `audit-only` or `enforce`. Default `enforce`. The stored mode is capped by it. |
| `ELITEA_CREDENTIAL_GOVERNANCE_ENABLED` | Deprecated alias of the ceiling (`true` = `enforce`, `false` = `off`). It never switches governance on. Setting it and `MAX_MODE` to different values stops the plugin from starting. |
| `ELITEA_CREDENTIAL_GOVERNANCE_ENVIRONMENT` | Environment label for audit records (at most 64 characters). When set, the field is read-only in the UI. |

Any former configuration key (`enabled`, `policy_revision`, `environment`, `admit_legacy_pat`,
`allow_runtime_domain_binding`, and the matching variables) is ignored with a warning. The plugin configuration
keeps one value, `enterprise_credential_governance.context_ttl_s` (60 to 86400, default 3600): how long an
admitted run may start or restart.

Everything else is stored in the table `credential_governance_settings` (public schema) and changed in the
Admin UI or with `PUT`:

| Field | Rule |
|---|---|
| `mode` | `off`, `audit_only`, `enforce`; refused above the ceiling (`MODE_ABOVE_CEILING`) |
| `policy_revision` | Free label of the approved rules (change ticket, date), at most 128 characters. Required for `audit_only` and `enforce`. It is stamped on every admission and approval. |
| `admit_legacy_pat` | Runs started with a personal API token may pass the identity check. They still cannot act for a person. Default off. |
| `allow_runtime_domain_binding` | Bindings of mode `runtime_domain_binding` may be activated and used. Default off. |
| `allow_development_runs` | The editor's test chat, toolkit tests, on-demand indexing and chats with credential-bearing toolkits may run without a binding as a *development run*: the signed-in author only, the author's own credentials only (personal, or project credentials granted to that person on any agent). Null (not chosen) means the default of the mode: on under `audit_only`, off under `off` and `enforce`. A write that switches into `enforce` and sends no value switches it on; an explicit value always wins; a deployment already in `enforce` keeps its value at upgrade. |
| `environment` | Label; refused with `FIELD_LOCKED` when the deployment sets it |
| `change_note`, `expected_revision` | Required. The write is a compare-and-set on the revision (`REVISION_CONFLICT`). |

Reading needs `governance.settings.view` (super administrators and platform administrators). Changing needs
`governance.settings.manage`, the `super_admin` role (a role-matrix edit cannot widen it), and an interactive
session (an API token cannot change it). Each change writes a history row and the audit events
`GOVERNANCE_SETTINGS_CHANGED` and `GOVERNANCE_MODE_<MODE>` in one transaction. The body is limited to 16 KiB.

Behaviour of the stored state:

- Dispatch never queries the table. Each replica keeps a snapshot, refreshed at most every 10 s and
  immediately on the `credential_governance_settings_changed` event.
- No row or no table reads as `off`. A row that cannot be read at start counts as `enforce` and unavailable
  (governed admissions are refused) unless the ceiling is `off`.
- Switching to `off` stops checks for new runs. Runs already admitted keep their admission.
- If the mode is on and a blocking preflight check fails, new admissions are refused with
  `DEPENDENCY_UNAVAILABLE` and audited, and the Admin UI shows why. Runs already admitted are not touched.

### Preflight

`audit_only` and `enforce` cannot be saved while a blocking check fails (`PREFLIGHT_FAILED`).

| Check id | Blocking | Passes when |
|---|---|---|
| `identity_verifier` | yes | The verifier is configured and the identity-by-subject and permission lookups exist |
| `auth_rpc_mode` | yes | Authentication runs in RPC mode |
| `security_audit` | yes | The tracing plugin exposes the audit record method |
| `audit_schema` | `audit_only` only | The audit schema accepts `would_deny` |
| `audit_backlog` | yes | The outbox backlog is under its limit (evaluated live by every admission) |
| `credential_facts` | yes | `secrets` and `configurations` provide the facts, release and secret-field methods, the sealed transport loads, and the facts version is at least 3 |
| `master_key` | yes | The `database` store has its master key. The run token is signed with a key derived from the same master key whatever the engine, so set it in every engine once governance is on. |
| `settings_table` | for saving | The settings tables exist |
| `corporate_vault` | warning | A corporate Vault address is configured |
| `governed_worker` | warning | A live worker announces the governed capabilities |

## Refusals

Every refusal has a stable code and a fixed message. The message never contains a title, id, claim, token or
dependency text.

| Group | Code | Meaning |
|---|---|---|
| Identity | `IDENTITY_EVIDENCE_MISSING`, `IDENTITY_SUBJECT_UNLINKED`, `IDENTITY_KIND_NOT_ADMITTED`, `IDENTITY_REVOKED`, `IDENTITY_EXPIRED`, `IDENTITY_SIGNATURE_INVALID`, `IDENTITY_AUDIENCE_MISMATCH`, `IDENTITY_TENANT_MISMATCH` | No verified person behind the run |
| Binding | `DENIED_BINDING` | The agent version has no usable active binding (from the editor: development runs are off) |
| | `DENIED_ENTRY_POINT` | This kind of run cannot be started from that entry point |
| Development | `DENIED_DEVELOPMENT_CREDENTIAL` | A development run asked for a project credential the author holds no active grant on |
| | `DENIED_DEVELOPMENT_CHILD` | A sub-agent of a development run asked for a credential the draft was not admitted for |
| | `DEVELOPMENT_RUNS_DISABLED` | Development runs are switched off |
| | `TOOLKIT_RUN_TOKEN_NOT_ADMITTED` | A toolkit run started with an API token |
| Run token | `INTERNAL_TOKEN_INVALID`, `INTERNAL_TOKEN_EXPIRED`, `CONTEXT_NOT_LIVE` | The run token is forged or on a route it may not use; past its time limit; its run is no longer admitted |
| | `INTERNAL_SCOPE_DENIED`, `INTERNAL_OPERATION_UNKNOWN`, `INTERNAL_TARGET_DENIED` | The operation is outside the binding's scope; the write has no operation; the call names another project |
| | `INTERNAL_TOKEN_UNAVAILABLE`, `INTERNAL_MCP_ORIGIN_NOT_ALLOWED` | No run token could be minted; an Elitea MCP toolkit points to another address |
| | `BINDING_DIGEST_MISMATCH` | The agent changed after its binding was approved |
| Mandate | `MANDATE_REQUIRED` | No approved mandate or verified person covers this run |
| | `AUTHORIZATION_REQUIRED` | A per-user schedule acts on a person's behalf without that person |
| | `DENIED_OPERATION`, `MANDATE_INACTIVE`, `MANDATE_EXPIRED` | Wrong trigger kind, suspended or revoked, past validity |
| | `RUNTIME_LIMIT_EXCEEDED` | The run overran `max_runtime_s` and was stopped |
| | `SUB_AGENT_CREDENTIAL_NOT_ADMITTED` | A scheduled or webhook run calls a sub-agent that uses credentials |
| Credential | `CREDENTIAL_REFERENCE_REQUIRED` | The credential is not registered for governed use, or is selected by title or inline |
| | `CREDENTIAL_REFERENCE_INACTIVE`, `CREDENTIAL_GENERATION_MISMATCH` | Reference not active or in a usable store; credential changed after approval |
| | `CREDENTIAL_CLASS_NOT_ALLOWED` | A personal credential that belongs to someone else |
| | `DENIED_GRANT`, `DENIED_RBAC` | No active grant for this agent, toolkit and subject; the person lacks `credentials.use` |
| | `UNSUPPORTED_AUTH_MODE` | The toolkit cannot use a governed credential yet |
| Release | `RATE_LIMITED`, `STORE_DENIED`, `STORE_SEALED`, `STORE_UNAVAILABLE`, `STORE_TLS_INVALID`, `STORE_NOT_FOUND`, `STORE_VERSION_CONFLICT` | Release bound exceeded; the credential store refused or is unavailable |
| Worker | `WORKER_CAPABILITY_UNAVAILABLE` | No live worker can honour the governed task |
| Context | `STALE_CONTEXT`, `EPOCH_MISMATCH`, `UNKNOWN_SCHEMA_VERSION` | The admission is no longer valid, access was revoked since, or the record is not understood |
| Delegated *(from PR #602/#7/#21/#786, pending)* | `DENIED_SUBJECT_MISMATCH`, `DENIED_ASSERTION_UNAVAILABLE`, `DENIED_ASSERTION_SOURCE_UNSUPPORTED`, `DENIED_MANDATE_DELEGATION_UNSUPPORTED`, `DENIED_DELEGATION_UNSUPPORTED_CALLER`, `DENIED_OAUTH_APP_UNAVAILABLE`, `DELEGATION_EXCHANGE_FAILED` | A Microsoft sign-in credential cannot be released for this run. Meaning and action: [operator runbook, section 8](operator-runbook.md#8-microsoft-sign-in-credentials-delegated) |
| Platform | `AUDIT_UNAVAILABLE`, `DEPENDENCY_UNAVAILABLE` | The decision could not be taken or recorded. Nothing is let through; try again. |

Allow reason codes: `ADMITTED`, `CREDENTIAL_USE_ALLOWED`, `CREDENTIAL_RELEASED`, `CREDENTIAL_PROBE_ALLOWED`,
`CREDENTIAL_PROBE_SUCCEEDED`.

### What the user sees

| Entry point | Shape |
|---|---|
| Chat (Socket.IO) | The error event carries `{code, error}` and the chat shows an error card with that message |
| Predict API | HTTP 403 `{code, error}` |
| Webhook | HTTP 403 `{code, error}`; HTTP 503 for `DEPENDENCY_UNAVAILABLE` and `AUDIT_UNAVAILABLE` so the sender retries |
| Pipeline schedule (legacy cron) | The refusal is written to the run's History entry; the cron slot is consumed, so it does not retry every minute |
| A person's schedule (agent or pipeline), including Run now and Test run | The run is marked failed at once with the readable reason, the owner gets a "schedule failed" notice, and the run's chat says why. The run never stays *running*. |
| Credential test (check connection) | HTTP 403 with `{success: false, code, message}`; a provider failure is `{success: false, message}` |
| Operator API | `{error, code}` with a fixed message and a status per code: 400 invalid, 401, 403 permission or self-approval, 404, 409 state or concurrent change, 413 too large, 503 dependency or audit |

## Audit events

Security audit events (schema v1, relayed from an outbox written in the same transaction as the change they
describe). Fields are closed and secret-shaped strings are rejected.

| `event_type` | `reason_code` examples | Written when |
|---|---|---|
| `admission` | `ADMITTED`, `ADMITTED_DEVELOPMENT`, or a refusal code | Each admission decision |
| `platform_operation` | `PLATFORM_OPERATION`, `INTERNAL_SCOPE_DENIED`, `INTERNAL_OPERATION_UNKNOWN`, `INTERNAL_TARGET_DENIED`, `INTERNAL_TOKEN_INVALID` | A write through the built-in Elitea tools, before it is made, and a refusal of the run token. Reads in scope are not recorded. |
| `decision` | `CREDENTIAL_USE_ALLOWED` (one per credential use; event id = `decision_id`), refusal codes | Admission credential decisions; refused context validation |
| `credential_acquired` | `CREDENTIAL_RELEASED`, refusal codes (event id = `release_id`) | Every release after the request is well formed |
| `probe` | `CREDENTIAL_PROBE_ALLOWED`, `_SUCCEEDED`, `_FAILED`; `CREDENTIAL_PROBE_STARTED`, `CREDENTIAL_REF_VALIDATED`, `CREDENTIAL_CONNECTION_CHECKED`, `_CHECK_FAILED` | Governed probe; operator probe of a reference |
| `approval` | `CREDENTIAL_REF_APPROVED`, `GRANT_ACTIVE`, `MANDATE_ACTIVE` | An approval |
| `lifecycle` | `CREDENTIAL_REF_PENDING`, `CREDENTIAL_IMPORT_STARTED`, `CREDENTIAL_GENERATION_COMMITTED`, `CREDENTIAL_REF_ACTIVE`, `CREDENTIAL_MATERIAL_DELETED`, `GRANT_PENDING_APPROVAL`, `BINDING_<STATE>`, `BINDING_REBIND_PENDING_APPROVAL`, `MANDATE_<STATE>`, `MANDATE_REJECTED`, `MANDATE_OWNER_SUSPENDED`, `GOVERNANCE_SETTINGS_CHANGED`, `GOVERNANCE_MODE_<MODE>`, `GOVERNANCE_SETTINGS_UNREADABLE`, `GOVERNANCE_PREFLIGHT_FAILED` | A state change by an operator or by the platform |

The schema also defines `refresh`, `rotation`, `withdrawal`, `issuer_action` and `execution_outcome`; slice 1
does not emit them. Events carry caller, user and mandate references, agent binding, run id and attempt,
toolkit instance, credential reference and generation, `decision`, `reason_code`, `would_deny`, policy
revision, revocation epoch, approver, and `traceparent`. Failure policy is set in the tracing plugin
(`security_audit.failure_policy`): `buffer` (default) or `fail_closed`, which refuses admissions with
`AUDIT_UNAVAILABLE` while the undelivered backlog is over `backlog_limit` (default 10000).

## Security metrics

Recorded as OpenTelemetry/Prometheus counters and as a structured `security_metric name=... value=...` log line
(at most one line per counter and label set every 10 s; increments in the window are folded). Labels are codes,
never identifiers.

| Counter | Labels | Counts |
|---|---|---|
| `admission_decision_total` | `decision` (`allow`, `deny`, `would_allow`, `would_deny`, `would_deny_error`), `reason_code` | Admission outcomes |
| `credential_decision_total` | `decision`, `reason_code` | Credential-use decisions |
| `admission_duration_seconds` (histogram) | `mode`, `decision` | Time to decide one admission |
| `execution_context_validation_total` | `result`, `reason_code` | Worker validations |
| `credential_release_total` | `decision` (`allow`, `deny`, `rejected_request`), `reason_code` | Releases |
| `credential_probe_total` | `decision`, `reason_code` | Governed probes |
| `governed_run_stop_total` | `result` (`stopped`, `stop_failed`), `reason_code` | Runs Main stopped (mandate bounds, revocation) |
| `governed_dispatch_unavailable_total`, `governed_dispatch_without_slot_batch_total` | none | Dispatches refused because no live worker supports the task; governed dispatches while no worker announces the batch release |
| `security_audit_outbox_depth`, `security_audit_delivery_lag_seconds`, `security_audit_drop_total` | none | Undelivered audit rows; age of the oldest; events lost because the outbox could not be written |

## Performance contract

Governance adds I/O only when it is on. Measured on one rehearsal stack (`database` store, 30 messages per cell
after warm-up, 1, 3 and 6 credential toolkits, p50), difference to `off`:

| Per chat message, before the model call | `enforce` | `audit_only` | `off` |
|---|---|---|---|
| Worker-bus calls | **2** (one validation, one release of all slots), flat in toolkit count | 0 | 0 |
| Database sessions added | **about 15** (15.4 at 1 and 6 toolkits) | +5 | 0 |
| Statements added | **about 26** (25.3 to 27.7) | +8 | 0 |
| Redis commands added | +16 to +36 | +4 to +7 | 0 |
| Time to model call | +43 to +55 ms in clean runs at 6 toolkits; two disturbed cells +91 and +139 ms | within noise (-17 to +23 ms) | baseline |

- Before the batch release the same path cost one bus call per toolkit (7 calls, +60 sessions, +72 statements at
  6 toolkits). The batch release and the run-level release brought it to a constant.
- Main's release handling is about 13 ms plus a 13 to 24 ms bus round trip at 6 slots.
- The `off` path adds no statements, sessions, Redis commands or governance RPCs (126.2/170.3/236.6 sessions at
  1/3/6 toolkits against a baseline of 126.3/169.9/236.1). Run-to-run noise on the machine is about 50 ms, so the
  millisecond figures are indicative and are not a latency commitment.
- Not measured: Vault store writes, probe timing, operator lists at 1000 rows.
- The counts are pinned by tests that fail when a path becomes more expensive again
  (`tests/unit/test_c1009_perf_admission_release.py`). Operator lists are page-bounded (default 20; at most 50 bindings, 100 references, grants or audit rows).

Fixed bounds: 64 governed credentials per run; 32 fields per slot; 16 releases per context, toolkit and field;
probe ticket 90 s; admin request bodies 8 KiB, settings 16 KiB, personal value 32 KiB (8 KiB per value,
32 fields); mandate validity 180 days; grant validity 365 days.

## Operator API surface

All routes are in administration mode: `/api/v2/<plugin>/<route>/administration[/<project_id>[/<id>]]`.
Secret material is never returned and there is no read route for it.

| Plugin | Route | Methods | Permission |
|---|---|---|---|
| `elitea_core` | `governance_settings` | GET, PUT | `governance.settings.view`; `.manage` plus super admin |
| | `governance_settings_history` | GET | `governance.settings.view` |
| | `governance_agents/<pid>` | GET | `governance.bindings.view` |
| | `agent_identities/<pid>`, `agent_identity/<pid>/<binding>` | GET; POST (create) | `governance.bindings.view`; `.manage` |
| | `agent_identity_action/<pid>/<binding>` | POST: `submit`, `activate`, `suspend`, `resume`, `retire`, `rebind` | `.manage`; `.approve` for activate |
| `secrets` | `credential_references/<pid>`, `credential_reference/<pid>/<ref>` | GET; POST (create) | `credentials.view`; `credentials.manage` |
| | `credential_reference_action/<pid>/<ref>` | POST: `import`, `probe`, `approve`, `activate`, `suspend`, `resume`, `retire` | `.manage`; `.probe`; `.approve` |
| | `credential_material/<pid>/<ref>` | PUT (write-only value, owner only; also in project mode) | `credentials.set_own_value` |
| | `credential_grants/<pid>` | GET; POST | `credentials.view`; `credentials.manage` |
| | `credential_grant_action/<pid>/<grant>` | POST: `approve`, `suspend`, `resume`, `revoke` | `.approve`; `.manage` |
| `configurations` | `credential_candidates/<pid>` | GET | `credentials.view` |
| `tracing` | `security_audit`, `security_audit_summary` | GET (filters incl. `would_deny`; summary by decision and code over 1 to 168 h) | `models.admin.audit_trail.view` |

The project-mode (non-administration) route `credential_references/<pid>?mine=true` lists a person's own
personal references. **Mandates have no REST route in slice 1**: they are managed with the admin task.

**Admin UI** (`/governance`, one sidebar entry): tabs *Overview* (the switch, preflight, settings history, the
security events drawer), *Agent identities*, *Credentials* (references, probe, approve, personal value for the
owner), *Grants*, and *Mandates* (a pointer to the admin task).

### Admin tasks

Created tables are additive. New projects also get them from `create_all`, and a startup guard creates them for
existing projects, except the settings tables, which are created only by their task. The `create_*` tasks are
idempotent and accept `dry_run`.

| Plugin | Task | Purpose |
|---|---|---|
| `elitea_core` | `create_governance_settings_tables` | `credential_governance_settings` and its history (public schema). Until it runs the mode reads `off` and the UI cannot save. |
| | `create_agent_identity_tables` | `agent_identity_binding`, `execution_security_context` per project |
| | `create_mandate_tables` | `autonomous_mandate` per project |
| | `manage_agent_identity_binding`, `manage_autonomous_mandate` | Scripted lifecycle (mandates: `create`, `submit`, `reject`, `approve`, `suspend`, `resume`, `revoke`) |
| | `report_trigger_principals` | Read-only report of schedules and webhooks that would be refused |
| `secrets` | `create_credential_governance_tables` | `credential_reference`, `credential_use_grant`, `credential_store_ledger` per project; also the personal-reference index and the `written_by` column |
| | `manage_credential_reference`, `manage_credential_grant` | Scripted lifecycle |
| | `reconcile_credential_store`, `credential_store_smoke` | Settle interrupted store writes; smoke-test the store |

## Non-goals of slice 1

| Not in slice 1 | Tracked in |
|---|---|
| Entra on-behalf-of exchange for **autonomous** runs (mandates on a person's refresh token). The exchange for a signed-in person's run is built (`entra_delegated`) but pending, see the [runbook](operator-runbook.md#8-microsoft-sign-in-credentials-delegated) | #7131 |
| OAuth-authorized toolkit tokens and MCP OAuth/DCR registrations as governed references; `mcp_tokens` removed from payloads | #7132 |
| Project-level governance (platform ceiling, optional floor, project override, project operator view); project administrators approving within their project | #7133 |
| Owner self-service for personal credentials in EliteaUI (the owner writes the value through the API today) | #7130 |
| Mandate REST routes and a Mandates tab; scheduled indexing under a mandate | Follow-up |
| Per-provider qualification matrix, adapters and capability descriptors | #6759 |
| Pinning the inbound MCP revision and OAuth resource-server metadata | Open gate (D8) |
| Revocation latency, outage and retention numbers agreed with the customer | Open gates (D5) |
