# Identity boundaries

Part of [credential governance](README.md). This file says where identity is trusted, who decides, what the
worker must prove, and how credential material travels. It also defines the three ways into the platform, the
subject model, and the run token, the identity a governed run carries. Credentials and approvals are in
[credential-profiles.md](credential-profiles.md). Operator steps are in [operator-runbook.md](operator-runbook.md).

Items marked *(from PR #602/#7/#21/#786, pending)* are on open pull requests and not on `main` yet.

```
browser / API client
   | session or API token
   v
 entry point (Main) --- builds AdmissionRequest from the authenticated holder only
   |
   v
 admit_execution (Main, policy decision point) -- verifier (auth plugin) ; bindings ; credential decision
   |   ExecutionSecurityContext row + audit event, one transaction
   v
 dispatch: payload carries credential slots, never material; envelope carries context_ref
   |
   v   worker bus (Redis)
 indexer worker --- restricted_validate_execution_context --> Main   "is my admission still valid?"
   |            --- restricted_release_credential(s)  -------> Main   "material of slot X, sealed to my key"
   v
 toolkit built with the material, in the process that runs the toolkit
```

## Where identity is trusted

| Point | Trusted source | Never trusted |
|---|---|---|
| Interactive chat (`chat_sio`, `chat_sio_llm`) | `auth.sio_users[sid]`, set when the socket connected | `user_context`, `user_id` or any id inside the payload |
| Predict API (`predict_api`) | `flask.g.auth` of the HTTP request | The same fields in the body |
| Schedule and webhook (`schedule`, `webhook`) | A single-use ticket minted in-process by the scheduler tick, by the owner-schedule starter (a person's schedule, entry point `schedule`) or by the webhook API after signature verification, bound to the agent version (60 s, 1000 entries, memory of one process) | A run that arrives at the predict RPC without a ticket is an ordinary system run and is refused (`MANDATE_REQUIRED`) |
| Toolkit test and on-demand indexing (`toolkit_test`, `index_on_demand`) | The same session or HTTP identity as chat; the entry point is set by the trusted test entry from the tool it was asked to run | The payload |
| Calls the run makes back to the platform | The run token (below), checked on every call | Any identity the worker or a toolkit supplies |
| Worker | Nothing by itself. It presents `context_ref`, and Main checks it against rows Main owns. | Any claim the worker makes about the user |

The entry point builds an `AdmissionRequest` from the holder (`type`, `id`, session reference) and from the
version row it loaded itself. An unreadable or unknown holder becomes the `public` caller, which admission
refuses. The session reference is a secret: it is never dumped, logged or stored.

### The verifier

`auth.verify_identity(evidence, admitted_kinds)` lives in the `auth` plugin and is configured in the
`identity_verifier` section. It turns evidence into a verified principal or a typed rejection.

| Evidence | Checked | Result |
|---|---|---|
| `session_user` (the OIDC session of a signed-in person) | Token signature against the issuer's keys, `iss`, `aud`, `tid` equals the configured tenant, `exp`, `nbf`, `oid` present, and the platform user is linked to **issuer + tid + oid** (never to an e-mail address). User and token epochs are read and the identity must be active. | `verified_user` evidence: `issuer`, `tid`, `oid`, audience. Becomes the delegation subject `idp:<tid>:<oid>`. |
| `legacy_pat` (a personal API token) | The token is active | `asserted_user` evidence. Admitted only when the operator switch *Admit legacy API tokens* is on. An asserted user is never converted into a verified one: such a run cannot act for a person and gets `MANDATE_REQUIRED`. |

Rejections are `IDENTITY_KIND_NOT_ADMITTED`, `IDENTITY_EVIDENCE_MISSING`, `IDENTITY_SIGNATURE_INVALID`,
`IDENTITY_EXPIRED`, `IDENTITY_AUDIENCE_MISMATCH`, `IDENTITY_TENANT_MISMATCH`, `IDENTITY_SUBJECT_UNLINKED`,
`IDENTITY_REVOKED` and `DEPENDENCY_UNAVAILABLE`. The messages are fixed text.

Governance requires authentication in RPC mode and a configured verifier. Both are preflight checks.

## Main is the policy decision point

`admit_execution` runs these steps for each governed run. Every step fails closed, and a refusal is audited in
its own transaction, so a failing audit write cannot let a run through.

| Step | Decision | Refusal |
|---|---|---|
| 1 | The security audit trail can take a decision | `AUDIT_UNAVAILABLE` |
| 2 | A run with no person behind it (system) needs a mandate | `MANDATE_REQUIRED` |
| 3 | The caller is a verified person (or an admitted token) | `IDENTITY_*` |
| 4 | The agent version has exactly one active binding of an admitted mode, and the definition digest still matches. A **development run** (entry point `editor_chat`, `toolkit_test`, `index_on_demand`, or a plain LLM chat that carries a credential; a signed-in person; *Allow development runs* on) reads the definition and its digest instead, and requires the author's right to edit the application; the decision is taken from the request kind before any binding is read | `DENIED_BINDING`, `BINDING_DIGEST_MISMATCH`, `DENIED_RBAC` |
| 5 | The subject is a verified delegation. A mandate run replaces steps 2 to 5 (below). | `MANDATE_REQUIRED`, `IDENTITY_EVIDENCE_MISSING` |
| 5b | Every credential the run uses passes the [credential decision](credential-profiles.md#the-credential-decision). A development run replaces the grant to the binding with the author's own right: a personal credential of the author, or an active grant of the author's subject on the credential, on any binding and toolkit | `CREDENTIAL_*`, `DENIED_GRANT`, `DENIED_RBAC`, `DENIED_DEVELOPMENT_CREDENTIAL` |
| 6 | The `ExecutionSecurityContext` row and the allow events are written in one transaction | `DEPENDENCY_UNAVAILABLE` |

A **mandate run** reads the mandate from the table (the request only names it) and requires: the mandate
exists for this exact agent version, is `active`, in time, and allows the trigger operation; the owner is
active and still linked to the same identity; the mandate's binding is the version's active binding; the owner
holds `models.applications.predict.post` in the project. The credential decision then runs with the mandate as
subject. The run is stopped by Main at the earlier of `max_runtime_s` and `valid_until`.

The context row holds references and limits only: project, application, version, binding, definition digest,
subject (`kind`, `ref`, delegation reference), caller, run id and attempt, `revocation_epochs` (user, token,
binding, mandate, credential), the credential decisions (`allowed_bindings`), policy revision, environment,
`expires_at` (the context TTL, default 3600 s, or the mandate bound) and `state`
(`admitted`, `running`, `completed`, `revoked`). It never holds a token, cookie or secret.

Children of a governed run (parallel children, resumed parents) carry the parent's `context_ref` and are sealed
against the parent's decisions: a credential the parent was not admitted for is refused. Under a **development**
parent, each sub-agent the draft calls gets its own decision and its own context row (`parent_context_ref` and the
ancestors in `call_lineage`): bound with one active binding and an unchanged digest means a governed child on its
own binding and grants; anything else is a development child of the same author. A parent's admission outlives a
switch to `off`.

## The worker proves its admission

The worker bus has no caller authentication. Authority therefore never comes from the caller. It comes from
identifiers that cannot be guessed (`context_ref`, `decision_id`, or a single-use `probe_ref`), checked against
rows that only Main writes.

Before it builds an LLM client or touches a tool, the worker calls
`restricted_validate_execution_context(project_id, context_ref, caller)` with
`caller = {kind: "indexer_worker", task_name, run_id}`. Main checks, in order:

1. The context exists, has a known schema version, is governed, is `admitted` or `running`, and is not expired.
2. The caller's task name is an admitted task and its run id equals the context's.
3. The user is still active and the user epoch equals the one stamped at admission.
4. The binding is `active`, its epoch and definition digest are unchanged.
5. For a mandate: the owner is active, the mandate epoch is unchanged, and it is still in time.
6. Every stored credential decision still holds (reference state, epoch, generation, grant).

A failure revokes the context (a conditional update, so a context that already left its live states is not
moved) and returns the reason as a code. A dependency that cannot answer returns `DEPENDENCY_UNAVAILABLE`: the
state is unknown, not revoked, and the run may be retried. Success moves the context to `running` and returns
references only.

### Task envelope and capability negotiation

Every governed dispatch carries `TaskEnvelope v1`: `schema_version`, `security_profile` (`legacy` or
`governed`), `required_capabilities` and `context_ref`. A governed envelope rejects unknown fields. The worker
refuses an envelope it cannot honour (`UNKNOWN_SCHEMA_VERSION`, `MALFORMED_ENVELOPE`, `CAPABILITY_MISSING`).
Main refuses to dispatch a governed task when no live worker announces what it needs
(`WORKER_CAPABILITY_UNAVAILABLE`). It never falls back to a worker that cannot enforce the contract.

| Capability | Announced by | Meaning | Required |
|---|---|---|---|
| `task_envelope.v1` | worker | Understands the governed envelope | Yes |
| `credential_slots.v1` | worker | Resolves credential slots through the trusted release | Yes, for agent runs and probes |
| `governed_ops_pool.v1` | worker | Runs the `governed_ops` pool | Yes, for probes |
| `credential_slots_batch.v1` | worker | Releases all slots of one toolkit build in one call (`restricted_release_credentials`) | No, preferred. Without it the worker releases slot by slot. |
| `credential_slots_run.v1` | worker | Releases every slot of the whole run in one call | No, preferred. Defined in `indexer_worker` and `elitea-sdk`; Main serves the batch handler and does not gate on it. |

Workers announce on `task_node_capabilities` and Main keeps what it last heard (TTL 120 s). When the validation
reply carries `batch_release: true`, Main has registered the batch handler and the worker may use it. A rolling
upgrade never leaves a governed task without a worker: the single-slot call stays available.

## Reference-only payloads and late release

At the single dispatch chokepoint (`admit_dispatch`), after a run is admitted or its inherited context is
accepted, and before the payload is handed to the task node, every secret field of each decided credential is
removed and replaced by a **credential slot**:

```
credential_slot = { v: 1, credential_ref, credential_generation, decision_id, fields: [<secret field paths>] }
```

- Secret fields come from the configuration type's schema (`configuration_secret_fields`). A map of secrets
  (for example HTTP headers) is listed as `<path>.*` and every key under it is removed and named.
- The seal covers the run's toolkits and the prefetched sub-agent toolkits. A credential that cannot be matched
  to an allowed decision, a type with no known secret fields, or a configuration that holds no secret value is
  refused. Several toolkits treat a missing secret as anonymous access, so a missing slot must never run.
- `mcp_tokens` (browser OAuth tokens for MCP servers) are not store material and stay as before (#7132).

The worker asks Main for the material of a slot with `restricted_release_credential` (or the batch
`restricted_release_credentials`, 1 to 64 slots), passing a recipient public key generated for that call.
Main checks, first failure wins:

| # | Check | Code |
|---|---|---|
| 1 | The request is well formed | `STALE_CONTEXT` (counted, not audited: nothing was admitted) |
| 2 | The audit trail can take a decision | `AUDIT_UNAVAILABLE` |
| 3 | The context is governed, `running`, in time, and belongs to this task and run (a probe: the ticket exists) | `STALE_CONTEXT` or the revoke reason |
| 4 | The context was admitted for exactly this toolkit, field, credential, generation and `decision_id` | `CREDENTIAL_REFERENCE_REQUIRED` |
| 5 | At most 16 releases per context, toolkit and field (128 for a context reopened for fan-out) | `RATE_LIMITED` |
| 6, 7 | The live admission still stands: user and binding epochs, digest, mandate, and the credential decision of this use is re-checked. A refusal revokes the context. | codes of validation |
| 8 | The store read by the `secrets` plugin, which re-reads the reference | `CREDENTIAL_REFERENCE_INACTIVE`, `CREDENTIAL_GENERATION_MISMATCH`, `STORE_*` |
| 9 | The `credential_acquired` event is committed **before** the reply leaves | `AUDIT_UNAVAILABLE` |

### Sealed transport

Main seals the reply to the recipient key: X25519 with a fresh ephemeral key on Main's side, HKDF-SHA256
(info `elitea-credential-release-v1`) to an AES-256-GCM key, with the request context bound as additional
authenticated data. Sealed form: `{v: 1, epk, nonce, ct}`. The plaintext is `{fields, generation, expires_at}`.

```
AAD = "v1|<project_id>|<context_ref or probe_ref>|<toolkit_instance_id>|<field>|<decision_id>|<release_id>"
```

A batch release seals each slot under its own AAD and its own `release_id`, so a reply for one slot cannot be
replayed as another. For a probe, `toolkit_instance_id` is `probe`, `field` is `configuration` and
`decision_id` is empty. Any transport failure is a fixed `TransportError`; nothing in it names which part was wrong.

The SDK applies the material at the last moment, into a local copy of the toolkit settings, when the toolkit
is built. An unresolved slot is a readable error. There is no fallback to an unauthenticated toolkit.

## What never leaves the boundary

Credential material is absent from: the dispatched task payload, Redis task arguments and pub/sub messages
(except as an AEAD ciphertext addressed to one requester), LangGraph checkpoints, parallel-child stashes,
logs, audit events, error messages, REST responses, and the context row. Audit events and metrics carry ids,
generations and codes, and the event schema rejects secret-shaped strings.

Scope of slice 1: it governs what is dispatched and released. It does not isolate a run's credentials from code
running inside the same agent process, and it does not remove or re-encrypt existing stored configuration
values. Material is never available to another run or to a binding the context was not admitted for.

## The `governed_ops` pool and probes

A saved credential is tested through a separate arbiter pool, `governed_ops`, so that probes do not run in the
agent pool and cannot starve it. It is a fork pool inside the indexer with its own finite limit
(`governed_ops_task_limit`, default 4). It runs one task, `indexer_governed_check_connection`. It is enabled by
worker configuration (`governed_ops_enabled`, default off, outside the admin schema) and announces
`governed_ops_pool.v1` only when it started and `credential_slots.v1` works.

`governed_check_connection(project_id, configuration_uuid, config_type, user_id, actor_user_id, onboarding)`
decides in Main: the actor holds `credentials.probe`; the configuration is a credential of that project and
type; its reference is `validated` or `active` (`pending` too while onboarding) at the current generation; a
worker can run the task. It then commits the `probe` event, stores a single-use ticket (metadata only, 90 s,
taken once by the release), and starts the task. The worker releases the material with `probe_ref` instead
of `context_ref`. Only a **saved** credential is tested; values typed into a form are not. The answer is
`{success: true, ...}`, a provider failure `{success: false, message}`, or a refusal
`{success: false, code, message}`. For a personal reference the actor may be an administrator while the
`user_id` is the owner whose value is tested.

## Three ways in

A run can start in three ways. The platform checks each by its own evidence and never converts one into
another. Behind all of them a governed run carries the same kind of internal identity, the run token (below).
The audit trail always names the way in.

| Way in | Evidence | Who the run acts for | What it may do under Enforce |
|---|---|---|---|
| **Session** | A signed-in person's OIDC session (`verified_user`) | That person: subject `idp:<tid>:<oid>` | Everything governance allows: bound agents with grants, development runs of their own drafts, toolkit tests, Microsoft sign-in credentials. |
| **API token** (a personal API token, PAT) | The token is active (`legacy_pat`, an asserted user). Admitted only with *Admit legacy API tokens*. | No one. An asserted user is never converted into a verified one. | Cannot act for a person: such a run needs a mandate (`MANDATE_REQUIRED`). Cannot author (no development runs). Toolkit runs started with it are refused (`TOOLKIT_RUN_TOKEN_NOT_ADMITTED`). Cannot use a Microsoft sign-in credential. |
| **Mandate** (autonomous) | A ticket from a trusted in-process caller (scheduler, owner-schedule starter, webhook API) and a mandate row | The mandate: subject `mandate:<uuid>` | Only the version the mandate names, the operations it allows, until its deadline, with credentials granted to the mandate. Cannot use a Microsoft sign-in credential. |

Personal API tokens stay in the platform. Agents and pipelines are also exposed as MCP servers and over the API,
and a caller there has no browser session. What changes is the token's role: inside a run, where a verified
context already exists, the run token replaces it. The token still identifies an **external caller**.

*Planned, not promised:* scoped, expiring API tokens (projects, operations, expiry, revocation visible to admission)
as their own evidence kind, so that they can be admitted by default; tracked as #7230. Mandates on the owner's
refresh token and a sharing model for schedules are tracked as #7131.

## The subject model

Every governed run has a subject: the party it acts for. It is stored on the context row as references only.

| Part | Meaning |
|---|---|
| `issuer` | Who vouched for the person (the token's `iss`). |
| `tid` | The tenant. Entra: the `tid` claim. Other issuers: a fixed GUID the operator assigns to the realm. |
| `oid` | The person inside that tenant. Entra: the `oid` claim. Other issuers: a claim you name, usually `sub`. |
| `ref` | `idp:<tid>:<oid>` for a person, `mandate:<uuid>` for a mandate. Grants and mandates are tied to it. |
| `kind` | `delegation` (a person) or `mandate`. |

The platform user is linked to **issuer + tid + oid**, never to an e-mail address. A person that signs in again
through the same issuer is the same subject. A person that signs in through a different identity provider is a
**different subject**: their grants and mandates do not follow them, and an operator makes the grants again.

**How a Keycloak login maps into it.** Keycloak has no tenant claim and its subject claim is `sub`. In `auth_oidc`
set `subject_claims: {tid: null, oid: sub}` and `subject_tenant: <GUID>`; in the `auth` plugin's
`identity_verifier.session_user` set the same `subject_claims` and the same GUID as `tenant_id`. The subject is then
`idp:<GUID>:<keycloak user id>` and the issuer is the realm URL. Keycloak omits `nbf` (set `require_nbf: false`),
and if its access token has no audience for the platform the verifier can read the ID token instead
(`delegation_token: id_token`). The operator steps are in the [runbook](operator-runbook.md#2-before-you-switch-it-on-preflight).

## The run token

When a run is admitted under Enforce, the platform (Main) mints one short-lived token for it. It is the run's
**only** platform identity: no personal token, no system token and no session is put on the run's payload, for a
signed-in person's run and for a run under a mandate. Main is the issuer. The platform's own endpoints, mainly the
built-in Elitea tools, are the resource. The audit trail names both: "user X through agent Y".

The token is a bearer. It never leaves the run's worker, goes only to the platform's own origins, and lives no
longer than its run. It is signed with HMAC-SHA256 under a key derived from the platform master key. It is **not**
an Entra token and nothing outside the platform accepts it.

| Claim | Meaning |
|---|---|
| `iss` | The issuer, `elitea:main`. |
| `aud` | The audience, `elitea:internal`. |
| `sub` | The subject reference (`idp:<tid>:<oid>` or `mandate:<uuid>`). |
| `uid` | The platform user the run acts as. |
| `act` | The actor: the agent binding id, or `development:<definition digest>` for a development run. |
| `scope` | The platform operations (`read`, `build`, `credentials`, `run`): the binding's operations **intersected with** the person's permissions, worked out once at admission. Empty for a token that only serves the run's own client calls. |
| `ctx` | The security context (the admission) the token belongs to. |
| `prj`, `tgt` | The agent's project, and the projects its built-in Elitea tools address. |
| `run`, `tsk` | The run id and the task. |
| `iat`, `exp`, `jti` | Issue time, expiry and a unique id. |
| `v` | Token version, `1`. |

Rules, in the order they are checked on every call:

1. The signature, version, issuer and audience, the claim shapes, the issue time and the expiry. A forged or
   malformed token is `INTERNAL_TOKEN_INVALID`; an expired one `INTERNAL_TOKEN_EXPIRED`.
2. The route. Only the built-in Elitea MCP routes and the run's own client routes accept the token (operator
   runbook, section 7). Any other route answers `INTERNAL_TOKEN_INVALID`, the same text for every route.
3. The project. The call must be in the token's projects, else `INTERNAL_TARGET_DENIED`.
4. The context. It must be admitted or running, in time, and every epoch it was admitted under must still hold
   (`CONTEXT_NOT_LIVE` otherwise). A positive answer is reused for about five seconds.
5. The operation, for the built-in Elitea tools: an operation outside `scope` is `INTERNAL_SCOPE_DENIED`; a write
   the platform has no operation for is `INTERNAL_OPERATION_UNKNOWN`.
6. The person's own permission check on the endpoint, as for a session. The token adds a limit; it never adds a
   right.

Every write through the built-in Elitea tools is recorded (`platform_operation`) before it is made.

**Lifetime and revocation.** There is no revocation list. The token is only as good as its context. A person's run
keeps its context while it is used: the idle window is `context_ttl_s` (default 3600 s), use slides it, and an
absolute cap of 24 hours applies. A mandate run keeps its fixed deadline and never slides. When the run ends, the
context is `completed` and the token stops working. Revoking a binding, grant, mandate or credential, or a change
of the person's access, changes an epoch, and the token stops working at the next check. A context that is ended,
revoked or expired is never revived by use.

**What a token cannot do.** It cannot reach tokens, governance, administration, secrets in general,
configurations, listings, public agents, or another project. It cannot create or change anything outside its
`scope`.

## Microsoft delegation beside the run token *(from PR #602/#7/#21/#786, pending)*

The run token is Elitea's own identity for Elitea's own resources. For a Microsoft resource (SharePoint through
Graph) the platform uses the person's **Microsoft** sign-in instead. The two never mix: the run token is never
sent to Microsoft, and a Microsoft token is never given to Elitea routes.

| | Run token | Microsoft delegation |
|---|---|---|
| Issuer | Elitea (Main) | Microsoft Entra, in exchange for the person's sign-in (OAuth 2.0 on-behalf-of) |
| Resource | The platform's own endpoints | Microsoft Graph (SharePoint) |
| Minted | At admission, once per run | At credential release, per use; cached in memory until it expires |
| Stored | Nowhere (signed, checked against the context) | The person's sign-in for the session; the Graph token never |
| Subject | The run's subject | The same person: tenant and object id must equal the run's subject, else `DENIED_SUBJECT_MISMATCH` |
| Mandate runs | Yes (their own deadline) | No (`DENIED_MANDATE_DELEGATION_UNSUPPORTED`) |
| API-token runs | No identity of a person | No (`DENIED_DELEGATION_UNSUPPORTED_CALLER`) |
| Keycloak login | Yes | Not yet (`DENIED_ASSERTION_SOURCE_UNSUPPORTED`) |

The binding to the person is checked before anything is exchanged, from the explicit fields of the run's context
(never by parsing `ref`): the run is a signed-in person's run, its subject carries issuer, tenant and object id,
the credential's tenant equals the subject's, a personal credential's custodian is that subject, and the stored
sign-in and its token name the same tenant and object id. Any difference is `DENIED_SUBJECT_MISMATCH`.

## Acceptance checks for this page

| Check | Observable |
|---|---|
| A session run under Enforce carries the run token and no other platform token | The dispatched payload has no `api_token`, `api_key`, `project_auth_token` or `auth_session` field |
| A forged or expired run token | `INTERNAL_TOKEN_INVALID` or `INTERNAL_TOKEN_EXPIRED` on every route |
| A run token used on a route outside its list, or in another project | `INTERNAL_TOKEN_INVALID` or `INTERNAL_TARGET_DENIED` |
| A binding without platform operations | The built-in Elitea tools can read only; a write is `INTERNAL_SCOPE_DENIED` |
| A run ended or revoked | The token answers `CONTEXT_NOT_LIVE` within about five seconds |
| A personal API token on a governed agent | `IDENTITY_KIND_NOT_ADMITTED`, or `MANDATE_REQUIRED` when tokens are admitted |
| A schedule whose owner is not the mandate's owner | `MANDATE_REQUIRED` and a failed run, never a run left in *running* |
| A Keycloak login | The same governance checks, with the subject `idp:<realm GUID>:<sub>` |
| A Microsoft token | Not in any table, log, payload or audit event; a delegated release makes no secret-store or database write *(pending)* |
