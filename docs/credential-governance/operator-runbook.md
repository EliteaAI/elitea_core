# Operator runbook

Part of [credential governance](README.md). Short procedures for platform administrators. The meaning of each
state and code is in [credential-profiles.md](credential-profiles.md) and
[acceptance-contract.md](acceptance-contract.md). Everything here is done in the Admin UI under **Governance**
(`/governance`); the scripted equivalent is the admin task named in brackets (System > Tasks).

Two operators are needed: every approval must come from someone other than the creator.

## 1. Enable

1. **Set the ceiling** (deployment, needs a restart of `pylon_main`):
   `ELITEA_CREDENTIAL_GOVERNANCE_MAX_MODE` = `audit-only` while rehearsing, `enforce` when enforcement may be
   switched on. Unset means `enforce`. Optionally set `ELITEA_CREDENTIAL_GOVERNANCE_ENVIRONMENT` to a label
   such as `prod-eu`. Do not use the deprecated `..._ENABLED`.
2. **Create the tables** (once per environment; `dry_run` first):
   `create_governance_settings_tables`, then `create_agent_identity_tables`, `create_mandate_tables` (elitea_core)
   and `create_credential_governance_tables` (secrets) with `project_id=all`. Run the first two and the secrets task
   again after an upgrade: `create_governance_settings_tables` adds the `allow_development_runs` column, the identity
   task lets a development run's context have no binding, and the secrets task adds the personal-reference index.
3. **Read the preflight** on Governance > Overview. Fix every failed check (identity verifier, RPC
   authentication mode, security audit, credential services, master key). Warnings (`corporate_vault`,
   `governed_worker`) limit features but do not block.
4. **Switch to Audit only.** Choose the mode, enter a **policy revision** (a change ticket or date; it is
   stamped on every admission and approval and does not stop runs that already started) and a change note,
   and save. Only a super administrator can save, in an interactive session.
5. **Prepare** bindings, references and grants (sections 2 to 5). Use Audit only to find what is not ready:
   Overview > Security events (the API filters by `would_deny=true`). The switch dialog shows how many runs in the
   last 24 hours enforcement would have refused, with the top reasons.
6. **Switch to Enforce** when the would-deny list holds nothing unexpected.

Audit only evaluates people's interactive runs. Schedules, webhooks, mandate runs and child runs are not
evaluated, so check them with `report_trigger_principals` (section 6) before enforcing.

## 2. Bind an agent

Governance > **Agent identities** (`manage_agent_identity_binding`).

1. Create a binding for the agent version, mode `elitea_native`. The digest of the current definition is stored.
2. Submit it. A **second operator** activates it with an approval reference (`CHG-12`) and the policy revision.
3. A digest chip shows when the agent no longer matches its binding. After the agent version is edited, use
   **Rebind**: the binding returns to *pending approval* for the new definition and live runs are revoked.

Mode `runtime_domain_binding` needs the *Allow runtime domain bindings* switch on Overview first.

## 3. Onboard a shared credential

Governance > **Credentials** (`manage_credential_reference`). The credential must already exist as a
configuration in the project.

1. **Register** the configuration: class `project_shared`, `service` or `technical`; store `database` or
   `corporate_vault`. State: *pending*.
2. **Import.** The server copies the configuration's secret values into the reference's store. The value is
   never shown, and the configuration's own stored copy is not removed. Each import is a new generation.
3. **Probe.** Checks that the store holds the current generation and that it still matches the configuration. State: *validated*.
4. **Approve** (second operator, approval reference and policy revision), then **Activate**.

Rotation: Suspend, update the configuration and Import again, Probe, Approve, Activate. Grants keep working.
Retire is final and deletes the value in the reference's store.

## 4. Onboard a personal credential

1. An administrator registers a **personal** reference for the configuration and names the owner, who must be a
   project member linked to an identity-provider account.
2. **The owner sets the value.** An administrator cannot. An owner who can open the Admin UI sets it on the
   Credentials tab; other members use the write-only route `credential_material` until self-service in EliteaUI
   exists (#7130).
3. Probe (with *check connection*, the value is also tested at the provider through the governed pool), then a **second administrator**
   (neither the one who registered it nor the owner) approves, then Activate.

From then on the owner's runs use the personal credential and no other. While it is pending, validated or
suspended their runs are refused with `CREDENTIAL_REFERENCE_INACTIVE`; they do not fall back to the shared one.

## 5. Grant use

Governance > **Grants** (`manage_credential_grant`). A grant ties one agent binding, one toolkit of that agent,
one active reference and one person. It starts *pending approval* and a second operator approves it.
Without a grant the run is refused with `DENIED_GRANT`. Set an end date for temporary access.

## 5a. Let developers test unbound versions (development runs)

Under Enforce the editor's test chat would refuse every version that has no active binding (`DENIED_BINDING`), so
authoring would stop. The switch **Allow development runs** on Overview (next to *Admit legacy API tokens*) lets
the agent and pipeline editor's test chat run such a version as a **development run**:

- the run acts as the signed-in author only (a verified identity; API tokens cannot author), who must hold the
  right to edit the application; there is no binding and no mandate;
- a toolkit credential is released only if the author could already use it: a personal credential of the author,
  or a project credential on which the author's subject holds an active grant on any agent and toolkit. Otherwise
  the turn is refused with `DENIED_DEVELOPMENT_CREDENTIAL` ("This credential needs a grant before a draft can use
  it"). Toolkits without a credential, built-in tools and attachments need nothing. The decision is made for every
  turn, so the author may change the draft's toolkits between turns; a sub-agent of the draft inherits exactly the
  draft's decisions and nothing more (`DENIED_DEVELOPMENT_CHILD`);
- only the editor's own private test conversation is a development run. Shared chats, public agents, the API,
  schedules, webhooks and mandate runs of the same version take the governed path and are refused as before. A
  schedule or webhook saved on an unbound version is kept, and the editor shows a notice that its runs need a binding
  and a mandate;
- the audit trail records the admission as `ADMITTED_DEVELOPMENT` with `development:<definition digest>` where a
  governed run names its binding, and every credential release of the run carries the same digest (a per-turn
  record of what was in play). The context row has `profile = development`. Both appear in Overview > Security
  events and in the summary.

Default: **on under Audit only** (the editor is then rehearsed as a development run instead of being recorded as a
would-deny), **off under Enforce** until an operator chooses it. The choice is a setting like the others: change note,
history row, super administrator. Switch it off for a hard Enforce: the editor then refuses unbound versions with
`DENIED_BINDING`. The built-in Elitea tools (internal MCP) still authenticate with the user's API token; under
Enforce they are refused unless *Admit legacy API tokens* is on, until #7214 gives the run its own identity.

## 6. Schedules and webhooks (mandates)

A governed agent's schedule or webhook is refused until it has an approved mandate.

1. `report_trigger_principals` with `project_id=all` lists what would be refused and why.
2. `manage_autonomous_mandate`: `create` (version, owner, `operations=run:schedule[,run:webhook]`,
   `max_runtime_s`, `valid_until` within 180 days), `submit`, then `approve` by someone who is neither creator
   nor owner.
3. Grant the credentials the run needs to the mandate (`subject_ref=mandate:<uuid>` in
   `manage_credential_grant`). A personal credential can be granted to a mandate only if the owner is its custodian.

Scheduled indexing has no mandate path yet and is refused while governance is enforcing.

## 7. Read a refusal

The user sees a short message with a code. Find the event in Overview > Security events, or
`GET /api/v2/tracing/security_audit/administration` with `reason_code`, `user_ref`, `agent_binding_ref` or
`credential_ref`.

| Code | First thing to check |
|---|---|
| `IDENTITY_*` | The person signed in with the identity provider; the user is linked to it; the verifier configuration |
| `DENIED_BINDING`, `BINDING_DIGEST_MISMATCH` | Binding of *this version* is active; rebind after an edit. From the editor's test chat: *Allow development runs* (section 5a) |
| `DENIED_DEVELOPMENT_CREDENTIAL` | A development run: the author has no personal credential and no active grant on this project credential (section 5a) |
| `DENIED_DEVELOPMENT_CHILD` | A sub-agent of a development run asked for a credential the draft was not admitted for |
| `IDENTITY_KIND_NOT_ADMITTED` | The run started with an API token (the built-in Elitea tools included): *Admit legacy API tokens*, or #7214 |
| `MANDATE_REQUIRED`, `MANDATE_INACTIVE`, `MANDATE_EXPIRED` | A schedule or webhook has no active mandate, or it was suspended or expired |
| `CREDENTIAL_REFERENCE_REQUIRED` | The toolkit selects its credential by title or inline; select the saved credential and register it |
| `CREDENTIAL_REFERENCE_INACTIVE`, `CREDENTIAL_GENERATION_MISMATCH` | Reference is active; the configuration changed after approval, so re-probe and re-approve |
| `CREDENTIAL_CLASS_NOT_ALLOWED` | The credential is another person's personal reference |
| `DENIED_GRANT`, `DENIED_RBAC` | A grant exists and is active for this binding, toolkit and person; the person has `credentials.use` |
| `WORKER_CAPABILITY_UNAVAILABLE` | A worker with the governed capabilities is online (preflight `governed_worker`) |
| `AUDIT_UNAVAILABLE`, `DEPENDENCY_UNAVAILABLE` | Audit backlog and preflight on Overview. Nothing was let through. |
| `STORE_*` | The credential store (database master key, corporate Vault address and policy) |

## 8. Roll back

- **Switch to Off** on Overview (super administrator). Each replica picks it up within about 10 seconds. New
  runs are unchecked and behave as before. Runs already admitted keep their admission.
- **Hard stop**: set `ELITEA_CREDENTIAL_GOVERNANCE_MAX_MODE=off` and restart. This overrides the stored mode and
  works even when the database cannot be read.
- Suspend a single binding, reference, grant or mandate to take one agent or credential out of use without
  changing the mode. Live runs under it are revoked at their next check.
- The tables are additive. Nothing needs to be dropped or migrated back; bindings, references, grants and
  mandates stay stored and are ignored while the mode is Off.
