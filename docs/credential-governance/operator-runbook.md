# Operator runbook

Part of [credential governance](README.md). Short procedures for platform administrators. The meaning of each
state and code is in [credential-profiles.md](credential-profiles.md) and
[acceptance-contract.md](acceptance-contract.md). Identity and run tokens are in
[identity-boundaries.md](identity-boundaries.md). What agent authors see is in
[for-agent-authors.md](for-agent-authors.md). Everything here is done in the Admin UI under **Governance**
(`/governance`); the scripted equivalent is the admin task named in brackets (System > Tasks).

Two operators are needed: every approval must come from someone other than the creator.

Items marked *(from PR #602/#7/#21/#786, pending)* are delivered on the open pull requests for the Microsoft
sign-in credentials (`elitea_core#602`, `auth_oidc#7`, `secrets#21`, `elitea-sdk#786`). They are not on `main` yet.
Items marked *planned* are not built and not promised.

## 1. Modes

One platform-wide setting. The deployment sets a ceiling (`ELITEA_CREDENTIAL_GOVERNANCE_MAX_MODE`); super
administrators choose the mode below it in the Admin UI.

| | `off` | `audit_only` | `enforce` |
|---|---|---|---|
| Person's chat, editor, predict API | Unchanged. Nothing is read or recorded. | Checked as enforce would check it and recorded. Never refused. | Refused unless identity, binding and credentials pass. |
| Schedule of a person (owner schedule) | Unchanged. | The version's mandate is looked at; if enforce would refuse it, one `would_deny` row is written (`MANDATE_REQUIRED`, `MANDATE_INACTIVE`, `MANDATE_EXPIRED` or `DENIED_OPERATION`). The run goes ahead. | Runs only on its owner's own active mandate. Otherwise refused with `MANDATE_REQUIRED` and ended at once. |
| Webhooks, legacy cron triggers, scheduled indexing, child dispatches, system runs | Unchanged. | Not evaluated. A child dispatch is not evaluated on its own: the sub-agents of a person's run are evaluated within that run's admission (#7227). | Webhooks and system runs need a mandate. Child dispatches follow their parent. |
| Platform identity of the run | The user's own token, as before. | As before. A built-in Elitea tools toolkit that still uses a personal token is recorded as `LEGACY_PAT_INTERNAL_MCP` (`would_deny`). | The run token only (section 7). |
| Development runs (section 5a) | Not needed. | On by default. | Off by default for a deployment already in Enforce. On when the switch to Enforce turns it on (see 5a). |
| Context row, sealed payload, credential release | No | No | Yes |

Switching is immediate on each replica within about 10 seconds. Runs that were already admitted keep their
admission when the mode goes to `off`.

The three switches next to the mode:

| Switch | Meaning | Default |
|---|---|---|
| Admit legacy API tokens (`admit_legacy_pat`) | A personal API token passes the identity check. It still cannot act for a person: such a run needs a mandate (`MANDATE_REQUIRED`). | Off |
| Allow runtime domain bindings (`allow_runtime_domain_binding`) | Bindings of mode `runtime_domain_binding` may be activated and used. | Off |
| Allow development runs (`allow_development_runs`) | Section 5a. | See 5a |

## 2. Before you switch it on (preflight)

Read **Governance > Overview**. Fix every failed check. Warnings limit a feature but do not block.

| Check | What it needs |
|---|---|
| `identity_verifier` | The `identity_verifier.session_user` section of the `auth` plugin and the OIDC login (see below). |
| `auth_rpc_mode` | Authentication runs in RPC mode. |
| `security_audit`, `audit_schema`, `audit_backlog` | The tracing plugin takes audit events. `audit_schema` blocks Audit only until the audit service is current. |
| `credential_facts` | `secrets` and `configurations` are loaded and current. |
| `master_key` | The secrets master key, see below. |
| `settings_table` | Run `create_governance_settings_tables`. |
| `corporate_vault` (warning) | A corporate Vault address, only if you register Vault-backed credentials. |
| `governed_worker` (warning) | A worker that announces the governed capabilities is online. |

**Master key.** The preflight fails when the `database` secrets engine has no `SECRETS_MASTER_KEY`. Set the key
in every secrets engine once governance is on, not only in `database`. The run token (section 7) is signed with a
key derived from it. Without the key no run token can be minted: a run that uses the built-in Elitea tools is
refused with `INTERNAL_TOKEN_UNAVAILABLE`, and other runs keep carrying their old platform token, so the
"run token is the only identity" guarantee does not hold.

**Identity verifier.** Governance verifies a person from the OIDC login: signature, issuer, audience, tenant,
expiry, and a link from the platform user to **issuer + tenant + subject** (never an e-mail address).

Entra (defaults) in the `auth` plugin:

```yaml
identity_verifier:
  session_user:
    tenant_id: <tenant GUID>
    audience: <client id of the API app>
    jwks_uri: https://login.microsoftonline.com/<tenant GUID>/discovery/v2.0/keys
```

Keycloak or another generic OIDC issuer has no tenant claim. Map the subject and give the realm a fixed GUID.
Use the same GUID in both places.

```yaml
# pylon_auth, auth_oidc
subject_claims: {tid: null, oid: sub}
subject_tenant: <GUID you assign to this realm>
```

```yaml
# pylon_main, auth
identity_verifier:
  session_user:
    tenant_id: <the same GUID>
    audience: <client id>
    issuer: https://<keycloak host>/realms/<realm>
    jwks_uri: https://<keycloak host>/realms/<realm>/protocol/openid-connect/certs
    subject_claims: {tid: null, oid: sub}
    require_nbf: false          # Keycloak omits nbf
    delegation_token: id_token  # when the access token has no audience for this platform
```

Grants and mandates are tied to the person's subject (`idp:<tenant>:<subject>`). After you change the identity
provider, the same people have new subjects and **every grant must be made again** for them.
The verifier refuses an expired ID token (`IDENTITY_EXPIRED`), so the token lifetime of the provider limits how
long after sign-in a governed run can start.

## 3. Enable

1. **Set the ceiling** (deployment, needs a restart of `pylon_main`):
   `ELITEA_CREDENTIAL_GOVERNANCE_MAX_MODE` = `audit-only` while rehearsing, `enforce` when enforcement may be
   switched on. Unset means `enforce`. Optionally set `ELITEA_CREDENTIAL_GOVERNANCE_ENVIRONMENT` to a label
   such as `prod-eu`. Do not use the deprecated `..._ENABLED`.
2. **Create the tables** (once per environment; `dry_run` first):
   `create_governance_settings_tables`, then `create_agent_identity_tables`, `create_mandate_tables` (elitea_core)
   and `create_credential_governance_tables` (secrets) with `project_id=all`. Run the first two and the secrets task
   again after an upgrade: `create_governance_settings_tables` adds the `allow_development_runs` column, the identity
   task lets a run's context have no binding, no agent and no version (development runs, toolkit tests), and the
   secrets task adds the personal-reference index.
3. **Read the preflight** (section 2).
4. **Switch to Audit only.** Choose the mode, enter a **policy revision** (a change ticket or date; it is
   stamped on every admission and approval and does not stop runs that already started) and a change note,
   and save. Only a super administrator can save, in an interactive session.
5. **Prepare** bindings, references, grants and mandates (sections 4 to 6). Use Audit only to find what is not
   ready: Overview > Security events (the API filters by `would_deny=true`). The switch dialog shows how many runs
   in the last 24 hours enforcement would have refused, with the top reasons.
6. **Switch to Enforce** when the would-deny list holds nothing unexpected.

Under Audit only, owner schedule runs (Schedules page) leave the mandate `would_deny` row described in section 1, and
the sub-agents of a person's run are evaluated within that run's admission. A scheduled run's own sub-agents are not
rehearsed (only its mandate row is). Webhooks, legacy cron triggers and scheduled indexing are not evaluated, so check
them with `report_trigger_principals` (section 6) before enforcing. The Admin UI text of the mode says the same.

## 4. Bind an agent

Governance > **Agent identities** (`manage_agent_identity_binding`).

1. Create a binding for the agent version, mode `elitea_native`. The digest of the current definition is stored.
   Set the platform operations (below).
2. Submit it. A **second operator** activates it with an approval reference (`CHG-12`) and the policy revision.
3. A digest chip shows when the agent no longer matches its binding. After the agent version is edited, use
   **Rebind**: the binding returns to *pending approval* for the new definition and live runs are revoked.

Mode `runtime_domain_binding` needs the *Allow runtime domain bindings* switch on Overview first.

### Platform operations of a binding

An agent can carry the built-in Elitea tools (the internal MCP toolkits). With them it can read and change
platform data as the person who runs it. A binding says which kinds of operation the agent may use. This is part
of what the second operator approves.

| Operation | Allows |
|---|---|
| `read` | Every read (GET) of the platform API. |
| `build` | Creating, changing and deleting platform entities: agents and versions, toolkits, skills, project context, folders, conversations and participants, artifact buckets, notifications. |
| `credentials` | Anything that creates, changes or returns a secret: configurations, models, secrets, S3 credentials. |
| `run` | Starting an agent or a skill, continuing a chat, drafting project context or skills. |

- **Empty or not set means `read` only.** There is no way to refuse reads: for that, leave the built-in Elitea
  tools out of the agent.
- The effective scope of a run is the binding's operations **and** what the person's own permissions allow, worked
  out once when the run is admitted. The person's permissions still decide every single call.
- A write that is not in the platform's operation map is refused (`INTERNAL_OPERATION_UNKNOWN`), even if the
  binding allows `build`. Every write is recorded as a `platform_operation` audit event before it is made. If the
  record cannot be written, the write is refused (`AUDIT_UNAVAILABLE`).

How to choose: start with `read`. Add `build` only for an agent whose job is to build Elitea entities. Add `run`
only if it must start other agents or skills through the platform. Add `credentials` only if creating credentials
is the job. Today the model composes that call, so the secret value would pass through the model. A secure
credential form that avoids this is *planned* (#7228), not built.

Set the operations when you create the binding or rebind it: `platform_operations=read,build,credentials,run` in
the admin task, `platform_operations` (a list) in the API. They cannot be changed on an active binding without a
rebind and a new approval.

## 5. Credentials and grants

### Onboard a shared credential

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

### Onboard a personal credential

1. An administrator registers a **personal** reference for the configuration and names the owner, who must be a
   project member linked to an identity-provider account.
2. **The owner sets the value.** An administrator cannot. An owner who can open the Admin UI sets it on the
   Credentials tab; other members use the write-only route `credential_material` until self-service in EliteaUI
   exists (#7130).
3. Probe (with *check connection*, the value is also tested at the provider through the governed pool), then a **second administrator**
   (neither the one who registered it nor the owner) approves, then Activate.

From then on the owner's runs use the personal credential and no other. While it is pending, validated or
suspended their runs are refused with `CREDENTIAL_REFERENCE_INACTIVE`; they do not fall back to the shared one.

### Grant use

Governance > **Grants** (`manage_credential_grant`). A grant ties one agent binding, one toolkit of that agent,
one active reference and one person. It starts *pending approval* and a second operator approves it.
Without a grant the run is refused with `DENIED_GRANT`. Set an end date for temporary access.

## 5a. Development runs

Under Enforce the editor's test chat would refuse every version that has no active binding (`DENIED_BINDING`), so
authoring would stop. The switch **Allow development runs** on Overview lets the author run what has no binding
yet as a **development run**:

- the run acts as the signed-in author only (a verified identity; API tokens cannot author). There is no binding
  and no mandate. The author must hold the right to edit the application;
- these runs are development runs: the editor's own private test chat of an agent or pipeline, **toolkit tests**
  (Test on a toolkit tool), **on-demand indexing** (Reindex), the **draft's sub-agents** at every depth, a plain
  LLM chat whose toolkits carry a credential, and a schedule owner's **Run now** / **Test run** of an unbound version
  (a bound version runs on its binding);
- a credential is released only if the author could already use it: a personal credential of the author, or a
  project credential on which the author's subject holds an active grant on any agent and toolkit. Otherwise
  the turn is refused with `DENIED_DEVELOPMENT_CREDENTIAL`. Toolkits without a credential, built-in tools and
  attachments need nothing. The decision is made for every turn, so the author may change the draft's toolkits
  between turns;
- a sub-agent that has exactly one active binding with an unchanged definition runs on its own binding and grants.
  Any other sub-agent runs as a development child of the same author. A sub-agent the draft calls that was not
  checked when the draft started is refused (`DENIED_DEVELOPMENT_CHILD`);
- nothing else takes this path. Shared chats, public agents, the API, schedules, webhooks and mandate runs of the
  same version take the governed path and are refused as before. Toolkit runs started with an API token (the MCP
  server's toolkit tools) are refused with `TOOLKIT_RUN_TOKEN_NOT_ADMITTED`. Scheduled indexing is refused too.
  A schedule or webhook saved on an unbound version is kept, and the editor shows a notice that its runs need a
  binding and a mandate;
- the audit trail records the admission as `ADMITTED_DEVELOPMENT` with `development:<definition digest>` where a
  governed run names its binding, and every credential release carries the same digest. The context row has
  `profile = development`. Both appear in Overview > Security events.

**Defaults and writes.**

| Situation | Value |
|---|---|
| Nothing chosen, mode Audit only | On. The editor is rehearsed as a development run instead of being recorded as a would-deny. |
| Nothing chosen, mode Off | Off. |
| You choose a value | Your value always wins. |
| An API write switches into Enforce and sends no value | Development runs are switched on, and the history row says so. |
| The Admin UI switches to Enforce | The toggle is shown pre-filled on. Switch it off in the same save for a hard Enforce. |
| A deployment already in Enforce at upgrade | Keeps its value. It does not flip. |

**When to turn it off under Enforce.** Turn it off when authors must not test with credentials at all, for
example in a production project where only reviewed, bound agents may run. Then the editor refuses unbound
versions with `DENIED_BINDING`, toolkit tests and indexing with `DEVELOPMENT_RUNS_DISABLED`, and a chat toolkit that
carries a credential with `DEVELOPMENT_RUNS_DISABLED` too. Authors then need a binding and a grant for every test.
The choice is a setting like the others: change note, history row, super administrator.

## 6. Schedules, webhooks and mandates

### What a mandate is

A mandate is an approved, time-boxed permission for **one agent version** to run without a signed-in person:
on a schedule or a webhook. It names an owner, the operations it allows (`run:schedule`, `run:webhook`), a
maximum run time and an end date (at most 180 days). It is created against the version's active binding and
approved by someone who is neither its creator nor its owner. Credentials the run needs are granted to the
mandate (`subject_ref=mandate:<uuid>` in `manage_credential_grant`). A personal credential can be granted to a
mandate only if the mandate's owner is its custodian. A version has one current mandate (its latest revision).

### Who needs one

| Trigger | Under Enforce |
|---|---|
| A person's schedule (owner schedule) of an agent or a pipeline: cron firings | An active mandate **of the schedule's owner** on that version. |
| The same schedule: Run now and Test run by its owner | No mandate: the owner's run, on the version's binding when it is bound; an unbound version needs development runs on and the edit right (Scheduled runs, below). |
| Webhook | An active mandate on that version. |
| Legacy cron trigger | An active mandate on that version. |
| A person chatting, using the editor, calling the API as themselves | No mandate. |
| Scheduled indexing | Refused (`AUTHORIZATION_REQUIRED`). There is no mandate path yet. |

### Decision: only the schedule owner's own mandate covers a scheduled run

A schedule belongs to a person. If another person's mandate covered it, any schedule author could borrow that
person's authority. So the platform checks that the version's current mandate was made for the schedule's own
owner. Another person's mandate does not cover the run. No names appear in the refusal text.

| Mode | Schedule owner has the version's mandate | Schedule owner is not the mandate's owner, or there is no mandate |
|---|---|---|
| Enforce | The run is admitted on the mandate (entry point `schedule`, subject `mandate:<id>`). The mandate can still refuse it: `MANDATE_INACTIVE`, `MANDATE_EXPIRED`, `DENIED_OPERATION`, `DENIED_BINDING`, `DENIED_RBAC`, `IDENTITY_SUBJECT_UNLINKED`, `SUB_AGENT_CREDENTIAL_NOT_ADMITTED`, a credential code, `DEPENDENCY_UNAVAILABLE` or `AUDIT_UNAVAILABLE`. | Refused with `MANDATE_REQUIRED` at once: the run is marked failed in under a second, the owner gets a "schedule failed" notice with the reason, and an agent run says so in the owner's open chats. The run never stays `running`. |
| Audit only | Runs. No row if the mandate is active and allows `run:schedule`; one `would_deny` row if enforce would refuse it (`MANDATE_INACTIVE`, `MANDATE_EXPIRED`, `DENIED_OPERATION`). | Runs. One `would_deny` row `MANDATE_REQUIRED`, caller `schedule:entity:<id>`. |
| Off | Unchanged. No read, no row. | Unchanged. |

The text the schedule owner sees is the fixed `MANDATE_REQUIRED` message:
"No approved mandate or delegation covers this run; it was not started." The notice reads
`Schedule "<name>" failed: The run failed: <that text>`.

Consequences:

- Every firing of a refused schedule creates a conversation and a failure notice, until the schedule pauses itself
  after three refusals in a row (below).
- **Run now** and **Test run** work for the schedule's owner only (below).
- An administrator who changes a schedule's owner, or edits it, can leave the new owner without a covering
  mandate. Check the mandate after such a change. *(Owner re-confirmation before a schedule runs on a mandate is
  planned with #7131.)*
- If you want one mandate to cover several people's schedules on the same version, that is not possible today.
  *Planned, not promised:* #7131 will decide the sharing model (one mandate per owner and version, or one mandate
  naming the allowed schedule owners).

### Scheduled runs

A schedule on the Schedules page (agent or pipeline) belongs to the person who saved it first, its **owner**. A
schedule made with "Build with AI" is no different: the draft is not saved, and the person who saves the form is the
owner.

**Who needs the mandate.** The schedule owner, on the schedule's version: one current mandate per version (its
latest revision), made for the owner (`owner` = the schedule owner), active, allowing `run:schedule`. Only cron
firings need it. **Run now** and **Test run** are the owner's own run (below).

| Mode | A cron firing | Run now / Test run (the owner, signed in) |
|---|---|---|
| Off | Runs as before. Nothing is read or recorded. | Runs as before. |
| Audit only | Runs. If enforce would refuse it for its mandate, one `would_deny` row (`MANDATE_REQUIRED`, `MANDATE_INACTIVE`, `MANDATE_EXPIRED` or `DENIED_OPERATION`, caller `schedule:entity:<id>`). Never paused. | Runs. Evaluated as the owner's run (on the binding, or as a draft); no mandate row. |
| Enforce | Runs on the owner's mandate, or is refused and ended at once (failed, owner notice, chat message). | Acts as the owner (entry point `schedule_test`, subject = the owner's verified sign-in; delegated credentials on the owner's live sign-in, `DENIED_ASSERTION_UNAVAILABLE` without one). A bound version runs its approved definition on its binding, as any bound run of that person (definition checked, `BINDING_DIGEST_MISMATCH` if it changed; sub-agents on their bindings; the owner's grants). An unbound version runs the draft, as the editor's test chat: the owner needs the right to edit the agent, and only the owner's own credentials and project ones granted to the owner are used. Either way the owner needs the right to run the agent (`DENIED_RBAC` otherwise) and no admin role; no mandate is read. An unbound version needs development runs on (`DEVELOPMENT_RUNS_DISABLED` otherwise); a bound one runs on its binding whatever the switch says. Started with an API token: refused `SCHEDULE_RUN_TOKEN_NOT_ADMITTED`. Its refusals end the run readably (run history and owner notice) but never count toward or reset the auto-pause. |

**Run now and Test run.** Only the owner can start them. An administrator sees the controls disabled, with the
hint "Only the schedule owner, <owner name>, can run it." ("Only the schedule owner can run it." when the owner has
no name); the API answers 403 `not_owner`. In the row menu the hint is the hover tooltip of the disabled Run now; on
the edit page it is a line next to the disabled Run test.

**Notice at save time.** When governance is on (audit only or enforce) and the version is bound but the owner has no
mandate a run could use, the schedule is saved and the save shows:
"Scheduled (cron) runs need an approved mandate for you on this version; Run now and Test run act as you. Ask a
project admin (Admin → Governance → Mandates)." Under audit only it adds "Until then each run is recorded as 'would
deny'." When an administrator saves another person's schedule the text says "for the schedule owner" and "act as the
owner". An unbound version shows the binding notice
instead. Off: no notice, no read.

**Auto-pause.** Under enforce, the third governance refusal in a row of the schedule's cron firings pauses the
schedule (`disabled_reason =
governance_refused`; the Enabled switch shows, on hover, "Paused: runs were refused by credential governance") and the
owner gets one notice: `Schedule "<name>" was switched off: 3 runs in a row were refused (<last refusal text>). Fix
the cause and resume it.` Only cron firings count: Run now and Test run neither count nor end the streak. Each refused run keeps its own reason in the
run list. A refusal for a platform outage (`DEPENDENCY_UNAVAILABLE`, `AUDIT_UNAVAILABLE`,
`WORKER_CAPABILITY_UNAVAILABLE`, `INTERNAL_TOKEN_UNAVAILABLE`, `STORE_UNAVAILABLE`) neither counts nor ends the
streak; a missing Microsoft sign-in or app (`DENIED_ASSERTION_UNAVAILABLE`, `DENIED_OAUTH_APP_UNAVAILABLE`) counts. A schedule that is already off (paused by hand,
expired) keeps its own reason and gets no pause notice. An admitted start ends the streak only while governance is on
(audit only or enforce); so does **Resume**. A count left from before an off period is kept until the next governed
run or a Resume, and counting goes on when governance is on again. Pausing by hand keeps the count. Off and audit
only refuse nothing, so nothing pauses there. To resume: fix the cause the notice names (for a mandate: create, approve
or renew it for the owner on the schedule's version), then press **Resume** on the schedule. The count starts again from zero.

### Steps

1. `report_trigger_principals` with `project_id=all` lists what would be refused and why (works with governance
   off).
2. `manage_autonomous_mandate`: `create` (version, owner, `operations=run:schedule[,run:webhook]`,
   `max_runtime_s`, `valid_until` within 180 days), `submit`, then `approve` by someone who is neither creator
   nor owner. The Admin UI Governance -> Mandates tab does the same (it runs the admin task's code).
3. Grant the credentials the run needs to the mandate (`subject_ref=mandate:<uuid>`).
4. The owner creates (or keeps) the schedule. The schedule's owner must be the mandate's owner.

A mandate run is stopped by Main at the earlier of `max_runtime_s` and `valid_until` (`RUNTIME_LIMIT_EXCEEDED`).
A mandate run carries the run token only, with the mandate's deadline. Its sub-agents that use credentials are
not admitted yet (`SUB_AGENT_CREDENTIAL_NOT_ADMITTED`). A mandate run uses a Microsoft sign-in credential only when
the mandate names the owner's identity and the owner consented (section 8a).

## 7. The run token

Under Enforce an admitted run has **one** platform identity: a short-lived token that Main mints for that run.
No personal token, system token or session is put on the run's payload, and none is looked up for it. This holds
for a signed-in person's run and for a run under a mandate. Claims and trust model are in
[identity-boundaries.md](identity-boundaries.md#the-run-token).

What the token can call, as the run's person and in the run's project only:

- model calls (the chat, responses, messages, embeddings, image and auto-routing endpoints of the model proxy,
  not its admin endpoints);
- the name, description and sealed details of the sub-agents it fetches;
- the person's platform MCP servers (list and call) and tool spend checks;
- the model list, artifacts and buckets (REST and S3);
- one platform-internal secret (the checkpoint database connection);
- the built-in Elitea tools, within the binding's operations (section 4).

What it cannot call: tokens, governance, administration, secrets in general, configurations, listings, public
agents, another project. All of these answer `INTERNAL_TOKEN_INVALID` with the same text. Each endpoint's own
permission check still applies.

**Lifetime.** A person's run stays admitted while it is in use. The idle window is `context_ttl_s` (default 3600
seconds, 60 to 86400, in the `enterprise_credential_governance` section). Use slides it, up to an absolute cap of
24 hours. A run that is idle longer than the window, or longer than the cap, loses platform access. A mandate run
keeps its fixed deadline and never slides. When the run ends, or an epoch changes (user, token, binding,
mandate, credential), the token stops working at the next check, within about five seconds.

| Code | Meaning | Operator action |
|---|---|---|
| `INTERNAL_TOKEN_INVALID` | The token is forged, malformed, or used on a route it may not use. | Nothing for a forged token. If a real run hit it, the route is outside the run's list (above). |
| `INTERNAL_TOKEN_EXPIRED` | Past the run's time limit. | Start the run again. If runs are long, raise `context_ttl_s` (cap 24 h). |
| `CONTEXT_NOT_LIVE` | The run was ended, revoked, or idle too long. | Find the revoke reason in the audit trail (binding, grant, mandate or credential changed). |
| `INTERNAL_SCOPE_DENIED` | The binding's operations do not include this one. | Section 4: extend the binding with a rebind and an approval, or do it by hand in Elitea. |
| `INTERNAL_OPERATION_UNKNOWN` | A write the platform has no operation for. | Nothing to configure. The tool is not available to agents. |
| `INTERNAL_TARGET_DENIED` | The call names a project outside the run's projects. | Check the toolkit's project. |
| `INTERNAL_MCP_ORIGIN_NOT_ALLOWED` | An Elitea MCP toolkit points to an address that is not the platform. Refused at start. | Use the built-in Elitea tools, or fix the address. |
| `INTERNAL_TOKEN_UNAVAILABLE` | No run token could be minted at admission. | Check the master key (section 2) and `pylon_main` logs. |
| `LEGACY_PAT_INTERNAL_MCP` | Audit only: a built-in Elitea tools toolkit still uses a personal token. | Expected before Enforce. No action needed for Enforce. |
| `INTERNAL_MCP_OWN_AUTH` | Enforce: a toolkit brought credentials of its own on an internal path. They were removed and the event was recorded. | Fix the toolkit. |

## 8. Microsoft sign-in credentials (delegated)

*(From PR #602/#7/#21/#786, pending. Not on `main` yet.)*

A credential of kind `entra_delegated` holds **no secret**. It tells the platform which Microsoft app to use, for
which tenant and scopes. When a person runs an agent, the platform exchanges that person's Microsoft sign-in for a
Microsoft Graph token (OAuth 2.0 on-behalf-of) and gives it to the toolkit for that run. Today this is for the
SharePoint toolkit only, and the resource is Microsoft Graph only. Only apps that an administrator manages
are supported. A custom app is not.

### Tenant prerequisites

| # | In the Microsoft tenant | Why |
|---|---|---|
| 1 | **Web client** (the login app): requests the API app's `access_as_user` scope. Consent is given by each person at first sign-in, or once by an administrator for the tenant. | The login token must be issued for the API app. |
| 2 | **API app** (the platform's): exposes the scope `access_as_user`, and lists the web client as a known client application. | One combined consent at sign-in. |
| 3 | API app manifest: `requestedAccessTokenVersion` is `2`. | Tokens are v2: the audience is the client id. The platform accepts the client id or `api://<client id>` as audience. A v1 token carries the Application ID URI, and if that is any other value it is refused (fail closed). |
| 4 | API app has these **delegated** permissions with **admin consent**: Microsoft Graph `offline_access`, `User.Read` and `Sites.Selected`; SharePoint `Sites.Selected`. Application permissions are not used. | The exchange asks for them. Without consent Entra refuses it (`DELEGATION_EXCHANGE_FAILED`). |
| 5 | A site collection administrator grants the API app access to **the specific SharePoint site** (a site-level grant, for example with PnP PowerShell). | `Sites.Selected` reaches no site by itself. A missing grant is a 403 from SharePoint after a **successful** exchange. It is not `DELEGATION_EXCHANGE_FAILED`. |
| 6 | The login scope uses the API app's **Application ID URI** exactly as listed under Expose an API. It may be `api://<name>` rather than `api://<client id>`. | A scope that names a URI that does not exist fails at sign-in (AADSTS500011). Today this looks like a login loop. |

### Platform configuration

| Where | Key | Value |
|---|---|---|
| `elitea_core` | `entra_obo_client_id` | Client id of the API app. Unset: delegated credentials are refused (`DENIED_OAUTH_APP_UNAVAILABLE`). |
| `elitea_core` | `entra_obo_client_secret_env` | Name of the environment variable that holds the API app's client secret (default `ELITEA_API_CLIENT_SECRET`). The secret is never in the file. |
| `elitea_core` and `auth_oidc` | `entra_obo_custody_key_env` / `obo_custody_key_env` | Name of the environment variable that holds a key of 32 or more characters (default `ELITEA_OBO_CUSTODY_KEY`). It signs each request for a person's sign-in between `pylon_main` and `pylon_auth`. |
| `auth_oidc` | `obo_custody_enabled` | `true` to keep the Microsoft sign-in for the session (default `false`). |
| `auth_oidc` | `obo_api_client_id` | Client id of the API app. Only a sign-in token for it is kept. |
| `auth_oidc` | `scope` | `openid profile email offline_access <Application ID URI>/access_as_user` |
| `auth` | `identity_verifier.session_user.audience` | Client id of the API app. |

Give the client secret to `pylon_main` only, and the custody key to `pylon_main` and `pylon_auth` only. Do not put
either into an env file that every pylon loads: `pylon_indexer` runs toolkit code. With custody enabled but the key
or `obo_api_client_id` missing, `pylon_auth` logs an ERROR at start and keeps nothing.
`pylon_main` logs an ERROR when the client is configured without the key.

### What the platform stores, and what it never stores

| | |
|---|---|
| **Kept for the session** | The person's Microsoft sign-in (access token, refresh token, expiry) in the session Redis under their tenant and subject, for as long as the session lives. It is dropped at logout. A Keycloak login keeps nothing. |
| **Kept in memory only** | The exchanged Graph token, in the process of `pylon_main`, until it expires. At most 1024 entries; a token is served only while at least 15 minutes of life are left. A restart empties it. Logout drops the person's entries. |
| **The credential itself** | The descriptor only: app, tenant, resource, scopes, assertion source. No value, no refresh token, no token. |
| **Never stored** | The Graph token: not in Vault, not in any table, log, task payload, audit event or error. A test proves a delegated release makes no secret-store or database write. |

### Onboard one

1. The configuration must be of type SharePoint and hold **no** secret value (`DELEGATED_SECRET_PRESENT`
   otherwise).
2. Create the reference through the credential references API with `auth_mode: "entra_delegated"` and a
   `delegation` descriptor. There is no value and no store. There is no UI form yet *(planned)*.

   | Field | Allowed |
   |---|---|
   | `oauth_app_id` | `platform` (the app set in `entra_obo_client_id`), or the id of an admin-managed OAuth app |
   | `tenant_id` | A tenant GUID |
   | `resource` | `https://graph.microsoft.com` only |
   | `scopes` | 1 to 16 of `https://graph.microsoft.com/<Name>`, or `.default` alone |
   | `assertion_source` | `login` (a Microsoft sign-in). `connected_account` is accepted but not delivered. |

   Any other field, or a secret-shaped value, is refused (`INVALID_DESCRIPTOR`).
3. **Probe** does one real exchange. A personal reference is probed by its owner only (`DELEGATED_OWNER_PROBE`);
   a project reference by an operator. A failed exchange is `DELEGATION_PROBE_FAILED`; see the audit event.
4. A second operator approves, then activate. Grants work as for any credential.

Each allowed release is audited as `CREDENTIAL_RELEASED_DELEGATED` with the resource as target.

### Direct Entra login and Keycloak

| | Direct Entra login | Keycloak-brokered login |
|---|---|---|
| The login token is issued by | Microsoft, for the API app | Keycloak |
| The platform keeps the person's Microsoft sign-in | Yes, for the session | No |
| Delegated credential in a run the person starts | Works | `DENIED_ASSERTION_SOURCE_UNSUPPORTED` |
| Governance (bindings, grants, mandates) | Works | Works (section 2) |

Keycloak brokering is the next slice. It requires the broker token endpoint configuration, documented when
delivered.

A delegated credential never works in a run that is not a signed-in person's own: a mandate run
(`DENIED_MANDATE_DELEGATION_UNSUPPORTED`), an API token or system run (`DENIED_DELEGATION_UNSUPPORTED_CALLER`).
With governance off the SDK refuses it with `DENIED_DELEGATION_UNSUPPORTED_CALLER` too. There is no fallback to a
stored secret. The toolkit's automatic *Check connection* of a delegated credential is refused for the same reason.

### Refusals of delegated release

None of them revokes the run. The person can sign in again and retry. Each is audited as a `credential_acquired`
deny event.

| Code | Cause | Operator action |
|---|---|---|
| `DENIED_ASSERTION_UNAVAILABLE` | No stored sign-in: signed out, expired, never signed in with Microsoft, or the refresh was refused. | Ask the person to sign in with Microsoft again. If it repeats for everyone, check `obo_custody_enabled`, the scope and the key. |
| `DENIED_SUBJECT_MISMATCH` | The credential, its custody or its token belongs to another person or tenant than the one running. | Check that the credential is the person's own (personal) and the tenant in the descriptor. |
| `DENIED_MANDATE_DELEGATION_UNSUPPORTED` | A scheduled or webhook run (mandate) uses it, and the mandate does not act with its owner's consent (section 8a). | Use an application credential, or a mandate that names `owner_delegated`. |
| `DENIED_DELEGATION_UNSUPPORTED_CALLER` | The run is not a signed-in person's own run (API token, system run, governance off). | Run it from Elitea as the person. |
| `DENIED_ASSERTION_SOURCE_UNSUPPORTED` | The person signed in through Keycloak, or the source is `connected_account`. | Sign in directly with Microsoft. |
| `DENIED_OAUTH_APP_UNAVAILABLE` | The app cannot be resolved: client id not set, secret missing, admin app disabled, or the login token was issued for another app. | Check `entra_obo_client_id`, the secret variable and the app. |
| `DELEGATION_EXCHANGE_FAILED` | Entra refused the exchange after one refresh: consent, policy. | Check prerequisites 3 and 4 above. |
| `STALE_CONTEXT` | The run was admitted before the identity fields were stored. | Start the run again. |

If the exchange works but SharePoint answers 403, the site grant (prerequisite 5) is missing.

At the time of writing, a refusal raised at release time can reach the chat card as the generic text "This run was
not authorized." The code and the readable cause are in the audit trail. Mapping them to the card is a follow-up.

## 8a. Mandates that name their identity (#7131)

A scheduled or webhook run has nobody signed in. A mandate states what its runs act with on resources that need an
identity (`identity_source`, part of what is approved, set at create):

| Identity | The runs act with | What DevOps configures |
|---|---|---|
| none (default) | the stored credentials the mandate is granted, as before | nothing |
| `application` | an application credential: a SharePoint App-only configuration (client id + client secret, or a certificate), an ordinary governed secret reference, granted to the mandate like any other. Named on the mandate for the record; the run is the same as with none | the API app's **application** permissions with admin consent (SharePoint/Graph `Sites.Selected`), one site grant per site the agent may reach (PnP `Grant-PnPAzureADAppSitePermission` or Graph `POST /sites/{id}/permissions`, role read or write), a client secret or certificate stored as the credential's value |
| `owner_delegated` | the mandate owner's Microsoft sign-in, through the owner's offline consent: the platform exchanges it on behalf of the owner exactly as for the owner's own runs (section 8) | section 8 (delegated `Sites.Selected`, the site grant for the platform API app, `offline_access` in the login scope), plus custody on (`obo_custody_enabled`, the request key) |

The deployment states which identities a mandate may name: `mandate_identity_sources` in elitea_core's plugin config
(`[]` by default: no mandate can name one and nothing about one is read).

Owner consent, `owner_delegated` only:
1. Create the mandate with "Acts with: Owner's Microsoft sign-in" (or the task's `identity=owner_delegated`), submit,
   approve (another operator).
2. Grant first: each delegated credential of the agent version to the mandate (`subject_ref=mandate:<uuid>`), and
   for a project (shared) one also to the **owner** (`subject_ref=idp:<tid>:<oid>`): the owner must be allowed to use
   it themselves. A personal one must be the owner's own.
3. The owner, signed in with Microsoft (directly or through the brokering realm) as the very identity the mandate was
   made for, opens Governance -> Mandates and chooses **Give consent**. The dialog names the toolkits whose
   credentials it covers. The platform keeps one refresh token of that sign-in as the owner's personal material in the
   governed store (never in the mandate, an event or a log; sealed whenever it crosses the internal bus) and records
   which credentials (and their approved generations) the consent covers. A credential granted or approved again
   later needs a new consent.
4. Signing out does not end the consent (it is offline by design). The owner (or an operator) **withdraws** it, or
   revoking the mandate, its expiry, or the owner leaving (suspended) ends it; the next run is refused. Microsoft
   refusing the consent (expired after its lifetime of inactivity, revoked, password reset, conditional access)
   withdraws it and the next run is refused; the owner consents again. Emptying `mandate_identity_sources` stops every
   such run; consents can still be withdrawn.
5. A schedule changed last by someone other than its owner does not run with the owner's sign-in: the owner opens it
   and saves it.

| Code | Cause | Action |
|---|---|---|
| `DENIED_MANDATE_CONSENT_REQUIRED` | No consent, withdrawn, the credential is not one the consent covers (granted or approved again since), or someone else changed the schedule last. | The owner gives consent again, or saves the schedule. |
| `DENIED_GRANT` | A shared delegated credential the owner holds no grant on. | Grant it to the owner, then the owner consents again if it was added after the consent. |
| `DENIED_MANDATE_CONSENT_EXPIRED` | Microsoft no longer accepts the consent. | The owner gives consent again. |
| `DENIED_SUBJECT_MISMATCH` | The consent or the run is not the mandate owner's identity. | Make a new mandate for the owner's current identity. |

A webhook run under an `owner_delegated` mandate acts with the owner's sign-in on input chosen by the webhook's
caller: grant `run:webhook` to such a mandate only for webhooks the owner trusts.

## 9. Read a refusal

The user sees a short message with a code. Find the event in Overview > Security events, or
`GET /api/v2/tracing/security_audit/administration` with `reason_code`, `user_ref`, `agent_binding_ref` or
`credential_ref`.

### Admission

| Code | First thing to check |
|---|---|
| `IDENTITY_EVIDENCE_MISSING`, `IDENTITY_SIGNATURE_INVALID`, `IDENTITY_AUDIENCE_MISMATCH`, `IDENTITY_TENANT_MISMATCH` | The person signed in with the identity provider; the verifier configuration (section 2). The messages are the same on purpose. |
| `IDENTITY_SUBJECT_UNLINKED` | The platform user is linked to the identity-provider subject. After an identity-provider change, link again. |
| `IDENTITY_EXPIRED`, `IDENTITY_REVOKED` | The person signs in again. |
| `IDENTITY_KIND_NOT_ADMITTED` | The run started with an API token: *Admit legacy API tokens*, or run it as the person. |
| `DENIED_BINDING`, `BINDING_DIGEST_MISMATCH` | Binding of *this version* is active; rebind after an edit. From the editor's test chat: *Allow development runs* (section 5a). |
| `DENIED_ENTRY_POINT` | This kind of run cannot start from there. Use the chat, the editor or the API as the person. |
| `CREDENTIAL_REFERENCE_REQUIRED` | The toolkit selects its credential by title or inline; select the saved credential and register it. A `{{secret.*}}` outside a credential has the same effect. |
| `CREDENTIAL_REFERENCE_INACTIVE`, `CREDENTIAL_GENERATION_MISMATCH` | Reference is active; the configuration changed after approval, so re-probe and re-approve. |
| `CREDENTIAL_CLASS_NOT_ALLOWED` | The credential is another person's personal reference. |
| `DENIED_GRANT`, `DENIED_RBAC` | A grant exists and is active for this binding, toolkit and person; the person has `credentials.use`. |
| `UNSUPPORTED_AUTH_MODE` | The toolkit cannot use a governed credential yet. |
| `WORKER_CAPABILITY_UNAVAILABLE` | A worker with the governed capabilities is online (preflight `governed_worker`). |
| `AUDIT_UNAVAILABLE`, `DEPENDENCY_UNAVAILABLE` | Audit backlog and preflight on Overview. Nothing was let through. |
| `STORE_DENIED`, `STORE_SEALED`, `STORE_UNAVAILABLE`, `STORE_TLS_INVALID`, `STORE_NOT_FOUND`, `STORE_VERSION_CONFLICT` | The credential store (database master key, corporate Vault address and policy) |
| `RATE_LIMITED` | A run asked for its credentials too often and was stopped. Look for a loop in the agent. |
| `STALE_CONTEXT`, `EPOCH_MISMATCH`, `UNKNOWN_SCHEMA_VERSION` | The admission is no longer valid, or access was revoked after the run was admitted (a suspended binding, grant or reference). |

### Development runs

| Code | First thing to check |
|---|---|
| `DENIED_DEVELOPMENT_CREDENTIAL` | The author has no personal credential and no active grant on this project credential (section 5a). Approve a grant or ask the author to use a personal credential. |
| `DENIED_DEVELOPMENT_CHILD` | A sub-agent of the draft asked for a credential the draft was not admitted for. Ask the author to call the sub-agent from the draft, or bind it. |
| `DEVELOPMENT_RUNS_DISABLED` | Switch *Allow development runs* on, or bind and grant. |
| `TOOLKIT_RUN_TOKEN_NOT_ADMITTED` | A toolkit run started with an API token or through the MCP server. Run the toolkit from Elitea, or through a governed agent. |

### Run token and built-in Elitea tools

See the table in section 7: `INTERNAL_TOKEN_INVALID`, `INTERNAL_TOKEN_EXPIRED`, `CONTEXT_NOT_LIVE`,
`INTERNAL_SCOPE_DENIED`, `INTERNAL_OPERATION_UNKNOWN`, `INTERNAL_TARGET_DENIED`, `INTERNAL_MCP_ORIGIN_NOT_ALLOWED`,
`INTERNAL_TOKEN_UNAVAILABLE`.

### Mandates and schedules

| Code | First thing to check |
|---|---|
| `MANDATE_REQUIRED` | A schedule or webhook has no mandate, or the schedule's owner is not the mandate's owner (section 6). |
| `MANDATE_INACTIVE`, `MANDATE_EXPIRED` | The mandate was suspended, revoked or expired. Renew or resume it. |
| `DENIED_OPERATION` | The mandate does not allow this trigger kind (`run:schedule` or `run:webhook`). |
| `RUNTIME_LIMIT_EXCEEDED` | The run overran `max_runtime_s`. Raise it in a new mandate revision, or shorten the run. |
| `AUTHORIZATION_REQUIRED` | Scheduled indexing. No mandate path yet. |
| `SUB_AGENT_CREDENTIAL_NOT_ADMITTED` | A scheduled or webhook run calls a sub-agent that uses credentials. Not admitted yet. |

### Microsoft sign-in credentials *(from PR #602/#7/#21/#786, pending)*

`DENIED_ASSERTION_UNAVAILABLE`, `DENIED_SUBJECT_MISMATCH`, `DENIED_MANDATE_DELEGATION_UNSUPPORTED`,
`DENIED_DELEGATION_UNSUPPORTED_CALLER`, `DENIED_ASSERTION_SOURCE_UNSUPPORTED`, `DENIED_OAUTH_APP_UNAVAILABLE`,
`DELEGATION_EXCHANGE_FAILED`: see section 8. Credential API errors: `INVALID_DESCRIPTOR`, `UNSUPPORTED_PROVIDER`,
`DELEGATED_SECRET_PRESENT`, `DELEGATED_NO_VALUE`, `OAUTH_APP_UNAVAILABLE`, `DELEGATED_OWNER_PROBE`,
`DELEGATION_PROBE_FAILED`.

## 10. Roll out to a development environment

1. Upgrade `elitea_core`, `secrets`, `tracing`, `auth` and `indexer_worker`. Release the SDK and pin it in
   `sdk_plugin` **first**, then deploy `elitea_core` and the worker together.
2. Run the two admin tasks: `create_governance_settings_tables` and `create_agent_identity_tables`
   (`project_id=all`, `dry_run` first). Add `create_mandate_tables` and `create_credential_governance_tables` if
   they were never run.
3. Restart `pylon_main` and `pylon_indexer` one at a time. Do not restart `pylon_auth` together with `pylon_main`:
   `pylon_main` calls `pylon_auth` at start and times out. Wait for "Running ready callbacks" and "Got N projects".
4. Check the preflight (section 2). Set the master key if it is missing.
5. If you use Keycloak, set `subject_claims` and `subject_tenant` (section 2). **Re-grant** after an
   identity-provider switch: grants and mandates are tied to the subject.
6. Switch to Audit only with a policy revision. Run the agents that matter. Read the would-deny list.
7. Bind the agents, approve grants and mandates. Decide *Allow development runs* (section 5a).
8. Switch to Enforce. Authors can keep editing and testing with their own credentials.

## 11. Roll back

- **Switch to Off** on Overview (super administrator). Each replica picks it up within about 10 seconds. New
  runs are unchecked and behave as before. Runs already admitted keep their admission.
- **Hard stop**: set `ELITEA_CREDENTIAL_GOVERNANCE_MAX_MODE=off` and restart. This overrides the stored mode and
  works even when the database cannot be read.
- Suspend a single binding, reference, grant or mandate to take one agent or credential out of use without
  changing the mode. Live runs under it are revoked at their next check.
- The tables are additive. Nothing needs to be dropped or migrated back; bindings, references, grants and
  mandates stay stored and are ignored while the mode is Off.
