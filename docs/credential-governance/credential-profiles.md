# Credential profiles

Part of [credential governance](README.md). This file defines what a governed run may use and how that is
approved. [identity-boundaries.md](identity-boundaries.md) defines who is trusted to ask;
[acceptance-contract.md](acceptance-contract.md) defines how refusals look.

## Two layers

A governed run is decided in two layers that are never merged:

1. **Delegation.** Who the run acts for: a verified person (`idp:<tid>:<oid>`) or an approved autonomous
   mandate (`mandate:<uuid>`), and which approved agent version runs.
2. **Provider credential.** Which stored credential the agent's toolkit may use, released only after layer 1
   and the credential decision allow it.

A credential held by a service or a shared account is never presented as the person's own authority. It is
either a *shared service reference* (approved, granted explicitly) or used under a *mandate*.

## Classes

| Class | Who holds the value | Subject of the grant | Status in slice 1 |
|---|---|---|---|
| Shared service reference | Stored once for the project. Registered as `project_shared`, `service` or `technical`. Value is imported from an existing credential configuration by the server. | A person (`user_delegation`) or a mandate (`mandate`) | Implemented |
| Personal reference | One project member, who sets the value. Administrators register, probe, approve, suspend and retire the reference but cannot set or read the value. | The owner only (a grant for another person is refused) | Implemented |
| `entra_obo` | No stored material. The release would be an on-behalf-of token exchange from the person's delegated session token. | A person | Planned (WP-2A, #6941 b). Not in the schema. |
| Autonomous mandate | Not a credential. An approved, time-boxed permission for one agent version to run on schedules and webhooks without a signed-in person. It uses shared or the owner's personal references through grants made to `mandate:<uuid>`. | The mandate | Implemented (admin task only, no REST) |

The schema reserves the class value `scheduled`. It cannot be onboarded in slice 1.

### Personal references

- The owner is a member of the project and is linked to an identity-provider subject. `custodian_ref` stores
  the owner's user id and subject. It never changes after creation.
- A person has at most one live personal reference per configuration, next to the one shared reference.
- If the person has a personal reference, their runs use it and nothing else. While it is `pending`,
  `validated` or `suspended`, their runs are refused. They do not fall back to the shared credential.
- Only the owner writes the value (`PUT` on the write-only material route, error `OWNER_ONLY` for anyone
  else). The value is never returned, logged or audited. A new value returns the reference to `pending`.
- A mandate may use a personal reference only if the mandate's owner is its custodian.

## Store kinds

| Kind | Meaning | Governed use |
|---|---|---|
| `database` | Per-reference rows in the encrypted project store of the `secrets` plugin | Yes |
| `corporate_vault` | HashiCorp Vault KV v2 operated by the customer. Path `<mount>/data/t/<tenant>/cred/<reference id>`. No fallback to another store. | Yes |
| `managed_vault` | The platform's own Vault integration | Accepted by the schema, **not** usable by a governed run (`CREDENTIAL_REFERENCE_INACTIVE`) |

Operators choose the kind when they create a reference. The store locator is restricted: it is never in an
API response, log line or audit event. Installing the feature migrates nothing. Existing configurations keep
working until an operator registers a reference for them and an agent run is governed.

## Reference lifecycle

```
pending -> validated -> active <-> suspended
   ^           |           |           |
   +-----------+           +--> retired <-+   (retire also from pending and validated)
```

| Transition | Action | Condition |
|---|---|---|
| (create) -> `pending` | `create` | Configuration is in the `credentials` section and has secret fields |
| `pending` -> `validated` | `probe` | The store holds the current generation and it matches the configuration (a personal reference: the configuration is unchanged since the value was written). `check_connection` adds a test against the provider for a personal reference. |
| `validated` -> `validated` (approved) | `approve` | Approver is not the creator and not the writer of the current generation |
| `validated` -> `active` | `activate` | Approved, and the validated generation equals the current one |
| `active` <-> `suspended` | `suspend`, `resume` | |
| any non-retired -> `retired` | `retire` | Terminal |

- **Generation.** Every committed value is a new generation. The reference remembers the configuration
  generation it was approved at; if the configuration changes afterwards, runs are refused with
  `CREDENTIAL_GENERATION_MISMATCH` until the reference is probed and approved again.
- **Rotation.** `suspend`, write the new value, `probe`, `approve`, `activate`. Grants keep the reference id.
  A re-import or a new value puts a `validated` or `suspended` reference back to `pending` and clears the approval.
- **Epoch.** Suspending, retiring or revoking raises the `revocation_epoch`. Runs admitted earlier carry the
  epoch they were admitted with and are revoked at their next validation or release.
- **Staged writes.** A store write is a three-step transaction around the store call. A ledger row
  (`credential_store_ledger`) lets `reconcile_credential_store` settle a crash between the store and the database.

## Grants

A grant allows **one agent binding to use one credential reference through one toolkit instance for one
subject**. All four parts are required, and a missing grant is a refusal, not a pass.

| Field | Rule |
|---|---|
| `agent_binding_ref`, `toolkit_instance_id`, `credential_ref` | Fixed at creation |
| `subject_kind`, `subject_ref` | `user_delegation` with `idp:<tid>:<oid>`, or `mandate` with `mandate:<uuid>`. The REST API takes the person (`subject_user_id`) and resolves the subject on the server; a raw subject string is refused. |
| `target_ref`, `operations` | `*` in slice 1 (whole target, all operations) |
| `valid_from`, `valid_until` | Optional end, at most 365 days through the REST API |
| States | `pending_approval` -> `active` <-> `suspended`; `revoked` is terminal. Approval activates in one statement. |

A grant made to a person does not satisfy a run under a mandate, and the reverse.

## The credential decision

For every credential a run uses, the checks below run in order and the first failure refuses the run. They are
evaluated at admission and re-evaluated from the stored decision at every validation and release.

| # | Check | Refusal code |
|---|---|---|
| 1 | The toolkit selects the credential by configuration uuid (title-only or inline credentials are not governed) | `CREDENTIAL_REFERENCE_REQUIRED` |
| 2 | The configuration is in the `credentials` section, and in the agent's own project | `CREDENTIAL_REFERENCE_REQUIRED`, `DENIED_GRANT` |
| 3 | A credential reference exists for it (the caller's personal one first, else the shared one) | `CREDENTIAL_REFERENCE_REQUIRED` |
| 4 | The reference is `active` and in the `database` or `corporate_vault` store | `CREDENTIAL_REFERENCE_INACTIVE` |
| 5 | A personal reference is used by its custodian only (for a mandate: the mandate owner) | `CREDENTIAL_CLASS_NOT_ALLOWED` |
| 6 | The configuration is unchanged since the reference was approved | `CREDENTIAL_GENERATION_MISMATCH` |
| 7 | The person (or mandate owner) holds `credentials.use` | `DENIED_RBAC` |
| 8 | An active grant exists for this binding, toolkit, credential and subject, of the right subject kind | `DENIED_GRANT` |

A run may use at most 64 governed credentials. A `{{secret.*}}` placeholder in a free-form field outside a
credential configuration is refused with `CREDENTIAL_REFERENCE_REQUIRED`, because no slot would govern it.

## Agent identity binding

A binding is the durable record that a particular **version** of an agent was approved to run governed. It is
keyed on project, application, version and `identity_mode`.

| `identity_mode` | Meaning |
|---|---|
| `elitea_native` | The platform vouches for the agent. No external identity is involved. |
| `dedicated_agent` | A dedicated external agent identity is referenced (`external_agent_identity_ref`). The registry of external identities is not part of slice 1. |
| `runtime_domain_binding` | The identity is resolved by the identity provider at run time. Activation is refused unless the operator switch *Allow runtime domain bindings* is on. |

States: `draft` -> `provisioning` -> `pending_approval` -> `active` <-> `suspended`; `active` or `suspended`
-> `retiring` -> `retired`. `rebind` returns an `active` or `suspended` binding to `pending_approval` for the
definition as it is now, clears the approval and revokes live runs.

- **Definition digest.** The binding stores a SHA-256 digest (`adv1:` prefix) of the normalised agent version:
  type, instructions, model settings, the toolkits and the tool subset it may call, and the version an
  agent-as-tool points at. Names, ids, descriptions, tags, variables and layout are left out. Credential
  content is not in the digest; grants and references govern it. Admission recomputes the digest. A changed
  definition is refused with `BINDING_DIGEST_MISMATCH` until the binding is rebound and approved.
- Activation needs an approval reference and a policy revision, and an approver who is neither the creator
  nor the last operator to change the binding's state.
- An agent version needs exactly one active binding of an admitted mode. A new version needs its own.

## Autonomous mandates

A mandate lets one agent version run from a schedule or a webhook as its owner's autonomous agent.

| Field | Rule |
|---|---|
| `owner_user_ref` | `issuer`, `tid`, `oid` of the owner; the owner must stay active and linked to the same identity |
| `agent_binding_ref` | The active binding of the version; a run is refused (`DENIED_BINDING`) if the version's active binding is another one |
| `allowed_operations` | `run:schedule`, `run:webhook` |
| `max_runtime_s` | 60 to 86400. Main stops a run that overruns it (`RUNTIME_LIMIT_EXCEEDED`). |
| `valid_until` | At most 180 days ahead. Expiry is by the clock (`MANDATE_EXPIRED`). |
| One live mandate | At most one non-terminal mandate per agent version. Changing scope means revoking and creating the next revision. |

States: `draft` -> `pending_approval` -> `active` <-> `suspended`; `expired` by the clock; `revoked`. Leaving
`active` raises the epoch and revokes the mandate's live runs. If the owner is suspended or deleted, the
mandate is suspended automatically (`OWNER_SUSPENDED`).

Without a mandate, a schedule or webhook of a governed agent runs as the system user and is refused with
`MANDATE_REQUIRED`. A per-user index schedule, which acts on a person's behalf without that person, is refused
with `AUTHORIZATION_REQUIRED`. Scheduled indexing has no mandate path in slice 1.
`report_trigger_principals` lists the schedules and webhooks that would be refused.

## Four-eyes rules

Enforced in the state change itself (a conditional `UPDATE` plus a database check), not only in the API.

| Object | Who may approve |
|---|---|
| Agent identity binding | Not the creator and not the last operator who changed its state (`SELF_APPROVAL`) |
| Credential reference | Not the creator and not the writer of the current generation. For a personal reference the owner writes the value, so a second administrator approves. |
| Credential grant | Not the creator |
| Autonomous mandate | Neither its creator nor its owner |

These are rules between distinct operators. Slice 1 operators are platform administrators; delegating approval
to project administrators (two distinct project administrators, escalation for single-administrator projects)
is #7133.
