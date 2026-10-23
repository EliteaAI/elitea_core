# Credential governance (C-1009)

Credential governance is an optional control layer for agent runs. It decides, for each run, which person (or
which approved autonomous mandate) the run acts for, which approved agent definition runs, and which stored
credentials that agent may use for that person. The decision is made in Main, recorded in the security audit
trail, and the credential material is handed to the worker only at the moment a toolkit is built.

With the switch **off** (the default) the platform behaves exactly as it did before this feature existed.

## Scope

| In scope (slice 1) | Not in scope (see [non-goals](acceptance-contract.md#non-goals-of-slice-1)) |
|---|---|
| The current platform: `elitea_core` with `secrets`, `configurations`, `shared`, `tracing`, `auth`, `indexer_worker`, `elitea-sdk` and the Admin UI | The replatform (new platform monorepo) |
| Interactive chat, predict API, pipeline schedules and webhooks, parallel children, credential probes | Entra on-behalf-of exchange, OAuth/MCP tokens as references, project-level governance, owner self-service in EliteaUI |
| Credentials stored in the `database` store or a corporate Vault, referenced by `CredentialReference` rows | Per-provider qualification matrix and provider adapters |

The text describes the implementation delivered by the C-1009 integration set (`elitea_core#569` and the
matching pull requests in the other repositories). Module paths and names below exist once that set is on
`main`. The control text, the decision gates and the work breakdown live in the story and its sub-issues, not here.

- Story: [EliteaAI/elitea_issues#6754](https://github.com/EliteaAI/elitea_issues/issues/6754)
- This documentation closes: [EliteaAI/elitea_issues#6755](https://github.com/EliteaAI/elitea_issues/issues/6755)
  (credential profiles, identity boundaries, acceptance contract)

## Reading order

1. [credential-profiles.md](credential-profiles.md): what kinds of credentials and identities exist, their
   lifecycle, grants and approval rules.
2. [identity-boundaries.md](identity-boundaries.md): where identity is trusted, what the worker has to prove,
   how material travels, what never leaves the boundary.
3. [acceptance-contract.md](acceptance-contract.md): the switch, refusal codes, audit events, metrics,
   performance contract, operator API and non-goals. Tests and reviews check against this file.
4. [operator-runbook.md](operator-runbook.md): enable, onboard, bind, read a refusal, roll back.

## Decision gates D1 to D8 at a glance

The story defines eight gates. This is where each is answered in slice 1. Numeric targets that need the
customer's agreement are listed as open, not decided.

| Gate | Answer in slice 1 | Where |
|---|---|---|
| D1 credential classes | Shared and personal references are governed. A personal reference belongs to one person and is usable only by that person's runs. Entra-protected resources are planned as a no-material class (`entra_obo`). | [profiles](credential-profiles.md) |
| D2 agent identity | Each approved agent version has an `AgentIdentityBinding` with an explicit `identity_mode`. Nothing is claimed to equal an Entra agent identity. | [profiles](credential-profiles.md#agent-identity-binding) |
| D3 enforcement authority | Main is the policy decision point and the only reader of the credential store. The worker holds no store credentials and receives sealed, per-slot material. | [boundaries](identity-boundaries.md) |
| D4 delegation | A run acts for a verified person or for an approved mandate. Anything else is refused. Delegated background work without a mandate is refused. | [profiles](credential-profiles.md#autonomous-mandates), [boundaries](identity-boundaries.md#where-identity-is-trusted) |
| D5 operational contract | Epoch-based revocation checked on every validation and release, bounded context lifetime, audit policy `buffer` or `fail_closed`. No revocation SLO, retention or outage number is committed; these are open. | [contract](acceptance-contract.md) |
| D6 providers | Governance is toolkit-agnostic: it covers any configuration of the `credentials` section whose type has secret fields. Per-provider qualification is open. | [contract](acceptance-contract.md#non-goals-of-slice-1) |
| D7 scale | 300 teams and 60 calling applications stay synthetic test dimensions. The measured cost of governance is in the performance contract. | [contract](acceptance-contract.md#performance-contract) |
| D8 inbound API and MCP | Caller authentication is separate from the logical agent and from downstream credentials. Legacy API tokens are refused in governed runs unless an operator admits them, and even then cannot act for a person. | [boundaries](identity-boundaries.md#where-identity-is-trusted) |

## Conventions

- Reference identifiers are UUIDs. Person subjects are written `idp:<tid>:<oid>`; mandate subjects
  `mandate:<uuid>`.
- Refusal codes are stable upper-case strings with a fixed message. No message carries a token, a title, a
  claim value, an id from the request or text from a dependency.
- Governance state, credential references, grants, bindings and mandates never contain secret material.
