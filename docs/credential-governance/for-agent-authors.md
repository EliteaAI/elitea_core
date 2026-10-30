# For agent authors

Part of [credential governance](README.md). This page is for people who build and test agents, pipelines and
toolkits on a platform where an administrator has switched credential governance to **Enforce**. It says what
changes for you, what you will see, and who to ask. Administrators: see the [operator runbook](operator-runbook.md).

With governance **off** nothing here applies. With **Audit only** nothing is refused; the platform only records
what Enforce would have refused.

Items marked *(from PR #602/#7/#21/#786, pending)* are on open pull requests and not on `main` yet. Items marked
*planned* are not built and not promised.

## What Enforce means for you

Every run is checked before it starts. The platform needs to know who runs it (you, signed in), which approved
agent version runs, and that each credential the agent uses has been approved and granted for that use. A run
that fails a check does not start. You see a short message and a code.

| Where you run | What happens |
|---|---|
| The test chat in the agent or pipeline editor | Runs as a **development run** if your administrator allows them (below). |
| Test on a toolkit tool, Reindex, a chat with toolkits that use credentials | Also development runs. |
| A chat with an agent that has an approved **binding** and a **grant** for you | Runs. |
| A shared chat, a public agent, or an agent without a binding | Refused (`DENIED_BINDING`). |
| An API call made with a personal API token | Refused unless an administrator admits tokens, and even then it cannot act for you. Use your own session, or a mandate. |
| A schedule | Runs only on your own active **mandate** for that version (below). |
| A webhook | Runs only on an active mandate for that version. |

Credentials are never in the payload of a run. The platform releases them to the worker at the moment a toolkit is
built, and only for a use it has approved. You do not handle them.

## Development runs on a draft

A draft has no binding, so without a special rule you could not test it. A **development run** lets you test
your own draft with **your own rights only**.

**What works**

- The editor's private test chat of an agent or pipeline.
- Sub-agents your draft calls, at any depth. A sub-agent that has its own active binding runs on its own binding
  and grants. Any other runs as part of your draft.
- Toolkit tests, and indexing a toolkit on demand (Reindex).
- A plain LLM chat whose toolkits carry a credential.
- Toolkits with no credential, built-in tools and attachments. They need nothing.
- The built-in Elitea tools, with your own permissions and what the platform allows (see below).

**Which credentials a draft may use**

- Your own **personal** credential, or
- a **project** credential on which you hold an **active grant** (on any agent and toolkit).

The check is made on every turn. You may change the draft's toolkits between turns.

**What is refused**

- A project credential you hold no grant on.
- A sub-agent that asks for a credential that was not checked when the draft started.
- Anything that leaves the editor: shared chats, public agents, API calls, schedules, webhooks. These need a
  binding, grants and, for schedules and webhooks, a mandate.
- Toolkit runs started with an API token, and scheduled indexing.
- Everything, if your administrator has switched development runs off.

**What to ask your operator for**

- A **grant** on the project credential you need for testing, or a personal credential.
- A **binding** (and grants) when the agent is ready to be used by others.
- A **mandate**, when the agent must run on a schedule or a webhook.
- **Development runs on**, if you see `DEVELOPMENT_RUNS_DISABLED`.

Development runs cannot be shared, published or scheduled. After you edit a bound agent, its binding no longer
matches (`BINDING_DIGEST_MISMATCH`) until an operator rebinds and approves it.

## The cards you will see

A refusal appears as a card in the chat with a readable text and a **code**. Quote the code to your operator: it
finds the event in the audit trail. The texts below are the fixed texts of the platform.

| Code | Text | What to do |
|---|---|---|
| `DENIED_BINDING` | This agent version has no active identity binding; the run was not started. | Ask for a binding. From the editor, ask for development runs. |
| `BINDING_DIGEST_MISMATCH` | The agent changed after its identity binding was approved; the run was not started. | Ask for a rebind and approval. |
| `DENIED_GRANT` | This agent is not allowed to use one of its credentials for you. | Ask for a grant for you on this agent and toolkit. |
| `DENIED_RBAC` | You do not have permission to use this agent's credentials. | Ask for the permission to use credentials in the project. |
| `DENIED_DEVELOPMENT_CREDENTIAL` | This credential needs a grant before a draft can use it. | Ask for a grant on that credential, or use a personal one. |
| `DENIED_DEVELOPMENT_CHILD` | A sub-agent of this draft asked for a credential that was not checked for it when the draft started (only the sub-agents the draft calls are checked, each on its own); the run was not started. | Call the sub-agent directly from the draft, or bind it. |
| `DEVELOPMENT_RUNS_DISABLED` | Development runs (the editor's test chat, toolkit tests, on-demand indexing and toolkits with credentials in a chat) are switched off on this platform; an administrator can switch them on in Admin → Governance. The run was not started. | Ask an administrator. |
| `CREDENTIAL_REFERENCE_REQUIRED` | This agent uses a credential that is not registered for governed use; the run was not started. | Select the saved credential in the toolkit (not a title or a typed value), and ask for it to be registered. |
| `CREDENTIAL_REFERENCE_INACTIVE` | A credential this agent uses is suspended or retired; the run was not started. | Ask the operator. Your personal credential may still be pending approval. |
| `CREDENTIAL_CLASS_NOT_ALLOWED` | This agent uses a personal credential that belongs to someone else; the run was not started. | Use a credential of your own. |
| `IDENTITY_EXPIRED` | Sign in again to run governed agents. | Sign in again. |
| `MANDATE_REQUIRED` | No approved mandate or delegation covers this run; it was not started. | Schedules and webhooks: ask for a mandate (below). |
| `INTERNAL_SCOPE_DENIED` | This agent is not allowed to do that on your behalf: its approved operations do not include it. Do it yourself in Elitea, or ask an operator to extend the agent's binding. | The agent's built-in Elitea tools may only read, unless the binding allows more. |
| `INTERNAL_OPERATION_UNKNOWN` | This Elitea tool is not available to agents acting on your behalf; the call was refused. Do it yourself in Elitea. | Nothing to configure. |
| `INTERNAL_TOKEN_EXPIRED` | The Elitea tools of this run are past the run's time limit; start the run again. | Start again. |

The card has an "Error debugging info" part with the same code. The full list of codes is in the
[operator runbook](operator-runbook.md#9-read-a-refusal).

**Built-in Elitea tools.** If your agent can read or change platform data through the built-in Elitea tools, it
does so as you, within the operations its binding allows (`read`, `build`, `credentials`, `run`). Without a binding
setting it can read only. Every change is recorded in the audit trail with your name and the agent. A development
run may use all four, still limited by your own permissions.

## Never type a credential into the chat

Do not write passwords, tokens or keys into a chat, a prompt or an agent's instructions. The model would see them.
Create credentials on the Credentials page and select the saved credential in the toolkit. A toolkit that holds a
typed value, or a `{{secret.*}}` outside a registered credential, is refused with `CREDENTIAL_REFERENCE_REQUIRED`.

Creating a credential **from the chat**, through a secure form that never passes the value through the model, is
*planned* (#7228) and not built.

## Scheduled runs

A schedule is yours: you are its owner, also when you made it with "Build with AI" and saved it. It runs as you. Under
Enforce its cron firings need an **active mandate for the same agent version, made for you as owner**. **Run now**
and **Test run** act as you while development runs are on: they are your development run (as the editor's test chat),
with your sign-in and your credentials, and need no mandate and no admin role. While development runs are off they
are refused with `DEVELOPMENT_RUNS_DISABLED`.

- Ask an operator for a mandate for that version with you as its owner and `run:schedule` allowed. It must be
  approved by a second person.
- A mandate made for **someone else** does not cover your schedule, even on the same version. You get
  `MANDATE_REQUIRED`.
- **Run now** and **Test run** are for the schedule's owner only. An administrator sees them disabled with
  "Only the schedule owner, <owner name>, can run it." ("Only the schedule owner can run it." when the owner has no
  name): a hover tooltip on Run now in the row menu, a line next to Run test on the edit page.
- When you save a schedule while governance is on and you have no mandate for its version, the schedule is saved
  and you see: "Scheduled (cron) runs need an approved mandate for you on this version; Run now and Test run act as
  you. Ask a project admin (Admin → Governance → Mandates)." Under audit only it adds "Until then each run is
  recorded as 'would deny'."
- A mandate that is suspended, revoked or expired stops the run.

| Mode | A cron firing | Run now / Test run |
|---|---|---|
| Off | Runs as before. | Runs as before. |
| Audit only | Runs. A missing or unusable mandate is recorded as "would deny" for the operators. | Runs; evaluated as your own run. |
| Enforce | Runs on your mandate, or is refused (below). | Acts as you while development runs are on (your development run, as the editor's test chat): your own credentials, project ones granted to you, Microsoft ones on your current sign-in. No mandate. Off: refused `DEVELOPMENT_RUNS_DISABLED`. Not from an API token. |

**What a failure looks like.** The run is marked **failed** at once (in under a second); it never sits in
*running*. The schedule's run list shows the reason:

> The run failed: No approved mandate or delegation covers this run; it was not started.

You get a notice (the bell): `Schedule "<name>" failed: The run failed: <reason>`. For an agent schedule the run's
chat also says "Agent run was not started: <reason>"; for a pipeline it says "Pipeline run". The text has no names
or ids. Other reasons you may see are `MANDATE_INACTIVE` (suspended or revoked), `MANDATE_EXPIRED`,
`DENIED_OPERATION` (the mandate does not allow schedules) and `SUB_AGENT_CREDENTIAL_NOT_ADMITTED` (a sub-agent
with credentials is not admitted in a scheduled run yet).

After **three refused cron firings in a row** the schedule pauses itself (Run now and Test run do not count). Its Enabled
switch shows, on hover, "Paused: runs were refused by credential governance" and you get one notice: `Schedule
"<name>" was switched off: 3 runs in a row were refused (<reason>). Fix the cause and resume it.` A refusal because
a platform service is unavailable ("Try again later") does not count. Once the cause is fixed (for example, the
mandate is in place), press **Resume**; the count starts again. A run that is admitted while governance is on also
starts the count again; while governance is off the count is kept as it was, and counting goes on when governance is
on again. A mandate run uses the run's own short-lived identity, not your personal token.

If an administrator changes the owner of a schedule, the new owner needs a mandate of their own.

Scheduled **indexing** has no mandate path yet and is refused under Enforce (`AUTHORIZATION_REQUIRED`).

## Microsoft credentials *(from PR #602/#7/#21/#786, pending)*

Some toolkits (SharePoint today) can act **with your own Microsoft sign-in** instead of a stored secret. The
platform keeps no secret and no token for it: while you are signed in, it exchanges your sign-in for a Microsoft
token when your run needs one, and uses that token only for that run.

What you need to know:

1. **Sign in with Microsoft directly.** It works when you signed in to Elitea with your Microsoft account. If your
   organisation signs you in through another login page (Keycloak), the run is refused with
   `DENIED_ASSERTION_SOURCE_UNSUPPORTED` ("You signed in through your organisation's login page; acting with your
   Microsoft account needs a connected Microsoft account, which is not available yet. The run was not started.").
2. **A second person approves the credential.** An operator registers it, you try it (probe), and someone else
   approves and activates it. A grant is needed for a bound agent, as for any credential. There is no form for
   creating it in the UI yet.
3. **It only works in a run you start yourself** from Elitea. API tokens, system runs and **mandate runs** (a
   schedule or a webhook) cannot use it yet: `DENIED_DELEGATION_UNSUPPORTED_CALLER` ("This credential acts with your
   Microsoft sign-in, which only works in runs you start yourself from Elitea; the run was not started.") and
   `DENIED_MANDATE_DELEGATION_UNSUPPORTED` ("Scheduled and webhook runs cannot act with a person's Microsoft sign-in
   yet; the run was not started.").
4. **If the sign-in is gone, sign in again.** When your session ended or the Microsoft sign-in is no longer kept,
   the run is refused with `DENIED_ASSERTION_UNAVAILABLE` ("Your Microsoft sign-in is not available to this run.
   Sign in with Microsoft again, then retry."). Sign in with Microsoft again and retry the run. The run is not
   revoked.
5. **It is your credential.** A personal Microsoft credential belongs to one person. Using it for another person's
   run is refused (`DENIED_SUBJECT_MISMATCH`).
6. **Microsoft can say no.** If Microsoft refuses to issue the token, the run is refused with
   `DELEGATION_EXCHANGE_FAILED` ("Microsoft refused to issue a token for this credential (consent or site access
   missing); ask an administrator. The run was not started."). If the run starts but SharePoint answers "forbidden",
   your administrator has not granted the platform access to that site.

At the time of writing, some of these refusals show in the card as the generic text "This run was not authorized."
Your operator can read the exact code in the audit trail. Showing the readable text on the card is a follow-up.
