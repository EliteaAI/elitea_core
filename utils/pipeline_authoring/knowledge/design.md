---
key: design
depends_on: [llm, agent, toolkit, mcp, code, router, decision, hitl, state_modifier]
---
# Designing a pipeline

## Agent or pipeline

- **Agent**: one model with tools, deciding the steps itself. For open-ended, conversational or
  exploratory work with no fixed order.
- **Pipeline**: fixed steps and branches, explicit state, the same result every run. For
  "fetch, extract, check, branch, report" work, scheduled jobs and anything repeated often.
- **Pipeline that calls agents**: a fixed backbone where some steps need their own reasoning and
  tools. Each agent it calls should be a simple agent; to nest further, nest pipelines.

When both fit, prefer the pipeline for anything that runs repeatedly: every model call adds time,
cost and variation.

## Pick the cheapest node that can do the step

| Step | Node |
|---|---|
| Format or combine values | `state_modifier` |
| Parse, compute, validate, build exact strings | `code` |
| Branch on a value | `router` |
| Call one tool | `toolkit` / `mcp` |
| Understand or write language | `llm` |
| A sub-task needing its own tools and reasoning | `agent` |
| Branch on meaning | `decision` |
| A person must approve | `hitl` (interactive pipelines only) |

Common swaps: an `llm` that formats text -> `state_modifier`; an `llm` that parses JSON -> `code`;
a `decision` on keywords -> `router`; an `agent` that only fetches -> `toolkit`.

## Rules of thumb

- Start with a node that validates and parses the input (`pattern:validate_input`).
- Every path ends at END; give every router and decision a `default_output`.
- Every node that can set an `error` is followed by a router that checks it; otherwise the
  pipeline carries on with nothing and reports success.
- Pass data through typed state variables; write `messages` only for the final answer.
- Put ids, URLs and settings in `state` with defaults rather than inside node text.
- Report successes and failures; never drop failed items silently.
- Loops need a step limit that covers them (about 4 steps per item).
