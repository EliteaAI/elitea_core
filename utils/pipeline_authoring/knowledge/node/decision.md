---
key: node:decision
depends_on: [decision]
---
# decision

Use when a model should choose the next node from a fixed list, guided by a description: intent
routing, "is this draft good enough". Every pass costs one model call; use `router` when a
template over state can decide.

## How it behaves

- `nodes` lists the candidate node ids; `description` tells the model when to pick each.
- The model sees the variables in `input` (or `decisional_inputs`).
- An answer that is not in `nodes` goes to `default_output` (END when not set).
- A decision has no `transition`, cannot be the `entry_point`, and should not follow another
  decision directly.

## Example

```yaml
- id: choose_flow
  type: decision
  input: [input]
  description: |
    Pick the workflow for the user's message:
    - billing: invoices, payments, refunds
    - support: bugs, errors, something does not work
    - chat: greetings and anything else
  nodes: [billing, support, chat]
  default_output: chat
```

## Mistakes

- A vague `description`: when unsure the model falls back to `default_output`. Name each option
  and when to choose it.
- An inline `decision:` block on another node still works but is deprecated; use this node.
