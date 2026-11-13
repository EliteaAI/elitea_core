---
key: node:hitl
depends_on: [hitl]
---
# hitl

Use to pause for a person to approve, reject or edit before the pipeline continues: before a
write, a send, or anything costly. It only works when a person is there: never in a scheduled or
webhook pipeline, or one that runs as another agent's tool.

## How it behaves

- `user_message` (`fixed`, `variable` or `fstring`) is what the person reviews; list the
  variables it uses in `input`.
- `routes` maps the action to the next node: `approve`, `reject`, `edit`. Without `approve` the
  approve button is not offered.
- `edit` writes the person's text into `edit_state_key` and continues at `routes.edit`.
- A hitl node has no `transition`, and should not be the `entry_point` (the user's first message
  is already in `input`).

## Example

```yaml
- id: review_summary
  type: hitl
  input: [draft]
  user_message:
    type: fstring
    value: "Publish this summary?\n\n{draft}"
  routes:
    approve: publish
    reject: END
    edit: publish
  edit_state_key: draft
```

## Mistakes

- An `edit` route without `edit_state_key`: the edit fails when chosen.
- `edit` routed to END: the edited value is thrown away.
- Showing values from a structured llm node that listed `messages` in its `output`: they arrive
  empty. Fix the llm node.
- Putting secrets in `user_message`.
