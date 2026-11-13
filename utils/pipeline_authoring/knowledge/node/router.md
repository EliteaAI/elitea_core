---
key: node:router
depends_on: [router]
---
# router

Use for branching on state values without a model: a flag, a status, a count, a keyword. Use
`decision` when the branch needs language understanding.

## How it behaves

- `condition` is a Jinja2 template over the variables in `input`; it must render the id of one of
  the `routes`.
- The rendered text is stripped of everything except letters, digits, `_`, `-` and `.`, then
  compared with `routes`; anything else, including an empty result, goes to `default_output`
  (END when not set).
- A router has no `transition`, and cannot be the `entry_point`.

## Example

```yaml
- id: route_by_priority
  type: router
  input: [priority]
  condition: |
    {% if priority in ["P0", "P1"] %}escalate{% elif priority == "P2" %}schedule{% else %}backlog{% endif %}
  routes: [escalate, schedule, backlog]
  default_output: backlog
```

## Mistakes

- A route id that is not a node id.
- Relying on the implicit `default_output: END`: an unexpected value then ends the run quietly.
  Set it.
- Rendering extra words around the id: "go to escalate" becomes `gotoescalate` and matches no
  route.
- Use `default_output: END` for the exit rather than listing END in `routes`.
