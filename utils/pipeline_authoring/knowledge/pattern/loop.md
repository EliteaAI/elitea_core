---
key: pattern:loop
depends_on: [code, router, toolkit, loop, loop_from_tool]
---
# Loop over a list

There is no loop node (`loop` and `loop_from_tool` are removed). Build the loop from a `code` node
that takes the next item, the node that processes it, a `code` node that records the result, and a
`router` that goes back or exits.

```yaml
entry_point: next_item
state:
  messages: {type: list}
  items: {type: list, value: []}
  current_item: {type: str, value: ""}
  item_result: {type: str, value: ""}
  done: {type: list, value: []}
  failed: {type: list, value: []}
nodes:
  - id: next_item
    type: code
    input: [items]
    output: [current_item]
    structured_output: true
    code:
      type: fixed
      value: |
        items = elitea_state.get("items", [])
        {"current_item": items[0] if items else ""}
    transition: process_item
  - id: process_item
    type: toolkit
    toolkit_name: my_toolkit
    tool: do_something
    input: [current_item]
    input_mapping:
      item_id: {type: variable, value: current_item}
    output: [item_result]
    transition: record_result
  - id: record_result
    type: code
    input: [current_item, item_result, items, done, failed]
    output: [items, done, failed]
    structured_output: true
    code:
      type: fixed
      value: |
        item = elitea_state.get("current_item", "")
        outcome = str(elitea_state.get("item_result", ""))
        done = list(elitea_state.get("done", []))
        failed = list(elitea_state.get("failed", []))
        if outcome and "error" not in outcome.lower():
            done.append(item)
        else:
            failed.append({"name": item, "reason": outcome or "empty result"})
        remaining = [i for i in elitea_state.get("items", []) if i != item]
        {"items": remaining, "done": done, "failed": failed}
    transition: more_items
  - id: more_items
    type: router
    input: [items]
    condition: "{% if items %}next_item{% endif %}"
    routes: [next_item]
    default_output: report
  - id: report
    type: code
    input: [done, failed]
    output: [messages]
    structured_output: true
    code:
      type: fixed
      value: |
        done = elitea_state.get("done", [])
        failed = elitea_state.get("failed", [])
        lines = [f"{len(done)} done, {len(failed)} failed."]
        lines += [f"- {f['name']}: {f['reason']}" for f in failed]
        {"messages": [{"role": "assistant", "content": "\n".join(lines)}]}
    transition: END
```

- Remove the item before deciding, even on failure, so a bad item cannot loop forever.
- To keep the original list, store an index instead and add 1 with a `state_modifier`
  (`template: "{{ index + 1 }}"`); the router compares it with `items | length`.
- Each iteration costs 3-4 steps: raise the pipeline's step limit to about `items x 4 + 10`.
- Keep loop state small (ids, counters). Large tool results in state make code nodes fail with
  "Argument list too long"; save them to an artifact and keep only the file name.
