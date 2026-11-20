---
key: pattern:final_answer
depends_on: [llm, code, state_modifier, printer]
---
# Show the result in chat

`state_modifier` nodes and structured `code` nodes write to state, not to the chat. To show a result,
end the path on a node that writes `messages`:

- an `llm` node with `output: [messages]` that phrases the answer, or
- a `code` node with `output: [messages]` and `structured_output: true` that returns
  `{"messages": [{"role": "assistant", "content": text}]}`. Use this for exact content (links, ids,
  tables) that a model must not rewrite. Without `structured_output` the reply would be the raw
  result wrapper (`{"result": ...}`), not your text.

```yaml
- id: build_report
  type: state_modifier
  input: [successes, failures]
  output: [report]
  template: |
    ## Result
    {{ successes | length }} done, {{ failures | length }} failed.
    {% for f in failures %}
    - {{ f.name }}: {{ f.reason }}
    {% endfor %}
  transition: show_report
- id: show_report
  type: code
  input: [report]
  output: [messages]
  structured_output: true
  code:
    type: fixed
    value: |
      report = elitea_state.get("report", "")
      {"messages": [{"role": "assistant", "content": report}]}
  transition: END
```

Several paths (success, error) can share one `show_report` node if they all write the same variable.

Do not end on a `printer` node: it pauses the run, and a pipeline started by a schedule, a webhook
or another agent never resumes.
