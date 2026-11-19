---
key: pattern:approval
depends_on: [hitl, llm, toolkit]
---
# Ask before acting

When a person must approve a write (create a ticket, send a message), draft first, then pause with
a `hitl` node, and act only on the approve or edit path.

```yaml
- id: draft
  type: llm
  input: [input]
  output: [draft_text]
  structured_output: true
  input_mapping:
    system: {type: fixed, value: 'Write the message. Return JSON {"draft_text": "..."}.'}
    task: {type: fstring, value: "{input}"}
  transition: review
- id: review
  type: hitl
  input: [draft_text]
  user_message: {type: fstring, value: "Send this message?\n\n{draft_text}"}
  routes: {approve: send, edit: send, reject: END}
  edit_state_key: draft_text
- id: send
  type: toolkit
  toolkit_name: slack
  tool: post_message
  input: [draft_text]
  input_mapping:
    channel: {type: fixed, value: "#team"}
    text: {type: variable, value: draft_text}
  transition: END
```

Only for pipelines a person runs from chat. When the pipeline is called by a schedule, a webhook or
another agent, end after the draft and return it with the question instead; the caller asks the
user and calls the pipeline again with the decision in `input`. Do nothing irreversible before
the decision.
