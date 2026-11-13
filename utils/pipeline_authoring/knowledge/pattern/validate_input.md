---
key: pattern:validate_input
depends_on: [code, router]
---
# Validate the input, route on errors

Start a pipeline that expects structured input with a `code` node that parses and checks it, then
a `router` that sends errors to a node that explains them. Later nodes can then trust the state.

```yaml
entry_point: parse_input
state:
  input: {type: str}
  messages: {type: list}
  error: {type: str, value: ""}
  ticket_id: {type: str, value: ""}
nodes:
  - id: parse_input
    type: code
    input: [input]
    output: [error, ticket_id]
    structured_output: true
    code:
      type: fixed
      value: |
        import re
        raw = elitea_state.get("input", "")
        match = re.search(r"[A-Z]+-\d+", raw or "")
        if match:
            result = {"error": "", "ticket_id": match.group(0)}
        else:
            result = {"error": "Send a ticket key such as PROJ-123.", "ticket_id": ""}
        result
    transition: check_input
  - id: check_input
    type: router
    input: [error]
    condition: "{% if not error %}process{% endif %}"
    routes: [process]
    default_output: explain_error
  - id: process
    type: llm
    input: [ticket_id]
    output: [messages]
    input_mapping:
      task: {type: fstring, value: "Summarise what is known about {ticket_id}."}
    transition: END
  - id: explain_error
    type: code
    input: [error]
    output: [messages]
    structured_output: true
    code:
      type: fixed
      value: |
        error = elitea_state.get("error", "Invalid input.")
        {"messages": [{"role": "assistant", "content": error}]}
    transition: END
```

- Collect every problem before returning, so the user can fix them in one go.
- `explain_error` posts the message as the assistant's reply (see `pattern:final_answer`).
