---
key: pattern:retry
depends_on: [code, router]
---
# Retry a failing step

Wrap a call that may fail in a counter and a `router`, with a fixed cap so the loop always ends.

```yaml
entry_point: call_service
state:
  messages: {type: list}
  attempts: {type: int, value: 0}
  ok: {type: bool, value: false}
  call_result: {type: str, value: ""}
nodes:
  - id: call_service
    type: toolkit
    toolkit_name: my_toolkit
    tool: fetch_report
    input_mapping:
      report_id: {type: fixed, value: weekly}
    output: [call_result]
    transition: check_call
  - id: check_call
    type: code
    input: [call_result, attempts]
    output: [ok, attempts]
    structured_output: true
    code:
      type: fixed
      value: |
        text = str(elitea_state.get("call_result", ""))
        {"ok": bool(text) and "error" not in text.lower(),
         "attempts": elitea_state.get("attempts", 0) + 1}
    transition: retry_or_stop
  - id: retry_or_stop
    type: router
    input: [ok, attempts]
    condition: "{% if ok %}use_result{% elif attempts < 3 %}call_service{% endif %}"
    routes: [use_result, call_service]
    default_output: report_failure
  - id: use_result
    type: llm
    input: [call_result]
    output: [messages]
    input_mapping:
      task: {type: fstring, value: "Summarise this report:\n{call_result}"}
    transition: END
  - id: report_failure
    type: code
    input: [call_result, attempts]
    output: [messages]
    structured_output: true
    code:
      type: fixed
      value: |
        text = f"Gave up after {elitea_state.get('attempts', 0)} attempts: {elitea_state.get('call_result', '')}"
        {"messages": [{"role": "assistant", "content": text}]}
    transition: END
```

The `default_output` is the "gave up" path: say what failed instead of ending silently.
