---
key: node:code
depends_on: [code]
---
# code

Use for deterministic work: parse or validate input, compute, build exact strings, call
`elitea_client`. Use `state_modifier` for a simple template, `toolkit`/`mcp` for one tool call,
`llm` for reasoning.

## How it behaves

- The Python runs in a sandbox as a module. `elitea_state` (a dict of state) and `elitea_client`
  are pre-defined; do not define or import them.
- The node receives only the variables listed in `input` (except `messages`). With no `input`, or
  `input: [messages]`, it receives every state variable.
- The value of the **last top-level expression** is the result. `return` at top level is a syntax
  error. Top-level `await` is allowed.
- `structured_output: true`: the result must be a dict; its keys land in the matching state
  variables listed in `output`. To reply in chat, return
  `{"messages": [{"role": "assistant", "content": text}]}` with `output: [messages]`.
- `structured_output: false`: the whole result, wrapped as `{"result": ...}`, goes into each
  `output` variable.
- Standard library and `httpx`/`requests` are available; for others use
  `import micropip; await micropip.install(["pkg"])`. No filesystem, no `os`.
- The listed state is compressed and passed to the sandbox as one command-line argument, so very
  large state fails with "Argument list too long". Keep loop state to ids, counters and short text.

## Example

```yaml
- id: normalise
  type: code
  input: [input]
  output: [ticket_id, is_valid]
  structured_output: true
  code:
    type: fixed
    value: |
      import re
      raw = elitea_state.get("input", "")
      match = re.search(r"[A-Z]+-\d+", raw)
      if match:
          result = {"ticket_id": match.group(0), "is_valid": True}
      else:
          result = {"ticket_id": "", "is_valid": False}
      result
  transition: route_on_valid
```

## Mistakes

- The result dict only inside an `if`/`for`/`try` branch, or inside a function: nothing is
  captured and downstream nodes see "Execution result is missing". Assign `result` on every
  branch and end with a bare `result` line.
- `code: "..."` as a plain string, or a `code` block without `type: fixed`.
- Reading `elitea_state.get("x")` without `x` in `input`: it always gets the default.
- `input` written as a list of objects instead of names.
- `raise` aborts the whole pipeline; return an `error` key instead and route on it.
- Never mutate `elitea_state`; return the new values.
