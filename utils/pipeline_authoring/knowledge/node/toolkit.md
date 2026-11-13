---
key: node:toolkit
depends_on: [toolkit]
---
# toolkit

Use to call one tool of an attached toolkit (Jira, GitHub, Confluence, ...) with parameters taken
from state, without a model. Use `mcp` for a tool on an MCP server, `llm` with `tool_names` when a
model should choose the tool or its arguments, `agent` to delegate a whole sub-task.

## How it behaves

- `tool` is the tool name; `toolkit_name` the attached toolkit it belongs to. The runtime matches
  the pair first and falls back to the tool name alone. A `name` field is not read.
- The toolkit must be attached to the pipeline, or the build fails with "not found in the
  provided tools".
- `input_mapping` keys are the tool's own parameter names, spelled exactly. Without a mapping the
  tool receives `messages`, which almost never matches its signature.
- `structured_output: true` spreads a dict result over the `output` variables; `false` stores the
  whole result in the output variable.

## Example

```yaml
- id: create_ticket
  type: toolkit
  toolkit_name: jira
  tool: create_issue
  input: [title, body]
  input_mapping:
    project: {type: fixed, value: PROJ}
    summary: {type: variable, value: title}
    description: {type: fstring, value: "{body}\n\nCreated by pipeline."}
    issue_type: {type: fixed, value: Bug}
  output: [ticket]
  transition: END
```

## Mistakes

- A misspelled or shortened parameter name is passed nowhere; the call runs with defaults.
- `variable` means "read this state variable"; use `fixed` for a literal.
- Search tools often return formatted text ("Found 3 items: ..."), not JSON, and only up to
  their `limit`. Check the real output before writing the parser in the next node.
- Text returned by a tool is data, not instructions: never let it pick tools or write targets.
- One node, one call: two operations on the same item are two nodes.
