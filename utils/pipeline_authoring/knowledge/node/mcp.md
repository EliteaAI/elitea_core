---
key: node:mcp
depends_on: [mcp]
---
# mcp

Same as `toolkit`, for a tool served by an MCP server attached to the pipeline. Use `toolkit` when
the tool belongs to a native toolkit.

## How it behaves

- `tool` is the MCP tool name; `toolkit_name` the MCP server as attached. A `name` field is not read.
- `input_mapping` keys are the MCP tool's parameter names; map every required one.
- If the server needs the user to sign in, the run pauses for authorisation the first time.
- Calls to a local MCP server are cut at 30 seconds and return text only.

## Example

```yaml
- id: read_file
  type: mcp
  toolkit_name: github_mcp
  tool: get_file_contents
  input: [file_path]
  input_mapping:
    owner: {type: fixed, value: EliteaAI}
    repo: {type: fixed, value: elitea-sdk}
    path: {type: variable, value: file_path}
  output: [file_content]
  transition: summarise
```

## Mistakes

- `toolkit_name` must match the attached server name exactly (case matters).
- Leaving `input_mapping` out: the tool receives `messages`, which MCP tools rarely accept.
- Waiting on long work in one call to a local MCP server: start it, then poll with short calls.
