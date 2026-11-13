---
key: node:llm
depends_on: [llm]
---
# llm

Use for generation, summarising, rewriting, classification or extraction, and reasoning that
calls tools. Use `router` for branching on a value, `decision` to let a model pick the next node,
`state_modifier` or `code` for transforms without a model, `agent` to delegate to an existing agent.

## How it behaves

- `system` is the instruction, `task` the request; give at least one. `chat_history` is optional:
  `{type: variable, value: messages}` shares the conversation, `{type: fixed, value: []}` starts clean.
  Without any mapping the node sends only the chat history.
- `structured_output: true`: the model must return JSON whose keys are the `output` variables;
  each key is a required field. Describe every key and its allowed values in `system`.
- `tool_names: {<toolkit_name>: [tool, ...]}` binds tools the model may call. Bind only what this
  node needs. Without `tool_names` a pipeline llm node gets no tools.
- `tool_execution_timeout` (seconds) caps each tool call.

## Example: extract into typed variables

```yaml
- id: extract_fields
  type: llm
  input: [ticket_text]
  output: [title, priority]
  structured_output: true
  input_mapping:
    system: {type: fixed, value: 'Return JSON {"title": short title, "priority": one of P1, P2, P3}.'}
    task: {type: fstring, value: "Ticket:\n{ticket_text}"}
    chat_history: {type: fixed, value: []}
  transition: END
```

## Example: final answer with tools

```yaml
- id: investigate
  type: llm
  input: [repo, messages]
  output: [messages]
  input_mapping:
    system: {type: fixed, value: You investigate repositories. Use the tools when needed.}
    task: {type: fstring, value: "List open security alerts in {repo}."}
    chat_history: {type: variable, value: messages}
  tool_names:
    github: [list_open_issues, get_repo_details]
  transition: END
```

## Mistakes

- `messages` in the `output` of a `structured_output: true` node: the model never returns it, so
  the whole structured result is dropped and every output variable ends up empty.
- A `{var}` used in `task` but missing from `input`.
- A `tool_names` toolkit that is not attached to the pipeline binds nothing, silently.
- Routing large or exact data (links, ids, long text) through a model: it truncates and invents.
  Build exact strings in a `code` node.
