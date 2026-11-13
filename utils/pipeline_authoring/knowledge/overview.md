---
key: overview
depends_on: []
---
# Pipeline YAML: the shape

A pipeline is a graph of nodes. The runtime starts at `entry_point`, runs a node, follows its
edge, and stops when a path reaches `END`.

```yaml
entry_point: classify          # id of the first node; required, never a router/decision/hitl
state:                         # optional; variables the nodes read and write
  input: {type: str}           # the user's message (always present)
  messages: {type: list}       # chat history (always present); keep it when you declare state
  intent: {type: str, value: ""}
nodes:
  - id: classify               # unique; use letters, digits, _ and - (other characters are dropped)
    type: llm
    input: [input]             # state variables this node reads
    output: [intent]           # state variables this node writes
    structured_output: true
    input_mapping:
      system: {type: fixed, value: 'Classify the request. Return JSON {"intent": "bug" or "question"}.'}
      task: {type: fstring, value: "Request: {input}"}
    transition: route          # next node id, or END
  - id: route
    type: router
    input: [intent]
    condition: "{% if intent == 'bug' %}file_bug{% else %}answer{% endif %}"
    routes: [file_bug, answer]
    default_output: answer
  - id: file_bug
    type: llm
    input: [input]
    output: [messages]
    input_mapping:
      task: {type: fstring, value: "Write a short bug report for: {input}"}
    transition: END
  - id: answer
    type: llm
    input: [input]
    output: [messages]
    input_mapping:
      task: {type: fstring, value: "Answer: {input}"}
    transition: END
```

## Values in input_mapping

Every entry is a `{type, value}` block:

| type | value is | example |
|---|---|---|
| `fixed` | a literal | `{type: fixed, value: "PROJ"}` |
| `variable` | the name of a state variable | `{type: variable, value: ticket_id}` |
| `fstring` | text with `{var}` placeholders | `{type: fstring, value: "Fix {ticket_id}"}` |

List every `{var}` you use in the node's `input`. Write literal braces as `{{` and `}}`.

## Edges

- Most nodes: `transition: <node id>` or `transition: END`. A node with no transition ends the run.
- `router`: `routes: [ids]` + `default_output`. `decision`: `nodes: [ids]` + `default_output`.
- `hitl`: `routes: {approve: <id>, reject: <id or END>, edit: <id>}`.
- `END` is a target, never a node. Do not add a node with `type: END`.
- A target that is not a node id does not fail the build: the run silently stops there.

## State

- `input` and `messages` always exist. Other variables need a `state` entry with a type
  (`str`, `int`, `float`, `bool`, `list`, `dict`).
- Pass intermediate results through typed variables. Write `messages` only in the node that
  produces the answer the user sees: every llm or agent node that reads `messages` re-sends the
  whole history.

## Tools, agents and MCP

A `toolkit`, `mcp` or `agent` node calls something attached to the pipeline. Set `tool` to the
exact tool (or agent) name and `toolkit_name` to the attached toolkit or MCP server. The runtime
reads `tool`; a `name` field is ignored. Map the tool's real parameter names in `input_mapping`.
