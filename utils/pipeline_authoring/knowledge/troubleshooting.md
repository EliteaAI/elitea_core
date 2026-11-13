---
key: troubleshooting
depends_on: [code, llm, agent, toolkit, router, decision, state_modifier, hitl, printer]
---
# Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| "Entry point is not defined" | no top-level `entry_point` | add `entry_point: <first node id>` |
| "Tool name is required for toolkit node" | the node has `name` but no `tool` | set `tool` (and `toolkit_name`) |
| "not found in the provided tools" | toolkit, MCP server or agent not attached, or name differs | attach it; copy the exact name from the error's list |
| "missing required input_mapping parameters: {'task'}" | agent node without `task` | add `input_mapping.task` |
| "'custom' which is not a valid execution type" | a node left as `type: custom` | set the real type |
| Run stops early with no error | a `transition` points to a misspelled node, or a node has no transition | fix the id; add the transition |
| Variable is empty in a code node | not listed in the node's `input` | add it to `input` |
| State not updated after a code node | dict not the last top-level expression, or keys not in `output` | end with the dict; list the keys |
| Every structured output is empty | `messages` (or another unreturned key) in the llm node's `output` | remove it |
| Router always takes `default_output` | the condition renders text other than a route id, or reads a variable not in `input` | render only the id; list the variable |
| Decision picks the wrong route | vague `description` | describe each option and when to choose it; use a router if a rule can decide |
| Results not visible in chat | the last node writes state, not `messages` | see `pattern:final_answer` |
| Run waits forever | a `printer` or `hitl` with no person to respond | remove it from unattended pipelines |
| Same 2-3 nodes repeat until the step limit | a loop node failed and never advanced its index, often "Argument list too long" | keep loop state small; check the loop node's error |
| "Step limit exceeded" on a correct loop | too many items for the limit | raise the step limit (about 4 per item) |
| Tool runs but ignores a parameter | parameter name misspelled or abbreviated | use the tool's exact parameter name |
| `json.loads` fails with "Invalid control character" | real newlines inside JSON strings | re-escape newlines inside strings before parsing |

To find the failing node: read the run log for the node name and error; add a node's id to
`interrupt_before` or `interrupt_after` at the top level to inspect state around it.
