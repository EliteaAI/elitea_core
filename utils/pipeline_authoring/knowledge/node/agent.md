---
key: node:agent
depends_on: [agent]
---
# agent

Use to hand a sub-task to an agent or pipeline attached to this pipeline; it runs its own
reasoning loop with its own tools. Use `llm` for a single model call, `toolkit`/`mcp` for one tool
call. Nested pipelines are called this way too (the `pipeline` node type is removed).

## How it behaves

- `tool` is the attached agent's name; `toolkit_name` is optional and only disambiguates two
  tools with the same name. A `name` field is not read.
- `input_mapping.task` is required: the child sees only this text, not the parent's history.
  `chat_history` is accepted but ignored.
- Other `input_mapping` entries are passed as the child's variables.
- The child's answer is written to `messages` (added to `output` automatically) and to any other
  `output` variable.

## Example

```yaml
- id: triage
  type: agent
  tool: Bug Triage
  input: [report, component]
  output: [triage_summary]
  input_mapping:
    task:
      type: fstring
      value: |
        Triage this bug report for component {component}.

        ## Report
        {report}
  transition: file_ticket
```

## Mistakes

- The agent is not attached to the pipeline: the build fails with "not found in the provided
  tools". The error lists the available names.
- A thin `task`: give the child every piece of context it needs, by section heading.
- Writing `{{variable}}` in the child agent's own instructions turns it into a required input the
  user is asked to fill in; pass content through `task` instead.
