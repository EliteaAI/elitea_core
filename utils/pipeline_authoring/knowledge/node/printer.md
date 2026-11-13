---
key: node:printer
depends_on: [printer]
---
# printer (deprecated)

Shows a message and pauses until the user continues. Kept for existing pipelines; do not use it
in new ones.

Instead:
- To show the result in chat, end the path on an `llm` or `code` node that writes the answer to
  `messages`.
- To have a person approve before continuing, use `hitl`.

A printer pauses every run, so a scheduled pipeline, a webhook pipeline or a pipeline used as a
tool waits forever.

```yaml
- id: show_result
  type: printer
  input_mapping:
    printer: {type: fstring, value: "Result: {result}"}
  transition: END
```
