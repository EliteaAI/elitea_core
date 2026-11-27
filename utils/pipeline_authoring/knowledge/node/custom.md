---
key: node:custom
depends_on: [custom]
---
# custom (editor only)

The editor shows `custom` while a node is edited as raw JSON. It is not a runtime type: a pipeline
that contains `type: custom` does not build. Set `type` to the real node type the JSON describes
(`llm`, `code`, `toolkit`, `mcp`, `agent`, `router`, `decision`, `hitl`, `state_modifier`).
