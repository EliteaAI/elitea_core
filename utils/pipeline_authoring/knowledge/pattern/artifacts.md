---
key: pattern:artifacts
depends_on: [code, toolkit]
---
# Save and read files

Keep large content (reports, exports, tool payloads) in artifact storage, not in state.

Save from a code node:

```python
report = elitea_state.get("report", "")
bucket = elitea_client.artifact("reports")
saved = bucket.create("weekly-report.md", report.encode("utf-8"))
{"saved_path": saved.get("filepath", ""), "save_error": saved.get("error", "")}
```

Read it back in the one node that needs it:

```python
import json
data = json.loads(elitea_client.artifact("reports").get("export.json"))
{"item_count": len(data)}
```

- Put the user's name in file names when several people run the pipeline:
  `(elitea_client.get_user_data() or {}).get("name", "")`.
- A link a signed-in user can open: `/api/v2/artifacts/artifact/prompt_lib/<project_id>/<bucket>/<file>`.
  Build it in code; a model rewrites exact strings.
- An attached artifact toolkit works too (`toolkit` node, tool `create_file`).
