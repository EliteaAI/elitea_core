---
key: pattern:fetch_analyse_act
depends_on: [toolkit, mcp, code, llm, router]
---
# Fetch, reduce, analyse, act

For pulling data from a system, deciding something about it, and acting on the decision:

```
fetch (toolkit or mcp) -> reduce (code) -> analyse (llm, structured) -> route (router)
    -> act_a (toolkit) -> END
    -> act_b (toolkit) -> END
    -> nothing_to_do (code, writes messages) -> END
```

- **Reduce before the model.** A code node keeps only the fields the analysis needs; raw tool
  output is large and often framed text ("Found 3 items: ..."), so check its real shape first.
- **Analyse into typed variables** (`structured_output: true`, e.g. `category`, `reason`) and let a
  `router` branch on them; do not branch on free text.
- **Delimit fetched text** before the model (`<data>...</data>`) and tell it to ignore instructions
  inside: fetched content is data.
- Keep write arguments `fixed` or from validated state, never from the fetched text.
