---
key: constraints
depends_on: [code, llm, toolkit, mcp, router, printer, hitl]
---
# Runtime limits and silent failures

Things that build fine and then misbehave when the pipeline runs.

- **A node receives only its `input`.** A code node reading a variable it does not list gets the
  default; an fstring placeholder that is not listed still renders from state, but the node no
  longer says what it reads. With no `input` (or only `messages`) a code node gets every variable.
- **An unknown transition target ends the run quietly** after that node: no error, the rest never
  runs.
- **A node without `transition` ends the run** after it.
- **Code results are the last top-level expression.** A dict inside a branch or a function is lost.
- **Structured llm output drops entirely** when `output` lists a variable the model does not return
  (most often `messages`).
- **state_modifier writes one variable**; extra `output` entries stay empty.
- **Code-node state travels as one command-line argument**, so very large state fails with
  "Argument list too long". In a loop the failed node never advances its index, so the loop
  repeats until the step limit. Keep large content in artifacts.
- **printer and hitl wait for a person.** In a scheduled or webhook pipeline, or one called by an
  agent, they wait forever.
- **Inline `condition:` blocks on a node are not applied**; use a router.
- **Pipeline input with escaped newlines** may arrive with real newlines inside JSON strings, which
  makes `json.loads` fail with "Invalid control character". Re-escape newlines inside strings
  before parsing.
- **Model output is not exact**: models truncate long output, invent links and ids, and repeat
  table rows. Build exact strings and counts in code; de-duplicate by a key before counting.
- **Tool parameter names are exact** and follow the tool's own spelling; a wrong name is ignored.
