---
key: pattern:branch_and_merge
depends_on: [router, agent, code]
---
# Run optional steps, then merge

Pipelines run one node at a time; there is no parallel branch. To run several optional steps,
chain routers that each either run their step or skip to the next check, then merge the results.

```yaml
- id: check_backend
  type: router
  input: [has_backend]
  condition: "{% if has_backend %}backend_tests{% else %}check_frontend{% endif %}"
  routes: [backend_tests, check_frontend]
  default_output: check_frontend
- id: backend_tests
  type: agent
  tool: Backend Test Writer
  input: [context]
  output: [backend_result]
  input_mapping:
    task: {type: fstring, value: "Write backend test cases for:\n{context}"}
  transition: check_frontend
- id: check_frontend
  type: router
  input: [has_frontend]
  condition: "{% if has_frontend %}frontend_tests{% else %}merge{% endif %}"
  routes: [frontend_tests, merge]
  default_output: merge
- id: frontend_tests
  type: agent
  tool: Frontend Test Writer
  input: [context]
  output: [frontend_result]
  input_mapping:
    task: {type: fstring, value: "Write frontend test cases for:\n{context}"}
  transition: merge
- id: merge
  type: state_modifier
  input: [backend_result, frontend_result]
  output: [test_plan]
  template: |
    {% if backend_result %}## Backend
    {{ backend_result }}
    {% endif %}{% if frontend_result %}## Frontend
    {{ frontend_result }}
    {% endif %}
  transition: END
```

Give `backend_result` and `frontend_result` a default (`""`) in `state`, so a skipped step leaves
an empty value rather than a missing one.
