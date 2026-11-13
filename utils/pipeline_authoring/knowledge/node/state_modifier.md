---
key: node:state_modifier
depends_on: [state_modifier]
---
# state_modifier

Use to build one value from state with a Jinja2 template: format a report, join a list, parse JSON,
reset scratch variables. Use `code` for real logic or several outputs, `llm` for generated text.
It cannot branch.

## How it behaves

- `template` sees the variables in `input`. Standard Jinja2 works (filters, loops, slicing such as
  `{{ s[:10] }}`), plus `from_json`, `base64_to_string`, `split_by_words(n)` and
  `split_by_regex('p')`.
- The result goes into the first `output` variable only, converted to that variable's declared
  type (`dict`/`list` via JSON, `int`, `float`, `bool`).
- `variables_to_clean` resets listed variables to their empty value after rendering.

## Example

```yaml
- id: build_report
  type: state_modifier
  input: [title, priority, labels]
  output: [report]
  template: |
    Title: {{ title }}
    Priority: {{ priority }}
    Labels: {{ labels | join(", ") }}
  transition: send_report
```

## Mistakes

- More than one `output`: only the first is written, the rest stay empty.
- An output variable declared with the wrong type: the conversion does not happen.
- A `{{ var }}` not listed in `input` renders as nothing.
