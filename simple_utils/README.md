# Simple Utils (Calculator and Text Processor)

A minimal Python project providing:
- A simple calculator module with common operations.
- A text processing module with common utilities.
- A CLI to use both from the terminal.

## Install
No external dependencies are required. Python 3.9+ recommended.

Optionally, add the project to your `PYTHONPATH` or run it in-place.

## CLI Usage
Run with Python module syntax from the project root:

```bash
python -m simple_utils.cli --help
```

### Calculator examples
```bash
python -m simple_utils.cli calc add 3 5
python -m simple_utils.cli calc div 10 4
python -m simple_utils.cli calc sqrt 9
python -m simple_utils.cli calc fact 5
```

### Text processor examples
```bash
python -m simple_utils.cli text wcount "Hello world!"
python -m simple_utils.cli text replace "foo bar baz" foo qux
python -m simple_utils.cli text pal "A man, a plan, a canal: Panama"
```

## Run tests
From the project root:
```bash
python -m unittest -v
```