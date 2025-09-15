# Guide to Merlin Documentation

Merlin uses [MkDocs](https://www.mkdocs.org/) to generate its documentation and [Read the Docs](https://about.readthedocs.com/?ref=readthedocs.com) to host it. This README will detail important information on handling the documentation.

## How to Build the Documentation

Ensure you're at the root of the Merlin repository:

```bash
cd /path/to/merlin/
```

Install the documentation with:

```bash
pip install -r docs/requirements.txt
```

Build the documentation with:

```bash
mkdocs serve
```

Once up and running, MkDocs should provide a message telling you where your browser is connected (this is typically `http://127.0.0.1:8000/`). If you're using VSCode, you should be able to `ctrl+click` on the address to open the browser window. An example is shown below:

```bash
(venv_name) [user@machine:merlin]$ mkdocs serve
INFO    -  Building documentation...
INFO    -  Cleaning site directory
WARNING -  Excluding 'README.md' from the site because it conflicts with 'index.md'.
WARNING -  A relative path to 'api_reference/' is included in the 'nav' configuration, which is not found in the documentation files.
INFO    -  Documentation built in 3.24 seconds
INFO    -  [09:16:00] Watching paths for changes: 'docs', 'mkdocs.yml'
INFO    -  [09:16:00] Serving on http://127.0.0.1:8000/
```

## Configuring the Documentation

MkDocs relies on an `mkdocs.yml` file for almost everything to do with configuration. See [their Configuration documentation](https://www.mkdocs.org/user-guide/configuration/) for more information.

## How Do API Docs Work?

The API documentation in this project is automatically generated using a combination of MkDocs plugins and a custom Python script. This ensures that the documentation stays up-to-date with your codebase and provides a structured reference for all Python modules, classes, and functions in the merlin directory.

This section will discuss:

- [Code Reference Generation](#code-reference-generation)
    - [How the Script Works](#how-the-script-works)
- [Viewing the API Docs](#viewing-the-api-docs)
- [Keeping API Docs Up-to-Date](#keeping-api-docs-up-to-date)
- [Example Docstring](#example-docstring)
- [Plugins Involved](#plugins-involved)

### Code Reference Generation

The script `docs/gen_ref_pages.py` is responsible for generating the API reference pages. It scans the `merlin` directory for Python files and creates Markdown files for each module. These Markdown files are then included in the `api_reference` section of the documentation.

#### How the Script Works

1. File Scanning:

    The script recursively scans all Python files in the `merlin` directory using `Path.rglob("*.py")`.

2. Ignore Patterns:

    Certain files and directories are excluded from the API docs based on the `IGNORE_PATTERNS` list. For example:

    - `merlin/examples/workflows`
    - `merlin/examples/dev_workflows`
    - `merlin/data`
    - Files like `ascii_art.py`

    The `should_ignore()` function checks each file against these patterns and skips them if they match.

3. Markdown File Creation:

    For each valid Python file:

    - The script determines the module path (e.g., merlin.module_name) and the corresponding Markdown file path.
    - Special cases like `__init__.py` are handled by renaming the generated file to index.md for better navigation.
    - Files like `__main__.py` are ignored entirely.
    
    The script then writes the mkdocstrings syntax (::: module_name) into the Markdown file, which tells the mkdocstrings plugin to generate the documentation for that module.

4. Navigation File:

    The script builds a navigation structure using the `mkdocs_gen_files.Nav` class. This structure is saved into a `SUMMARY.md` file, which is used by the `literate-nav` plugin to define the navigation for the API reference section.

### Viewing the API Docs

Once the script generates the Markdown files, they are included in the documentation site under the `api_reference` section. You can explore the API docs in the navigation bar under `Reference Guide` -> `API Reference`, with the navigation organized based on the module hierarchy.

### Keeping API Docs Up-to-Date

To ensure the API documentation remains accurate:

Update the docstrings in Merlin's code whenever changes are made to functions, classes, or modules. The `docs/gen_ref_pages.py` file will run automatically when docs are created (i.e. when you run `mkdocs serve`).

### Example Docstring

The API documentation relies on properly formatted docstrings. Here’s an example using the Google-style docstring format:

```python
def add_numbers(a: int, b: int) -> int:
    """
    Adds two numbers.

    Args:
        a (int): The first number.
        b (int): The second number.

    Returns:
        The sum of the two numbers.
    """
```

### Plugins Involved

Several MkDocs plugins work together to generate and display the API documentation:

- `mkdocstrings`: Parses Python docstrings and generates the actual API content.
- `mkdocs-gen-files`: Handles the creation of Markdown files and navigation structure.
- `literate-nav`: Uses the `SUMMARY.md` file to organize the API reference section in the documentation sidebar.
