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

Coming soon...
