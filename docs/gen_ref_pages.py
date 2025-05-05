"""Generate the code reference pages."""

from pathlib import Path

import mkdocs_gen_files

nav = mkdocs_gen_files.Nav()

IGNORE_PATTERNS = [
    Path("merlin/examples/workflows"),
    Path("merlin/examples/dev_workflows"),
    Path("merlin/data"),
    "*/ascii_art.py",
]


def should_ignore(path):
    """Check if the given path matches any ignore patterns."""
    for pattern in IGNORE_PATTERNS:
        pattern = str(pattern)
        if path.is_relative_to(Path(pattern)):
            return True
        if path.match(pattern):
            return True
    return False


for path in sorted(Path("merlin").rglob("*.py")):
    if should_ignore(path):
        continue
    module_path = path.relative_to("merlin").with_suffix("")
    doc_path = path.relative_to("merlin").with_suffix(".md")
    full_doc_path = Path("api_reference", doc_path)

    parts = list(module_path.parts)

    if parts[-1] == "__init__":  #
        parts = parts[:-1]
        doc_path = doc_path.with_name("index.md")
        full_doc_path = full_doc_path.with_name("index.md")
        if len(parts) == 0:
            continue
    elif parts[-1] == "__main__":
        continue

    nav[parts] = doc_path.as_posix()

    with mkdocs_gen_files.open(full_doc_path, "w") as fd:
        identifier = ".".join(parts)
        print("::: " + identifier, file=fd)

    mkdocs_gen_files.set_edit_path(full_doc_path, path)


# NOTE: SUMMARY.md has to be the name of the nav file
with mkdocs_gen_files.open("api_reference/SUMMARY.md", "w") as nav_file:
    nav_file.writelines(nav.build_literate_nav())
