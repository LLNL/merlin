##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Defines the entity registry used for dynamic CLI command generation in the Merlin database interface.

This registry maps entity types (e.g., study, run, logical-worker, physical-worker) to their
corresponding CLI argument metadata and supported filtering options. It is used
by CLI subcommands (such as `get`, `delete`, and `info`) to automatically construct argument
parsers and handle entity-specific logic in a generic, extensible way.

Each entry in the registry includes:
- `filters`: A list of supported filters, where each filter specifies a name and its type
  (and optionally `nargs` for multi-valued arguments).
- `identifiers`: A human-readable description of valid identifiers for referencing entities.
- `ident_arg`: The name used for the positional identifier argument in CLI commands.
- `ident_help`: The help string template for CLI identifier arguments, parameterized with `{verb}`.

This design allows new entity types to be added to the CLI with minimal changes to the command logic.
"""


ENTITY_REGISTRY = {
    "study": {
        "filters": [
            {"name": "name", "type": str},
        ],
        "identifiers": "ID or name",
        "ident_arg": "study",
        "ident_help": "IDs or names of the studies to {verb}.",
    },
    "run": {
        "filters": [
            {"name": "study_id", "type": str},
            {"name": "run_complete", "type": bool},
            {"name": "queues", "type": str, "nargs": "+"},
            {"name": "workers", "type": str, "nargs": "+"},
        ],
        "identifiers": "ID or workspace",
        "ident_arg": "run",
        "ident_help": "IDs or workspaces of the runs to {verb}.",
    },
    "logical-worker": {
        "filters": [
            {"name": "name", "type": str},
            {"name": "queues", "type": str, "nargs": "+"},
        ],
        "identifiers": "ID",
        "ident_arg": "worker",
        "ident_help": "IDs of the logical workers to {verb}.",
    },
    "physical-worker": {
        "filters": [
            {"name": "logical_worker_id", "type": str},
            {"name": "name", "type": str},
            {"name": "status", "type": str},
            {"name": "host", "type": str},
        ],
        "identifiers": "ID or name",
        "ident_arg": "worker",
        "ident_help": "IDs or names of the physical workers to {verb}.",
    },
}
