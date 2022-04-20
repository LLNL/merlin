import logging
import os

import yaml


LOG = logging.getLogger("merlin")

MERLIN_CONFIG_DIR = os.path.expanduser("~") + "/.merlin/"
MERLIN_SERVER_SUBDIR = "server/"
MERLIN_SERVER_CONFIG = "merlin_server.yaml"


def parse_redis_output(redis_stdout):
    if redis_stdout is None:
        return False, "None passed as redis output"
    server_init = False
    redis_config = {}
    for line in redis_stdout:
        if not server_init:
            values = [ln for ln in line.split() if b"=" in ln]
            for val in values:
                key, value = val.split(b"=")
                redis_config[key.decode("utf-8")] = value.strip(b",").decode("utf-8")
            if b"Server initialized" in line:
                server_init = True
        if b"Ready to accept connections" in line:
            return True, redis_config
        if b"aborting" in line:
            return False, line.decode("utf-8")


def pull_server_config() -> dict:
    """
    Pull the main configuration file and corresponding format configuration file
    as well. Returns the values as a dictionary.

    :return: A dictionary containing the main and corresponding format configuration file
    """
    return_data = {}
    format_needed_keys = ["command", "run_command", "stop_command", "pull_command"]
    process_needed_keys = ["status", "kill"]

    config_dir = os.path.join(MERLIN_CONFIG_DIR, MERLIN_SERVER_SUBDIR)
    config_path = os.path.join(config_dir, MERLIN_SERVER_CONFIG)
    if not os.path.exists(config_path):
        LOG.error(f'Unable to pull merlin server configuration from {config_path}')
        return None

    with open(config_path, "r") as cf:
        server_config = yaml.load(cf, yaml.Loader)
        return_data.update(server_config)

    if "container" in server_config:
        if "format" in server_config["container"]:
            format_file = os.path.join(config_dir, server_config["container"]["format"] + ".yaml")
            with open(format_file, "r") as ff:
                format_data = yaml.load(ff, yaml.Loader)
                for key in format_needed_keys:
                    if key not in format_data:
                        LOG.error(f'Unable to find necessary {key} in format config file')
                        return None
                return_data.update(format_data)
        else:
            LOG.error(f'Unable to find "format" in {MERLIN_SERVER_CONFIG}')
            return None
    else:
        LOG.error(f'Unable to find "container" object in {MERLIN_SERVER_CONFIG}')
        return None

    # Checking for process values that are needed for main functions and defaults
    if "process" not in server_config:
        LOG.error("Process config not found in " + MERLIN_SERVER_CONFIG)
        return None

    for key in process_needed_keys:
        if key not in server_config["process"]:
            LOG.error(f'Process necessary "{key}" command configuration not found in {MERLIN_SERVER_CONFIG}')
            return None

    return return_data


def pull_container_config(container_path):
    pass
