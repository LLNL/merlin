##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
This module provides functionality for managing and loading application configuration files,
default settings, and SSL-related configurations. It includes utilities for locating,
reading, and processing configuration files, as well as handling SSL certificates and protocols
for various server types.

It houses the `CONFIG` object that's used throughout Merlin's codebase.
"""
import getpass
import logging
import os
import ssl
from typing import Dict, Optional, Union

from merlin.config import Config
from merlin.config.config_filepaths import APP_FILENAME, CONFIG_PATH_FILE, MERLIN_HOME
from merlin.utils import load_yaml


LOG: logging.Logger = logging.getLogger(__name__)

CONFIG: Optional[Config] = None
IS_LOCAL_MODE: bool = False


def set_local_mode(enable: bool = True):
    """
    Sets Merlin to run in local mode, which doesn't require a configuration file.

    Args:
        enable (bool): True to enable local mode, False to disable it.
    """
    global IS_LOCAL_MODE  # pylint: disable=global-statement
    IS_LOCAL_MODE = enable
    if enable:
        LOG.info("Running Merlin in local mode (no configuration file required)")


def is_local_mode() -> bool:
    """
    Checks if Merlin is running in local mode.

    Returns:
        True if running in local mode, False otherwise.
    """
    return IS_LOCAL_MODE


def load_config(filepath: str) -> Dict:
    """
    Reads a Merlin YAML configuration file and returns its contents as a dictionary.

    Args:
        filepath (str): The path to the YAML configuration file.

    Returns:
        A dictionary containing the contents of the YAML file or None if file doesn't exist.
    """
    if not os.path.isfile(filepath):
        LOG.info(f"No app config file at {filepath}")
        return None
    LOG.info(f"Reading app config from file {filepath}")
    return load_yaml(filepath)


def find_config_file(path: str = None) -> str:
    """
    Locate the Merlin application configuration file (`app.yaml`).

    This function searches for the configuration file based on a given directory or,
    if no directory is provided, uses a fallback sequence:
      1. Check for `app.yaml` in the current working directory.
      2. Check if `CONFIG_PATH_FILE` exists and points to a valid config file.
      3. Check for `app.yaml` in the `MERLIN_HOME` directory.

    If a `path` is explicitly provided, the function checks only that directory
    for `app.yaml`.

    Args:
        path (str, optional): A specific directory to look for `app.yaml`.

    Returns:
        The full path to the `app.yaml` file if found, otherwise `None`.
    """
    # Fallback to default logic
    if path is None:
        # Check current working directory for an active config path
        local_app = os.path.join(os.getcwd(), APP_FILENAME)
        if os.path.isfile(local_app):
            return local_app

        # Check the config_path.txt file for the active config path
        if os.path.isfile(CONFIG_PATH_FILE):
            with open(CONFIG_PATH_FILE, "r") as f:
                config_path = f.read().strip()
            if os.path.isfile(config_path):
                return config_path

        # Check the Merlin home directory for an active config path
        path_app = os.path.join(MERLIN_HOME, APP_FILENAME)
        if os.path.isfile(path_app):
            return path_app

        return None

    app_path = os.path.join(path, APP_FILENAME)
    if os.path.exists(app_path):
        return app_path

    return None


def set_username_and_vhost(config: Dict):
    """
    Ensures that `broker.username` and `broker.vhost` default values are set in the
    configuration if they are not already defined.

    This function checks the `config` object for the presence of `broker.username`
    and `broker.vhost`. If either is missing, it sets their default values using
    the current system username. This prevents other parts of the code from having
    to handle missing values for these fields.

    Args:
        config (Dict): The configuration object containing the `broker` namespace.
    """
    # Ensure broker key exists
    if "broker" not in config:
        config["broker"] = {}

    try:
        config["broker"]["username"]
    except KeyError:
        username = getpass.getuser()
        config["broker"]["username"] = username
    try:
        config["broker"]["vhost"]
    except KeyError:
        vhost = getpass.getuser()
        config["broker"]["vhost"] = vhost


def get_default_config() -> Dict:
    """
    Creates a minimal default configuration for local mode.

    Returns:
        Dict: A configuration dictionary with essential default values.
    """
    default_config = {
        "broker": {
            "username": "user",
            "vhost": "vhost",
            "server": "localhost",
            "name": "rabbitmq",  # Default broker name
            "port": 5672,  # Default RabbitMQ port
            "protocol": "amqp",  # Default protocol
        },
        "celery": {"omit_queue_tag": False, "queue_tag": "[merlin]_", "override": None},
        "results_backend": {
            "name": "sqlite",  # Default results backend
            "port": 1234,
        },
    }
    return default_config


def get_config(path: Optional[str] = None) -> Dict:
    """
    Loads a Merlin configuration file and returns a dictionary containing the configuration data.

    This function locates the configuration file using the provided `path` or default search locations,
    loads the configuration data, and applies default values where necessary.

    Args:
        path (str, optional): The directory path to search for the configuration file.
            If `None`, default search paths are used.

    Returns:
        A dictionary containing all the configuration data, including broker,
            results backend, and task manager settings.

    Raises:
        ValueError: If the configuration file cannot be found and it's not a local run.
    """
    if is_local_mode():
        LOG.info("Using default configuration (local mode)")
        config = get_default_config()
        load_defaults(config)
        return config

    filepath: Optional[str] = find_config_file(path)
    if filepath is None:
        raise ValueError(
            "Cannot find a merlin config file! Run 'merlin config create' and edit the file "
            f"'{os.path.join(MERLIN_HOME, APP_FILENAME)}'"
        )
    config: Dict = load_config(filepath)
    load_defaults(config)
    return config


def load_default_celery(config: Dict):
    """
    Initializes the default Celery configuration within the provided configuration object.

    This function ensures that the `celery` section of the configuration exists and sets
    default values for specific Celery-related settings if they are not already defined.
    These defaults include `omit_queue_tag`, `queue_tag`, and `override`.

    Args:
        config (Dict): The configuration object where the Celery settings will be initialized.
    """
    try:
        config["celery"]
    except KeyError:
        config["celery"] = {}
    try:
        config["celery"]["omit_queue_tag"]
    except KeyError:
        config["celery"]["omit_queue_tag"] = False
    try:
        config["celery"]["queue_tag"]
    except KeyError:
        config["celery"]["queue_tag"] = "[merlin]_"
    try:
        config["celery"]["override"]
    except KeyError:
        config["celery"]["override"] = None


def load_defaults(config: Dict):
    """
    Loads default configuration values into the provided configuration dictionary.

    This function initializes default values for various configuration sections,
    including user-related settings and Celery-specific settings, by calling
    `set_username_and_vhost` and `load_default_celery`.

    Args:
        config (Dict): The configuration dictionary to be updated with default values.
    """
    set_username_and_vhost(config)
    load_default_celery(config)


def is_debug() -> bool:
    """
    Determines whether the application is running in debug mode.

    This function checks the environment variable `MERLIN_DEBUG`. If the variable
    exists and its value is set to `1`, debug mode is enabled.

    Returns:
        True if `MERLIN_DEBUG` is set to `1` in the environment, otherwise False.
    """
    if "MERLIN_DEBUG" in os.environ and int(os.environ["MERLIN_DEBUG"]) == 1:
        return True
    return False


def default_config_info() -> Dict:
    """
    Returns information about Merlin's default configurations.

    This function gathers and returns key details about the current configuration
    of the Merlin application, including the location of the configuration file,
    debug mode status, the Merlin home directory, and whether the Merlin home
    directory exists.

    Returns:
        A dictionary containing the following keys:\n
            - `config_file` (str): Path to the Merlin configuration file.
            - `is_debug` (bool): Whether debug mode is enabled.
            - `merlin_home` (str): Path to the Merlin home directory.
            - `merlin_home_exists` (bool): True if the Merlin home directory exists, otherwise False.
    """
    return {
        "config_file": find_config_file(),
        "is_debug": is_debug(),
        "merlin_home": MERLIN_HOME,
        "merlin_home_exists": os.path.exists(MERLIN_HOME),
    }


def get_cert_file(server_type: str, config: Config, cert_name: str, cert_path: str) -> str:
    """
    Determines the SSL certificate file for a given server configuration.

    This function checks if an SSL certificate file is specified in the server configuration.
    If the file does not exist, it attempts to locate the certificate in an optional
    certificate path. If the certificate file cannot be found, an error is logged.

    Args:
        server_type (str): The type of server (e.g., Broker, Results Backend) for logging purposes.
        config (config.Config): The server configuration object containing certificate details.
        cert_name (str): The name of the certificate attribute in the configuration.
        cert_path (str): An optional directory path to search for the certificate file.

    Returns:
        The absolute path to the certificate file if found, otherwise `None`.
    """
    cert_file = None
    try:
        cert_file = getattr(config, cert_name)
        cert_file = os.path.abspath(os.path.expanduser(cert_file))
        if not os.path.exists(cert_file) and cert_path:
            base_cert_file = os.path.basename(cert_file)
            new_cert_file = os.path.join(cert_path, base_cert_file)
            new_cert_file = os.path.abspath(os.path.expanduser(new_cert_file))
            if os.path.exists(new_cert_file):
                cert_file = new_cert_file
            else:
                LOG.error(f"{server_type}: The file for {cert_name} does not exist, searched {cert_file} and {new_cert_file}")
        LOG.debug(f"{server_type}: {cert_name} = {cert_file}")
    except (AttributeError, KeyError):
        LOG.debug(f"{server_type}: {cert_name} not present")

    return cert_file


def get_ssl_entries(
    server_type: str, server_name: str, server_config: Config, cert_path: str
) -> Dict[str, Union[str, ssl.VerifyMode]]:
    """
    Retrieves SSL configuration entries for a given server.

    This function checks for SSL certificate files and other SSL-related settings
    in the server configuration. It builds and returns a dictionary containing
    the necessary data to manage SSL certificates and protocols for the server.

    Args:
        server_type (str): The type of server (e.g., Broker, Results Backend) for logging purposes.
        server_name (str): The name of the server, used for output and mapping SSL configurations.
        server_config (config.Config): The server configuration object containing SSL settings.
        cert_path (str): An optional directory path to search for certificate files.

    Returns:
        A dictionary containing SSL configuration entries, including:\n
            - `keyfile` (str): Path to the SSL key file, if present.
            - `certfile` (str): Path to the SSL certificate file, if present.
            - `ca_certs` (str): Path to the CA certificates file, if present.
            - `cert_reqs` (ssl.VerifyMode): SSL certificate requirements (e.g., `CERT_REQUIRED`, `CERT_OPTIONAL`, `CERT_NONE`).
            - `ssl_protocol` (str): SSL protocol used, if specified.
    """
    server_ssl: Dict[str, Union[str, ssl.VerifyMode]] = {}

    keyfile: Optional[str] = get_cert_file(server_type, server_config, "keyfile", cert_path)
    if keyfile:
        server_ssl["keyfile"] = keyfile

    certfile: Optional[str] = get_cert_file(server_type, server_config, "certfile", cert_path)
    if certfile:
        server_ssl["certfile"] = certfile

    ca_certsfile: Optional[str] = get_cert_file(server_type, server_config, "ca_certs", cert_path)
    if ca_certsfile:
        server_ssl["ca_certs"] = ca_certsfile

    try:
        if server_config.cert_reqs == "required":
            server_ssl["cert_reqs"] = ssl.CERT_REQUIRED
        elif server_config.cert_reqs == "optional":
            server_ssl["cert_reqs"] = ssl.CERT_OPTIONAL
        elif server_config.cert_reqs == "none":
            server_ssl["cert_reqs"] = ssl.CERT_NONE
        LOG.debug(f"{server_type}: cert_reqs = {server_ssl['cert_reqs']}")
    except (AttributeError, KeyError):
        LOG.debug(f"{server_type}: ssl cert_reqs not present")

    try:
        server_ssl["ssl_protocol"] = server_config.ssl_protocol
        LOG.debug(f"{server_type}: ssl_protocol = {server_ssl['ssl_protocol']}")
    except (AttributeError, KeyError):
        LOG.debug(f"{server_type}: ssl ssl_protocol not present")

    if server_ssl and "cert_reqs" not in server_ssl:
        server_ssl["cert_reqs"] = ssl.CERT_REQUIRED

    ssl_map: Dict[str, str] = process_ssl_map(server_name)

    if server_ssl and ssl_map:
        server_ssl = merge_sslmap(server_ssl, ssl_map)

    return server_ssl


def process_ssl_map(server_name: str) -> Dict[str, str]:
    """
    Processes and returns a mapping of SSL-related configuration keys
    specific to certain server types (e.g., Redis and MySQL).

    This function generates a dictionary mapping standard SSL configuration keys
    (e.g., `keyfile`, `certfile`, `ca_certs`, `cert_reqs`) to server-specific key names
    required by Redis (`rediss`) or MySQL server configurations.

    Args:
        server_name (str): The name of the server (e.g., "rediss", "mysql") used to determine
            the appropriate SSL key mappings.

    Returns:
        A dictionary containing the SSL key mappings for the given server type. Returns an empty
            dictionary if the server type is not `rediss` or `mysql`.
    """
    ssl_map: Dict[str, str] = {}
    # The redis server requires key names with ssl_
    if server_name == "rediss":
        ssl_map = {
            "keyfile": "ssl_keyfile",
            "certfile": "ssl_certfile",
            "ca_certs": "ssl_ca_certs",
            "cert_reqs": "ssl_cert_reqs",
        }

    # The mysql server requires key names with ssl_ and different var names
    if "mysql" in server_name:
        ssl_map = {"keyfile": "ssl_key", "certfile": "ssl_cert", "ca_certs": "ssl_ca"}

    return ssl_map


def merge_sslmap(server_ssl: Dict[str, Union[str, ssl.VerifyMode]], ssl_map: Dict[str, str]) -> Dict:
    """
    Updates the keys of the `server_ssl` dictionary based on the `ssl_map` for specific server types.

    This function modifies the `server_ssl` dictionary by replacing its keys with the corresponding
    keys from the `ssl_map` when the server type requires specialized key names (e.g., `rediss` or `mysql`).
    If a key in `server_ssl` is not found in `ssl_map`, it remains unchanged.

    Args:
        server_ssl (Dict[str, Union[str, ssl.VerifyMode]]): The dictionary constructed in `get_ssl_entries`,
            containing SSL configuration entries such as `keyfile`, `certfile`, `ca_certs`, and `cert_reqs`.
        ssl_map (Dict[str, str]): A dictionary mapping standard SSL keys to server-specific keys.

    Returns:
        A new dictionary with updated keys based on the `ssl_map`. Keys not present in `ssl_map`
            remain unchanged.
    """
    new_server_ssl: Dict[str, Union[str, ssl.VerifyMode]] = {}

    for key in server_ssl:
        if key in ssl_map:
            new_server_ssl[ssl_map[key]] = server_ssl[key]
        else:
            new_server_ssl[key] = server_ssl[key]

    return new_server_ssl


def initialize_config(path: Optional[str] = None, local_mode: bool = False) -> Config:
    """
    Initializes and returns the Merlin configuration.

    This function can be used to explicitly initialize the configuration when needed,
    rather than relying on the module-level CONFIG constant.

    Args:
        path (Optional[str]): Path to look for configuration file
        local_mode (bool): Whether to use local mode (no config file required)

    Returns:
        The initialized configuration object
    """
    if local_mode:
        set_local_mode(True)

    global CONFIG  # pylint: disable=global-statement

    try:
        app_config = get_config(path)
        CONFIG = Config(app_config)
    except ValueError as e:
        LOG.warning(f"Error loading configuration: {e}. Falling back to default configuration.")
        # Fallback to default config
        CONFIG = Config(get_default_config())


initialize_config()
