##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""Utils relating to merlin server"""

import hashlib
import logging
import os
from typing import Dict, List

import redis
import yaml

import merlin.utils


LOG = logging.getLogger("merlin")

# Constants for main merlin server configuration values.
CONTAINER_TYPES = ["singularity", "docker", "podman"]
CONFIG_DIR = os.path.abspath("./merlin_server/")
MERLIN_CONFIG_DIR = os.path.join(os.path.expanduser("~"), ".merlin")
MERLIN_SERVER_SUBDIR = "server/"
MERLIN_SERVER_CONFIG = "merlin_server.yaml"


def valid_ipv4(ip: str) -> bool:  # pylint: disable=C0103
    """
    Validates whether a given string is a valid IPv4 address.

    An IPv4 address consists of four octets separated by dots, where each octet
    is a number between 0 and 255 (inclusive). This function checks if the input
    string meets these criteria.

    Args:
        ip: The string to validate as an IPv4 address.

    Returns:
        True if the input string is a valid IPv4 address, False otherwise.
    """
    if not ip:
        return False

    arr = ip.split(".")
    if len(arr) != 4:
        return False

    for i in arr:
        if int(i) < 0 or int(i) > 255:
            return False

    return True


def valid_port(port: int) -> bool:
    """
    Validates whether a given integer is a valid network port number.

    A valid network port number is an integer in the range 1 to 65535 (inclusive).
    This function checks if the provided port falls within this range.

    Args:
        port: The port number to validate.

    Returns:
        True if the port is valid, False otherwise.
    """
    if 0 < port < 65536:
        return True
    return False


# Pylint complains about too many instance variables but it's necessary here so ignore
class ContainerConfig:  # pylint: disable=R0902
    """
    A class for parsing and interacting with container configuration values.

    The `ContainerConfig` class provides an interface for handling container-related
    configuration values specified in the `merlin_server.yaml` file. It initializes
    with a dictionary of configuration values, allowing for default values to be
    populated for a Singularity container if any values are missing.

    The configuration contains values for setting up containers and storing alues specific
    to each container. These values are used to manage container setup and store configuration
    details specific to each container run.

    Attributes:
        FORMAT (str): Default container format (e.g., "singularity").
        IMAGE_TYPE (str): Default image type (e.g., "redis").
        IMAGE_NAME (str): Default image name (e.g., "redis_latest.sif").
        REDIS_URL (str): Default URL for the container image (e.g., "docker://redis").
        CONFIG_FILE (str): Default name of the configuration file (e.g., "redis.conf").
        CONFIG_DIR (str): Default path to the configuration directory.
        PROCESS_FILE (str): Default name of the process file (e.g., "merlin_server.pf").
        PASSWORD_FILE (str): Default name of the password file (e.g., "redis.pass").
        USERS_FILE (str): Default name of the users file (e.g., "redis.users").

        format (str): Container format, initialized with the provided data or default.
        image_type (str): Image type, initialized with the provided data or default.
        image (str): Image name, initialized with the provided data or default.
        url (str): Image URL, initialized with the provided data or default.
        config (str): Configuration file name, initialized with the provided data or default.
        config_dir (str): Configuration directory path, initialized with the provided data or default.
        pfile (str): Process file name, initialized with the provided data or default.
        pass_file (str): Password file name, initialized with the provided data or default.
        user_file (str): Users file name, initialized with the provided data or default.

    Methods:
        __eq__: Determines equality between two `ContainerConfig` instances.
        get_format: Returns the container format.
        get_image_type: Returns the image type.
        get_image_name: Returns the image name.
        get_image_url: Returns the image URL.
        get_image_path: Returns the full path to the image file.
        get_config_name: Returns the name of the configuration file.
        get_config_path: Returns the full path to the configuration file.
        get_config_dir: Returns the configuration directory path.
        get_pfile_name: Returns the name of the process file.
        get_pfile_path: Returns the full path to the process file.
        get_pass_file_name: Returns the name of the password file.
        get_pass_file_path: Returns the full path to the password file.
        get_user_file_name: Returns the name of the users file.
        get_user_file_path: Returns the full path to the users file.
        get_container_password: Reads and returns the container password from the password file.
    """

    # Default values for configuration
    FORMAT: str = "singularity"
    IMAGE_TYPE: str = "redis"
    IMAGE_NAME: str = "redis_latest.sif"
    REDIS_URL: str = "docker://redis"
    CONFIG_FILE: str = "redis.conf"
    PROCESS_FILE: str = "merlin_server.pf"
    PASSWORD_FILE: str = "redis.pass"
    USERS_FILE: str = "redis.users"

    format: str = FORMAT
    image_type: str = IMAGE_TYPE
    image: str = IMAGE_NAME
    url: str = REDIS_URL
    config: str = CONFIG_FILE
    config_dir: str = CONFIG_DIR
    pfile: str = PROCESS_FILE
    pass_file: str = PASSWORD_FILE
    user_file: str = USERS_FILE

    def __init__(self, data: Dict):
        """
        Initializes a `ContainerConfig` instance with configuration values.

        Take in a dictionary of configuration values and set up the attributes of the
        `ContainerConfig` instance. If any values are missing from the provided dictionary,
        default values for a Singularity container are used.

        Args:
            data: A dictionary containing configuration values. Keys can include:\n
                - `format` (str): Container format (e.g., "singularity").
                - `image_type` (str): Image type (e.g., "redis").
                - `image` (str): Image name (e.g., "redis_latest.sif").
                - `url` (str): URL for the container image (e.g., "docker://redis").
                - `config` (str): Name of the configuration file (e.g., "redis.conf").
                - `config_dir` (str): Path to the configuration directory.
                - `pfile` (str): Name of the process file (e.g., "merlin_server.pf").
                - `pass_file` (str): Name of the password file (e.g., "redis.pass").
                - `user_file` (str): Name of the users file (e.g., "redis.users").
        """
        self.format: str = data["format"] if "format" in data else self.FORMAT
        self.image_type: str = data["image_type"] if "image_type" in data else self.IMAGE_TYPE
        self.image: str = data["image"] if "image" in data else self.IMAGE_NAME
        self.url: str = data["url"] if "url" in data else self.REDIS_URL
        self.config: str = data["config"] if "config" in data else self.CONFIG_FILE
        self.config_dir: str = os.path.abspath(data["config_dir"]) if "config_dir" in data else CONFIG_DIR
        self.pfile: str = data["pfile"] if "pfile" in data else self.PROCESS_FILE
        self.pass_file: str = data["pass_file"] if "pass_file" in data else self.PASSWORD_FILE
        self.user_file: str = data["user_file"] if "user_file" in data else self.USERS_FILE

    def __eq__(self, other: "ContainerConfig") -> bool:
        """
        Checks equality between two `ContainerConfig` instances.

        This magic method overrides the equality operator (`==`) to compare two
        `ContainerConfig` objects. It checks if all relevant attributes
        of the two objects are equal.

        Args:
            other: Another instance of `ContainerConfig` to compare against.

        Returns:
            True if all attributes are equal between the two objects. False otherwise.

        Example:
            ```python
            >>> config1_data = {
            ...     "format": "singularity",
            ...     "image": "redis_latest.sif",
            ...     "config_dir": "/configs",
            ... }
            >>> config2_data = {
            ...     "format": "singularity",
            ...     "image": "redis_latest.sif",
            ...     "config_dir": "/configs",
            ... }
            >>> config3_data = {
            ...     "format": "singularity",
            ...     "image": "redis_latest.sif",
            ...     "config_dir": "/other_configs",
            ... }
            >>> config1 = ContainerConfig(config1_data)
            >>> config2 = ContainerConfig(config2_data)
            >>> config3 = ContainerConfig(config3_data)
            >>> config1 == config2
            True
            >>> config1 == config3
            False
            ```
        """
        variables = ("format", "image_type", "image", "url", "config", "config_dir", "pfile", "pass_file", "user_file")
        return all(getattr(self, attr) == getattr(other, attr) for attr in variables)

    def __str__(self) -> str:
        """
        Returns a human-readable string representation of the ContainerConfig.

        This method provides a formatted, user-friendly view of the container
        configuration, showing the key attributes in a readable format.

        Returns:
            A human-readable string representation of the configuration.
        """
        return (
            f"ContainerConfig:\n"
            f"  Format: {self.format}\n"
            f"  Image Type: {self.image_type}\n"
            f"  Image: {self.image}\n"
            f"  URL: {self.url}\n"
            f"  Config Dir: {self.config_dir}\n"
            f"  Config File: {self.config}\n"
            f"  Process File: {self.pfile}\n"
            f"  Password File: {self.pass_file}\n"
            f"  Users File: {self.user_file}"
        )

    def __repr__(self) -> str:
        """
        Returns an unambiguous string representation of the ContainerConfig.

        This method provides a string that could be used to recreate the object,
        showing the constructor call with the current configuration values.

        Returns:
            An unambiguous string representation that shows how to recreate the object.
        """
        config_dict = {
            "format": self.format,
            "image_type": self.image_type,
            "image": self.image,
            "url": self.url,
            "config": self.config,
            "config_dir": self.config_dir,
            "pfile": self.pfile,
            "pass_file": self.pass_file,
            "user_file": self.user_file,
        }
        return f"ContainerConfig({config_dict!r})"

    def get_format(self) -> str:
        """
        Retrieves the container format.

        This method returns the format of the container, which specifies the type
        of container being used (e.g., "singularity", "docker").

        Returns:
            The container format.

        Example:
            ```python
            >>> config_data = {
            ...     "format": "singularity",
            ...     "image_type": "redis",
            ...     "image": "redis_latest.sif",
            ...     "url": "docker://redis",
            ...     "config": "redis.conf",
            ...     "config_dir": "/configs",
            ...     "pfile": "merlin_server.pf",
            ...     "pass_file": "redis.pass",
            ...     "user_file": "redis.users",
            ... }
            >>> container_config = ContainerConfig(config_data)
            >>> container_config.get_format()
            'singularity'
            ```
        """
        return self.format

    def get_image_type(self) -> str:
        """
        Retrieves the image type.

        This method returns the type of the container image, which typically
        describes the application or service associated with the image
        (e.g., "redis", "mysql").

        Returns:
            The image type.

        Example:
            ```python
            >>> config_data = {
            ...     "format": "singularity",
            ...     "image_type": "redis",
            ...     "image": "redis_latest.sif",
            ...     "url": "docker://redis",
            ...     "config": "redis.conf",
            ...     "config_dir": "/configs",
            ...     "pfile": "merlin_server.pf",
            ...     "pass_file": "redis.pass",
            ...     "user_file": "redis.users",
            ... }
            >>> container_config = ContainerConfig(config_data)
            >>> container_config.get_image_type()
            'redis'
            ```
        """
        return self.image_type

    def get_image_name(self) -> str:
        """
        Retrieves the image name.

        This method returns the name of the container image, which may include
        the version or tag (e.g., "redis_latest.sif", "mysql:8.0").

        Returns:
            The image name.

        Example:
            ```python
            >>> config_data = {
            ...     "format": "singularity",
            ...     "image_type": "redis",
            ...     "image": "redis_latest.sif",
            ...     "url": "docker://redis",
            ...     "config": "redis.conf",
            ...     "config_dir": "/configs",
            ...     "pfile": "merlin_server.pf",
            ...     "pass_file": "redis.pass",
            ...     "user_file": "redis.users",
            ... }
            >>> container_config = ContainerConfig(config_data)
            >>> container_config.get_image_name()
            'redis_latest.sif'
            ```
        """
        return self.image

    def get_image_url(self) -> str:
        """
        Retrieves the URL of the image.

        This method returns the URL where the container image is hosted or can
        be downloaded from (e.g., a public or private registry URL).

        Returns:
            The URL of the container image.

        Example:
            ```python
            >>> config_data = {
            ...     "format": "singularity",
            ...     "image_type": "redis",
            ...     "image": "redis_latest.sif",
            ...     "url": "docker://redis",
            ...     "config": "redis.conf",
            ...     "config_dir": "/configs",
            ...     "pfile": "merlin_server.pf",
            ...     "pass_file": "redis.pass",
            ...     "user_file": "redis.users",
            ... }
            >>> container_config = ContainerConfig(config_data)
            >>> container_config.get_image_url()
            'docker://redis'
            ```
        """
        return self.url

    def get_image_path(self) -> str:
        """
        Retrieves the full path to the image file.

        This method constructs and returns the absolute path to the container
        image by combining the configuration directory and the image name.

        Returns:
            The full path to the container image file.

        Example:
            ```python
            >>> config_data = {
            ...     "format": "singularity",
            ...     "image_type": "redis",
            ...     "image": "redis_latest.sif",
            ...     "url": "docker://redis",
            ...     "config": "redis.conf",
            ...     "config_dir": "/configs",
            ...     "pfile": "merlin_server.pf",
            ...     "pass_file": "redis.pass",
            ...     "user_file": "redis.users",
            ... }
            >>> container_config = ContainerConfig(config_data)
            >>> container_config.get_image_path()
            '/configs/redis_latest.sif'
            ```
        """
        return os.path.join(self.config_dir, self.image)

    def get_config_name(self) -> str:
        """
        Retrieves the name of the configuration file.

        This method returns the name of the configuration file associated with
        the container or application (e.g., "redis.conf", "my.cnf").

        Returns:
            The name of the configuration file.

        Example:
            ```python
            >>> config_data = {
            ...     "format": "singularity",
            ...     "image_type": "redis",
            ...     "image": "redis_latest.sif",
            ...     "url": "docker://redis",
            ...     "config": "redis.conf",
            ...     "config_dir": "/configs",
            ...     "pfile": "merlin_server.pf",
            ...     "pass_file": "redis.pass",
            ...     "user_file": "redis.users",
            ... }
            >>> container_config = ContainerConfig(config_data)
            >>> container_config.get_config_name()
            'redis.conf'
            ```
        """
        return self.config

    def get_config_path(self) -> str:
        """
        Retrieves the full path to the configuration file.

        This method constructs and returns the absolute path to the configuration
        file by combining the configuration directory and the configuration file name.

        Returns:
            The full path to the configuration file.

        Example:
            ```python
            >>> config_data = {
            ...     "format": "singularity",
            ...     "image_type": "redis",
            ...     "image": "redis_latest.sif",
            ...     "url": "docker://redis",
            ...     "config": "redis.conf",
            ...     "config_dir": "/configs",
            ...     "pfile": "merlin_server.pf",
            ...     "pass_file": "redis.pass",
            ...     "user_file": "redis.users",
            ... }
            >>> container_config = ContainerConfig(config_data)
            >>> container_config.get_config_path()
            '/configs/redis.conf'
            ```
        """
        return os.path.join(self.config_dir, self.config)

    def get_config_dir(self) -> str:
        """
        Retrieves the path to the configuration directory.

        This method returns the directory where configuration files are stored,
        which can be used as a base path for accessing specific configuration files.

        Returns:
            The path to the configuration directory.

        Example:
            ```python
            >>> config_data = {
            ...     "format": "singularity",
            ...     "image_type": "redis",
            ...     "image": "redis_latest.sif",
            ...     "url": "docker://redis",
            ...     "config": "redis.conf",
            ...     "config_dir": "/configs",
            ...     "pfile": "merlin_server.pf",
            ...     "pass_file": "redis.pass",
            ...     "user_file": "redis.users",
            ... }
            >>> container_config = ContainerConfig(config_data)
            >>> container_config.get_config_dir()
            '/configs'
            ```
        """
        return self.config_dir

    def get_pfile_name(self) -> str:
        """
        Retrieves the name of the process file.

        This method returns the name of the process file, which may represent
        a file used to store process-related information (e.g., PID files or
        other runtime data files).

        Returns:
            The name of the process file.

        Example:
            ```python
            >>> config_data = {
            ...     "format": "singularity",
            ...     "image_type": "redis",
            ...     "image": "redis_latest.sif",
            ...     "url": "docker://redis",
            ...     "config": "redis.conf",
            ...     "config_dir": "/configs",
            ...     "pfile": "merlin_server.pf",
            ...     "pass_file": "redis.pass",
            ...     "user_file": "redis.users",
            ... }
            >>> container_config = ContainerConfig(config_data)
            >>> container_config.get_pfile_name()
            'merlin_server.pf'
            ```
        """
        return self.pfile

    def get_pfile_path(self) -> str:
        """
        Retrieves the full path to the process file.

        This method constructs and returns the absolute path to the process file
        by combining the configuration directory and the process file name.

        Returns:
            The full path to the process file.

        Example:
            ```python
            >>> config_data = {
            ...     "format": "singularity",
            ...     "image_type": "redis",
            ...     "image": "redis_latest.sif",
            ...     "url": "docker://redis",
            ...     "config": "redis.conf",
            ...     "config_dir": "/configs",
            ...     "pfile": "merlin_server.pf",
            ...     "pass_file": "redis.pass",
            ...     "user_file": "redis.users",
            ... }
            >>> container_config = ContainerConfig(config_data)
            >>> container_config.get_pfile_path()
            '/configs/merlin_server.pf'
            ```
        """
        return os.path.join(self.config_dir, self.pfile)

    def get_pass_file_name(self) -> str:
        """
        Retrieves the name of the password file.

        This method returns the name of the password file, which is typically used
        to store sensitive information such as user credentials or authentication keys.

        Returns:
            The name of the password file.

        Example:
            ```python
            >>> config_data = {
            ...     "format": "singularity",
            ...     "image_type": "redis",
            ...     "image": "redis_latest.sif",
            ...     "url": "docker://redis",
            ...     "config": "redis.conf",
            ...     "config_dir": "/configs",
            ...     "pfile": "merlin_server.pf",
            ...     "pass_file": "redis.pass",
            ...     "user_file": "redis.users",
            ... }
            >>> container_config = ContainerConfig(config_data)
            >>> container_config.get_pass_file_name()
            'redis.pass'
            ```
        """
        return self.pass_file

    def get_pass_file_path(self) -> str:
        """
        Retrieves the full path to the password file.

        This method constructs and returns the absolute path to the password file
        by combining the configuration directory and the password file name.

        Returns:
            The full path to the password file.

        Example:
            ```python
            >>> config_data = {
            ...     "format": "singularity",
            ...     "image_type": "redis",
            ...     "image": "redis_latest.sif",
            ...     "url": "docker://redis",
            ...     "config": "redis.conf",
            ...     "config_dir": "/configs",
            ...     "pfile": "merlin_server.pf",
            ...     "pass_file": "redis.pass",
            ...     "user_file": "redis.users",
            ... }
            >>> container_config = ContainerConfig(config_data)
            >>> container_config.get_pass_file_path()
            '/configs/redis.pass'
            ```
        """
        return os.path.join(self.config_dir, self.pass_file)

    def get_user_file_name(self) -> str:
        """
        Retrieves the name of the user file.

        This method returns the name of the user file, which may be used to store
        information related to users, such as user configurations or metadata.

        Returns:
            The name of the user file.

        Example:
            ```python
            >>> config_data = {
            ...     "format": "singularity",
            ...     "image_type": "redis",
            ...     "image": "redis_latest.sif",
            ...     "url": "docker://redis",
            ...     "config": "redis.conf",
            ...     "config_dir": "/configs",
            ...     "pfile": "merlin_server.pf",
            ...     "pass_file": "redis.pass",
            ...     "user_file": "redis.users",
            ... }
            >>> container_config = ContainerConfig(config_data)
            >>> container_config.get_user_file_name()
            'redis.users'
            ```
        """
        return self.user_file

    def get_user_file_path(self) -> str:
        """
        Retrieves the full path to the user file.

        This method constructs and returns the absolute path to the user file
        by combining the configuration directory and the user file name.

        Returns:
            The full path to the user file.

        Example:
            ```python
            >>> config_data = {
            ...     "format": "singularity",
            ...     "image_type": "redis",
            ...     "image": "redis_latest.sif",
            ...     "url": "docker://redis",
            ...     "config": "redis.conf",
            ...     "config_dir": "/configs",
            ...     "pfile": "merlin_server.pf",
            ...     "pass_file": "redis.pass",
            ...     "user_file": "redis.users",
            ... }
            >>> container_config = ContainerConfig(config_data)
            >>> container_config.get_user_file_path()
            '/configs/redis.users'
            ```
        """
        return os.path.join(self.config_dir, self.user_file)

    def get_container_password(self) -> str:
        """
        Retrieves the password for the container.

        This method reads the container password from the password file, which is
        located at the path returned by
        [`get_pass_file_path`][server.server_util.ContainerConfig.get_pass_file_path].
        The password is read as plain text from the file.

        Returns:
            The container password.

        Example:
            ```python
            >>> with open("redis.pass", "w") as passfile:
            ...     passfile.write("redis_password")
            >>> config_data = {
            ...     "format": "singularity",
            ...     "image_type": "redis",
            ...     "image": "redis_latest.sif",
            ...     "url": "docker://redis",
            ...     "config": "redis.conf",
            ...     "config_dir": "/configs",
            ...     "pfile": "merlin_server.pf",
            ...     "pass_file": "redis.pass",
            ...     "user_file": "redis.users",
            ... }
            >>> container_config = ContainerConfig(config_data)
            >>> container_config.get_container_password()
            'redis_password'
            ```
        """
        password = None
        with open(self.get_pass_file_path(), "r") as f:  # pylint: disable=C0103
            password = f.read()
        return password


class ContainerFormatConfig:
    """
    `ContainerFormatConfig` provides an interface for parsing and interacting with container-specific
    configuration files, such as <container_name>.yaml. These configuration files define
    container-specific commands for containerizers like Singularity, Docker, and Podman.

    This class allows you to customize and retrieve commands for running, stopping, and pulling
    container images.

    Attributes:
        COMMAND (str): Default command for running the container (default is "singularity").
        RUN_COMMAND (str): Default template for the run command.
        STOP_COMMAND (str): Default command for stopping the container (default is "kill").
        PULL_COMMAND (str): Default template for the pull command.

        command (str): The container command, initialized from the configuration data or defaulting to `COMMAND`.
        run_command (str): The run command, initialized from the configuration data or defaulting to `RUN_COMMAND`.
        stop_command (str): The stop command, initialized from the configuration data or defaulting to `STOP_COMMAND`.
        pull_command (str): The pull command, initialized from the configuration data or defaulting to `PULL_COMMAND`.

    Methods:
        __eq__: Compares two `ContainerFormatConfig` objects for equality.
        get_command: Retrieves the container command.
        get_run_command: Retrieves the run command.
        get_stop_command: Retrieves the stop command.
        get_pull_command: Retrieves the pull command.
    """

    COMMAND: str = "singularity"
    RUN_COMMAND: str = "{command} run {image} {config}"
    STOP_COMMAND: str = "kill"
    PULL_COMMAND: str = "{command} pull {image} {url}"

    command: str = COMMAND
    run_command: str = RUN_COMMAND
    stop_command: str = STOP_COMMAND
    pull_command: str = PULL_COMMAND

    def __init__(self, data: Dict):
        """
        Initializes a `ContainerFormatConfig` object with container-specific configuration data.

        This constructor takes a dictionary of configuration data and initializes the container
        command attributes. If any of the keys are missing in the provided data, default values
        are used instead.

        Args:
            data: A dictionary containing configuration data. Expected keys are:\n
                - `command` (str): The container command (e.g., "singularity", "docker").
                - `run_command` (str): The template for the run command.
                - `stop_command` (str): The command to stop the container.
                - `pull_command` (str): The template for the pull command.
        """
        self.command: str = data["command"] if "command" in data else self.COMMAND
        self.run_command: str = data["run_command"] if "run_command" in data else self.RUN_COMMAND
        self.stop_command: str = data["stop_command"] if "stop_command" in data else self.STOP_COMMAND
        self.pull_command: str = data["pull_command"] if "pull_command" in data else self.PULL_COMMAND

    def __eq__(self, other: "ContainerFormatConfig") -> bool:
        """
        Checks equality between two `ContainerFormatConfig` objects.

        This method compares the attributes of the current object with those of another
        `ContainerFormatConfig` object to determine if they are equal.

        Args:
            other: Another instance of the `ContainerFormatConfig` class to compare against.

        Returns:
            True if all corresponding attributes are equal between the two objects, otherwise False.

        Example:
            ```python
            >>> config1 = ContainerFormatConfig({"command": "singularity"})
            >>> config2 = ContainerFormatConfig({"command": "singularity"})
            >>> config1 == config2
            True
            ```
        """
        variables = ("command", "run_command", "stop_command", "pull_command")
        return all(getattr(self, attr) == getattr(other, attr) for attr in variables)

    def __str__(self) -> str:
        """
        Returns a human-readable string representation of the ContainerFormatConfig.

        This method provides a formatted, user-friendly view of the container format
        configuration, showing the key command attributes in a readable format.

        Returns:
            A human-readable string representation of the configuration.
        """
        return (
            f"ContainerFormatConfig:\n"
            f"  Command: {self.command}\n"
            f"  Run Command: {self.run_command}\n"
            f"  Stop Command: {self.stop_command}\n"
            f"  Pull Command: {self.pull_command}"
        )

    def __repr__(self) -> str:
        """
        Returns an unambiguous string representation of the ContainerFormatConfig.

        This method provides a string that could be used to recreate the object,
        showing the constructor call with the current configuration values.

        Returns:
            An unambiguous string representation that shows how to recreate the object.
        """
        config_dict = {
            "command": self.command,
            "run_command": self.run_command,
            "stop_command": self.stop_command,
            "pull_command": self.pull_command,
        }
        return f"ContainerFormatConfig({config_dict!r})"

    def get_command(self) -> str:
        """
        Retrieves the container command.

        This method returns the value of the `command` attribute,
        which specifies the container command (e.g., "singularity", "docker").

        Returns:
            The container command.

        Example:
            ```python
            >>> config = ContainerFormatConfig(
            ...     command="docker",
            ...     run_command="docker run --name my_container",
            ...     stop_command="docker stop my_container",
            ...     pull_command="docker pull my_image"
            ... )
            >>> config.get_command()
            'docker'
            ```
        """
        return self.command

    def get_run_command(self) -> str:
        """
        Retrieves the run command.

        This method returns the value of the `run_command` attribute,
        which specifies the template or command used to run the container.

        Returns:
            The run command.

        Example:
            ```python
            >>> config = ContainerFormatConfig(
            ...     command="docker",
            ...     run_command="docker run --name my_container",
            ...     stop_command="docker stop my_container",
            ...     pull_command="docker pull my_image"
            ... )
            >>> config.get_run_command()
            'docker run --name my_container'
            ```
        """
        return self.run_command

    def get_stop_command(self) -> str:
        """
        Retrieves the stop command.

        This method returns the value of the `stop_command` attribute,
        which specifies the command used to stop the container.

        Returns:
            The stop command.

        Example:
            ```python
            >>> config = ContainerFormatConfig(
            ...     command="docker",
            ...     run_command="docker run --name my_container",
            ...     stop_command="docker stop my_container",
            ...     pull_command="docker pull my_image"
            ... )
            >>> config.get_stop_command()
            'docker stop my_container'
            ```
        """
        return self.stop_command

    def get_pull_command(self) -> str:
        """
        Retrieves the pull command.

        This method returns the value of the `pull_command` attribute,
        which specifies the template or command used to pull the container image.

        Returns:
            The pull command.

        Example:
            ```python
            >>> config = ContainerFormatConfig(
            ...     command="docker",
            ...     run_command="docker run --name my_container",
            ...     stop_command="docker stop my_container",
            ...     pull_command="docker pull my_image"
            ... )
            >>> config.get_pull_command()
            'docker pull my_image'
            ```
        """
        return self.pull_command


class ProcessConfig:
    """
    `ProcessConfig` provides an interface for parsing and interacting with process configuration
    specified in the `merlin_server.yaml` configuration file. This configuration defines commands
    for interacting with the host machine while containers are running, such as checking the status
    of processes or terminating them.

    Attributes:
        STATUS_COMMAND (str): Default template for the status command, which checks if a process
            is running using its parent process ID (PID). Default is "pgrep -P {pid}".
        KILL_COMMAND (str): Default template for the kill command, which terminates a process
            using its PID. Default is "kill {pid}".

        status (str): The status command template to check the status of a process. This is
            initialized from the provided configuration or defaults to `STATUS_COMMAND`.
        kill (str): The kill command template to terminate a process. This is initialized from
            the provided configuration or defaults to `KILL_COMMAND`.

    Methods:
        __eq__: Compares two ProcessConfig objects for equality based on their `status` and
            `kill` attributes.
        get_status_command: Retrieves the status command template.
        get_kill_command: Retrieves the kill command template.
    """

    STATUS_COMMAND: str = "pgrep -P {pid}"
    KILL_COMMAND: str = "kill {pid}"

    status: str = STATUS_COMMAND
    kill: str = KILL_COMMAND

    def __init__(self, data: Dict):
        """
        Initializes the ProcessConfig object with custom or default process commands.

        This constructor takes a dictionary containing configuration data and initializes
        the `status` and `kill` attributes. If the keys `status` or `kill` are not present
        in the provided dictionary, their values default to `STATUS_COMMAND` and `KILL_COMMAND`,
        respectively.

        Args:
            data: A dictionary containing process configuration. Expected keys are:\n
                - "status": A string representing the status command template.
                - "kill": A string representing the kill command template.
        """
        self.status: str = data["status"] if "status" in data else self.STATUS_COMMAND
        self.kill: str = data["kill"] if "kill" in data else self.KILL_COMMAND

    def __eq__(self, other: "ProcessConfig") -> bool:
        """
        Checks equality between two `ProcessConfig` objects.

        This method compares the attributes of the current object with those of another
        `ProcessConfig` object to determine if they are equal.

        Args:
            other: Another instance of the `ProcessConfig` class to compare with.

        Returns:
            `True` if the `status` and `kill` attributes of both instances are equal,
                otherwise `False`.

        Example:
            ```python
            >>> config1 = ProcessConfig({"status": "check_status", "kill": "terminate_process"})
            >>> config2 = ProcessConfig({"status": "check_status", "kill": "terminate_process"})
            >>> config3 = ProcessConfig({"status": "check_status", "kill": "stop_process"})
            >>> config1 == config2
            True
            >>> config1 == config3
            False
            ```
        """
        variables = ("status", "kill")
        return all(getattr(self, attr) == getattr(other, attr) for attr in variables)

    def __str__(self) -> str:
        """
        Returns a human-readable string representation of the ProcessConfig.

        This method provides a formatted, user-friendly view of the process
        configuration, showing the status and kill command templates in a readable format.

        Returns:
            A human-readable string representation of the configuration.
        """
        return f"ProcessConfig:\n" f"  Status Command: {self.status}\n" f"  Kill Command: {self.kill}"

    def __repr__(self) -> str:
        """
        Returns an unambiguous string representation of the ProcessConfig.

        This method provides a string that could be used to recreate the object,
        showing the constructor call with the current configuration values.

        Returns:
            An unambiguous string representation that shows how to recreate the object.
        """
        config_dict = {"status": self.status, "kill": self.kill}
        return f"ProcessConfig({config_dict!r})"

    def get_status_command(self) -> str:
        """
        Retrieves the status command for the process.

        This method returns the command used to check the status of the process
        managed by the `ProcessConfig` instance.

        Returns:
            The status command as a string.

        Example:
            ```python
            >>> process_config = ProcessConfig(status="ps aux | grep process_name", kill="kill -9 process_id")
            >>> process_config.get_status_command()
            'ps aux | grep process_name'
            ```
        """
        return self.status

    def get_kill_command(self) -> str:
        """
        Retrieves the kill command for the process.

        This method returns the command used to terminate the process
        managed by the `ProcessConfig` instance.

        Returns:
            The kill command as a string.

        Example:
            ```python
            >>> process_config = ProcessConfig(status="ps aux | grep process_name", kill="kill -9 process_id")
            >>> process_config.get_kill_command()
            'kill -9 process_id'
            ```
        """
        return self.kill


# Pylint complains there's not enough methods here but this is essentially a wrapper for other
# classes so we can ignore it
class ServerConfig:  # pylint: disable=R0903
    """
    `ServerConfig` is an interface for storing all the necessary configuration for the Merlin server.

    This class encapsulates configurations related to containers, processes, and container formats,
    making it easier to manage and access these settings in a structured way.

    Attributes:
        container (ContainerConfig): Configuration related to the container.
        process (ProcessConfig): Configuration related to the process.
        container_format (ContainerFormatConfig): Configuration for the container format.
    """

    container: ContainerConfig = None
    process: ProcessConfig = None
    container_format: ContainerFormatConfig = None

    def __init__(self, data: Dict):
        """
        Initializes a ServerConfig instance with the provided configuration data.

        Args:
            data: A dictionary containing configuration data. Expected keys include:\n
                - `container` ([`ContainerConfig`][server.server_util.ContainerConfig]): Configuration data for the container.
                - `process` ([`ProcessConfig`][server.server_util.ProcessConfig]): Configuration data for the process.
                - `container_format` ([`ContainerFormatConfig`][server.server_util.ContainerFormatConfig]): Configuration
                    data for the container format.
        """
        self.container: ContainerConfig = ContainerConfig(data["container"]) if "container" in data else None
        self.process: ProcessConfig = ProcessConfig(data["process"]) if "process" in data else None
        container_format_data: str = data.get(self.container.get_format() if self.container else None)
        self.container_format: ContainerFormatConfig = (
            ContainerFormatConfig(container_format_data) if container_format_data else None
        )

    def __str__(self) -> str:
        """
        Returns a human-readable string representation of the ServerConfig.

        This method provides a formatted, user-friendly view of the complete server
        configuration, showing all nested configuration objects in a readable hierarchical format.

        Returns:
            A human-readable string representation of the server configuration.
        """
        lines = ["ServerConfig:"]

        if self.container:
            lines.append("  Container Configuration:")
            container_str = str(self.container)
            # Indent each line of the container config
            for line in container_str.split("\n")[1:]:  # Skip the first line (class name)
                lines.append("  " + line)
        else:
            lines.append("  Container Configuration: None")

        if self.process:
            lines.append("  Process Configuration:")
            process_str = str(self.process)
            # Indent each line of the process config
            for line in process_str.split("\n")[1:]:  # Skip the first line (class name)
                lines.append("  " + line)
        else:
            lines.append("  Process Configuration: None")

        if self.container_format:
            lines.append("  Container Format Configuration:")
            format_str = str(self.container_format)
            # Indent each line of the container format config
            for line in format_str.split("\n")[1:]:  # Skip the first line (class name)
                lines.append("  " + line)
        else:
            lines.append("  Container Format Configuration: None")

        return "\n".join(lines)

    def __repr__(self) -> str:
        """
        Returns an unambiguous string representation of the ServerConfig.

        This method provides a string that shows the constructor call with the current
        configuration values. Since ServerConfig contains complex nested objects, this
        representation focuses on showing the structure rather than being directly evaluable.

        Returns:
            An unambiguous string representation that shows the server configuration structure.
        """
        container_repr = repr(self.container) if self.container else "None"
        process_repr = repr(self.process) if self.process else "None"
        container_format_repr = repr(self.container_format) if self.container_format else "None"

        return (
            f"ServerConfig("
            f"container={container_repr}, "
            f"process={process_repr}, "
            f"container_format={container_format_repr})"
        )


class RedisConfig:
    """
    RedisConfig is an interface for parsing and interacing with redis.conf file that is provided
    by redis. This allows users to parse the given redis configuration and make edits and allow users
    to write those changes into a redis readable config file.

    `RedisConfig` is an interface for parsing and interacting with a Redis configuration file (`redis.conf`).

    This class allows users to:
    - Parse an existing Redis configuration file.
    - Modify configuration settings such as IP address, port, password, snapshot settings, directories, etc.
    - Write the updated configuration back to a file in a Redis-readable format.

    Attributes:
        filename (str): The path to the Redis configuration file.
        changed (bool): A flag indicating whether any changes have been made to the configuration.
        entry_order (List): A list maintaining the order of configuration entries as they appear in the file.
        entries (Dict): A dictionary storing configuration keys and their corresponding values.
        comments (Dict): A dictionary storing comments associated with each configuration entry.
        trailing_comments (str): Any comments that appear at the end of the configuration file.

    Methods:
        parse: Parses the Redis configuration file and populates the `entries`, `comments`, and `entry_order`
            attributes.
        write: Writes the current configuration (including comments) back to the file.
        set_filename: Updates the filename of the configuration file.
        set_config_value: Updates the value of a given configuration key.
        get_config_value: Retrieves the value of a given configuration key.
        changes_made: Returns whether any changes have been made to the configuration.
        get_ip_address: Retrieves the IP address (`bind`) setting from the configuration.
        set_ip_address: Validates and sets the IP address (`bind`) in the configuration.
        get_port: Retrieves the port setting from the configuration.
        set_port: Validates and sets the port in the configuration.
        set_password: Sets the Redis password (`requirepass`) in the configuration.
        get_password: Retrieves the Redis password (`requirepass`) from the configuration.
        set_directory: Sets the save directory (`dir`) in the configuration. Creates the directory if it
            does not exist.
        set_snapshot: Updates the snapshot settings (`save`) for the configuration.
        set_snapshot_file: Sets the snapshot file name (`dbfilename`) in the configuration.
        set_append_mode: Sets the append mode (`appendfsync`) in the configuration.
        set_append_file: Sets the append file name (`appendfilename`) in the configuration.
    """

    def __init__(self, filename: str):
        """
        Initializes a `RedisConfig` instance and parses the given Redis configuration file.

        Args:
            filename: The path to the Redis configuration file (`redis.conf`).

        Notes:
            - Parses the configuration file immediately after initialization by calling the `parse()` method.
            - Populates the `entries`, `comments`, and `entry_order` attributes based on the file's contents.
        """
        self.filename: str = filename
        self.changed: bool = False
        self.entry_order: List[str] = []
        self.entries: Dict[str, str] = {}
        self.comments: Dict[str, str] = {}
        self.trailing_comments: str = ""
        self.changed: bool = False
        self.parse()

    def parse(self):
        """
        Parses the Redis configuration file and populates the configuration data.

        This method reads the Redis configuration file specified by `self.filename` and extracts:

        - Configuration entries (key-value pairs).
        - Associated comments for each entry.
        - The order of entries as they appear in the file.
        - Any trailing comments at the end of the file.

        Behavior:
            - Lines starting with `#` are treated as comments and stored in the `comments` dictionary or `trailing_comments`.
            - Non-comment lines are split into key-value pairs and stored in the `entries` dictionary.
            - The order of configuration keys is preserved in the `entry_order` list.
            - Handles duplicate keys by appending additional parts to the key (e.g., for special cases like `save`).

        Attributes Updated:
            - `self.entries`: Stores configuration key-value pairs.
            - `self.comments`: Stores comments associated with each configuration key.
            - `self.entry_order`: Preserves the order of configuration keys as they appear in the file.
            - `self.trailing_comments`: Stores any comments that appear at the end of the file.

        Example:
            Assume we have a Redis configuration file named "redis.conf" with the following content:
            ```
            # Redis configuration file
            maxmemory 256mb
            save 900 1
            # End of configuration
            ```

            Then we'd see the following:
            ```python
            >>> config = RedisConfig("redis.conf")
            >>> config.parse()
            >>> print(config.entries)
            {'maxmemory': '256mb', 'save 900': 1}
            >>> print(config.comments)
            {'maxmemory': '# Redis configuration file\\n', 'save 900': ''}
            >>> print(config.entry_order)
            ['maxmemory', 'save 900']
            >>> print(config.trailing_comments)
            '# End of configuration'
            ```
        """
        self.entries = {}
        self.comments = {}
        with open(self.filename, "r+") as f:  # pylint: disable=C0103
            file_contents = f.read()
            file_lines = file_contents.split("\n")
            comments = ""
            for line in file_lines:
                if len(line) > 0 and line[0] != "#":
                    line_contents = line.split(maxsplit=1)
                    if line_contents[0] in self.entries:
                        sub_split = line_contents[1].split(maxsplit=1)
                        line_contents[0] += " " + sub_split[0]
                        line_contents[1] = sub_split[1]
                    self.entry_order.append(line_contents[0])
                    self.entries[line_contents[0]] = line_contents[1]
                    self.comments[line_contents[0]] = comments
                    comments = ""
                else:
                    comments += line + "\n"
            self.trailing_comments = comments[:-1]

    def write(self):
        """
        Writes the current configuration and comments back to the Redis configuration file.

        This method writes the configuration entries, their associated comments, and any
        trailing comments to the file specified by `self.filename`. The order of entries is
        preserved as per the `self.entry_order` list.

        Example:
            Assume we start with a Redis configuration file named "redis.conf" with the following content:
            ```
            # Redis configuration file
            maxmemory 256mb
            save 900 1
            # End of configuration
            ```

            We then update it with the write method:
            ```python
            >>> config = RedisConfig("redis.conf")
            >>> config.set_config_value("maxmemory", "512mb")
            >>> config.write()
            ```

            Which updates our "redis.conf" file to be:
            ```
            # Redis configuration file
            maxmemory 512mb
            save 900 1
            # End of configuration
            ```
        """
        with open(self.filename, "w") as f:  # pylint: disable=C0103
            for entry in self.entry_order:
                f.write(self.comments[entry])
                f.write(f"{entry} {self.entries[entry]}\n")
            f.write(self.trailing_comments)

    def set_filename(self, filename: str):
        """
        Sets a new filename for the Redis configuration file.

        Args:
            filename: The new file path to be set as the Redis configuration file.

        Example:
            ```python
            >>> config = RedisConfig("redis.conf")
            >>> print(config.filename)
            'redis.conf'
            >>> config.set_filename("/path/to/new/redis.conf")
            >>> print(config.filename)
            '/path/to/new/redis.conf'
            ```
        """
        self.filename = filename

    def set_config_value(self, key: str, value: str) -> bool:
        """
        Updates the value of a specific configuration key.

        This method changes the value of an existing configuration key in the `entries` dictionary.
        If the key does not exist, the method returns `False`. If the key is updated successfully,
        the `changed` attribute is set to `True`.

        Args:
            key: The configuration key to update.
            value: The new value to set for the specified key.

        Returns:
            True if the key exists and the value is successfully updated. False otherwise.

        Example:
            ```python
            >>> config = RedisConfig("redis.conf")
            >>> success = config.set_config_value("maxmemory", "512mb")
            >>> if success:
            ...     print("Configuration updated successfully!")
            ... else:
            ...     print("Key not found in the configuration.")
            ```
        """
        if key not in self.entries:
            return False
        self.entries[key] = value
        self.changed = True
        return True

    def get_config_value(self, key: str) -> str:
        """
        Retrieves the value of a specific configuration key.

        This method looks up the value of the specified key in the `entries` dictionary
        and returns it. If the key does not exist, the method returns `None`.

        Args:
            key: The configuration key to retrieve the value for.

        Returns:
            The value associated with the specified key as a string, or `None` if the key is not found.

        Example:
            ```python
            >>> config = RedisConfig("redis.conf")
            >>> value = config.get_config_value("maxmemory")
            >>> print(value)
            '256mb'
            >>> value = config.get_config_value("nonexistent_key")
            >>> print(value)
            None
            ```
        """
        if key in self.entries:
            return self.entries[key]
        return None

    def changes_made(self) -> bool:
        """
        Checks if any changes have been made to the configuration.

        This method returns the value of the `self.changed` attribute, which indicates
        whether any configuration values have been modified since the last parse or write.

        Returns:
            True if changes have been made to the configuration, False otherwise.

        Example:
            ```python
            >>> config = RedisConfig("redis.conf")
            >>> print(config.changes_made())
            False
            >>> config.set_config_value("maxmemory", "512mb")
            >>> print(config.changes_made())
            True
            ```
        """
        return self.changed

    def get_ip_address(self) -> str:
        """
        Retrieves the IP address bound in the Redis configuration.

        This method uses the [`get_config_value`][server.server_util.RedisConfig.get_config_value]
        method to fetch the value of the `bind` key from the configuration. The `bind` key typically
        specifies the IP address that Redis binds to. If the `bind` key is not present in the configuration,
        the method returns `None`.

        Returns:
            The IP address as a string if the `bind` key exists, or `None` if it does not.

        Example:
            ```python
            >>> config = RedisConfig("redis.conf")
            >>> ip_address = config.get_ip_address()
            >>> print(ip_address)
            '127.0.0.1'
            ```
        """
        return self.get_config_value("bind")

    def set_ip_address(self, ipaddress: str) -> bool:
        """
        Validates and sets the given IP address in the Redis configuration.

        This method checks if the provided IP address is a valid IPv4 address.
        If valid, it updates the `bind` key in the Redis configuration with the new IP address.
        If the IP address is invalid or the update fails, the method logs an error and returns `False`.

        Args:
            ipaddress: The IP address to set in the Redis configuration.

        Returns:
            True if the IP address is successfully validated and set, False otherwise.

        Example:
            ```python
            >>> config = RedisConfig("redis.conf")
            >>> success = config.set_ip_address("192.168.1.1")
            >>> print(success)
            True
            >>> success = config.set_ip_address("invalid_ip")
            >>> print(success)
            False
            ```
        """
        if ipaddress is None:
            return False
        # Check if ipaddress is valid
        if valid_ipv4(ipaddress):
            # Set ip address in redis config
            if not self.set_config_value("bind", ipaddress):
                LOG.error("Unable to set ip address for redis config")
                return False
        else:
            LOG.error("Invalid IPv4 address given.")
            return False
        LOG.info(f"Ipaddress is set to {ipaddress}")
        return True

    def get_port(self) -> str:
        """
        Retrieves the port number from the Redis configuration.

        This method fetches the value of the `port` key from the configuration
        using the [`get_config_value`][server.server_util.RedisConfig.get_config_value]
        method. If the `port` key is not present, the method returns `None`.

        Returns:
            The port number as a string if the `port` key exists, or `None` if it does not.

        Example:
            ```python
            >>> config = RedisConfig("redis.conf")
            >>> port = config.get_port()
            >>> print(port)
            '6379'
            ```
        """
        return self.get_config_value("port")

    def set_port(self, port: int) -> bool:
        """
        Validates and sets the given port number in the Redis configuration.

        This method checks if the provided port number is valid. If valid, it updates
        the `port` key in the Redis configuration with the new port number. If the port
        is invalid or the update fails, the method logs an error and returns `False`.

        Args:
            port: The port number to set in the Redis configuration.

        Returns:
            True if the port number is successfully validated and set, False otherwise.

        Example:
            ```python
            >>> config = RedisConfig("redis.conf")
            >>> success = config.set_port(6379)
            >>> print(success)
            True
            >>> success = config.set_port(99999)
            ERROR: Invalid port given
            >>> print(success)
            False
            ```
        """
        if port is None:
            return False
        # Check if port is valid
        if valid_port(port):
            # Set port in redis config
            if not self.set_config_value("port", port):
                LOG.error("Unable to set port for redis config")
                return False
        else:
            LOG.error("Invalid port given.")
            return False
        LOG.info(f"Port is set to {port}")
        return True

    def set_password(self, password: str) -> bool:
        """
        Sets a new password in the Redis configuration.

        This method updates the `requirepass` key in the Redis configuration with the provided password.
        If the password is `None`, the method returns `False` without making any changes.

        Args:
            password: The new password to set in the Redis configuration.

        Returns:
            True if the password is successfully set, False otherwise.

        Example:
            ```python
            >>> config = RedisConfig("redis.conf")
            >>> success = config.set_password("my_secure_password")
            >>> print(success)
            True
            ```
        """
        if password is None:
            return False
        self.set_config_value("requirepass", password)
        LOG.info("New password set")
        return True

    def get_password(self) -> str:
        """
        Retrieves the password from the Redis configuration.

        This method fetches the value of the `requirepass` key from the configuration
        using the [`get_config_value`][server.server_util.RedisConfig.get_config_value]
        method. If the `requirepass` key is not present, the method returns `None`.

        Returns:
            The password as a string if the `requirepass` key exists, or `None` if it does not.

        Example:
            ```python
            >>> config = RedisConfig("redis.conf")
            >>> password = config.get_password()
            >>> print(password)
            'my_secure_password'
            ```
        """
        return self.get_config_value("requirepass")

    def set_directory(self, directory: str) -> bool:
        """
        Sets the save directory in the Redis configuration file.

        This method updates the `dir` key in the Redis configuration with the provided directory path.
        If the directory does not exist, it is created. If the directory is `None` or the update fails,
        the method logs an error and returns `False`.

        Args:
            directory: The directory path to set as the save directory in the Redis configuration.

        Returns:
            True if the directory is successfully validated, created (if necessary), and set, False otherwise.

        Example:
            ```python
            >>> config = RedisConfig("redis.conf")
            >>> success = config.set_directory("/var/lib/redis")
            >>> print(success)
            True
            ```
        """
        if directory is None:
            return False
        # Create the directory if it doesn't exist
        if not os.path.exists(directory):
            os.mkdir(directory)
            LOG.info(f"Created directory {directory}")
        # Set the save directory to the redis config
        if not self.set_config_value("dir", directory):
            LOG.error("Unable to set directory for redis config")
            return False
        LOG.info(f"Directory is set to {directory}")
        return True

    def set_snapshot(self, seconds: int = None, changes: int = None) -> bool:
        """
        Updates the snapshot configuration in the Redis settings.

        This method allows you to set the snapshot parameters, which determine
        when Redis creates a snapshot of the dataset. The snapshot is triggered
        based on a combination of time (`seconds`) and the number of changes
        (`changes`) made to the dataset. If either parameter is `None`, it will
        remain unchanged.

        Args:
            seconds: The time interval (in seconds) after which a snapshot should be created.
                If `None`, the existing value for `seconds` remains unchanged.
            changes: The number of changes to the dataset that trigger a snapshot.
                If `None`, the existing value for `changes` remains unchanged.

        Returns:
            True if the snapshot configuration is successfully updated, False otherwise.

        Example:
            ```python
            >>> config = RedisConfig("redis.conf")
            >>> success = config.set_snapshot(seconds=300, changes=10)
            >>> print(success)
            True
            >>> success = config.set_snapshot(seconds=600)
            >>> print(success)
            True
            >>> success = config.set_snapshot()  # No changes
            >>> print(success)
            False
            ```
        """

        # If both values are None, this method is doing nothing
        if seconds is None and changes is None:
            return False

        # Grab the snapshot value from the redis config
        value = self.get_config_value("save")
        if value is None:
            LOG.error("Unable to get exisiting parameter values for snapshot")
            return False

        # Update the snapshot value
        value = value.split()
        log_msg = ""
        if seconds is not None:
            value[0] = str(seconds)
            log_msg += f"Snapshot wait time is set to {seconds} seconds. "
        if changes is not None:
            value[1] = str(changes)
            log_msg += f"Snapshot threshold is set to {changes} changes."
        value = " ".join(value)

        # Set the new snapshot value
        if not self.set_config_value("save", value):
            LOG.error("Unable to set snapshot value")
            return False

        LOG.info(log_msg)
        return True

    def set_snapshot_file(self, file: str) -> bool:
        """
        Sets the name of the snapshot file in the Redis configuration.

        This method updates the `dbfilename` parameter in the Redis configuration
        with the provided file name. The snapshot file is where Redis saves the
        dataset during a snapshot operation.

        Args:
            file: The name of the snapshot file to set in the Redis configuration.

        Returns:
            True if the snapshot file name is successfully set, False otherwise.

        Example:
            ```python
            >>> config = RedisConfig("redis.conf")
            >>> success = config.set_snapshot_file("dump.rdb")
            >>> print(success)
            True
            ```
        """
        if file is None:
            return False
        # Set the snapshot file in the redis config
        if not self.set_config_value("dbfilename", file):
            LOG.error("Unable to set snapshot_file name")
            return False

        LOG.info(f"Snapshot file is set to {file}")
        return True

    def set_append_mode(self, mode: str) -> bool:
        """
        Sets the append mode in the Redis configuration.

        The append mode determines how Redis handles data persistence to the append-only file (AOF).
        Valid modes are:

        - "always": Redis appends data to the AOF after every write operation.
        - "everysec": Redis appends data to the AOF every second (default and recommended).
        - "no": Disables AOF persistence.

        Args:
            mode: The append mode to set. Must be one of "always", "everysec", or "no".

        Returns:
            True if the append mode is successfully set, False otherwise.

        Example:
            ```python
            >>> config = RedisConfig("redis.conf")
            >>> success = config.set_append_mode("everysec")
            >>> print(success)
            True
            ```
        """
        if mode is None:
            return False
        valid_modes = ["always", "everysec", "no"]

        # Validate the append mode (always, everysec, no)
        if mode in valid_modes:
            # Set the append mode in the redis config
            if not self.set_config_value("appendfsync", mode):
                LOG.error("Unable to set append_mode in redis config")
                return False
        else:
            LOG.error("Not a valid append_mode (Only valid modes are always, everysec, no)")
            return False

        LOG.info(f"Append mode is set to {mode}")
        return True

    def set_append_file(self, file: str) -> bool:
        """
        Sets the name of the append-only file (AOF) in the Redis configuration.

        The append-only file is used by Redis for data persistence in append-only mode.
        This method updates the `appendfilename` parameter in the Redis configuration
        with the provided file name.

        Args:
            file: The name of the append-only file to set in the Redis configuration.

        Returns:
            True if the append file name is successfully set, False otherwise.

        Example:
            ```python
            >>> config = RedisConfig("redis.conf")
            >>> success = config.set_append_file("appendonly.aof")
            >>> print(success)
            True
            ```
        """
        if file is None:
            return False
        # Set the append file in the redis config
        if not self.set_config_value("appendfilename", f'"{file}"'):
            LOG.error("Unable to set append filename.")
            return False
        LOG.info(f"Append file is set to {file}")
        return True


class RedisUsers:
    """
    `RedisUsers` provides an interface for parsing and interacting with a Redis `users` configuration
    file. This class allows users and the Merlin server to create, remove, and edit user entries
    within the Redis configuration files. Changes can be synchronized and pushed to an existing
    Redis server if one is available.

    Attributes:
        filename (str): The path to the Redis user configuration file.
        users (Dict[str, User]): A dictionary
            containing user data, where keys are usernames and values are instances of the
            `User` class.

    Methods:
        parse: Parses the Redis user configuration file and populates the `users` dictionary with
            [`User`][server.server_util.RedisUsers.User] objects.
        write: Writes the current users' data back to the Redis user configuration file.
        add_user: Adds a new user to the `users` dictionary.
        set_password: Sets a new password for a specific user.
        remove_user: Removes a user from the `users` dictionary.
        apply_to_redis: Applies the current user configuration to a running Redis server by synchronizing
            the local user data with the Redis server's ACL configuration.
    """

    class User:
        """
        An embedded class that represents an individual Redis user with attributes and methods for managing
        user-specific data.

        Attributes:
            status (str): The status of the user, either "on" (enabled) or "off" (disabled).
            hash_password (str): The hashed password of the user.
            keys (str): The keys the user has access to (e.g., "*" for all keys).
            channels (str): The channels the user can access (e.g., "*" for all channels).
            commands (str): The commands the user is allowed to execute (e.g., "@all" for all commands).

        Methods:
            parse_dict: Parses a dictionary of user data and updates the `User` object's attributes.
            get_user_dict: Returns a dictionary representation of the `User` object.
            __repr__(): Returns a string representation of the `User` object.
            __str__(): Returns a string representation of the `User` object (same as `__repr__`).
            set_password: Sets the user's hashed password based on the provided plaintext password.
        """

        status: str = "on"
        hash_password: str = hashlib.sha256(b"password").hexdigest()
        keys: str = "*"
        channels: str = "*"
        commands: str = "@all"

        def __init__(  # pylint: disable=R0913
            self,
            status: str = "on",
            keys: str = "*",
            channels: str = "*",
            commands: str = "@all",
            password: str = None,
        ):
            """
            Initializes a `User` object with the provided attributes.

            Args:
                status: The status of the user, either "on" (enabled) or "off" (disabled).
                keys: The keys the user has access to (e.g., "*" for all keys).
                channels: The channels the user can access (e.g., "*" for all channels).
                commands: The commands the user is allowed to execute (e.g., "@all" for all commands).
                password: The plaintext password for the user. If provided, it will be hashed and stored.
            """
            self.status: str = status
            self.keys: str = keys
            self.channels: str = channels
            self.commands: str = commands
            if password is not None:
                self.set_password(password)

        def parse_dict(self, dictionary: Dict[str, str]):
            """
            Parses a dictionary containing user information and updates the attributes of the `User` object.

            Args:
                dictionary: A dictionary containing user data. Expected keys are:\n
                    - `status` (str): The user's status ("on" or "off").
                    - `keys` (str): The keys the user can access.
                    - `channels` (str): The channels the user can access.
                    - `commands` (str): The commands the user can execute.
                    - `hash_password` (str): The hashed password of the user.

            Example:
                ```python
                >>> user_data = {
                ...     "status": "on",
                ...     "keys": "*",
                ...     "channels": "*",
                ...     "commands": "@all",
                ...     "hash_password": "hashed_password_value"
                ... }
                >>> user = User()
                >>> user.parse_dict(user_data)
                >>> print(user.status)
                'on'
                >>> print(user.hash_password)
                'hashed_password_value'
                ```
            """
            self.status = dictionary["status"]
            self.keys = dictionary["keys"]
            self.channels = dictionary["channels"]
            self.commands = dictionary["commands"]
            self.hash_password = dictionary["hash_password"]

        def get_user_dict(self) -> Dict:
            """
            Returns a dictionary representation of the `User` object.

            Returns:
                A dictionary containing the user's attributes.

            Example:
                ```python
                >>> user = User(status="on", keys="*", channels="*", commands="@all", password="secure_password")
                >>> user_dict = user.get_user_dict()
                >>> print(user_dict)
                {
                    "status": "on",
                    "hash_password": "hashed_password_value",
                    "keys": "*",
                    "channels": "*",
                    "commands": "@all"
                }
                ```
            """
            self.status = "on"
            return {
                "status": self.status,
                "hash_password": self.hash_password,
                "keys": self.keys,
                "channels": self.channels,
                "commands": self.commands,
            }

        def __repr__(self) -> str:
            """
            Returns a string representation of the `User` object for debugging purposes.

            Returns:
                A string representation of the user's attributes in dictionary format.
            """
            return str(self.get_user_dict())

        def __str__(self) -> str:
            """
            Returns a string representation of the `User` object.

            Returns:
                A string representation of the user's attributes in dictionary format.
            """
            return self.__repr__()

        def set_password(self, password: str):
            """
            Sets the user's hashed password based on the provided plaintext password.

            Args:
                password: The plaintext password to hash and store.

            Example:
                ```python
                >>> user = User()
                >>> user.set_password("secure_password")
                >>> print(user.hash_password)
                # Output: The hashed value of "secure_password"
                ```
            """
            self.hash_password = hashlib.sha256(bytes(password, "utf-8")).hexdigest()

    filename: str = ""
    users: Dict[str, User] = {}

    def __init__(self, filename: str):
        """
        Initializes a `RedisUsers` object and parses the Redis user configuration file if it exists.

        Args:
            filename: The path to the Redis user configuration file.
        """
        self.filename: str = filename
        if os.path.exists(self.filename):
            self.parse()

    def parse(self):
        """
        Parses the Redis user configuration file and populates the `users` dictionary.

        This method reads the YAML configuration file specified by `self.filename` and converts
        the user data into a dictionary. Each user entry is converted into an instance of the
        [`User`][server.server_util.RedisUsers.User] class, which stores the user's attributes.

        Example:
            Assume `users.yaml` contains:
            ```
            user1:
              status: "on"
              hash_password: "hashed_password_1"
              keys: "*"
              channels: "*"
              commands: "@all"
            user2:
              status: "off"
              hash_password: "hashed_password_2"
              keys: "key1"
              channels: "channel1"
              commands: "command1"
            ```

            This would then be parsed like so:
            ```python
            >>> redis_users = RedisUsers("users.yaml")
            >>> redis_users.parse()
            >>> print(redis_users.users["user1"].status)
            'on'
            >>> print(redis_users.users["user2"].commands)
            'command1'
            ```
        """
        with open(self.filename, "r") as f:  # pylint: disable=C0103
            self.users = yaml.load(f, yaml.Loader)
            for user in self.users:
                new_user = self.User()
                new_user.parse_dict(self.users[user])
                self.users[user] = new_user

    def write(self):
        """
        Writes the current `users` dictionary back to the Redis user configuration file.

        This method converts the `users` dictionary, which contains [`User`][server.server_util.RedisUsers.User]
        objects, into a format suitable for storage in the YAML configuration file. The file
        specified by `self.filename` is then updated with the current user data.

        Example:
            ```python
            >>> redis_users = RedisUsers("users.yaml")
            >>> redis_users.add_user(
            ...     user="new_user",
            ...     status="on",
            ...     keys="*",
            ...     channels="*",
            ...     commands="@all",
            ...     password="secure_password"
            ... )
            >>> redis_users.write()
            ```

            After calling `write`, the `users.yaml` file will be updated with the new user's data.
        """
        data = self.users.copy()
        for key in data:
            data[key] = self.users[key].get_user_dict()
        with open(self.filename, "w") as f:  # pylint: disable=C0103
            yaml.dump(data, f, yaml.Dumper)

    def add_user(  # pylint: disable=R0913
        self,
        user: str,
        status: str = "on",
        keys: str = "*",
        channels: str = "*",
        commands: str = "@all",
        password: str = None,
    ) -> bool:
        """
        Adds a new user to the dictionary of Redis users.

        Args:
            user: The username of the new user.
            status: The status of the user, either "on" (enabled) or "off" (disabled).
            keys: The keys the user has access to (e.g., "*" for all keys).
            channels: The channels the user can access (e.g., "*" for all channels).
            commands: The commands the user is allowed to execute (e.g., "@all" for all commands).
            password: The plaintext password for the user. If provided, it will be hashed and stored.

        Returns:
            True if the user was successfully added, False if the user already exists.

        Example:
            ```python
            >>> redis_users = RedisUsers("users.yaml")
            >>> success = redis_users.add_user(
            ...     user="new_user",
            ...     status="on",
            ...     keys="*",
            ...     channels="*",
            ...     commands="@all",
            ...     password="secure_password"
            ... )
            >>> print(success)
            True
            ```
        """
        if user in self.users:
            return False
        self.users[user] = self.User(status, keys, channels, commands, password)
        return True

    def set_password(self, user: str, password: str) -> bool:
        """
        Sets the password for an existing user.

        Args:
            user: The username of the user whose password is to be updated.
            password: The plaintext password to hash and store for the user.

        Returns:
            True if the password was successfully updated, False if the user does not exist.

        Example:
            ```python
            >>> redis_users = RedisUsers("users.yaml")
            >>> redis_users.add_user("existing_user", password="old_password")
            >>> success = redis_users.set_password("existing_user", "new_password")
            >>> print(success)
            True
            ```
        """
        if user not in self.users:
            return False
        self.users[user].set_password(password)
        return True

    def remove_user(self, user: str) -> bool:
        """
        Removes a user from the dictionary of Redis users.

        Args:
            user: The username of the user to be removed.

        Returns:
            True if the user was successfully removed, False if the user does not exist.

        Example:
            ```python
            >>> redis_users = RedisUsers("users.yaml")
            >>> redis_users.add_user("user_to_remove", password="password")
            >>> success = redis_users.remove_user("user_to_remove")
            >>> print(success)
            True
            ```
        """
        if user in self.users:
            del self.users[user]
            return True
        return False

    def apply_to_redis(self, host: str, port: int, password: str):
        """
         Applies the user configuration changes to a Redis instance.

        This method synchronizes the current user configuration stored in `self.users`
        with the Redis instance specified by the provided connection details. It performs
        the following actions:

        - Adds or updates users in Redis based on the `self.users` dictionary.
        - Removes users from Redis that are not present in `self.users`.

        Args:
            host: The hostname or IP address of the Redis server.
            port: The port number of the Redis server.
            password: The password for authenticating with the Redis server.

        Example:
            ```python
            >>> redis_users = RedisUsers("users.yaml")
            >>> redis_users.add_user("user1", password="password1")
            >>> redis_users.add_user("user2", password="password2")
            >>> redis_users.apply_to_redis(host="127.0.0.1", port=6379, password="redis_password")
            ```
        """
        database = redis.Redis(host=host, port=port, password=password)
        current_users = database.acl_users()
        for user in self.users:
            if user not in current_users:
                data = self.users[user]
                database.acl_setuser(
                    username=user,
                    hashed_passwords=[f"+{data.hash_password}"],
                    enabled=(data.status == "on"),
                    keys=data.keys,
                    channels=data.channels,
                    commands=[f"+{data.commands}"],
                )

        for user in current_users:
            if user not in self.users:
                database.acl_deluser(user)


class AppYaml:
    """
    `AppYaml` provides a structured way to interact with the main `app.yaml` configuration file for Merlin.
    This class allows users to parse, edit, configure, and write the `app.yaml` file, which contains
    the application's main configuration settings.

    Attributes:
        default_filename (str): The default file path for the `app.yaml` configuration file.
        data (Dict): A dictionary that holds the parsed configuration data from the `app.yaml` file.
        broker_name (str): The key name in the configuration file representing the broker settings.
        results_name (str): The key name in the configuration file representing the results backend settings.

    Methods:
        apply_server_config: Updates the `data` dictionary with Redis server configuration details based on
            the provided `ServerConfig` object.
        update_data: Updates the `data` dictionary with new entries.
        get_data: Returns the current configuration data stored in the `data` attribute.
        read: Reads a YAML file and populates the `data` attribute with its contents.
        write: Writes the current `data` dictionary to the specified YAML file.
    """

    default_filename: str = os.path.join(CONFIG_DIR, "app.yaml")
    data: Dict = {}
    broker_name: str = "broker"
    results_name: str = "results_backend"

    def __init__(self, filename: str = default_filename):
        """
        Initializes the `AppYaml` object and loads the configuration file.

        Args:
            filename: The path to the `app.yaml` file. If the file does not exist,
                the default file path (`default_filename`) is used.
        """
        if not os.path.exists(filename):
            filename = self.default_filename
        LOG.debug(f"Reading configuration from {filename}")
        self.read(filename)

    def apply_server_config(self, server_config: ServerConfig):
        """
        Updates the `data` dictionary with Redis server configuration details
        based on the provided `ServerConfig` object.

        Args:
            server_config: An object containing the server configuration.

        Example:
            ```python
            >>> server_config = ServerConfig(...)
            >>> app_yaml = AppYaml()
            >>> app_yaml.apply_server_config(server_config)
            ```
        """
        redis_config = RedisConfig(server_config.container.get_config_path())

        if self.broker_name not in self.data:
            self.data[self.broker_name] = {}

        self.data[self.broker_name]["name"] = server_config.container.get_image_type()
        self.data[self.broker_name]["username"] = "default"
        self.data[self.broker_name]["password"] = server_config.container.get_pass_file_path()
        self.data[self.broker_name]["server"] = redis_config.get_ip_address()
        self.data[self.broker_name]["port"] = redis_config.get_port()

        if self.results_name not in self.data:
            self.data[self.results_name] = {}

        self.data[self.results_name]["name"] = server_config.container.get_image_type()
        self.data[self.results_name]["username"] = "default"
        self.data[self.results_name]["password"] = server_config.container.get_pass_file_path()
        self.data[self.results_name]["server"] = redis_config.get_ip_address()
        self.data[self.results_name]["port"] = redis_config.get_port()

    def update_data(self, new_data: Dict):
        """
        Updates the `data` dictionary with new entries.

        Args:
            new_data: A dictionary containing the new data to be merged
                into the existing `data` dictionary.

        Example:
            ```python
            >>> new_data = {"custom_key": {"sub_key": "value"}}
            >>> app_yaml = AppYaml()
            >>> app_yaml.update_data(new_data)
            ```
        """
        self.data.update(new_data)

    def get_data(self) -> Dict:
        """
        Retrieves the current configuration data stored in the `data` attribute.

        Returns:
            The current configuration data.

        Example:
            ```python
            >>> app_yaml = AppYaml()
            >>> current_data = app_yaml.get_data()
            ```
        """
        return self.data

    def read(self, filename: str = default_filename):
        """
        Reads a YAML file and populates the `data` attribute with its contents.

        Args:
            filename: The path to the YAML file to be read. If not provided,
                the default file path (`default_filename`) is used.

        Example:
            ```python
            >>> app_yaml = AppYaml()
            >>> app_yaml.read("custom_app.yaml")
            ```
        """
        try:
            self.data = merlin.utils.load_yaml(filename)
        except FileNotFoundError:
            self.data = {}

    def write(self, filename: str = default_filename):
        """
        Writes the current `data` dictionary to the specified YAML file.

        Args:
            filename: The path to the YAML file where the data should be written.
                If not provided, the default file path (`default_filename`) is used.

        Example:
            ```python
            >>> app_yaml = AppYaml()
            >>> app_yaml.write("output_app.yaml")
            ```
        """
        try:
            with open(filename, "w+") as f:  # pylint: disable=C0103
                yaml.dump(self.data, f, yaml.Dumper)
        except FileNotFoundError:
            with open(filename, "w") as f:  # pylint: disable=C0103
                yaml.dump(self.data, f, yaml.Dumper)
