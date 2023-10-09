###############################################################################
# Copyright (c) 2023, Lawrence Livermore National Security, LLC.
# Produced at the Lawrence Livermore National Laboratory
# Written by the Merlin dev team, listed in the CONTRIBUTORS file.
# <merlin@llnl.gov>
#
# LLNL-CODE-797170
# All rights reserved.
# This file is part of Merlin, Version: 1.11.0.
#
# For details, see https://github.com/LLNL/merlin.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
###############################################################################
"""Utils relating to merlin server"""

import hashlib
import logging
import os

import redis
import yaml

import merlin.utils


LOG = logging.getLogger("merlin")

# Constants for main merlin server configuration values.
CONTAINER_TYPES = ["singularity", "docker", "podman"]
MERLIN_CONFIG_DIR = os.path.join(os.path.expanduser("~"), ".merlin")
MERLIN_SERVER_SUBDIR = "server/"
MERLIN_SERVER_CONFIG = "merlin_server.yaml"


def valid_ipv4(ip: str) -> bool:  # pylint: disable=C0103
    """
    Checks valid ip address
    """
    if not ip:
        return False

    arr = ip.split(".")
    if len(arr) != 4:
        return False

    for i in arr:
        if int(i) < 0 and int(i) > 255:
            return False

    return True


def valid_port(port: int) -> bool:
    """
    Checks valid network port
    """
    if 0 < port < 65536:
        return True
    return False


# Pylint complains about too many instance variables but it's necessary here so ignore
class ContainerConfig:  # pylint: disable=R0902
    """
    ContainerConfig provides interface for parsing and interacting with the container value specified within
    the merlin_server.yaml configuration file. Dictionary of the config values should be passed when initialized
    to parse values. This can be done after parsing yaml to data dictionary.
    If there are missing values within the configuration it will be populated with default values for
    singularity container.

    Configuration contains values for setting up containers and storing values specific to each container.
    Values that are stored consist of things within the local configuration directory as different runs
    can have differnt configuration values.
    """

    # Default values for configuration
    FORMAT = "singularity"
    IMAGE_TYPE = "redis"
    IMAGE_NAME = "redis_latest.sif"
    REDIS_URL = "docker://redis"
    CONFIG_FILE = "redis.conf"
    CONFIG_DIR = os.path.abspath("./merlin_server/")
    PROCESS_FILE = "merlin_server.pf"
    PASSWORD_FILE = "redis.pass"
    USERS_FILE = "redis.users"

    format = FORMAT
    image_type = IMAGE_TYPE
    image = IMAGE_NAME
    url = REDIS_URL
    config = CONFIG_FILE
    config_dir = CONFIG_DIR
    pfile = PROCESS_FILE
    pass_file = PASSWORD_FILE
    user_file = USERS_FILE

    def __init__(self, data: dict) -> None:
        self.format = data["format"] if "format" in data else self.FORMAT
        self.image_type = data["image_type"] if "image_type" in data else self.IMAGE_TYPE
        self.image = data["image"] if "image" in data else self.IMAGE_NAME
        self.url = data["url"] if "url" in data else self.REDIS_URL
        self.config = data["config"] if "config" in data else self.CONFIG_FILE
        self.config_dir = os.path.abspath(data["config_dir"]) if "config_dir" in data else self.CONFIG_DIR
        self.pfile = data["pfile"] if "pfile" in data else self.PROCESS_FILE
        self.pass_file = data["pass_file"] if "pass_file" in data else self.PASSWORD_FILE
        self.user_file = data["user_file"] if "user_file" in data else self.USERS_FILE

    def get_format(self) -> str:
        """Getter method to get the container format"""
        return self.format

    def get_image_type(self) -> str:
        """Getter method to get the image type"""
        return self.image_type

    def get_image_name(self) -> str:
        """Getter method to get the image name"""
        return self.image

    def get_image_url(self) -> str:
        """Getter method to get the image url"""
        return self.url

    def get_image_path(self) -> str:
        """Getter method to get the path to the image"""
        return os.path.join(self.config_dir, self.image)

    def get_config_name(self) -> str:
        """Getter method to get the configuration file name"""
        return self.config

    def get_config_path(self) -> str:
        """Getter method to get the configuration file path"""
        return os.path.join(self.config_dir, self.config)

    def get_config_dir(self) -> str:
        """Getter method to get the configuration directory"""
        return self.config_dir

    def get_pfile_name(self) -> str:
        """Getter method to get the process file name"""
        return self.pfile

    def get_pfile_path(self) -> str:
        """Getter method to get the process file path"""
        return os.path.join(self.config_dir, self.pfile)

    def get_pass_file_name(self) -> str:
        """Getter method to get the password file name"""
        return self.pass_file

    def get_pass_file_path(self) -> str:
        """Getter method to get the password file path"""
        return os.path.join(self.config_dir, self.pass_file)

    def get_user_file_name(self) -> str:
        """Getter method to get the user file name"""
        return self.user_file

    def get_user_file_path(self) -> str:
        """Getter method to get the user file path"""
        return os.path.join(self.config_dir, self.user_file)

    def get_container_password(self) -> str:
        """Getter method to get the container password"""
        password = None
        with open(self.get_pass_file_path(), "r") as f:  # pylint: disable=C0103
            password = f.read()
        return password


class ContainerFormatConfig:
    """
    ContainerFormatConfig provides an interface for parsing and interacting with container specific
    configuration files <container_name>.yaml. These configuration files contain container specific
    commands to run containerizers such as singularity, docker, and podman.
    """

    COMMAND = "singularity"
    RUN_COMMAND = "{command} run {image} {config}"
    STOP_COMMAND = "kill"
    PULL_COMMAND = "{command} pull {image} {url}"

    command = COMMAND
    run_command = RUN_COMMAND
    stop_command = STOP_COMMAND
    pull_command = PULL_COMMAND

    def __init__(self, data: dict) -> None:
        self.command = data["command"] if "command" in data else self.COMMAND
        self.run_command = data["run_command"] if "run_command" in data else self.RUN_COMMAND
        self.stop_command = data["stop_command"] if "stop_command" in data else self.STOP_COMMAND
        self.pull_command = data["pull_command"] if "pull_command" in data else self.PULL_COMMAND

    def get_command(self) -> str:
        """Getter method to get the container command"""
        return self.command

    def get_run_command(self) -> str:
        """Getter method to get the run command"""
        return self.run_command

    def get_stop_command(self) -> str:
        """Getter method to get the stop command"""
        return self.stop_command

    def get_pull_command(self) -> str:
        """Getter method to get the pull command"""
        return self.pull_command


class ProcessConfig:
    """
    ProcessConfig provides an interface for parsing and interacting with process config specified
    in merlin_server.yaml configuration. This configuration provide commands for interfacing with
    host machine while the containers are running.
    """

    STATUS_COMMAND = "pgrep -P {pid}"
    KILL_COMMAND = "kill {pid}"

    status = STATUS_COMMAND
    kill = KILL_COMMAND

    def __init__(self, data: dict) -> None:
        self.status = data["status"] if "status" in data else self.STATUS_COMMAND
        self.kill = data["kill"] if "kill" in data else self.KILL_COMMAND

    def get_status_command(self) -> str:
        """Getter method to get the status command"""
        return self.status

    def get_kill_command(self) -> str:
        """Getter method to get the kill command"""
        return self.kill


# Pylint complains there's not enough methods here but this is essentially a wrapper for other
# classes so we can ignore it
class ServerConfig:  # pylint: disable=R0903
    """
    ServerConfig is an interface for storing all the necessary configuration for merlin server.
    These configuration container things such as ContainerConfig, ProcessConfig, and ContainerFormatConfig.
    """

    container: ContainerConfig = None
    process: ProcessConfig = None
    container_format: ContainerFormatConfig = None

    def __init__(self, data: dict) -> None:
        if "container" in data:
            self.container = ContainerConfig(data["container"])
        if "process" in data:
            self.process = ProcessConfig(data["process"])
        if self.container.get_format() in data:
            self.container_format = ContainerFormatConfig(data[self.container.get_format()])


class RedisConfig:
    """
    RedisConfig is an interface for parsing and interacing with redis.conf file that is provided
    by redis. This allows users to parse the given redis configuration and make edits and allow users
    to write those changes into a redis readable config file.
    """

    filename = ""
    entry_order = []
    entries = {}
    comments = {}
    trailing_comments = ""
    changed = False

    def __init__(self, filename) -> None:
        self.filename = filename
        self.changed = False
        self.parse()

    def parse(self) -> None:
        """Parses the redis configuration file"""
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

    def write(self) -> None:
        """Writes to the redis configuration file"""
        with open(self.filename, "w") as f:  # pylint: disable=C0103
            for entry in self.entry_order:
                f.write(self.comments[entry])
                f.write(f"{entry} {self.entries[entry]}\n")
            f.write(self.trailing_comments)

    def set_filename(self, filename: str) -> None:
        """Setter method to set the filename"""
        self.filename = filename

    def set_config_value(self, key: str, value: str) -> bool:
        """Changes a configuration value"""
        if key not in self.entries:
            return False
        self.entries[key] = value
        self.changed = True
        return True

    def get_config_value(self, key: str) -> str:
        """Given an entry in the config, get the value"""
        if key in self.entries:
            return self.entries[key]
        return None

    def changes_made(self) -> bool:
        """Getter method to get the changes made"""
        return self.changed

    def get_ip_address(self) -> str:
        """Getter method to get the ip from the redis config"""
        return self.get_config_value("bind")

    def set_ip_address(self, ipaddress: str) -> bool:
        """Validates and sets a given ip address"""
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
        """Getter method to get the port from the redis config"""
        return self.get_config_value("port")

    def set_port(self, port: str) -> bool:
        """Validates and sets a given port"""
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
        """Changes the password"""
        if password is None:
            return False
        self.set_config_value("requirepass", password)
        LOG.info("New password set")
        return True

    def get_password(self) -> str:
        """Getter method to get the config password"""
        return self.get_config_value("requirepass")

    def set_directory(self, directory: str) -> bool:
        """
        Sets the save directory in the redis config file.
        Creates the directory if necessary.
        """
        if directory is None:
            return False
        if not os.path.exists(directory):
            os.mkdir(directory)
            LOG.info(f"Created directory {directory}")
        # Validate the directory input
        if os.path.exists(directory):
            # Set the save directory to the redis config
            if not self.set_config_value("dir", directory):
                LOG.error("Unable to set directory for redis config")
                return False
        else:
            LOG.error(f"Directory {directory} given does not exist and could not be created.")
            return False
        LOG.info(f"Directory is set to {directory}")
        return True

    def set_snapshot_seconds(self, seconds: int) -> bool:
        """Sets the snapshot wait time"""
        if seconds is None:
            return False
        # Set the snapshot second in the redis config
        value = self.get_config_value("save")
        if value is None:
            LOG.error("Unable to get exisiting parameter values for snapshot")
            return False

        value = value.split()
        value[0] = str(seconds)
        value = " ".join(value)
        if not self.set_config_value("save", value):
            LOG.error("Unable to set snapshot value seconds")
            return False

        LOG.info(f"Snapshot wait time is set to {seconds} seconds")
        return True

    def set_snapshot_changes(self, changes: int) -> bool:
        """Sets the snapshot threshold"""
        if changes is None:
            return False
        # Set the snapshot changes into the redis config
        value = self.get_config_value("save")
        if value is None:
            LOG.error("Unable to get exisiting parameter values for snapshot")
            return False

        value = value.split()
        value[1] = str(changes)
        value = " ".join(value)
        if not self.set_config_value("save", value):
            LOG.error("Unable to set snapshot value seconds")
            return False

        LOG.info(f"Snapshot threshold is set to {changes} changes")
        return True

    def set_snapshot_file(self, file: str) -> bool:
        """Sets the snapshot file"""
        if file is None:
            return False
        # Set the snapshot file in the redis config
        if not self.set_config_value("dbfilename", file):
            LOG.error("Unable to set snapshot_file name")
            return False

        LOG.info(f"Snapshot file is set to {file}")
        return True

    def set_append_mode(self, mode: str) -> bool:
        """Sets the append mode"""
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
            LOG.error("Not a valid append_mode(Only valid modes are always, everysec, no)")
            return False

        LOG.info(f"Append mode is set to {mode}")
        return True

    def set_append_file(self, file: str) -> bool:
        """Sets the append file"""
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
    RedisUsers provides an interface for parsing and interacting with redis.users configuration
    file. Allow users and merlin server to create, remove, and edit users within the redis files.
    Changes can be sync and push to an exisiting redis server if one is available.
    """

    class User:
        """Embedded class to store user specific information"""

        status = "on"
        hash_password = hashlib.sha256(b"password").hexdigest()
        keys = "*"
        channels = "*"
        commands = "@all"

        def __init__(  # pylint: disable=R0913
            self, status="on", keys="*", channels="*", commands="@all", password=None
        ) -> None:
            self.status = status
            self.keys = keys
            self.channels = channels
            self.commands = commands
            if password is not None:
                self.set_password(password)

        def parse_dict(self, dictionary: dict) -> None:
            """
            Given a dict of user info, parse the dict and store
            the values as class attributes.
            :param `dictionary`: The dict to parse
            """
            self.status = dictionary["status"]
            self.keys = dictionary["keys"]
            self.channels = dictionary["channels"]
            self.commands = dictionary["commands"]
            self.hash_password = dictionary["hash_password"]

        def get_user_dict(self) -> dict:
            """Getter method to get the user info"""
            self.status = "on"
            return {
                "status": self.status,
                "hash_password": self.hash_password,
                "keys": self.keys,
                "channels": self.channels,
                "commands": self.commands,
            }

        def __repr__(self) -> str:
            """Repr magic method for User class"""
            return str(self.get_user_dict())

        def __str__(self) -> str:
            """Str magic method for User class"""
            return self.__repr__()

        def set_password(self, password: str) -> None:
            """Setter method to set the user's hash password"""
            self.hash_password = hashlib.sha256(bytes(password, "utf-8")).hexdigest()

    filename = ""
    users = {}

    def __init__(self, filename) -> None:
        self.filename = filename
        if os.path.exists(self.filename):
            self.parse()

    def parse(self) -> None:
        """Parses the redis user configuration file"""
        with open(self.filename, "r") as f:  # pylint: disable=C0103
            self.users = yaml.load(f, yaml.Loader)
            for user in self.users:
                new_user = self.User()
                new_user.parse_dict(self.users[user])
                self.users[user] = new_user

    def write(self) -> None:
        """Writes to the redis user configuration file"""
        data = self.users.copy()
        for key in data:
            data[key] = self.users[key].get_user_dict()
        with open(self.filename, "w") as f:  # pylint: disable=C0103
            yaml.dump(data, f, yaml.Dumper)

    def add_user(  # pylint: disable=R0913
        self, user, status="on", keys="*", channels="*", commands="@all", password=None
    ) -> bool:
        """Add a user to the dict of Redis users"""
        if user in self.users:
            return False
        self.users[user] = self.User(status, keys, channels, commands, password)
        return True

    def set_password(self, user: str, password: str):
        """Set the password for a specific user"""
        if user not in self.users:
            return False
        self.users[user].set_password(password)
        return True

    def remove_user(self, user) -> bool:
        """Remove a user from the dict of users"""
        if user in self.users:
            del self.users[user]
            return True
        return False

    def apply_to_redis(self, host: str, port: int, password: str) -> None:
        """Apply the changes to users to redis"""
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
    AppYaml allows for an structured way to interact with any app.yaml main merlin configuration file.
    It helps to parse each component of the app.yaml and allow users to edit, configure and write the
    file.
    """

    default_filename = os.path.join(MERLIN_CONFIG_DIR, "app.yaml")
    data = {}
    broker_name = "broker"
    results_name = "results_backend"

    def __init__(self, filename: str = default_filename) -> None:
        if not os.path.exists(filename):
            filename = self.default_filename
        self.read(filename)

    def apply_server_config(self, server_config: ServerConfig):
        """Store the redis configuration"""
        redis_config = RedisConfig(server_config.container.get_config_path())

        self.data[self.broker_name]["name"] = server_config.container.get_image_type()
        self.data[self.broker_name]["username"] = "default"
        self.data[self.broker_name]["password"] = server_config.container.get_pass_file_path()
        self.data[self.broker_name]["server"] = redis_config.get_ip_address()
        self.data[self.broker_name]["port"] = redis_config.get_port()

        self.data[self.results_name]["name"] = server_config.container.get_image_type()
        self.data[self.results_name]["username"] = "default"
        self.data[self.results_name]["password"] = server_config.container.get_pass_file_path()
        self.data[self.results_name]["server"] = redis_config.get_ip_address()
        self.data[self.results_name]["port"] = redis_config.get_port()

    def update_data(self, new_data: dict):
        """Update the data dict with new entries"""
        self.data.update(new_data)

    def get_data(self):
        """Getter method to obtain the data"""
        return self.data

    def read(self, filename: str = default_filename):
        """Load in a yaml file and save it to the data attribute"""
        self.data = merlin.utils.load_yaml(filename)

    def write(self, filename: str = default_filename):
        """Given a filename, dump the data to the file"""
        with open(filename, "w+") as f:  # pylint: disable=C0103
            yaml.dump(self.data, f, yaml.Dumper)
