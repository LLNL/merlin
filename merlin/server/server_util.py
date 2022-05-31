import logging
import hashlib
import os

import yaml


LOG = logging.getLogger("merlin")


def valid_ipv4(ip: str):
    if not ip:
        return False

    arr = ip.split(".")
    if len(arr) != 4:
        return False

    for i in arr:
        if int(i) < 0 and int(i) > 255:
            return False

    return True


def valid_port(port: int):
    if port > 0 and port < 65536:
        return True
    return False


class RedisConfig:
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

    def parse(self):
        self.entries = {}
        self.comments = {}
        with open(self.filename, "r+") as f:
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
        with open(self.filename, "w") as f:
            for entry in self.entry_order:
                f.write(self.comments[entry])
                f.write(f"{entry} {self.entries[entry]}\n")
            f.write(self.trailing_comments)

    def set_filename(self, filename):
        self.filename = filename

    def set_config_value(self, key: str, value: str):
        if key not in self.entries:
            return False
        self.entries[key] = value
        self.changed = True
        return True

    def get_config_value(self, key: str):
        if key in self.entries:
            return self.entries[key]
        return None

    def changes_made(self):
        return self.changed

    def set_ip_address(self, ipaddress):
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

    def set_port(self, port):
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

    def set_master_user(self, master_user):
        if master_user is None:
            return False
        # Set the main user for the container
        if not self.set_config_value("masteruser", master_user):
            LOG.error("Unable to set masteruser.")
            return False
        LOG.info(f"Master user is set to {master_user}")
        return True

    def set_password(self, password):
        if password is None:
            return False
        if os.path.exists(password):
            # Save the location of the password file in merlin_server_config
            if not self.set_config_value("requirepass", password):
                LOG.error("Unable to set password file for redis config")
                return False
        else:
            LOG.error(f"Password file {password} doesn't exist.")
            return False
        LOG.info(f"Password file set to {password}")
        return True

    def set_directory(self, directory):
        if directory is None:
            return False
        # Validate the directory input
        if os.path.exists(directory):
            # Set the save directory to the redis config
            if not self.set_config_value("dir", directory):
                LOG.error("Unable to set directory for redis config")
                return False
        else:
            LOG.error("Directory given does not exist.")
            return False
        LOG.info(f"Directory is set to {directory}")
        return True

    def set_snapshot_seconds(self, seconds):
        if seconds is None:
            return False
        # Set the snapshot second in the redis config
        value = self.get_config_value("save")
        if value is None:
            LOG.error("Unable to get exisiting parameter values for snapshot")
            return False
        else:
            value = value.split()
            value[0] = str(seconds)
            value = " ".join(value)
            if not self.set_config_value("save", value):
                LOG.error("Unable to set snapshot value seconds")
                return False
        LOG.info(f"Snapshot wait time is set to {seconds} seconds")
        return True

    def set_snapshot_changes(self, changes):
        if changes is None:
            return False
        # Set the snapshot changes into the redis config
        value = self.get_config_value("save")
        if value is None:
            LOG.error("Unable to get exisiting parameter values for snapshot")
            return False
        else:
            value = value.split()
            value[1] = str(changes)
            value = " ".join(value)
            if not self.set_config_value("save", value):
                LOG.error("Unable to set snapshot value seconds")
                return False
        LOG.info(f"Snapshot threshold is set to {changes} changes")
        return True

    def set_snapshot_file(self, file):
        if file is None:
            return False
        # Set the snapshot file in the redis config
        if not self.set_config_value("dbfilename", file):
            LOG.error("Unable to set snapshot_file name")
            return False

        LOG.info(f"Snapshot file is set to {file}")
        return True

    def set_append_mode(self, mode):
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

    def set_append_file(self, file):
        if file is None:
            return False
        # Set the append file in the redis config
        if not self.set_config_value("appendfilename", file):
            LOG.error("Unable to set append filename.")
            return False
        LOG.info(f"Append file is set to {file}")
        return True


class RedisUsers:
    class User:
        status = "on"
        hash_password = hashlib.sha256(b"password").hexdigest()
        keys = "*"
        channels = "@all"
        pattern = None
        def __init__(self,
                    status="on",
                    keys="*",
                    channels="@all",
                    password=None,
                    pattern=None) -> None:
            self.status = status
            self.keys = keys
            self.channels = channels
            self.pattern = pattern
            if password is not None:
                self.set_password(password)
        
        def parse_dict(self, dict):
            self.status = dict["status"]
            self.keys = dict["keys"]
            self.channels = dict["channels"]
            self.hash_password = dict["hash_password"]
            self.pattern = dict["pattern"]


        def get_user_dict(self) -> dict:
            self.status = "on"
            return {"status": self.status,
                    "hash_password": self.hash_password,
                    "keys": self.keys,
                    "channels": self.channels,
                    "pattern": self.pattern}
        
        def __repr__(self) -> str:
            return str(self.get_user())
        
        def __str__(self) -> str:
            self.__repr__()

        def set_password(self, password:str):
            self.hash_password = hashlib.sha256(bytes(password, 'utf-8')).hexdigest()

    filename = ""
    users = {}

    def __init__(self, filename) -> None:
        self.filename = filename
        if os.path.exists(self.filename):
            self.parse()

    def parse(self):
        with open(self.filename, "r") as f:
            self.users = yaml.load(f, yaml.Loader)
            for user in self.users:
                new_user = self.User()
                new_user.parse_dict(self.users[user])
                self.users[user] = new_user

    def write(self):
        data = self.users
        for key in data:
            data[key] = self.users[key].get_user_dict()
            print(self.users[key])
        with open(self.filename, "w") as f:
            yaml.dump(data, f, yaml.Dumper)

    def add_user(self,
                user,
                status="on",
                keys="*",
                channels="@all",
                password=None,
                pattern=None):
        if user in self.users:
            return False
        self.users[user] = self.User(status, keys, channels, password, pattern)
        return True

    def remove_user(self, user):
        if user in self.users:
            del self.users[user]
            return True
        return False
