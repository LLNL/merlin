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


if __name__ == "__main__":
    ipv4_tests = ["0.0.0.0", "255.255.255.255", "-0.0.0.0", "1.1.1.1", "040.30.-20", "123", "11.0", "53.12.75"]

    for i in ipv4_tests:
        print(i, valid_ipv4(i))


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

class RedisUsers:
    filename = ""

    def __init__(self) -> None:
        pass