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
