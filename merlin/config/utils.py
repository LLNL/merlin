from merlin.config.configfile import CONFIG


def get_priority(priority):
    broker = CONFIG.broker.name.lower()
    # if broker not in ["", ""]:
    #    LOG.error(f"Unrecognized api '{api}'! Options: {apis}")
    priorities = ["high", "mid", "low"]
    if priority not in priorities:
        LOG.error(f"Unrecognized priority '{priority}'! Options: {priorities}")
    if priority == "mid":
        return 5
    if broker == "rabbitmq":
        if priority == "low":
            return 1
        if priority == "high":
            return 10
    if broker == "redis":
        if priority == "low":
            return 10
        if priority == "high":
            return 1
