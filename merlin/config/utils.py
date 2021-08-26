import enum

from merlin.config.configfile import CONFIG


class Priority(enum.Enum):
    high = 1
    mid = 2
    low = 3

def is_rabbit_broker(broker):
    return broker in ["rabbitmq", "amqps", "amqp"]

def is_redis_broker(broker):
    return broker in ["redis", "rediss", "redis+socket"]

def get_priority(priority):
    broker = CONFIG.broker.name.lower()
    priorities = [Priority.high, Priority.mid, Priority.low]
    if priority not in priorities:
        raise ValueError(
            f"Unrecognized priority '{priority}'! Priority enum options: {[x.name for x in priorities]}"
        )
    if priority == Priority.mid:
        return 5
    if is_rabbit_broker(broker):
        if priority == Priority.low:
            return 1
        if priority == Priority.high:
            return 10
    if is_redis_broker(broker):
        if priority == Priority.low:
            return 10
        if priority == Priority.high:
            return 1
    raise ValueError(
        f"Function get_priority has reached unknown state! Maybe unsupported broker {broker}?"
    )
