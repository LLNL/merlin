"""
Tests for the merlin/config/utils.py module.
"""

import pytest

from merlin.config.configfile import CONFIG
from merlin.config.utils import Priority, get_priority, is_rabbit_broker, is_redis_broker


def test_is_rabbit_broker():
    """Test the `is_rabbit_broker` by passing in rabbit as the broker"""
    assert is_rabbit_broker("rabbitmq") is True
    assert is_rabbit_broker("amqp") is True
    assert is_rabbit_broker("amqps") is True


def test_is_rabbit_broker_invalid():
    """Test the `is_rabbit_broker` by passing in an invalid broker"""
    assert is_rabbit_broker("redis") is False
    assert is_rabbit_broker("") is False


def test_is_redis_broker():
    """Test the `is_redis_broker` by passing in redis as the broker"""
    assert is_redis_broker("redis") is True
    assert is_redis_broker("rediss") is True
    assert is_redis_broker("redis+socket") is True


def test_is_redis_broker_invalid():
    """Test the `is_redis_broker` by passing in an invalid broker"""
    assert is_redis_broker("rabbitmq") is False
    assert is_redis_broker("") is False


def test_get_priority_rabbit_broker(rabbit_broker_config: "fixture"):  # noqa: F821
    """
    Test the `get_priority` function with rabbit as the broker.
    Low priority for rabbit is 1 and high is 10.

    :param rabbit_broker_config: A fixture to set the CONFIG object to a test configuration that we'll use here
    """
    assert get_priority(Priority.LOW) == 1
    assert get_priority(Priority.MID) == 5
    assert get_priority(Priority.HIGH) == 10


def test_get_priority_redis_broker(redis_broker_config: "fixture"):  # noqa: F821
    """
    Test the `get_priority` function with redis as the broker.
    Low priority for redis is 10 and high is 1.

    :param redis_broker_config: A fixture to set the CONFIG object to a test configuration that we'll use here
    """
    assert get_priority(Priority.LOW) == 10
    assert get_priority(Priority.MID) == 5
    assert get_priority(Priority.HIGH) == 1


def test_get_priority_invalid_broker(redis_broker_config: "fixture"):  # noqa: F821
    """
    Test the `get_priority` function with an invalid broker.
    This should raise a ValueError.

    :param redis_broker_config: A fixture to set the CONFIG object to a test configuration that we'll use here
    """
    CONFIG.broker.name = "invalid"
    with pytest.raises(ValueError) as excinfo:
        get_priority(Priority.LOW)
    assert "Function get_priority has reached unknown state! Maybe unsupported broker invalid?" in str(excinfo.value)


def test_get_priority_invalid_priority(redis_broker_config: "fixture"):  # noqa: F821
    """
    Test the `get_priority` function with an invalid priority.
    This should raise a TypeError.

    :param redis_broker_config: A fixture to set the CONFIG object to a test configuration that we'll use here
    """
    with pytest.raises(TypeError) as excinfo:
        get_priority("invalid_priority")
    assert "Unrecognized priority 'invalid_priority'!" in str(excinfo.value)
