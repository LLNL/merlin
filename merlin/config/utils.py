###############################################################################
# Copyright (c) 2023, Lawrence Livermore National Security, LLC.
# Produced at the Lawrence Livermore National Laboratory
# Written by the Merlin dev team, listed in the CONTRIBUTORS file.
# <merlin@llnl.gov>
#
# LLNL-CODE-797170
# All rights reserved.
# This file is part of Merlin, Version: 1.12.2.
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
"""
This module provides utility functions and classes for handling broker priorities
and determining configurations for supported brokers such as RabbitMQ and Redis.
It includes functionality for mapping priority levels to integer values based on
the broker type and validating broker configurations.
"""

import enum
from typing import Dict

from merlin.config.configfile import CONFIG


class Priority(enum.Enum):
    """
    Enumerated Priorities.

    This enumeration defines the different priority levels that can be used
    for message handling with brokers.

    Attributes:
        HIGH (int): Represents the highest priority level. Numeric value: 1.
        MID (int): Represents the medium priority level. Numeric value: 2.
        LOW (int): Represents the lowest priority level. Numeric value: 3.
        RETRY (int): Represents the priority level for retrying messages. Numeric value: 4.
    """

    HIGH = 1
    MID = 2
    LOW = 3
    RETRY = 4


def is_rabbit_broker(broker_name: str) -> bool:
    """
    Check if the given broker is a RabbitMQ server.

    This function checks whether the provided broker name matches any of the
    RabbitMQ-related broker types.

    Args:
        broker_name: The name of the broker to check.

    Returns:
        True if the broker is a RabbitMQ server, False otherwise.
    """
    return broker_name in ["rabbitmq", "amqps", "amqp"]


def is_redis_broker(broker_name: str) -> bool:
    """
    Check if the given broker is a Redis server.

    This function checks whether the provided broker name matches any of the
    Redis-related broker types.

    Args:
        broker_name: The name of the broker to check.

    Returns:
        True if the broker is a Redis server, False otherwise.
    """
    return broker_name in ["redis", "rediss", "redis+socket"]


def determine_priority_map(broker_name: str) -> Dict[Priority, int]:
    """
    Determine the priority mapping for the given broker name.

    This function returns a mapping of [`Priority`][config.utils.Priority]
    enum values to integer priority levels based on the type of broker provided.

    Args:
        broker_name: The name of the broker for which to determine the priority map.

    Returns:
        (Dict[config.utils.Priority, int]): A dictionary mapping
            [`Priority`][config.utils.Priority] enum values to integer levels.

    Raises:
        ValueError: If the broker name is not supported.
    """
    if is_rabbit_broker(broker_name):
        return {Priority.LOW: 1, Priority.MID: 5, Priority.HIGH: 9, Priority.RETRY: 10}
    if is_redis_broker(broker_name):
        return {Priority.LOW: 10, Priority.MID: 5, Priority.HIGH: 2, Priority.RETRY: 1}

    raise ValueError(f"Unsupported broker name: {broker_name}")


def get_priority(priority: Priority) -> int:
    """
    Get the integer priority level for a given [`Priority`][config.utils.Priority]
    enum value.

    This function determines the priority level as an integer based on the
    broker configuration. For RabbitMQ brokers, lower numbers represent lower
    priorities, while for Redis brokers, higher numbers represent lower
    priorities.

    Args:
        priority (config.utils.Priority): The [`Priority`][config.utils.Priority]
            enum value for which to get the integer level.

    Returns:
        The integer priority level corresponding to the given [`Priority`][config.utils.Priority].

    Raises:
        ValueError: If the provided `priority` is invalid or not part of the
            [`Priority`][config.utils.Priority] enum.
    """
    priority_err_msg = f"Invalid priority: {priority}"
    try:
        # In python 3.12+ if something is not in the enum it will just return False
        if priority not in Priority:
            raise ValueError(priority_err_msg)
    # In python 3.11 and below, a TypeError is raised when looking for something in an enum that is not there
    except TypeError:
        raise ValueError(priority_err_msg)

    priority_map = determine_priority_map(CONFIG.broker.name.lower())
    return priority_map.get(priority, priority_map[Priority.MID])  # Default to MID priority for unknown priorities
