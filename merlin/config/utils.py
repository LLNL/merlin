###############################################################################
# Copyright (c) 2023, Lawrence Livermore National Security, LLC.
# Produced at the Lawrence Livermore National Laboratory
# Written by the Merlin dev team, listed in the CONTRIBUTORS file.
# <merlin@llnl.gov>
#
# LLNL-CODE-797170
# All rights reserved.
# This file is part of Merlin, Version: 1.12.0.
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
"""This module contains priority handling"""

import enum
from typing import Dict

from merlin.config.configfile import CONFIG


class Priority(enum.Enum):
    """Enumerated Priorities"""

    HIGH = 1
    MID = 2
    LOW = 3
    RETRY = 4


def is_rabbit_broker(broker: str) -> bool:
    """Check if the broker is a rabbit server"""
    return broker in ["rabbitmq", "amqps", "amqp"]


def is_redis_broker(broker: str) -> bool:
    """Check if the broker is a redis server"""
    return broker in ["redis", "rediss", "redis+socket"]


def determine_priority_map(broker_name: str) -> Dict[Priority, int]:
    """
    Returns the priority mapping for the given broker name.

    :param broker_name: The name of the broker that we need the priority map for
    :returns: The priority map associated with `broker_name`
    """
    if is_rabbit_broker(broker_name):
        return {Priority.LOW: 1, Priority.MID: 5, Priority.HIGH: 9, Priority.RETRY: 10}
    if is_redis_broker(broker_name):
        return {Priority.LOW: 10, Priority.MID: 5, Priority.HIGH: 2, Priority.RETRY: 1}

    raise ValueError(f"Unsupported broker name: {broker_name}")


def get_priority(priority: Priority) -> int:
    """
    Gets the priority level as an integer based on the broker.
    For a rabbit broker a low priority is 1 and high is 10. For redis it's the opposite.

    :param priority: The priority value that we want
    :returns: The priority value as an integer
    """
    if priority not in Priority:
        raise ValueError(f"Invalid priority: {priority}")

    priority_map = determine_priority_map(CONFIG.broker.name.lower())
    return priority_map.get(priority, priority_map[Priority.MID])  # Default to MID priority for unknown priorities
