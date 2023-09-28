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
"""This module contains priority handling"""

import enum
from typing import List

from merlin.config.configfile import CONFIG


class Priority(enum.Enum):
    """Enumerated Priorities"""

    HIGH = 1
    MID = 2
    LOW = 3


def is_rabbit_broker(broker: str) -> bool:
    """Check if the broker is a rabbit server"""
    return broker in ["rabbitmq", "amqps", "amqp"]


def is_redis_broker(broker: str) -> bool:
    """Check if the broker is a redis server"""
    return broker in ["redis", "rediss", "redis+socket"]


def get_priority(priority: Priority) -> int:
    """
    Get the priority based on the broker. For a rabbit broker
    a low priority is 1 and high is 10. For redis it's the opposite.
    :returns: An int representing the priority level
    """
    broker: str = CONFIG.broker.name.lower()
    priorities: List[Priority] = [Priority.HIGH, Priority.MID, Priority.LOW]
    if not isinstance(priority, Priority):
        raise TypeError(f"Unrecognized priority '{priority}'! Priority enum options: {[x.name for x in priorities]}")
    if priority == Priority.MID:
        return 5
    if is_rabbit_broker(broker):
        if priority == Priority.LOW:
            return 1
        if priority == Priority.HIGH:
            return 10
    if is_redis_broker(broker):
        if priority == Priority.LOW:
            return 10
        if priority == Priority.HIGH:
            return 1
    raise ValueError(f"Function get_priority has reached unknown state! Maybe unsupported broker {broker}?")
