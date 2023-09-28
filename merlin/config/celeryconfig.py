"""
Default celery configuration for merlin
"""

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

from merlin.log_formatter import FORMATS


DICT = {
    "task_serializer": "pickle",
    "accept_content": ["pickle"],
    "result_serializer": "pickle",
    "task_acks_late": True,
    "task_reject_on_worker_lost": True,
    "task_publish_retry_policy": {
        "interval_start": 10,
        "interval_step": 10,
        "interval_max": 60,
    },
    "redis_max_connections": 100000,
    "broker_transport_options": {
        "visibility_timeout": 60 * 60 * 24,
        "max_connections": 100,
    },
    "broker_pool_limit": 0,
    "task_default_queue": "merlin",
    "worker_log_color": True,
    "worker_log_format": FORMATS["DEFAULT"],
    "worker_task_log_format": FORMATS["WORKER"],
}
