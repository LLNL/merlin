###############################################################################
# Copyright (c) 2023, Lawrence Livermore National Security, LLC.
# Produced at the Lawrence Livermore National Laboratory
# Written by the Merlin dev team, listed in the CONTRIBUTORS file.
# <merlin@llnl.gov>
#
# LLNL-CODE-797170
# All rights reserved.
# This file is part of Merlin, Version: 1.12.2b1.
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
This module stores a manager for redis connections.
"""
import logging
import redis

LOG = logging.getLogger(__name__)


class RedisConnectionManager:
    """
    A context manager for handling redis connections.
    This will ensure safe opening and closing of Redis connections.
    """

    def __init__(self, db_num: int):
        self.db_num = db_num
        self.connection = None

    def __enter__(self):
        self.connection = self.get_redis_connection()
        return self.connection

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connection:
            LOG.debug(f"MANAGER: Closing connection at db_num: {self.db_num}")
            self.connection.close()

    def get_redis_connection(self) -> redis.Redis:
        """
        Generic redis connection function to get the results backend redis server with a given db number increment.

        :return: Redis connection object that can be used to access values for the manager.
        """
        from merlin.config.results_backend import get_backend_password
        from merlin.config.configfile import CONFIG

        password_file = CONFIG.results_backend.password
        try:
            password = get_backend_password(password_file)
        except IOError:
            password = CONFIG.results_backend.password
        return redis.Redis(
            host=CONFIG.results_backend.server,
            port=CONFIG.results_backend.port,
            db=CONFIG.results_backend.db_num + self.db_num,  # Increment db_num to avoid conflicts
            username=CONFIG.results_backend.username,
            password=password,
            decode_responses=True,
            ssl=True,
            ssl_cert_reqs=CONFIG.results_backend.cert_reqs,
        )
