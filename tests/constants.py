##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
This module will store constants that will be used throughout our test suite.
"""

SERVER_PASS = "merlin-test-server"

CERT_FILES = {
    "ssl_cert": "test-rabbit-client-cert.pem",
    "ssl_ca": "test-mysql-ca-cert.pem",
    "ssl_key": "test-rabbit-client-key.pem",
}
