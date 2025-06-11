##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
This module houses dataclasses to be used with pytest fixtures.
"""

from dataclasses import dataclass

from tests.fixture_types import FixtureInt, FixtureModification, FixtureRedis, FixtureStr


@dataclass
class RedisBrokerAndBackend:
    """
    Data class to encapsulate all Redis-related fixtures required for
    establishing connections to Redis for both the broker and backend.

    This class simplifies the management of Redis fixtures by grouping
    them into a single object, reducing the number of individual fixture
    imports needed in tests that require Redis functionality.

    Attributes:
        client: A fixture that provides a client for interacting
            with the Redis server.
        server: A fixture providing the connection string to the
            Redis server instance.
        broker_config: A fixture that modifies the configuration
            to point to the Redis server used as the message broker.
        results_backend_config: A fixture that modifies the
            configuration to point to the Redis server used for storing
            results.
    """

    client: FixtureRedis
    server: FixtureStr
    results_backend_config: FixtureModification
    broker_config: FixtureModification


@dataclass
class FeatureDemoSetup:
    """
    Data class to encapsulate all feature-demo-related fixtures required
    for testing the feature demo workflow.

    This class simplifies the management of feature demo setup fixtures
    by grouping them into a single object, reducing the number of individual
    fixture imports needed in tests that require feature demo setup.

    Attributes:
        testing_dir: The path to the temp output directory for feature_demo workflow tests.
        num_samples: An integer representing the number of samples to use in the feature_demo
            workflow.
        name: A string representing the name to use for the feature_demo workflow.
        path: The path to the feature demo YAML file.
    """

    testing_dir: FixtureStr
    num_samples: FixtureInt
    name: FixtureStr
    path: FixtureStr


@dataclass
class ChordErrorSetup:
    """
    Data class to encapsulate all chord-error-related fixtures required
    for testing the chord error workflow.

    This class simplifies the management of chord error setup fixtures
    by grouping them into a single object, reducing the number of individual
    fixture imports needed in tests that require chord error setup.

    Attributes:
        testing_dir: The path to the temp output directory for chord_err workflow tests.
        name: A string representing the name to use for the chord_err workflow.
        path: The path to the chord error YAML file.
    """

    testing_dir: FixtureStr
    name: FixtureStr
    path: FixtureStr
