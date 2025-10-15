##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for file system utility functions of the `merlin/utils.py` module.
"""

from pathlib import Path

from pytest_mock import MockerFixture

from merlin.utils import get_accessible_mounts


class TestGetAccessibleMounts:
    """Tests for the get_accessible_mounts function."""

    def test_includes_root_by_default(self, mocker: MockerFixture):
        """
        Test that root filesystem ('/') is included when exclude_root=False (default behavior).

        Args:
            mocker: Pytest mocker fixture.
        """
        mock_partitions = [
            mocker.Mock(mountpoint="/"),
            mocker.Mock(mountpoint="/home"),
        ]
        
        mocker.patch("psutil.disk_partitions", return_value=mock_partitions)

        result = get_accessible_mounts(exclude_root=False)

        assert Path("/") in result
        assert Path("/home") in result
        assert len(result) == 2

    def test_excludes_root_when_requested(self, mocker: MockerFixture):
        """
        Test that root filesystem is excluded when exclude_root=True.

        Args:
            mocker: Pytest mocker fixture.
        """
        mock_partitions = [
            mocker.Mock(mountpoint="/"),
            mocker.Mock(mountpoint="/home"),
            mocker.Mock(mountpoint="/mnt/data"),
        ]
        
        mocker.patch("psutil.disk_partitions", return_value=mock_partitions)

        result = get_accessible_mounts(exclude_root=True)

        assert Path("/") not in result
        assert Path("/home") in result
        assert Path("/mnt/data") in result
        assert len(result) == 2

    def test_handles_empty_partitions(self, mocker: MockerFixture):
        """
        Test behavior when no partitions are found.

        Args:
            mocker: Pytest mocker fixture.
        """
        mocker.patch("psutil.disk_partitions", return_value=[])

        result = get_accessible_mounts()

        assert result == set()
        assert len(result) == 0

    def test_handles_only_root_partition(self, mocker: MockerFixture):
        """
        Test behavior when only root partition exists.

        Args:
            mocker: Pytest mocker fixture.
        """
        mock_partitions = [mocker.Mock(mountpoint="/")]
        
        mocker.patch("psutil.disk_partitions", return_value=mock_partitions)

        result = get_accessible_mounts()

        assert result == {Path("/")}
        assert len(result) == 1

    def test_handles_only_root_partition_with_exclude(self, mocker: MockerFixture):
        """
        Test that excluding root when only root exists returns empty set.

        Args:
            mocker: Pytest mocker fixture.
        """
        mock_partitions = [mocker.Mock(mountpoint="/")]
        
        mocker.patch("psutil.disk_partitions", return_value=mock_partitions)

        result = get_accessible_mounts(exclude_root=True)

        assert result == set()
        assert len(result) == 0

    def test_handles_duplicate_mount_points(self, mocker: MockerFixture):
        """
        Test that duplicate mount points are deduplicated (set behavior).

        Args:
            mocker: Pytest mocker fixture.
        """
        # Some systems might have multiple partitions with same mountpoint
        mock_partitions = [
            mocker.Mock(mountpoint="/home"),
            mocker.Mock(mountpoint="/home"),  # Duplicate
            mocker.Mock(mountpoint="/mnt/data"),
        ]
        
        mocker.patch("psutil.disk_partitions", return_value=mock_partitions)

        result = get_accessible_mounts()

        assert len(result) == 2  # /home should only appear once
        assert Path("/home") in result
        assert Path("/mnt/data") in result

    def test_handles_mount_points_with_spaces(self, mocker: MockerFixture):
        """
        Test handling of mount points with spaces in the path.

        Args:
            mocker: Pytest mocker fixture.
        """
        mock_partitions = [
            mocker.Mock(mountpoint="/mnt/my mount"),
            mocker.Mock(mountpoint="/mnt/another mount point"),
        ]
        
        mocker.patch("psutil.disk_partitions", return_value=mock_partitions)

        result = get_accessible_mounts()

        assert Path("/mnt/my mount") in result
        assert Path("/mnt/another mount point") in result

    def test_handles_nested_mount_points(self, mocker: MockerFixture):
        """
        Test that nested mount points are all included.

        Args:
            mocker: Pytest mocker fixture.
        """
        mock_partitions = [
            mocker.Mock(mountpoint="/"),
            mocker.Mock(mountpoint="/p"),
            mocker.Mock(mountpoint="/p/lustre1"),
            mocker.Mock(mountpoint="/p/lustre2"),
            mocker.Mock(mountpoint="/p/lustre3"),
        ]
        
        mocker.patch("psutil.disk_partitions", return_value=mock_partitions)

        result = get_accessible_mounts()

        assert Path("/") in result
        assert Path("/p") in result
        assert Path("/p/lustre1") in result
        assert Path("/p/lustre2") in result
        assert Path("/p/lustre3") in result
        assert len(result) == 5

    def test_exclude_root_with_nested_mounts(self, mocker: MockerFixture):
        """
        Test that exclude_root only excludes '/', not nested mounts.

        Args:
            mocker: Pytest mocker fixture.
        """
        mock_partitions = [
            mocker.Mock(mountpoint="/"),
            mocker.Mock(mountpoint="/p"),
            mocker.Mock(mountpoint="/p/lustre3"),
        ]
        
        mocker.patch("psutil.disk_partitions", return_value=mock_partitions)

        result = get_accessible_mounts(exclude_root=True)

        assert Path("/") not in result
        assert Path("/p") in result
        assert Path("/p/lustre3") in result
        assert len(result) == 2

    def test_handles_windows_style_mount_points(self, mocker: MockerFixture):
        """
        Test handling of Windows-style drive letters (for cross-platform compatibility).

        Args:
            mocker: Pytest mocker fixture.
        """
        mock_partitions = [
            mocker.Mock(mountpoint="C:\\"),
            mocker.Mock(mountpoint="D:\\"),
        ]
        
        mocker.patch("psutil.disk_partitions", return_value=mock_partitions)

        result = get_accessible_mounts()

        # Path will handle platform-specific paths
        assert Path("C:\\") in result or Path("C:/") in result
        assert len(result) == 2
