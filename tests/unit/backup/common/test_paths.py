"""Unit tests for the backup common paths module (common/paths.py)."""

import os
import sys
from pathlib import Path
from unittest.mock import patch

from ami.dataops.backup.common import paths
from ami.dataops.backup.common.paths import _WorkspaceRootCache


class TestPaths:
    """Unit tests for the path utility functions."""

    def test_get_workspace_root_from_env(self):
        """Test finding workspace root via environment variable."""
        test_root = "/fake/project/root"
        original_cache = _WorkspaceRootCache._value
        _WorkspaceRootCache._value = None
        try:
            with patch.dict(os.environ, {"AMI_PROJECT_ROOT": test_root}):
                root = paths.get_workspace_root()
                assert str(root) == test_root
        finally:
            _WorkspaceRootCache._value = original_cache

    def test_get_workspace_root_from_file(self):
        """Test finding workspace root from file location finds .boot-linux."""
        original_cache = _WorkspaceRootCache._value
        _WorkspaceRootCache._value = None
        try:
            root = paths.get_workspace_root()
            # Should find AMI-AGENTS root (has .boot-linux)
            assert (root / ".boot-linux").is_dir()
        finally:
            _WorkspaceRootCache._value = original_cache

    def test_get_project_root_alias(self):
        """Test get_project_root is an alias for get_workspace_root."""
        assert paths.get_project_root is paths.get_workspace_root

    @patch("ami.dataops.backup.common.paths.get_workspace_root")
    def test_setup_sys_path(self, mock_get_root):
        """Test that workspace root is added to sys.path."""
        mock_root = Path("/fake/root")
        mock_get_root.return_value = mock_root

        original_path = sys.path[:]
        try:
            paths.setup_sys_path()
            assert str(mock_root) in sys.path
            assert sys.path[0] == str(mock_root)
        finally:
            sys.path = original_path

    @patch("ami.dataops.backup.common.paths.get_workspace_root")
    @patch("pathlib.Path.exists")
    def test_find_gcloud_local(self, mock_exists, mock_get_root):
        """Test finding bootstrap ami-gcloud symlink first."""
        mock_root = Path("/fake/root")
        mock_get_root.return_value = mock_root

        mock_exists.return_value = True

        result = paths.find_gcloud()
        assert "/fake/root/.boot-linux/bin/ami-gcloud" in str(result)

    @patch("ami.dataops.backup.common.paths.get_workspace_root")
    @patch("shutil.which")
    def test_find_gcloud_system(self, mock_which, mock_get_root):
        """Test finding system gcloud binary when local is missing."""
        mock_root = Path("/fake/root")
        mock_get_root.return_value = mock_root

        with patch.object(Path, "exists", return_value=False):
            mock_which.return_value = "/usr/bin/gcloud"
            result = paths.find_gcloud()
            assert result == "/usr/bin/gcloud"

    @patch("ami.dataops.backup.common.paths.get_workspace_root")
    @patch("shutil.which")
    def test_find_gcloud_not_found(self, mock_which, mock_get_root):
        """Test find_gcloud returns None when not found anywhere."""
        mock_root = Path("/fake/root")
        mock_get_root.return_value = mock_root

        with patch.object(Path, "exists", return_value=False):
            mock_which.return_value = None
            result = paths.find_gcloud()
            assert result is None
