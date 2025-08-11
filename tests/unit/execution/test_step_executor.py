##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Unit tests for GenericStepExecutor.

Tests the backend-agnostic step execution logic extracted from the original
merlin_step function for use across different task distribution backends.
"""

import unittest
from unittest.mock import MagicMock, patch, Mock
from typing import Dict, Any

from merlin.common.enums import ReturnCode
from merlin.execution.step_executor import GenericStepExecutor


class TestGenericStepExecutor(unittest.TestCase):
    """Test cases for GenericStepExecutor."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.executor = GenericStepExecutor()
        
        # Create mock step
        self.mock_step = MagicMock()
        self.mock_step.name.return_value = "test_step"
        self.mock_step.get_workspace.return_value = "/tmp/test_workspace"
        self.mock_step.max_retries = 3
        self.mock_step.execute.return_value = ReturnCode.OK
        self.mock_step.mstep = MagicMock()
        
        # Mock adapter config
        self.adapter_config = {'type': 'local'}
        
    def test_initialization(self):
        """Test GenericStepExecutor initializes properly."""
        executor = GenericStepExecutor()
        self.assertIsNotNone(executor)
        
    def test_successful_step_execution(self):
        """Test successful step execution."""
        # Mock successful execution
        self.mock_step.execute.return_value = ReturnCode.OK
        
        with patch('os.path.exists', return_value=False), \
             patch('builtins.open', MagicMock()):
            
            result = self.executor.execute_step(
                self.mock_step, 
                self.adapter_config,
                retry_count=0
            )
            
        # Verify execution
        self.mock_step.execute.assert_called_once_with(self.adapter_config)
        self.mock_step.mstep.mark_end.assert_called_once_with(ReturnCode.OK)
        self.assertEqual(result.return_code, ReturnCode.OK)
        self.assertEqual(result.step_name, "test_step")
        
    def test_step_already_finished(self):
        """Test skipping already finished step."""
        with patch('os.path.exists', return_value=True):
            result = self.executor.execute_step(
                self.mock_step,
                self.adapter_config
            )
            
        # Should not execute step if already finished
        self.mock_step.execute.assert_not_called()
        self.assertEqual(result.return_code, ReturnCode.OK)
        
    def test_step_execution_with_soft_fail(self):
        """Test step execution with soft failure."""
        self.mock_step.execute.return_value = ReturnCode.SOFT_FAIL
        
        with patch('os.path.exists', return_value=False):
            result = self.executor.execute_step(
                self.mock_step,
                self.adapter_config
            )
            
        self.assertEqual(result.return_code, ReturnCode.SOFT_FAIL)
        self.mock_step.mstep.mark_end.assert_called_once_with(ReturnCode.SOFT_FAIL)
        
    def test_step_execution_with_hard_fail(self):
        """Test step execution with hard failure."""
        self.mock_step.execute.return_value = ReturnCode.HARD_FAIL
        self.mock_step.get_task_queue.return_value = 'test_queue'
        
        with patch('os.path.exists', return_value=False), \
             patch('merlin.execution.step_executor.shutdown_workers') as mock_shutdown:
            
            mock_shutdown_sig = MagicMock()
            mock_shutdown.s.return_value = mock_shutdown_sig
            
            result = self.executor.execute_step(
                self.mock_step,
                self.adapter_config
            )
            
        self.assertEqual(result.return_code, ReturnCode.HARD_FAIL)
        self.mock_step.mstep.mark_end.assert_called_once_with(ReturnCode.HARD_FAIL)
        
        # Verify shutdown was scheduled
        mock_shutdown.s.assert_called_once_with(['test_queue'])
        mock_shutdown_sig.set.assert_called_once_with(queue='test_queue')
        mock_shutdown_sig.apply_async.assert_called_once()
        
    def test_step_execution_with_restart(self):
        """Test step execution with restart return code."""
        self.mock_step.execute.return_value = ReturnCode.RESTART
        
        with patch('os.path.exists', return_value=False):
            result = self.executor.execute_step(
                self.mock_step,
                self.adapter_config,
                retry_count=1
            )
            
        self.assertEqual(result.return_code, ReturnCode.RESTART)
        self.mock_step.mstep.mark_restart.assert_called_once()
        
    def test_step_execution_with_retry(self):
        """Test step execution with retry return code."""
        self.mock_step.execute.return_value = ReturnCode.RETRY
        
        with patch('os.path.exists', return_value=False):
            result = self.executor.execute_step(
                self.mock_step,
                self.adapter_config,
                retry_count=2
            )
            
        self.assertEqual(result.return_code, ReturnCode.RETRY)
        self.mock_step.mstep.mark_restart.assert_called_once()
        
    def test_step_execution_retry_limit_exceeded(self):
        """Test step execution when retry limit is exceeded."""
        self.mock_step.execute.return_value = ReturnCode.RESTART
        
        with patch('os.path.exists', return_value=False):
            result = self.executor.execute_step(
                self.mock_step,
                self.adapter_config,
                retry_count=3,  # equals max_retries
                max_retries=3
            )
            
        # Should convert to SOFT_FAIL when retry limit exceeded
        self.assertEqual(result.return_code, ReturnCode.SOFT_FAIL)
        self.mock_step.mstep.mark_end.assert_called_once_with(ReturnCode.SOFT_FAIL, max_retries=True)
        
    def test_step_execution_with_dry_ok(self):
        """Test step execution with DRY_OK return code."""
        self.mock_step.execute.return_value = ReturnCode.DRY_OK
        
        with patch('os.path.exists', return_value=False):
            result = self.executor.execute_step(
                self.mock_step,
                self.adapter_config
            )
            
        self.assertEqual(result.return_code, ReturnCode.DRY_OK)
        
    def test_step_execution_with_stop_workers(self):
        """Test step execution with STOP_WORKERS return code."""
        self.mock_step.execute.return_value = ReturnCode.STOP_WORKERS
        self.mock_step.get_task_queue.return_value = 'test_queue'
        
        with patch('os.path.exists', return_value=False), \
             patch('merlin.execution.step_executor.shutdown_workers') as mock_shutdown:
            
            mock_shutdown_sig = MagicMock()
            mock_shutdown.s.return_value = mock_shutdown_sig
            
            result = self.executor.execute_step(
                self.mock_step,
                self.adapter_config
            )
            
        self.assertEqual(result.return_code, ReturnCode.STOP_WORKERS)
        
        # Verify shutdown all workers was scheduled
        mock_shutdown.s.assert_called_once_with(None)
        
    def test_step_execution_with_raise_error(self):
        """Test step execution with RAISE_ERROR return code."""
        self.mock_step.execute.return_value = ReturnCode.RAISE_ERROR
        
        with patch('os.path.exists', return_value=False):
            with self.assertRaises(Exception) as context:
                self.executor.execute_step(
                    self.mock_step,
                    self.adapter_config
                )
                
            self.assertIn("Exception raised by request from the user", str(context.exception))
            
    def test_step_execution_with_unknown_return_code(self):
        """Test step execution with unknown return code."""
        # Use a return code that isn't handled specifically
        self.mock_step.execute.return_value = 999  # Unknown code
        
        with patch('os.path.exists', return_value=False):
            result = self.executor.execute_step(
                self.mock_step,
                self.adapter_config
            )
            
        # Should still return the unknown code
        self.assertEqual(result.return_code, 999)
        
    def test_step_execution_with_next_in_chain(self):
        """Test step execution with next_in_chain parameter."""
        next_step = MagicMock()
        
        with patch('os.path.exists', return_value=False), \
             patch('builtins.open', MagicMock()):
            
            result = self.executor.execute_step(
                self.mock_step,
                self.adapter_config,
                next_in_chain=next_step
            )
            
        # Verify execution completed successfully
        self.assertEqual(result.return_code, ReturnCode.OK)
        
    def test_step_execution_with_missing_workspace(self):
        """Test step execution when workspace creation might fail."""
        self.mock_step.get_workspace.return_value = "/nonexistent/path"
        
        with patch('os.path.exists', return_value=False):
            # Should handle gracefully even if workspace doesn't exist
            result = self.executor.execute_step(
                self.mock_step,
                self.adapter_config
            )
            
        # Execution should still proceed
        self.mock_step.execute.assert_called_once()
        
    def test_step_execution_exception_handling(self):
        """Test step execution when step.execute raises exception."""
        self.mock_step.execute.side_effect = Exception("Execution failed")
        
        with patch('os.path.exists', return_value=False):
            # Should propagate exceptions from step execution
            with self.assertRaises(Exception):
                self.executor.execute_step(
                    self.mock_step,
                    self.adapter_config
                )
                
    def test_step_execution_file_creation(self):
        """Test that finished file is created on successful completion."""
        with patch('os.path.exists', return_value=False), \
             patch('builtins.open', MagicMock()) as mock_open:
            
            result = self.executor.execute_step(
                self.mock_step,
                self.adapter_config
            )
            
        # Verify finished file was created
        expected_path = "/tmp/test_workspace/MERLIN_FINISHED"
        mock_open.assert_called_once_with(expected_path, "a")
        
    def test_different_adapter_configs(self):
        """Test execution with different adapter configurations."""
        configs = [
            {'type': 'local'},
            {'type': 'slurm', 'nodes': 4},
            {'type': 'flux', 'tasks_per_node': 8}
        ]
        
        for config in configs:
            with patch('os.path.exists', return_value=False), \
                 patch('builtins.open', MagicMock()):
                
                result = self.executor.execute_step(
                    self.mock_step,
                    config
                )
                
            # Each config should execute successfully
            self.assertEqual(result.return_code, ReturnCode.OK)
            
        # Verify step was executed for each config
        self.assertEqual(self.mock_step.execute.call_count, len(configs))
        
    def test_execution_result_object(self):
        """Test ExecutionResult object contains expected data."""
        with patch('os.path.exists', return_value=False), \
             patch('builtins.open', MagicMock()):
            
            result = self.executor.execute_step(
                self.mock_step,
                self.adapter_config
            )
            
        # Verify result object structure
        self.assertEqual(result.return_code, ReturnCode.OK)
        self.assertEqual(result.step_name, "test_step")
        self.assertEqual(result.step_dir, "/tmp/test_workspace")
        self.assertTrue(result.finished_filename.endswith("MERLIN_FINISHED"))


if __name__ == '__main__':
    unittest.main()