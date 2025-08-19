##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Generate executable scripts for backend-independent task execution.

This module provides the TaskScriptGenerator class which creates standalone
executable scripts from Merlin task configurations, eliminating Celery context
dependencies and enabling message size optimization through reference-based
data passing.
"""

import os
import json
import tempfile
import hashlib
import time
from pathlib import Path
from typing import Dict, Any, Optional
from dataclasses import dataclass


@dataclass
class ScriptConfig:
    """Configuration for script generation."""
    task_id: str
    task_type: str
    workspace_path: str
    step_config: Dict[str, Any]
    environment_vars: Dict[str, str]
    shared_storage_path: str = "/shared/storage"
    execution_timeout: int = 3600  # 1 hour default


class TaskScriptGenerator:
    """Generate standalone executable scripts for Merlin tasks."""
    
    def __init__(self, shared_storage_path: str = "/shared/storage"):
        self.shared_storage_path = Path(shared_storage_path)
        self.scripts_dir = self.shared_storage_path / "scripts"
        self.configs_dir = self.shared_storage_path / "configs"
        self.workspace_dir = self.shared_storage_path / "workspace"
        
        # Ensure directories exist
        for directory in [self.scripts_dir, self.configs_dir, self.workspace_dir]:
            directory.mkdir(parents=True, exist_ok=True)
    
    def generate_merlin_step_script(self, config: ScriptConfig) -> Dict[str, str]:
        """Generate script for merlin_step task execution."""
        
        # Create unique script filename
        script_hash = hashlib.md5(f"{config.task_id}_{config.task_type}".encode()).hexdigest()[:8]
        script_filename = f"merlin_step_{config.task_id}_{script_hash}.sh"
        config_filename = f"step_config_{config.task_id}_{script_hash}.json"
        
        script_path = self.scripts_dir / script_filename
        config_path = self.configs_dir / config_filename
        workspace_path = self.workspace_dir / config.task_id
        
        # Ensure workspace exists
        workspace_path.mkdir(parents=True, exist_ok=True)
        
        # Generate script content
        script_content = self._generate_step_script_content(config, config_path, workspace_path)
        
        # Write script file
        with open(script_path, 'w') as f:
            f.write(script_content)
        
        # Make script executable
        os.chmod(script_path, 0o755)
        
        # Write configuration file
        with open(config_path, 'w') as f:
            json.dump(config.step_config, f, indent=2)
        
        return {
            'script_path': str(script_path),
            'config_path': str(config_path),
            'workspace_path': str(workspace_path),
            'script_filename': script_filename,
            'config_filename': config_filename
        }
    
    def _generate_step_script_content(self, config: ScriptConfig, config_path: Path, workspace_path: Path) -> str:
        """Generate the actual script content for step execution."""
        
        # Extract step information
        step_config = config.step_config
        step_name = step_config.get('name', 'unknown_step')
        step_run = step_config.get('run', {})
        step_cmd = step_run.get('cmd', '')
        
        # Build environment variables section
        env_vars_section = self._build_env_vars_section(config.environment_vars)
        
        # Build pre-execution setup
        setup_section = self._build_setup_section(step_config)
        
        # Build main command execution
        command_section = self._build_command_section(step_cmd, step_run)
        
        # Build post-execution cleanup
        cleanup_section = self._build_cleanup_section()
        
        script_template = f"""#!/bin/bash
# Generated Merlin Step Execution Script
# Task ID: {config.task_id}
# Task Type: {config.task_type}
# Step Name: {step_name}
# Generated: $(date)

set -e  # Exit immediately on error
set -u  # Exit on undefined variables
set -o pipefail  # Exit on pipe failures

# Script configuration
TASK_ID="{config.task_id}"
TASK_TYPE="{config.task_type}"
STEP_NAME="{step_name}"
CONFIG_PATH="{config_path}"
WORKSPACE_PATH="{workspace_path}"
EXECUTION_TIMEOUT={config.execution_timeout}

# Logging functions
log_info() {{
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [INFO] $1" >&2
}}

log_error() {{
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [ERROR] $1" >&2
}}

log_warn() {{
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [WARN] $1" >&2
}}

# Error handling
cleanup_on_error() {{
    local exit_code=$?
    log_error "Script execution failed with exit code $exit_code"
    {cleanup_section}
    exit $exit_code
}}

trap cleanup_on_error ERR

# Start execution
log_info "Starting execution of step: $STEP_NAME"
log_info "Task ID: $TASK_ID"
log_info "Workspace: $WORKSPACE_PATH"

{env_vars_section}

{setup_section}

# Change to workspace directory
cd "$WORKSPACE_PATH"
log_info "Changed to workspace directory: $(pwd)"

# Load step configuration
if [[ -f "$CONFIG_PATH" ]]; then
    log_info "Loading step configuration from $CONFIG_PATH"
    # Configuration is available as JSON file for script access
else
    log_warn "No configuration file found at $CONFIG_PATH"
fi

# Set execution timeout
timeout $EXECUTION_TIMEOUT bash -c '{command_section}' || {{
    if [[ $? -eq 124 ]]; then
        log_error "Command timed out after $EXECUTION_TIMEOUT seconds"
        exit 124
    else
        log_error "Command failed with exit code $?"
        exit $?
    fi
}}

# Capture exit code
EXIT_CODE=$?
log_info "Command completed with exit code: $EXIT_CODE"

# Generate result metadata
cat > step_result.json << EOF
{{
    "task_id": "$TASK_ID",
    "step_name": "$STEP_NAME",
    "exit_code": $EXIT_CODE,
    "execution_time": $(date +%s),
    "workspace": "$WORKSPACE_PATH",
    "hostname": "$(hostname)",
    "status": "$(if [[ $EXIT_CODE -eq 0 ]]; then echo 'completed'; else echo 'failed'; fi)"
}}
EOF

{cleanup_section}

log_info "Script execution completed successfully"
exit $EXIT_CODE
"""
        return script_template
    
    def _build_env_vars_section(self, env_vars: Dict[str, str]) -> str:
        """Build environment variables section."""
        if not env_vars:
            return "# No additional environment variables"
        
        lines = ["# Environment variables"]
        for key, value in env_vars.items():
            # Escape special characters for bash
            escaped_value = value.replace('"', '\\"').replace('$', '\\$')
            lines.append(f'export {key}="{escaped_value}"')
        
        return "\n".join(lines)
    
    def _build_setup_section(self, step_config: Dict[str, Any]) -> str:
        """Build pre-execution setup section."""
        setup_lines = [
            "# Pre-execution setup",
            "umask 022  # Set default permissions"
        ]
        
        # Add any step-specific setup
        step_run = step_config.get('run', {})
        
        # Handle task_type specific setup
        task_type = step_run.get('task_type', 'local')
        if task_type == 'slurm':
            setup_lines.extend([
                "# SLURM environment setup",
                "export SLURM_JOB_ID=${SLURM_JOB_ID:-'local'}",
                "export SLURM_PROCID=${SLURM_PROCID:-0}"
            ])
        elif task_type == 'flux':
            setup_lines.extend([
                "# Flux environment setup", 
                "export FLUX_JOB_ID=${FLUX_JOB_ID:-'local'}"
            ])
        
        return "\n".join(setup_lines)
    
    def _build_command_section(self, step_cmd: str, step_run: Dict[str, Any]) -> str:
        """Build the main command execution section."""
        if not step_cmd:
            return 'log_error "No command specified for execution"; exit 1'
        
        # Handle different task types
        task_type = step_run.get('task_type', 'local')
        
        if task_type == 'local':
            return f"""
log_info "Executing local command: {step_cmd}"
{step_cmd}
"""
        elif task_type == 'slurm':
            return f"""
log_info "Submitting SLURM job: {step_cmd}"
sbatch --wait {step_cmd}
"""
        elif task_type == 'flux':
            return f"""
log_info "Submitting Flux job: {step_cmd}"
flux submit --wait {step_cmd}
"""
        else:
            return f"""
log_info "Executing command with task_type '{task_type}': {step_cmd}"
{step_cmd}
"""
    
    def _build_cleanup_section(self) -> str:
        """Build post-execution cleanup section."""
        return """
# Post-execution cleanup
log_info "Performing cleanup operations"
# Add any cleanup operations here
"""

    def generate_sample_expansion_script(self, config: ScriptConfig, sample_range: tuple) -> Dict[str, str]:
        """Generate script for sample expansion tasks."""
        
        script_hash = hashlib.md5(f"{config.task_id}_expand".encode()).hexdigest()[:8]
        script_filename = f"expand_samples_{config.task_id}_{script_hash}.sh"
        config_filename = f"expand_config_{config.task_id}_{script_hash}.json"
        
        script_path = self.scripts_dir / script_filename
        config_path = self.configs_dir / config_filename
        workspace_path = self.workspace_dir / config.task_id
        
        # Ensure workspace exists
        workspace_path.mkdir(parents=True, exist_ok=True)
        
        start_idx, end_idx = sample_range
        
        script_content = f"""#!/bin/bash
# Generated Sample Expansion Script
# Task ID: {config.task_id}
# Sample Range: {start_idx} to {end_idx}

set -e

log_info() {{
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [INFO] $1" >&2
}}

log_info "Starting sample expansion for range {start_idx}-{end_idx}"

# Load samples configuration
SAMPLES_CONFIG=$(cat "{config_path}")

# Process sample range
for ((i={start_idx}; i<{end_idx}; i++)); do
    log_info "Processing sample $i"
    
    # Create sample-specific workspace
    SAMPLE_WORKSPACE="{workspace_path}/sample_$i"
    mkdir -p "$SAMPLE_WORKSPACE"
    
    # Generate sample-specific script
    # Implementation depends on sample structure
    
    log_info "Completed processing sample $i"
done

log_info "Sample expansion completed for range {start_idx}-{end_idx}"
"""
        
        # Write script and config
        with open(script_path, 'w') as f:
            f.write(script_content)
        os.chmod(script_path, 0o755)
        
        with open(config_path, 'w') as f:
            json.dump({
                'sample_range': [start_idx, end_idx],
                'config': config.step_config
            }, f, indent=2)
        
        return {
            'script_path': str(script_path),
            'config_path': str(config_path),
            'workspace_path': str(workspace_path),
            'script_filename': script_filename,
            'config_filename': config_filename
        }