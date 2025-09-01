#!/usr/bin/env python3
"""
File Utility Functions
Handles file operations and validation
"""

import os


def find_twbx_files(directory='.'):
    """Find all TWBX files in the specified directory."""
    twbx_files = [f for f in os.listdir(directory) if f.endswith('.twbx')]
    return twbx_files


def create_safe_filename(name):
    """Create a safe filename by removing/replacing unsafe characters."""
    # Replace spaces and slashes with underscores
    safe_name = name.replace(' ', '_').replace('/', '_')
    # Keep only alphanumeric characters, underscores, and hyphens
    safe_name = ''.join(c for c in safe_name if c.isalnum() or c in '_-')
    return safe_name


def ensure_directory_exists(directory):
    """Ensure a directory exists, create if it doesn't."""
    os.makedirs(directory, exist_ok=True)
    return directory


def validate_twbx_file(file_path):
    """Validate that a file is a valid TWBX file."""
    if not os.path.exists(file_path):
        return False, "File does not exist"
    
    if not file_path.endswith('.twbx'):
        return False, "File is not a TWBX file"
    
    if os.path.getsize(file_path) == 0:
        return False, "File is empty"
    
    return True, "File is valid"
