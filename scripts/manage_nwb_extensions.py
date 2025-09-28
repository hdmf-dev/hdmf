#!/usr/bin/env python3
"""
Helper script to manage the list of NWB extensions tested by the workflow.

This script can be used to:
1. Validate that repositories exist and are accessible
2. Update the workflow file with new extensions
3. Check which extensions are still active
"""

import os
import sys
import yaml
import subprocess
from pathlib import Path

# Current list of extensions from the workflow
EXTENSIONS = [
    {
        "name": "ndx-pose",
        "repository": "https://github.com/rly/ndx-pose.git",
        "active": True
    },
    {
        "name": "ndx-events",
        "repository": "https://github.com/rly/ndx-events.git",
        "active": True
    },
    {
        "name": "ndx-spectrum",
        "repository": "https://github.com/bendichter/ndx-spectrum.git",
        "active": True
    },
    {
        "name": "ndx-photostim", 
        "repository": "https://github.com/catalystneuro/ndx-photostim.git",
        "active": True
    },
    {
        "name": "ndx-miniscope",
        "repository": "https://github.com/bendichter/ndx-miniscope.git",
        "active": True
    },
    {
        "name": "ndx-dandi-icephys",
        "repository": "https://github.com/AllenInstitute/ndx-dandi-icephys.git",
        "active": True
    },
    {
        "name": "ndx-structured-behavior",
        "repository": "https://github.com/AllenInstitute/ndx-structured-behavior.git",
        "active": True
    },
    {
        "name": "ndx-bipolar-scheme",
        "repository": "https://github.com/catalystneuro/ndx-bipolar-scheme.git",
        "active": True
    },
    {
        "name": "ndx-sound",
        "repository": "https://github.com/rly/ndx-sound.git", 
        "active": True
    }
]

def check_repository_exists(repo_url):
    """Check if a repository exists and is accessible"""
    try:
        result = subprocess.run([
            "git", "ls-remote", "--heads", repo_url
        ], capture_output=True, text=True, timeout=30)
        return result.returncode == 0
    except (subprocess.TimeoutExpired, subprocess.CalledProcessError):
        return False

def validate_extensions():
    """Validate all extensions in the list"""
    print("Validating NWB extensions...")
    print("=" * 50)
    
    valid_count = 0
    for ext in EXTENSIONS:
        print(f"Checking {ext['name']}...")
        if check_repository_exists(ext['repository']):
            print(f"  ✅ {ext['repository']}")
            valid_count += 1
        else:
            print(f"  ❌ {ext['repository']} - Not accessible")
    
    print("=" * 50)
    print(f"Valid: {valid_count}/{len(EXTENSIONS)} extensions")
    return valid_count == len(EXTENSIONS)

def print_workflow_snippet():
    """Print the YAML snippet for the workflow matrix"""
    print("\n# Workflow matrix snippet:")
    print("matrix:")
    print("  extension:")
    
    for ext in EXTENSIONS:
        print(f"    - name: {ext['name']}")
        print(f"      repository: {ext['repository']}")
        print(f"      active: {str(ext['active']).lower()}")

def main():
    if len(sys.argv) < 2:
        print("Usage: python manage_extensions.py [validate|print]")
        print("  validate: Check if all extension repositories are accessible")
        print("  print: Print the workflow YAML snippet")
        return 1
    
    command = sys.argv[1]
    
    if command == "validate":
        if validate_extensions():
            print("All extensions are valid!")
            return 0
        else:
            print("Some extensions are not accessible!")
            return 1
    elif command == "print":
        print_workflow_snippet()
        return 0
    else:
        print(f"Unknown command: {command}")
        return 1

if __name__ == "__main__":
    sys.exit(main())