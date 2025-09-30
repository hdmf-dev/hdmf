#!/usr/bin/env python3
"""
Helper script to manage and validate NWB extensions.

This script can be used to:
1. Test fetching the NWB extensions catalog
2. Validate that repositories exist and are accessible
3. Preview what extensions would be tested by the workflow
"""

import os
import sys
import subprocess
from pathlib import Path

# Import the matrix generation functionality
try:
    from generate_extension_matrix import fetch_extensions_from_catalog, get_fallback_extensions
except ImportError:
    # If run from different directory, adjust path
    script_dir = Path(__file__).parent
    sys.path.insert(0, str(script_dir))
    from generate_extension_matrix import fetch_extensions_from_catalog, get_fallback_extensions


def check_repository_exists(repo_url):
    """Check if a repository exists and is accessible"""
    try:
        result = subprocess.run([
            "git", "ls-remote", "--heads", repo_url
        ], capture_output=True, text=True, timeout=30)
        return result.returncode == 0
    except (subprocess.TimeoutExpired, subprocess.CalledProcessError):
        return False


def test_catalog_fetch():
    """Test fetching the NWB extensions catalog"""
    print("Testing NWB Extensions Catalog Integration")
    print("=" * 50)
    
    # Try to fetch from catalog
    extensions = fetch_extensions_from_catalog()
    
    if not extensions:
        print("Catalog fetch failed, showing fallback extensions:")
        extensions = get_fallback_extensions()
    
    print(f"\nFound {len(extensions)} extensions:")
    for ext in extensions:
        print(f"- {ext['name']}: {ext['repository']}")
        if 'description' in ext:
            print(f"  {ext['description']}")
    
    return extensions


def validate_extensions(extensions):
    """Validate all extensions in the list"""
    print("\nValidating extension repositories...")
    print("=" * 50)
    
    valid_count = 0
    for ext in extensions:
        print(f"Checking {ext['name']}...")
        if check_repository_exists(ext['repository']):
            print(f"  ✅ {ext['repository']}")
            valid_count += 1
        else:
            print(f"  ❌ {ext['repository']} - Not accessible")
    
    print("=" * 50)
    print(f"Valid: {valid_count}/{len(extensions)} extensions")
    return valid_count == len(extensions)


def print_workflow_matrix(extensions):
    """Print the YAML matrix that would be generated"""
    print("\n# Workflow matrix that would be generated:")
    print("matrix:")
    print("  extension:")
    
    for ext in extensions:
        print(f"    - name: {ext['name']}")
        print(f"      repository: {ext['repository']}")
        print(f"      active: {str(ext['active']).lower()}")


def main():
    if len(sys.argv) < 2:
        print("Usage: python manage_nwb_extensions.py [test|validate|matrix]")
        print("  test:     Test fetching the NWB extensions catalog")
        print("  validate: Check if all extension repositories are accessible")
        print("  matrix:   Show the workflow matrix that would be generated")
        return 1
    
    command = sys.argv[1]
    
    if command == "test":
        extensions = test_catalog_fetch()
        return 0
    elif command == "validate":
        extensions = fetch_extensions_from_catalog()
        if not extensions:
            extensions = get_fallback_extensions()
        
        if validate_extensions(extensions):
            print("All extensions are valid!")
            return 0
        else:
            print("Some extensions are not accessible!")
            return 1
    elif command == "matrix":
        extensions = fetch_extensions_from_catalog()
        if not extensions:
            extensions = get_fallback_extensions()
        print_workflow_matrix(extensions)
        return 0
    else:
        print(f"Unknown command: {command}")
        return 1


if __name__ == "__main__":
    sys.exit(main())