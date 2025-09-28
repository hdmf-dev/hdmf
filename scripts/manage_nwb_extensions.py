#!/usr/bin/env python3
"""
Helper script to manage the NWB extensions catalog integration.

This script can be used to:
1. Test fetching the NWB extensions catalog
2. Validate that repositories exist and are accessible
3. Preview what extensions would be tested by the workflow
"""

import os
import sys
import yaml
import json
import subprocess
import urllib.request
import urllib.error
from pathlib import Path

def fetch_extensions_from_catalog():
    """Fetch extensions from the NWB extensions catalog"""
    catalog_urls = [
        "https://raw.githubusercontent.com/nwb-extensions/nwb-extensions/main/index.yaml",
        "https://raw.githubusercontent.com/NeurodataWithoutBorders/nwb-extensions/main/index.yaml",
        "https://api.github.com/repos/nwb-extensions/nwb-extensions/contents/index.yaml",
        "https://api.github.com/repos/NeurodataWithoutBorders/nwb-extensions/contents/index.yaml"
    ]
    
    extensions = []
    
    for url in catalog_urls:
        try:
            print(f"Trying to fetch catalog from: {url}")
            with urllib.request.urlopen(url, timeout=10) as response:
                content = response.read().decode('utf-8')
            
            # Handle GitHub API response (base64 encoded)
            if 'api.github.com' in url:
                import base64
                data = json.loads(content)
                content = base64.b64decode(data['content']).decode('utf-8')
            
            # Parse the YAML content
            catalog = yaml.safe_load(content)
            
            # Extract extensions from catalog
            catalog_extensions = catalog.get('extensions', []) if isinstance(catalog, dict) else []
            
            for ext in catalog_extensions:
                if isinstance(ext, dict):
                    name = ext.get('name', '')
                    repo = ext.get('repository', ext.get('homepage', ''))
                    
                    # Convert homepage to git repository if needed
                    if repo and 'github.com' in repo and not repo.endswith('.git'):
                        repo = repo.replace('github.com', 'github.com').rstrip('/') + '.git'
                    
                    if name and repo and name.startswith('ndx-'):
                        extensions.append({
                            "name": name,
                            "repository": repo,
                            "active": True,
                            "description": ext.get('description', 'No description available')
                        })
            
            if extensions:
                print(f"Successfully fetched {len(extensions)} extensions from catalog")
                return extensions
                
        except Exception as e:
            print(f"Failed to fetch from {url}: {e}")
            continue
    
    return []

def get_fallback_extensions():
    """Get the fallback extension list used when catalog is unavailable"""
    return [
        {"name": "ndx-pose", "repository": "https://github.com/rly/ndx-pose.git", "active": True},
        {"name": "ndx-events", "repository": "https://github.com/rly/ndx-events.git", "active": True},
        {"name": "ndx-spectrum", "repository": "https://github.com/bendichter/ndx-spectrum.git", "active": True},
        {"name": "ndx-photostim", "repository": "https://github.com/catalystneuro/ndx-photostim.git", "active": True},
        {"name": "ndx-miniscope", "repository": "https://github.com/bendichter/ndx-miniscope.git", "active": True},
        {"name": "ndx-dandi-icephys", "repository": "https://github.com/AllenInstitute/ndx-dandi-icephys.git", "active": True},
        {"name": "ndx-structured-behavior", "repository": "https://github.com/AllenInstitute/ndx-structured-behavior.git", "active": True},
        {"name": "ndx-bipolar-scheme", "repository": "https://github.com/catalystneuro/ndx-bipolar-scheme.git", "active": True},
        {"name": "ndx-sound", "repository": "https://github.com/rly/ndx-sound.git", "active": True}
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