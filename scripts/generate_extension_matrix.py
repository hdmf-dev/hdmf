#!/usr/bin/env python3
"""
Generate workflow matrix for NWB extensions testing.

This script fetches the NWB extensions catalog and generates a JSON matrix
that can be used by GitHub Actions workflows to dynamically test extensions.
"""

import json
import yaml
import sys
import urllib.request
import urllib.error
import base64


def get_catalog_urls():
    """Get the list of potential catalog URLs to try"""
    return [
        "https://raw.githubusercontent.com/nwb-extensions/nwb-extensions/main/index.yaml",
        "https://raw.githubusercontent.com/NeurodataWithoutBorders/nwb-extensions/main/index.yaml",
        "https://api.github.com/repos/nwb-extensions/nwb-extensions/contents/index.yaml",
        "https://api.github.com/repos/NeurodataWithoutBorders/nwb-extensions/contents/index.yaml"
    ]


def get_fallback_extensions():
    """Get fallback extensions in case catalog is unavailable"""
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


def fetch_extensions_from_catalog():
    """Fetch extensions from the NWB extensions catalog"""
    catalog_urls = get_catalog_urls()
    extensions = []

    for url in catalog_urls:
        try:
            print(f"Trying to fetch catalog from: {url}")
            with urllib.request.urlopen(url, timeout=10) as response:
                content = response.read().decode('utf-8')

            # Handle GitHub API response (base64 encoded)
            if 'api.github.com' in url:
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
                            "active": True
                        })

            if extensions:
                print(f"Successfully fetched {len(extensions)} extensions from catalog")
                return extensions

        except Exception as e:
            print(f"Failed to fetch from {url}: {e}")
            continue

    return []


def generate_workflow_matrix():
    """Generate the workflow matrix for GitHub Actions"""
    extensions = fetch_extensions_from_catalog()

    # Use fallback if catalog fetch failed
    if not extensions:
        print("Could not fetch catalog, using fallback extension list")
        extensions = get_fallback_extensions()

    # Generate the matrix
    matrix = {"extension": extensions}
    return matrix


def main():
    """Main entry point"""
    if len(sys.argv) > 1 and sys.argv[1] == "--github-actions":
        # Generate matrix for GitHub Actions workflow
        matrix = generate_workflow_matrix()
        matrix_json = json.dumps(matrix)
        print(f"Generated matrix with {len(matrix['extension'])} extensions")
        print(f"::set-output name=matrix::{matrix_json}")
    else:
        # Interactive mode for testing
        matrix = generate_workflow_matrix()
        print("\nGenerated workflow matrix:")
        print(json.dumps(matrix, indent=2))
        print(f"\nTotal extensions: {len(matrix['extension'])}")


if __name__ == "__main__":
    main()
