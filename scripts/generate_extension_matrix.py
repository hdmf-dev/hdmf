#!/usr/bin/env python3
"""
Generate workflow matrix for NWB extensions testing.

This script fetches the NWB extensions catalog and generates a JSON matrix
that can be used by GitHub Actions workflows to dynamically test extensions.
"""

import argparse
import json
import os
import sys
from typing import Dict, List, Optional, Any

import requests
import yaml

# Configuration constants
CATALOG_API_URL = "https://api.github.com/orgs/nwb-extensions/repos"
DEFAULT_PER_PAGE = 100

INACTIVE_EXTENSIONS = {
    "ndx-icephys-meta",  # Deprecated, use NWB core
    "ndx-nirs",  # Requires Python <3.11,>=3.7
    "ndx-extract",  # Cannot install from source on Linux due to https://github.com/catalystneuro/ndx-extract/issues/5
    "ndx-miniscope",  # Tests cannot run from pypi installation due to ImportError: attempted relative import beyond top-level package
    "ndx-simulation-output",  # Tests cannot run from pypi installation due to ImportError: cannot import name 'call_docval_func' from 'hdmf.utils'
    "ndx-ophys-devices",  # Missing `pip` key in record metadata
    "ndx-microscopy",  # Not yet resolved ValueError: 'DeviceModel' - cannot overwrite existing specification
    "ndx-multichannel-volume",  # Possibly broken tests: ModuleNotFoundError: No module named 'MultiChannelVol'
    "ndx-photostim",  # One test fails due to AssertionError: ValueError not raised. The test is inconsistent with the extension code
    "ndx-ecog",  # Requires Python <3.9 because of ImportError: cannot import name 'Iterable' from 'collections'
}

FALLBACK_EXTENSIONS = [
    {
        "name": "ndx-miniscope",
        "repository": "https://github.com/catalystneuro/ndx-miniscope.git",
    },
    {
        "name": "ndx-simulation-output",
        "repository": "https://github.com/catalystneuro/ndx-simulation-output.git",
    },
]


def get_github_headers() -> Dict[str, str]:
    """Get headers for GitHub API requests with authentication if token is available."""
    headers = {}
    github_token = os.getenv('GITHUB_TOKEN')
    if github_token:
        headers = {
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {github_token}",
        }
        print("Using GitHub token for authenticated requests", file=sys.stderr)
    else:
        print("No GitHub token found - using unauthenticated requests", file=sys.stderr)
    return headers


def get_extension_record_repos() -> List[Dict[str, Any]]:
    """Get all extension record repositories using pagination."""
    all_repos = []
    page = 1
    headers = get_github_headers()

    while True:
        params = {'per_page': DEFAULT_PER_PAGE, 'page': page}

        try:
            response = requests.get(CATALOG_API_URL, headers=headers, params=params)
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"Error: Failed to fetch repos from {CATALOG_API_URL}: {e}", file=sys.stderr)
            raise

        repos = response.json()
        if not repos:  # No more pages
            break

        # Filter for extension record repositories
        record_repos = [
            repo for repo in repos
            if repo["name"].startswith("ndx-") and repo["name"].endswith("-record")
        ]
        all_repos.extend(record_repos)

        # Check if we've reached the last page
        if len(repos) < DEFAULT_PER_PAGE:
            break

        page += 1

    print(f"Found {len(all_repos)} NWB extension record repositories", file=sys.stderr)
    return all_repos


def fetch_extension_metadata(repo: Dict[str, Any], headers: Dict[str, str]) -> Optional[Dict[str, Any]]:
    """Fetch extension metadata from ndx-meta.yaml file."""
    repo_name = repo["name"]
    repo_url = repo["html_url"]
    default_branch = repo.get("default_branch", "main")

    raw_url = f"https://raw.githubusercontent.com/nwb-extensions/{repo_name}/{default_branch}/ndx-meta.yaml"

    try:
        response = requests.get(raw_url, headers=headers)
        response.raise_for_status()

        meta = yaml.safe_load(response.text)
        extension_name = meta["name"]
        source_repo_url = meta["src"]
        pypi_url = meta["pip"]

        if extension_name in INACTIVE_EXTENSIONS:
            print(f"Skipping inactive extension '{extension_name}'", file=sys.stderr)
            return None

        return {
            "name": extension_name,
            "repository": source_repo_url,
            "pypi": pypi_url,
        }

    except requests.RequestException as e:
        print(f"Warning: Could not fetch metadata from {raw_url}: {e}", file=sys.stderr)
        return None

    except yaml.YAMLError as e:
        print(f"Warning: Could not parse YAML from {raw_url}: {e}", file=sys.stderr)
        return None

    except Exception as e:
        print(f"Warning: Unexpected error processing {raw_url}: {e}", file=sys.stderr)
        return None


def fetch_extensions_from_catalog() -> List[Dict[str, Any]]:
    """Fetch all extensions from the NWB extensions catalog."""
    try:
        repos = get_extension_record_repos()
    except Exception as e:
        print(f"Error: Failed to fetch repository list: {e}", file=sys.stderr)
        return []

    headers = get_github_headers()
    extensions = []
    for repo in repos:
        extension_info = fetch_extension_metadata(repo, headers)
        if extension_info:
            extensions.append(extension_info)

    print(f"Successfully fetched {len(extensions)} extensions from catalog", file=sys.stderr)
    return extensions


def generate_matrix() -> Dict[str, List[Dict[str, Any]]]:
    """Generate the complete extension matrix."""
    extensions = fetch_extensions_from_catalog()

    # Use fallback if catalog fetch failed
    if not extensions:
        print("Warning: Could not fetch catalog, using fallback extension list", file=sys.stderr)
        extensions = FALLBACK_EXTENSIONS.copy()

    return {"extension": extensions}


def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Generate workflow matrix for NWB extensions testing"
    )
    parser.add_argument(
        "--output-format",
        choices=["github-actions", "json"],
        default="github-actions",
        help="Output format (default: github-actions)"
    )
    return parser.parse_args()


def main() -> int:
    """Main entry point."""
    args = parse_arguments()

    try:
        matrix = generate_matrix()

        if args.output_format == "github-actions":
            matrix_json = json.dumps(matrix, separators=(',', ':'))
            print(f"Generated matrix with {len(matrix['extension'])} extensions")
            print(f"matrix={matrix_json}")
            if 'GITHUB_OUTPUT' in os.environ:
                with open(os.environ['GITHUB_OUTPUT'], 'a') as f:
                    f.write(f"matrix={matrix_json}\n")
        else:
            print(json.dumps(matrix, indent=2))

        return 0

    except Exception as e:
        print(f"Error: Failed to generate extension matrix: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
