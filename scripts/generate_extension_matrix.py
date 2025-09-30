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


CATALOG_API_URL = "https://api.github.com/orgs/nwb-extensions/repos"


def get_fallback_extensions():
    """Get fallback extensions in case catalog is unavailable"""
    return [
        {"name": "ndx-miniscope", "repository": "https://github.com/catalystneuro/ndx-miniscope.git", "active": True},
        {"name": "ndx-simulation-output", "repository": "https://github.com/catalystneuro/ndx-simulation-output.git", "active": True},
    ]

def get_all_extension_record_repos():
    """Get all record repositories from the extensions organization using pagination."""
    all_record_repos = []
    page = 1
    per_page = 100  # Maximum allowed by GitHub API

    while True:
        params = {'per_page': per_page, 'page': page}
        response = requests.get(CATALOG_API_URL, headers=headers, params=params)

        success = response.status_code == 200
        if success:
            repos = response.json()
        else:
            raise ValueError(f'Error at {url}')

        if not repos:  # Empty response means no more pages
            break

        record_repos = [d for d in repos if d["name"].startswith("ndx-") and d["name"].endswith("-record")]
        all_record_repos.extend(record_repos)

        # If we got fewer repos than per_page, we've reached the last page
        if len(repos) < per_page:
            break

        page += 1

    print(f'Found {len(all_record_repos)} NWB extension record repositories')
    return all_record_repos


def fetch_extensions_from_catalog():
    """Fetch extensions from the NWB extensions catalog"""
    extensions = []

    headers = dict()
    GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')
    if GITHUB_TOKEN is not None:
        print('Token found, will save in headers')
        headers = {
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {GITHUB_TOKEN}",
        }

    # Use GitHub API to get the metadata for all NWB extension record repositories
    record_repos = get_all_extension_record_repos()

    # Use GitHub API to read the ndx-meta.yaml in each repo and extract the important metadata
    # TODO

    print(f"Successfully fetched {len(extensions)} extensions from catalog")
    return extensions


def main():
    # Generate matrix for GitHub Actions workflow
    """Generate the workflow matrix for GitHub Actions"""
    extensions = fetch_extensions_from_catalog()

    # Use fallback if catalog fetch failed
    if not extensions:
        print("Could not fetch catalog, using fallback extension list")
        extensions = get_fallback_extensions()

    # Generate the matrix
    matrix = {"extension": extensions}
    matrix_json = json.dumps(matrix)
    print(f"Generated matrix with {len(matrix['extension'])} extensions")
    print(f"::set-output name=matrix::{matrix_json}")


if __name__ == "__main__":
    main()
