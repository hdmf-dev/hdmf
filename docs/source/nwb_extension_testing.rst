NWB Extension Testing Workflow
=============================

This workflow runs tests for NWB extensions to ensure compatibility when changes are made to HDMF core functionality.

## When it runs

- Daily at 6 AM UTC (1 AM ET) via scheduled cron job
- Manual trigger via workflow_dispatch

## What it tests

The workflow automatically discovers and tests all NWB extensions from the official catalog at ``https://nwb-extensions.github.io``.

If the catalog is unavailable, it falls back to testing:

- **ndx-miniscope** - NWB extension for miniscope data
- **ndx-simulation-output** - NWB extension for simulation output data

## How it works

1. Generates a dynamic list of extensions from the NWB extensions catalog
2. Tests each extension in parallel by:
   - Cloning the extension repository
   - Installing the extension
   - Testing basic import functionality
   - Running the extension's tests

Extensions marked as inactive (like ``ndx-icephys-meta``) are skipped.

## Usage

Generate extension matrix for testing:

```bash
# GitHub Actions format (default)
python scripts/generate_extension_matrix.py

# JSON format for inspection
python scripts/generate_extension_matrix.py --output-format json
```

## Adding new extensions

Extensions are automatically discovered from the NWB extensions catalog.

For temporary additions, modify the fallback list in ``scripts/generate_extension_matrix.py``.
