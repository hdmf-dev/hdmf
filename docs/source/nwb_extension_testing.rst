NWB Extension Testing Workflow
=============================

This workflow runs tests for known NWB extensions against the current HDMF branch to ensure compatibility when changes are made to HDMF core functionality such as:

- Spec resolution
- Object mapper
- Class generator
- Docval
- Other core HDMF components

## When it runs

- On pull requests (for active extensions only)
- Daily at 1 AM ET via scheduled cron job (all extensions)
- Manual trigger via workflow_dispatch

## What it tests

The workflow dynamically fetches the list of NWB extensions from the official NWB extensions catalog and tests all available extensions. If the catalog is unavailable, it falls back to testing these well-known extensions:

1. **ndx-pose** - NWB extension for storing pose estimation data
2. **ndx-events** - NWB extension for storing timestamped event data
3. **ndx-spectrum** - NWB extension for spectral data
4. **ndx-photostim** - NWB extension for photostimulation data
5. **ndx-miniscope** - NWB extension for miniscope data
6. **ndx-dandi-icephys** - DANDI intracellular electrophysiology extension
7. **ndx-structured-behavior** - Extension for structured behavioral data
8. **ndx-bipolar-scheme** - Extension for bipolar referencing scheme
9. **ndx-sound** - Extension for audio data

The catalog is fetched from:
- `https://raw.githubusercontent.com/nwb-extensions/nwb-extensions/main/index.yaml`
- `https://raw.githubusercontent.com/NeurodataWithoutBorders/nwb-extensions/main/index.yaml`
- GitHub API endpoints as fallbacks

## How it works

The workflow consists of two jobs:

1. **generate-extension-matrix**: Uses `scripts/generate_extension_matrix.py` to fetch the NWB extensions catalog and generate a dynamic matrix of extensions to test
2. **run-nwb-extension-tests**: Tests each extension from the generated matrix

For each extension, the testing job:

1. Clones the extension repository
2. Installs the current HDMF branch
3. Attempts to install the extension
4. Runs the extension's tests if available
5. Falls back to basic import testing if no tests are found
6. Continues testing other extensions even if one fails

## Adding new extensions

Extensions are automatically discovered from the NWB extensions catalog, so no manual addition is needed. The workflow will automatically test new extensions as they are added to the official catalog.

If you need to add an extension that's not in the catalog, you can:
1. Add it to the official NWB extensions catalog, OR
2. Add it to the fallback list in `scripts/generate_extension_matrix.py` as a temporary measure

## Managing extensions

Use the helper scripts to interact with the catalog integration:

```bash
# Test fetching the NWB extensions catalog
python scripts/manage_nwb_extensions.py test

# Validate all extension repositories
python scripts/manage_nwb_extensions.py validate

# Show the workflow matrix that would be generated
python scripts/manage_nwb_extensions.py matrix

# Generate matrix for GitHub Actions (used by workflow)
python scripts/generate_extension_matrix.py --github-actions

# Test matrix generation interactively
python scripts/generate_extension_matrix.py
```

## Extension compatibility notes

- Extensions marked as `active: false` are only tested in scheduled runs, not on PRs
- Some extensions may not be actively maintained and could fail due to dependency issues
- The workflow is designed to be tolerant of individual extension failures
- Failed extension tests are logged but don't fail the entire workflow
