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

The workflow currently tests the following NWB extensions:

1. **ndx-pose** - NWB extension for storing pose estimation data
2. **ndx-events** - NWB extension for storing timestamped event data  
3. **ndx-spectrum** - NWB extension for spectral data
4. **ndx-photostim** - NWB extension for photostimulation data
5. **ndx-miniscope** - NWB extension for miniscope data
6. **ndx-dandi-icephys** - DANDI intracellular electrophysiology extension
7. **ndx-structured-behavior** - Extension for structured behavioral data
8. **ndx-bipolar-scheme** - Extension for bipolar referencing scheme
9. **ndx-sound** - Extension for audio data

## How it works

For each extension, the workflow:

1. Clones the extension repository
2. Installs the current HDMF branch
3. Attempts to install the extension
4. Runs the extension's tests if available
5. Falls back to basic import testing if no tests are found
6. Continues testing other extensions even if one fails

## Adding new extensions

To add a new NWB extension to the test matrix:

1. Add an entry to the `matrix.extension` array in `.github/workflows/run_nwb_extension_tests.yml`
2. Include the extension name, repository URL, and active status
3. Use the helper script `scripts/manage_nwb_extensions.py` to validate repositories

Example entry:
```yaml
- name: ndx-new-extension
  repository: https://github.com/user/ndx-new-extension.git
  active: true
```

## Managing extensions

Use the helper script to manage the extension list:

```bash
# Validate all extension repositories
python scripts/manage_nwb_extensions.py validate

# Print workflow YAML snippet
python scripts/manage_nwb_extensions.py print
```

## Extension compatibility notes

- Extensions marked as `active: false` are only tested in scheduled runs, not on PRs
- Some extensions may not be actively maintained and could fail due to dependency issues
- The workflow is designed to be tolerant of individual extension failures
- Failed extension tests are logged but don't fail the entire workflow