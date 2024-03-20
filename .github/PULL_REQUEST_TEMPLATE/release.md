Prepare for release of HDMF [version]

### Before merging:
- [ ] Major and minor releases: Update package versions in `requirements.txt`, `requirements-dev.txt`,
  `requirements-doc.txt`, `requirements-opt.txt`, and `environment-ros3.yml` to the latest versions,
  and update dependency ranges in `pyproject.toml` and minimums in `requirements-min.txt` as needed.
  Run `pip install pur && pur -r requirements-dev.txt -r requirements.txt -r requirements-opt.txt`
  and manually update `environment-ros3.yml`.
- [ ] Check legal file dates and information in `Legal.txt`, `license.txt`, `README.rst`, `docs/source/conf.py`,
  and any other locations as needed
- [ ] Update `pyproject.toml` as needed
- [ ] Update `README.rst` as needed
- [ ] Update `src/hdmf/common/hdmf-common-schema` submodule as needed. Check the version number and commit SHA manually. Make sure we are using the latest release and not the latest commit on the `main` branch.
- [ ] Update changelog (set release date) in `CHANGELOG.md` and any other docs as needed
- [ ] Run tests locally including gallery tests, and inspect all warnings and outputs
  (`pytest && python test_gallery.py`)
- [ ] Run PyNWB tests locally including gallery and validation tests, and inspect all warnings and outputs
  (`cd pynwb; python test.py -v > out.txt 2>&1`)
- [ ] Run HDMF-Zarr tests locally including gallery and validation tests, and inspect all warnings and outputs
  (`cd hdmf-zarr; pytest && python test_gallery.py`)
- [ ] Test docs locally and inspect all warnings and outputs `cd docs; make clean && make html`
- [ ] Push changes to this PR and make sure all PRs to be included in this release have been merged
- [ ] Check that the readthedocs build for this PR succeeds (build latest to pull the new branch, then activate and
  build docs for new branch): https://readthedocs.org/projects/hdmf/builds/

### After merging:
1. Create release by following steps in `docs/source/make_a_release.rst` or use alias `git pypi-release [tag]` if set up
2. After the CI bot creates the new release (wait ~10 min), update the release notes on the
   [GitHub releases page](https://github.com/hdmf-dev/hdmf/releases) with the changelog
3. Check that the readthedocs "latest" and "stable" builds run and succeed
4. Update [conda-forge/hdmf-feedstock](https://github.com/conda-forge/hdmf-feedstock) with the latest version number
   and SHA256 retrieved from PyPI > HDMF > Download Files > View hashes for the `.tar.gz` file. Re-render as needed
