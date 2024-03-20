# HDMF Changelog

## HDMF 3.13.0 (March 20, 2024)

### Enhancements
- Added docs page that lists limitations of support for the HDMF specification language. @rly [#1069](https://github.com/hdmf-dev/hdmf/pull/1069)
- Added warning when using `add_row` or `add_column` to add a ragged array to `DynamicTable` without an index parameter. @stephprince [#1066](https://github.com/hdmf-dev/hdmf/pull/1066)

## HDMF 3.12.2 (February 9, 2024)

### Bug fixes
- Fixed recursion error in html representation generation in jupyter notebooks. @stephprince [#1038](https://github.com/hdmf-dev/hdmf/pull/1038)

## HDMF 3.12.1 (February 5, 2024)

### Bug fixes
- Fixed retrieving the correct path for a `HERD` zip file on read. [#1046](https://github.com/hdmf-dev/hdmf/pull/1046)
- Fixed internal links in docstrings and tutorials. @stephprince [#1031](https://github.com/hdmf-dev/hdmf/pull/1031)
- Fixed issue with creating documentation links to classes in docval arguments. @rly [#1036](https://github.com/hdmf-dev/hdmf/pull/1036)
- Fixed issue with validator not validating against the spec that defines the data type of the builder. @rly [#1050](https://github.com/hdmf-dev/hdmf/pull/1050)

## HDMF 3.12.0 (January 16, 2024)

### Enhancements
- Add Data.set_data_io(), which allows for setting a `DataIO` to a data object after-the-fact. @bendichter and @CodyCBakerPhD [#1013](https://github.com/hdmf-dev/hdmf/pull/1013)
- Added `add_ref_termset`, updated helper methods for `HERD`, revised `add_ref` to support validations prior to populating the tables
  and added `add_ref_container`.  @mavaylon1 [#968](https://github.com/hdmf-dev/hdmf/pull/968)
- Use `stacklevel` in most warnings. @rly [#1027](https://github.com/hdmf-dev/hdmf/pull/1027)
- Fixed broken links in documentation and added internal link checking to workflows. @stephprince [#1031](https://github.com/hdmf-dev/hdmf/pull/1031)

### Minor Improvements
- Updated `__gather_columns` to ignore the order of bases when generating columns from the super class. @mavaylon1 [#991](https://github.com/hdmf-dev/hdmf/pull/991)
- Update `get_key` to return all the keys if there are multiple within a `HERD` instance. @mavaylon1 [#999](https://github.com/hdmf-dev/hdmf/pull/999)
- Improve HTML rendering of tables. @bendichter [#998](https://github.com/hdmf-dev/hdmf/pull/998)
- Improved issue and PR templates. @rly [#1004](https://github.com/hdmf-dev/hdmf/pull/1004)
- Added check during validation for if a variable length dataset is empty. @bendichter, @oruebel [#789](https://github.com/hdmf-dev/hdmf/pull/789)

### Bug fixes
- Fixed issue with custom class generation when a spec has a `name`. @rly [#1006](https://github.com/hdmf-dev/hdmf/pull/1006)
- Fixed issue with usage of deprecated `ruamel.yaml.safe_load` in `src/hdmf/testing/validate_spec.py`. @rly [#1008](https://github.com/hdmf-dev/hdmf/pull/1008)
- Fixed issue where `ElementIdentifiers` data could be set to non-integer values. @rly [#1009](https://github.com/hdmf-dev/hdmf/pull/1009)
- Fixed issue where string datasets/attributes with isodatetime-formatted values failed validation against a text spec. @rly [#1026](https://github.com/hdmf-dev/hdmf/pull/1026)

## HDMF 3.11.0 (October 30, 2023)

### Enhancements
- Added `target_tables` attribute to `DynamicTable` to allow users to specify the target table of any predefined
`DynamicTableRegion` columns of a `DynamicTable` subclass. @rly [#971](https://github.com/hdmf-dev/hdmf/pull/971)
- Updated `TermSet` to include `_repr_html_` for easy to read notebook representation. @mavaylon1 [#967](https://github.com/hdmf-dev/hdmf/pull/967)

### Minor improvements
- Set up GitHub dependabot to check for updates to GitHub Actions. @rly [#977](https://github.com/hdmf-dev/hdmf/pull/977)
- Simplify tox configuration. @rly [#988](https://github.com/hdmf-dev/hdmf/pull/988)
- Add testing for Python 3.12. @rly [#988](https://github.com/hdmf-dev/hdmf/pull/988)

### Bug fixes
- Updated custom class generation to handle specs with fixed values and required names. @rly [#800](https://github.com/hdmf-dev/hdmf/pull/800)
- Fixed custom class generation of `DynamicTable` subtypes to set attributes corresponding to column names for correct write. @rly [#800](https://github.com/hdmf-dev/hdmf/pull/800)
- Added a `.mailmap` file to correct mapping of names/emails in git logs. @oruebel [#976](https://github.com/hdmf-dev/hdmf/pull/976)

## HDMF 3.10.0 (October 3, 2023)

Since version 3.9.1 should have been released as 3.10.0 but failed to release on PyPI and conda-forge, this release
will increase the minor version number to 3.10.0. See the 3.9.1 release notes below for new features.

### Bug fixes
- Fixed issue with testing and deployment of releases. @rly [#957](https://github.com/hdmf-dev/hdmf/pull/957)

## HDMF 3.9.1 (September 29, 2023)

### Enhancements
- Updated `TermSet` to be used with `TermSetWrapper`, allowing for general use of validation for datasets and attributes. This also brings updates to `HERD` integration and updates on `write` to easily add references for wrapped datasets/attributes. @mavaylon1 [#950](https://github.com/hdmf-dev/hdmf/pull/950)

### Minor improvements
- Removed warning when namespaces are loaded and the attribute marking where the specs are cached is missing. @bendichter [#926](https://github.com/hdmf-dev/hdmf/pull/926)

### Bug fixes
- Fixed bug allowing `DynamicTable` to be constructed with empty `id` column when initializing all columns via `AbstractDataChunkIterator` objects. @oruebel [#953](https://github.com/hdmf-dev/hdmf/pull/953)

## HDMF 3.9.0 (August 25, 2023)

### New features and minor improvements
- Increased raw data chunk cache size for reading HDF5 files from 1 MiB to 32 MiB. @bendichter, @rly [#925](https://github.com/hdmf-dev/hdmf/pull/925)
- Increased default chunk size for `GenericDataChunkIterator` from 1 MB to 10 MB. @bendichter, @rly [#925](https://github.com/hdmf-dev/hdmf/pull/925)
- Added the magic `__reduce__` method as well as two private semi-abstract helper methods to enable pickling of the `GenericDataChunkIterator`. @codycbakerphd [#924](https://github.com/hdmf-dev/hdmf/pull/924)
- Updated `add_ref_termset` to add all instances of `TermSet` within a given root container. @mavaylon1 [#935](https://github.com/hdmf-dev/hdmf/pull/935)
- Added Dynamic Enumerations and Schemasheets support to `TermSet`. @mavaylon1 [#923](https://github.com/hdmf-dev/hdmf/pull/923)
- Updated `HERD` to support user defined file name for the `HERD` zip file. @mavaylon1 [#941](https://github.com/hdmf-dev/hdmf/pull/941)
- Added method `Container.set_data_io`, which wraps an existing data field in a `DataIO`. @bendichter [#938](https://github.com/hdmf-dev/hdmf/pull/938)

## HDMF 3.8.1 (July 25, 2023)

### Bug fixes
- Fixed error when calling `HDF5IO.read` twice. @rly [#915](https://github.com/hdmf-dev/hdmf/pull/915)

## HDMF 3.8.0 (July 21, 2023)

### New features and minor improvements
- Added the ability to write ExternalResources if the path is provided and the container has a linked instance of ExternalResources. @mavaylon1 [#910](https://github.com/hdmf-dev/hdmf/pull/910)

### Bug fixes
- Fixed bug on `add_ref_term_set` in which attributes that were not subscribtable returned an error. @mavaylon1 [#909](https://github.com/hdmf-dev/hdmf/pull/909)

## HDMF 3.7.0 (July 10, 2023)

### New features and minor improvements
- Updated `ExternalResources` to have EntityKeyTable with updated tests/documentation and minor bug fix to ObjectKeyTable. @mavaylon1 [#872](https://github.com/hdmf-dev/hdmf/pull/872)
- Added abstract static method `HDMFIO.can_read()` and concrete static method `HDF5IO.can_read()`. @bendichter [#875](https://github.com/hdmf-dev/hdmf/pull/875)
- Added warning for `DynamicTableRegion` links that are not added to the same parent as the original container object. @mavaylon1 [#891](https://github.com/hdmf-dev/hdmf/pull/891)
- Added the `TermSet` class along with integrated validation methods for any child of `AbstractContainer`, e.g., `VectorData`, `Data`, `DynamicTable`. @mavaylon1 [#880](https://github.com/hdmf-dev/hdmf/pull/880)
- Added `AbstractContainer.read_io` property to be able to retrieve the HDMFIO object used for reading from the container and to ensure the I/O object used for reading is not garbage collected before the container is being deleted. @bendichter @oruebel [#882](https://github.com/hdmf-dev/hdmf/pull/882)
- Allow for `datetime.date` to be used instead of `datetime.datetime`. @bendichter [#874](https://github.com/hdmf-dev/hdmf/pull/874)
- Updated `HDMFIO` and `HDF5IO` to support `ExternalResources`. @mavaylon1 [#895](https://github.com/hdmf-dev/hdmf/pull/895)
- Dropped Python 3.7 support. @rly [#897](https://github.com/hdmf-dev/hdmf/pull/897)
- Added HTML repr to `Container` objects which displays an interactive tree of a container's values and children in a Jupyter notebook and other HTML representations. @edeno [#883](https://github.com/hdmf-dev/hdmf/pull/883)
- Update software_process.rst with the correct external links. @mavaylon1 [#900](https://github.com/hdmf-dev/hdmf/pull/900)

### Documentation and tutorial enhancements:
- Added tutorial for the new `TermSet` class @mavaylon1 [#880](https://github.com/hdmf-dev/hdmf/pull/880)

### Bug fixes
- Fixed CI testing of minimum installation requirements, and removed some gallery tests run on each PR. @rly
  [#877](https://github.com/hdmf-dev/hdmf/pull/877)
- Fixed reporting of version when installed using conda. @rly [#890](https://github.com/hdmf-dev/hdmf/pull/890)


## HMDF 3.6.1 (May 18, 2023)

### Bug fixes
- Fixed compatibility with hdmf_zarr for converting string arrays from Zarr to HDF5 by adding logic to determine the dtype for object arrays. @oruebel [#866](https://github.com/hdmf-dev/hdmf/pull/866)

## HDMF 3.6.0 (May 12, 2023)

### New features and minor improvements
- Updated `ExternalResources` to have `FileTable` and new methods to query data. the `ResourceTable` has been removed along with methods relating to `Resource`. @mavaylon [#850](https://github.com/hdmf-dev/hdmf/pull/850)
- Updated hdmf-common-schema version to 1.6.0. @mavaylon [#850](https://github.com/hdmf-dev/hdmf/pull/850)
- Added testing of HDMF-Zarr on PR and nightly. @rly [#859](https://github.com/hdmf-dev/hdmf/pull/859)
- Replaced `setup.py` with `pyproject.toml`. @rly [#844](https://github.com/hdmf-dev/hdmf/pull/844)
- Use `ruff` instead of `flake8`. @rly [#844](https://github.com/hdmf-dev/hdmf/pull/844)
- Replaced `versioneer` with `setuptools_scm` for versioning. @rly [#844](https://github.com/hdmf-dev/hdmf/pull/844)
- Now recommending developers use `pre-commit`. @rly [#844](https://github.com/hdmf-dev/hdmf/pull/844)

### Bug fixes
- Export cached specs that are loaded in source IO manager but not in export IO manager. @rly
  [#855](https://github.com/hdmf-dev/hdmf/pull/855)
- Fixed issue with passing an S3 URL for source in the constructor of ``HDMFIO`` @rly
  [#853](https://github.com/hdmf-dev/hdmf/pull/853)

### Documentation and tutorial enhancements:
- Updated `ExternalResources` how to tutorial to include the new features. @mavaylon [#850](https://github.com/hdmf-dev/hdmf/pull/850)

## HDMF 3.5.6 (April 28, 2023)

### Bug fixes
- Removed references to discontinued `requires.io` service in documentation. @rly
  [#854](https://github.com/hdmf-dev/hdmf/pull/854)

## HDMF 3.5.5 (April 13, 2023)

### Bug fixes
- Fixed error during export where an external link to the same file was created in some situations.
  @rly [#847](https://github.com/hdmf-dev/hdmf/pull/847)
- Removed unused, deprecated `codecov` package from dev installation requirements. @rly
  [#849](https://github.com/hdmf-dev/hdmf/pull/849)
- Fixed export with `'link_data': False'` not copying datasets in some situations. @rly
  [#848](https://github.com/hdmf-dev/hdmf/pull/848)

## HDMF 3.5.4 (April 7, 2023)

### Bug fixes
- Fixed typo in deploy release workflow. @rly [#845](https://github.com/hdmf-dev/hdmf/pull/845)

## HDMF 3.5.3 (April 7, 2023)

### Bug fixes
- Fixed search bar and missing jquery in ReadTheDocs documentation. @rly
  [#840](https://github.com/hdmf-dev/hdmf/pull/840)
- Fixed issue with dynamic class generation for a multi-container interface where one or more are required. @rly
  [#839](https://github.com/hdmf-dev/hdmf/pull/839)

### Minor improvements
- Added support for Python 3.11. @rly [#803](https://github.com/hdmf-dev/hdmf/pull/803)
- No longer set upper bounds on dependencies. @rly [#803](https://github.com/hdmf-dev/hdmf/pull/803)
- `DynamicTable.to_dataframe()` now sets the index (id) column dtype to int64 rather than the OS default (int32 on Windows, int64 on Mac, Linux). @rly [#803](https://github.com/hdmf-dev/hdmf/pull/803)

## HDMF 3.5.2 (March 13, 2023)

### Bug fixes
- Fixed issue with conda CI. @rly [#823](https://github.com/hdmf-dev/hdmf/pull/823)
- Fixed issue with deprecated `pkg_resources`. @mavaylon [#822](https://github.com/hdmf-dev/hdmf/pull/822)
- Fixed `hdmf.common` deprecation warning. @mavaylon [#826]((https://github.com/hdmf-dev/hdmf/pull/826)

### Internal improvements
- A number of typos fixed and Github action running codespell to ensure that no typo sneaks in [#825](https://github.com/hdmf-dev/hdmf/pull/825) was added.
- Added additional documentation for `__fields__` in `AbstactContainer`. @mavaylon [#827](https://github.com/hdmf-dev/hdmf/pull/827)
- Updated warning message for broken links. @mavaylon [#829](https://github.com/hdmf-dev/hdmf/pull/829)

## HDMF 3.5.1 (January 26, 2023)

### Bug fixes
- Fixed bug when closing a ``HDF5IO`` on delete via the new ``HDMFIO.__del__`` before ``HDF5IO.__init__`` has been completed. @oruebel [#817](https://github.com/hdmf-dev/hdmf/pull/817)

### Documentation and tutorial enhancements:
- Updated `DynamicTable` how to tutorial to clarify the status of `EnumData`. @oruebel [#819](https://github.com/hdmf-dev/hdmf/pull/819)

## HDMF 3.5.0 (January 17, 2023)

### Bug fixes
- Fixed an issue with `ExternalResources` where retrieving an object resources wasn't in the proper format for a Pandas DataFrame. Additionally, a boolean parameter for creating an object when checking the existence of an object was added. @mavaylon1 [#790](https://github.com/hdmf-dev/hdmf/pull/790)
- Fixed an issue with the `tox` tool when upgrading to tox 4. @rly [#802](https://github.com/hdmf-dev/hdmf/pull/802)
- Fixed export of newly added links to existing elements of the exported file. @rly [#808](https://github.com/hdmf-dev/hdmf/pull/808)
- Added ``HDMFIO.__del__`` to ensure that I/O objects are being closed on delete. @oruebel[#811](https://github.com/hdmf-dev/hdmf/pull/811)

### Minor improvements
- Added support for reading and writing `ExternalResources` to and from denormalized TSV files. @mavaylon [#799](https://github.com/hdmf-dev/hdmf/pull/799)
- Changed the name of `ExternalResources.export_to_sqlite` to `ExternalResources.to_sqlite`. @mavaylon [#799](https://github.com/hdmf-dev/hdmf/pull/799)
- Updated the tutorial for `ExternalResources`. @mavaylon [#799](https://github.com/hdmf-dev/hdmf/pull/799)
- Added `message` argument for assert methods defined by `hdmf.testing.TestCase` to allow developers to include custom error messages with asserts. @oruebel [#812](https://github.com/hdmf-dev/hdmf/pull/812)
- Clarify the expected chunk shape behavior for `DataChunkIterator`. @oruebel [#813](https://github.com/hdmf-dev/hdmf/pull/813)

## HDMF 3.4.7 (November 9, 2022)

### Minor improvements
- Update CI, versioneer, and some requirements. @rly ([#786](https://github.com/hdmf-dev/hdmf/pull/786))

### Bug fixes
- Fix an issue where not providing an optional argument to `__init__` of an auto-generated `MultiContainerInterface`
  class raised an error. @rly ([#779](https://github.com/hdmf-dev/hdmf/pull/779))
- Fixed an issue with the `data_utils.GenericDataChunkIterator` where if the underlying dataset was such that the `numpy.product` of the `maxshape` exceeded the range of the default `int32`, buffer overflow would occur and cause the true buffer shape to exceed available memory. This has been resolved by dropping all `numpy` operations (which forced casting within the passed data type) in favor of the unlimited precision of Python builtin integer types @codycbakerphd ([#780](https://github.com/hdmf-dev/hdmf/pull/780)) ([#781](https://github.com/hdmf-dev/hdmf/pull/781))

## HDMF 3.4.6 (October 4, 2022)

### Minor improvements
- When data is not specified in DataIO, 1) require dtype and shape both be specified and 2) determine length from shape. @ajtritt ([#771](https://github.com/hdmf-dev/hdmf/pull/771))

### Bug fixes
- Fix an issue when autogenerating a class that extends a class where the constructor docval does not include all of
the fields (i.e., when the constructor sets some fields to fixed values). @rly
([#773](https://github.com/hdmf-dev/hdmf/pull/773))

## HDMF 3.4.5 (September 22, 2022)

### Minor improvements
- Allow passing arguments through to column class constructor (argument `col_cls`) when calling `DynamicTable.add_column`. @ajtritt ([#769](https://github.com/hdmf-dev/hdmf/pull/769))

## HDMF 3.4.4 (September 20, 2022)

### Bug fixes
- Fixed missing dependency "packaging" introduced in 3.4.3. The code has been updated to avoid the dependency. @rly @oruebel ([#770](https://github.com/hdmf-dev/hdmf/pull/770))

## HDMF 3.4.3 (September 14, 2022)

### Minor improvements
- Began to deprecate the use of the testing script `test.py` in favor of `pytest` and `test_gallery.py`.
  @rly ([#760](https://github.com/hdmf-dev/hdmf/pull/760))
- Updated installation and maintainer documentation. @rly ([#760](https://github.com/hdmf-dev/hdmf/pull/760))

### Bug fixes
- Fixed CI and flake8 issues. @rly ([#760](https://github.com/hdmf-dev/hdmf/pull/760))
- Updated uses of pandas.DataFrame.set_index to avoid FutureWarnings for pandas >=1.5.x @oruebel ([#762](https://github.com/hdmf-dev/hdmf/pull/762))
- Fixed broken `hdmf.common.get_hdf5io` function. @rly ([#765](https://github.com/hdmf-dev/hdmf/pull/765))

## HDMF 3.4.2 (August 26, 2022)

### Minor improvements
- Updated ``MultiContainerInterface.__make_add`` to clarify the docstring for ``add_*`` methods generated by the function. @oruebel ([#758](https://github.com/hdmf-dev/hdmf/pull/758))
- Support "allow_none=True" in docval for args with non-None default. @rly ([#757](https://github.com/hdmf-dev/hdmf/pull/757))

### Bug fixes
- Fixed deploy release CI. @rly ([#759](https://github.com/hdmf-dev/hdmf/pull/759))

## HDMF 3.4.1 (August 8, 2022)

### Bug fixes
- Fixed deploy release CI. @rly ([#754](https://github.com/hdmf-dev/hdmf/pull/754))

## HDMF 3.4.0 (August 5, 2022)

### Minor improvements
- Allow manual triggering of some GitHub Actions. @rly ([#744](https://github.com/hdmf-dev/hdmf/pull/744))
- Relaxed input validation of `HDF5IO` to allow for s3fs support. Existing arguments of `HDF5IO` are modified as follows: i) `mode` was given a default value of "r", ii) `path` was given a default value of `None`, and iii) `file` can now accept an `S3File` type argument. @bendichter ([#746](https://github.com/hdmf-dev/hdmf/pull/746))
- Added ability to create and get back handle to empty HDF5 dataset. @ajtritt ([#747](https://github.com/hdmf-dev/hdmf/pull/747))
- Added `AbstractContainer._in_construct_mode` that is set and modified only by the ObjectMapper when constructing an
  object from a builder read from a file. Subclasses of `AbstractContainer` can check `_in_construct_mode`
  during the initialization phase as part of ``__init__`` to distinguish between actions during construction
  (i.e., read from disk) vs. creation by the user, e.g., to determine whether to raise a warning or error when
  encountering invalid data to support reading and correcting data that is invalid while preventing creation
  of new data that is invalid. @rly ([#751](https://github.com/hdmf-dev/hdmf/pull/751))

### Bug fixes
- Fixed PyNWB dev CI. @rly ([#749](https://github.com/hdmf-dev/hdmf/pull/749))

## HDMF 3.3.2 (June 27, 2022)

### Bug fixes
- Fix error message when using ``GenericDataChunkIterator`` and chunk shape is larger than dataset size. @rly (#743)
- Fix automatic class generation of extension data types that inherit from both another data type and
  ``MultiContainerInteface``. @rly (#741)

## HDMF 3.3.1 (May 20, 2022)

### Bug fixes
- Fixed release deployment CI. @rly (#734, #735, #736)

## HDMF 3.3.0 (May 18, 2022)

### New features
- Added utility functions and classes useful for implementation of I/O backend to ease maintainability and reuse of functionality across I/O backends. @oruebel (#697)
  - Added `HDF5IODataChunkIteratorQueue` class in `hdmf.backends.hdf5.h5_utils` as a new helper class for managing the write of `DataChunkIterator`s to HDF5. @oruebel (#697)
  - Added new `hdmf/io/utils.py` module with new I/O utility classes: 1) `WriteStatusTracker` as a simple data structure for tracking the write status of `Builders`, and 2) `NamespaceToBuilderHelper` to help with converting of a namespace to a `Builder` for I/O. @oruebel (#697)
- Added `get_min_bounds` function to `hdmf.data_utils.DataChunk`. This functionality was originally part of `HDF5IO.__write_chunk__()` and has been moved here to enable reuse of the code across data backends. @oruebel (#697)
- Added `ignore_string_to_byte` option for `TestCase.assertContainerEqual` to allow us to ignore conversion from string to bytes and just compare unicode values, as different backends may store strings differently. @oruebel (#697)
- Allow `zarr.Array` as array datatype in docval to support conversion data stored in Zarr to HDMF HDF5 data. @oruebel (#721)
- Allow `hdmf.common.table.DynamicTable.add_column` to accept nested lists as column data and automatically create the `VectorIndex` and `VectorData` objects required to represent the ragged array. @oruebel (#728)
- Added a warning when the ``__init__`` of a class mapped to an HDMF-common data type or an autogenerated data type class
  is passed positional arguments instead of all keyword arguments. @rly (#730)
- Added helper function `hdmf.utils.popargs_to_dict` for popping docval args into a dict. @rly (#732)

### Bug fixes
- Fixed error with modifying files that contain external links to other files (e.g., shallow copies). @rly (#709)
- Fixed opening of files in append mode on Windows when the files contain links to other open files. @rly (#710)
- Updated `HDF5IO` to always set the `location` attribute of `GroupBuilders`, `DatasetBuilders`, and `LinkBuilders` on read. @oruebel (#697)
- Updated `HDF5IO.get_types` to correctly determine the data type for `bytes` data. @oruebel (#697)

### Minor improvements
- Updated `HDF5IO` to use the new `WriteStatusTracker`, `NamespaceToBuilderHelper`, and `HDF5IODataChunkIteratorQueue` helper classes. @oruebel (#697)
- Updated `hdmf.common.sparse.CSRMatrix` to avoid direct dependency on h5py as a particular storage backend. @oruebel (#697)
- Improved readability of ``Container`` code. @rly (#707)
- Use GitHub Actions for all CI. @rly (#718)
- Allow `write_args=None` to be passed to `HDF5IO.export`. @rly (#733)
- Updated requirements and fixed minor documentation issues and warnings. @rly (#731)

### Test enhancements
- Moved test functions to ease reuse and updated tests accordingly. @oruebel (#697)
- Moved `Baz`, `BazData`, `BazCpdData`, `BazBucket`, `get_baz_buildmanager` test data classes from `tests.unit.test_io_hdf5_h5tools` to `tests.unit.utils` to ease reuse and updated tests accordingly. Also `_get_baz_manager` was renamed to `get_baz_buildmanager` as part of this move. @oruebel (#697)
- Added numerous tests to `tests/unit/common/test_sparse.py` to enhance testing of the `CSRMatrix` type. @oruebel (#697)

### Documentation and tutorial enhancements:
- Add copy button to code blocks. @weiglszonja @oruebel (#726)

## HDMF 3.2.1 (February 22, 2022)

### Bug fixes
- Fixed release CI that prevented distribution from being uploaded to PyPI. @rly (#699)

## HDMF 3.2.0 (February 22, 2022)

### New features
- Added ``hdmf.container.Row.__str__`` to improve print of rows. @oruebel (#667)
- Added ``to_dataframe`` method for ``hdmf.common.resources.ExternalResource`` to improve visualization. @oruebel (#667)
- Added ``export_to_sqlite`` method for ``hdmf.common.resources.ExternalResource``. @oruebel (#667)
- Added ``reset_parent`` method for ``hdmf.container.Container``. @rly (#692)

### Minor improvements
- Plotted results in external resources tutorial. @oruebel (#667)
- Added support for Python 3.10. @rly (#679)
- Updated requirements. @rly @TheChymera (#681)
- Improved testing for `ExternalResources`. @mavaylon (#673)
- Improved docs for export. @rly (#674)
- Enhanced data chunk iteration speeds through new ``GenericDataChunkIterator`` class.  @CodyCBakerPhD (#672)
- Enhanced issue template forms on GitHub. @CodyCBakerPHD (#700)

### Bug fixes
- Fixed `setup.py` not being able to import `versioneer` when installing in an embedded Python environment. @rly (#662)
- Fixed broken tests in Python 3.10. @rly (#664)
- Fixed broken LaTeX PDF build of the docs. @oruebel (#669)
- Fixed adding containers as a child to a parent container sometimes not marking the parent container as modified. @rly
  (#683)
- Fixed `to_hierarchcial_dataframe` failing when a table contains a `VectorIndex` column as a regular data column.
  @oruebel (#666)
- Stop testing against base Python error messages because they may change in the future. @rly (#689)

## HDMF 3.1.1 (July 29, 2021)

### Bug fixes
- Updated the new ``DynamicTableRegion.get_linked_tables`` function (added in 3.1.0) to return lists of ``typing.NamedTuple``
  objects rather than lists of dicts. @oruebel (#660)

## HDMF 3.1.0 (July 29, 2021)

### New features
- Added several features to simplify interaction with ``DynamicTable`` objects that link to other tables via
  ``DynamicTableRegion`` columns. @oruebel (#645)
    - Added ``DynamicTable.get_foreign_columns`` to find all columns in a table that are a ``DynamicTableRegion``
    - Added ``DynamicTable.has_foreign_columns`` to identify if a ``DynamicTable`` contains ``DynamicTableRegion`` columns
    - Added ``DynamicTable.get_linked_tables`` to retrieve all tables linked to either directly or indirectly from
      the current table via ``DynamicTableRegion``
    - Implemented the new ``get_foreign_columns``, ``has_foreign_columns``, and ``get_linked_tables`` also for
      ``AlignedDynamicTable``
    - Added new module ``hdmf.common.hierarchicaltable`` with helper functions to facilitate conversion of
      hierarchically nested ``DynamicTable`` objects via the following new functions:
      - ``to_hierarchical_dataframe`` to merge linked tables into a single consolidated pandas DataFrame.
      - ``drop_id_columns`` to remove "id" columns from a DataFrame.
      - ``flatten_column_index`` to replace a ``pandas.MultiIndex`` with a regular ``pandas.Index``

### Bug fixes
- Do not build wheels compatible with Python 2 because HDMF requires Python 3.7. @rly (#642)
- ``AlignedDynamicTable`` did not overwrite its ``get`` function. When using ``DynamicTableRegion`` to reference ``AlignedDynamicTable`` this led to cases where the columns of the category subtables where omitted during data access (e.g., conversion to pandas.DataFrame). This fix adds the ``AlignedDynamicTable.get`` based on the existing ``AlignedDynamicTable.__getitem__``. @oruebel (#645)
- Fixed #651 to support selection of cells in an ``AlignedDynamicTable`` via slicing with  ``[int, (str, str)]``(and ``[int, str, str]``) to select a single cell, and ``[int, str]`` to select a single row of a category table. @oruebel (#645)

### Minor improvements
- Updated ``DynamicTable.to_dataframe()`` and ``DynamicTable.get`` functions to set the ``.name`` attribute
  on generated pandas DataFrame objects. @oruebel (#645)
- Added ``AlignedDynamicTable.get_colnames(...)`` to support look-up of the full list of columns as the
  ``AlignedDynamicTable.colnames`` property only includes the columns of the main table for compliance with
  ``DynamicTable`` @oruebel (#645)
- Fix documentation for `DynamicTable.get` and `DynamicTableRegion.get`. @rly (#650)
- Allow passing string column name to `DynamicTableRegion`, i.e., `dtr['col_name']` is a shortcut to
  `dtr.table['col_name']`. @rly (#657)

## HDMF 3.0.1 (July 7, 2021)

### Bug fixes
- Fixed release CI that prevented distribution from being uploaded to PyPI. @rly (#641)

## HDMF 3.0.0 (July 6, 2021)

### New features
- Add support for Python 3.9, drop support for Python 3.6. @rly (#620)
- Add support for h5py 3. @ajtritt (#480)
  - h5py 3 introduced [breaking changes regarding how strings are handled]
  (https://docs.h5py.org/en/latest/whatsnew/3.0.html#breaking-changes-deprecations), specifically that
  variable-length UTF-8 strings in datasets are now read as `bytes` objects instead of `str` by default.
  To reduce the impact of this change on HDMF users, when HDMF reads a variable-length UTF-8 string
  dataset, instead of returning an `h5py.Dataset` that is read as `bytes` objects, HDMF will return a
  `hdmf.utils.StrDataset` object that extends `h5py.Dataset` and is read as `str` objects, which preserves
  previous behavior. For example, under HDMF 2.x, an HDF5 dataset `d` with data ['a', 'b'] is read as a
  `h5py.Dataset` object, and `d[:]` returns `str` objects. Under HDMF 3.x, the same dataset `d` is read
  as a `hdmf.utils.StrDataset` object and `d[:]` still returns `str` objects.
- Add RRID to docs. @oruebel (#633)
- Allow passing ``index=True`` to ``DynamicTable.to_dataframe()`` to support returning `DynamicTableRegion` columns
  as indices or Pandas DataFrame. @rly (#579)
- Improve ``DynamicTable`` documentation. @rly (#639)
- Updated external resources tutorial. @mavaylon (#611)

### Breaking changes and deprecations
- Previously, when using ``DynamicTable.__getitem__`` or ``DynamicTable.get`` to access a selection of a
  ``DynamicTable`` containing a ``DynamicTableRegion``, new columns with mangled names for the table data referred to
  by the ``DynamicTableRegion`` were added to the returned DataFrame. This did not work properly for ragged
  ``DynamicTableRegion``, multiple levels of nesting, or multiple rows returned.
  Now, these methods will by default return columns of indices of the ``DynamicTableRegion``. If ``index=False`` is
  passed to ``DynamicTable.get``, then nested DataFrames will be returned, one DataFrame per row of the original
  resulting DataFrame. @rly (#579)

### Minor improvements
- Updated requirements and tests. @rly (#640)

### Bug fixes
- Update the validator to allow extensions to data types which only define data_type_inc. @dsleiter (#609)
- Fix error when validating lazy-loaded datasets containing references. @dsleiter (#609)
- Fix error when using ``DynamicTable.__getitem__`` or ``DynamicTable.get`` when table has a ragged
  ``DynamicTableRegion``. @rly (#579)

## HDMF 2.5.8 (June 16, 2021)
- Fix incorrect dtype precision upgrade for VectorIndex (#631)

### Minor improvements
- Improve Sphinx documentation. @rly (#627)

### Bug fix
- Fix error with representing an indexed table column when the `VectorIndex` dtype precision is upgraded more
  than one step, e.g., uint8 to uint32. This can happen when, for example, a single `add_row` call is used to
  add more than 65535 elements to an empty indexed column. @rly (#631)

## HDMF 2.5.7 (June 4, 2021)

### Bug fix
- Fix generation of extension classes that extend `MultiContainerInterface` and use a custom _fieldsname. @rly (#626)

## HDMF 2.5.6 (May 19, 2021)

### Bug fix
- Raise minimum version of pandas from 0.23 to 1.0.5 to be compatible with numpy 1.20. @rly (#618)
- Update documentation and update structure of requirements files. @rly (#619)

## HDMF 2.5.5 (May 17, 2021)

### Bug fix
- Fix incompatibility issue with downstream github-release tool used to deploy releases to GitHub. @rly (#614)

## HDMF 2.5.4 (May 17, 2021)

### Bug fix
- Fix incompatibility issue with downstream github-release tool used to deploy releases to GitHub. @rly (#607)
- Fix issue where dependencies of included types were not being loaded in namespaces / extensions. @rly (#613)

## HDMF 2.5.3 (May 12, 2021)

### Bug fix
- Fix issue where tables with multi-indexed columns defined using `__columns__` did not have attributes properly set.
  @rly (#605)

## HDMF 2.5.2 (May 11, 2021)

### Bug fix
- Add explicit `setuptools` requirement. @hrnciar (#596)
- Fix issue with generated custom classes that use a custom fields name (e.g., PyNWB uses `__nwbfields__` instead
  of `__fields__`). @rly (#598)
- Fix issue with Sphinx Gallery. @rly (#601)

## HDMF 2.5.1 (April 23, 2021)

### Bug fix
- Revert breaking change in `TypeMap.get_container_cls`. While this function is returned to its original behavior,
  it will be modified at the next major release. Please use the new `TypeMap.get_dt_container_cls` instead. @rly (#590)

## HDMF 2.5.0 (April 22, 2021)

### New features
- `DynamicTable` can be automatically generated using `get_class`. Now the HDMF API can read files with extensions
  that contain a `DynamicTable` without needing to import the extension first. @rly and @bendichter (#536)
- Add `HDF5IO.get_namespaces(path=path, file=file)` method which returns a dict of namespace name mapped to the
  namespace version (the largest one if there are multiple) for each namespace cached in the given HDF5 file.
  @rly (#527)
- Use HDMF common schema 1.5.0.
  - Add experimental namespace to HDMF common schema. New data types should go in the experimental namespace
    (hdmf-experimental) prior to being added to the core (hdmf-common) namespace. The purpose of this is to provide
    a place to test new data types that may break backward compatibility as they are refined. @ajtritt (#545)
  - `ExternalResources` was changed to support storing both names and URIs for resources. @mavaylon (#517, #548)
  - The `VocabData` data type was replaced by `EnumData` to provide more flexible support for data from a set of
    fixed values.
  - Added `AlignedDynamicTable`, which defines a `DynamicTable` that supports storing a collection of sub-tables.
    Each sub-table is itself a `DynamicTable` that is aligned with the main table by row index. Each sub-table
    defines a sub-category in the main table effectively creating a table with sub-headings to organize columns.
  - See https://hdmf-common-schema.readthedocs.io/en/latest/format_release_notes.html#april-19-2021 for more
    details.
- Add `EnumData` type for storing data that comes from a fixed set of values. This replaces `VocabData` i.e.
  `VocabData` has been removed. `VocabData` stored vocabulary elements in an attribute, which has a size limit.
  `EnumData` now stores elements in a separate dataset, referenced by an attribute stored on the `EnumData` dataset.
  @ajtritt (#537)
- Add `AlignedDynamicTable` type which defines a DynamicTable that supports storing a collection of subtables.
  Each sub-table is itself a DynamicTable that is aligned with the main table by row index. Each subtable
  defines a sub-category in the main table effectively creating a table with sub-headings to organize columns.
  @oruebel (#551)
- Add tutoral for new `AlignedDynamicTable` type. @oruebel (#571)
- Equality check for `DynamicTable` now also checks that the name and description of the table are the same. @rly (#566)

### Internal improvements
- Update CI and copyright year. @rly (#523, #524)
- Refactor class generation code. @rly (#533, #535)
- Equality check for `DynamicTable` returns False if the other object is a `DynamicTable` instead of raising an error.
  @rly (#566)
- Update ruamel.yaml usage to new API. @rly (#587)
- Remove use of ColoredTestRunner for more readable verbose test output. @rly (#588)

### Bug fixes
- Fix CI testing on Python 3.9. @rly (#523)
- Fix certain edge cases where `GroupValidator` would not validate all of the child groups or datasets
  attached to a `GroupBuilder`. @dsleiter (#526)
- Fix bug for generating classes from link specs and ignored 'help' fields. @rly (#535)
- Various fixes for dynamic class generation. @rly (#561)
- Fix generation of classes that extends both `MultiContainerInterface` and another class that extends
  `MultiContainerInterface`. @rly (#567)
- Fix `make clean` command for docs to clean up sphinx-gallery tutorial files. @oruebel (#571)
- Make sure we cannot set ``AlignedDynamicTable`` as a category on an ``AlignedDynamicTable``. @oruebel (#571)
- Fix included data type resolution between HDMF and custom classes that customize the data_type_inc key. @rly (#503)
- Fix classification of attributes as new/overridden. @rly (#503)

## HDMF 2.4.0 (February 23, 2021)

### New features
- `GroupValidator` now checks if child groups, datasets, and links have the correct quantity of elements and returns
  an `IncorrectQuantityError` for each mismatch. @dsleiter (#500)

### Internal improvements
- Update CI. @rly (#432)
- Added  driver option for ros3. @bendichter (#506)

### Bug fixes
- Allow `np.bool_` as a valid `bool` dtype when validating. @dsleiter (#505)
- Fix building of Data objects where the spec has no dtype and the Data object value is a DataIO wrapping an
  AbstractDataChunkIterator. @rly (#512)
- Fix TypeError when validating a group with an illegally-linked child.
  @dsleiter (#515)
- Fix `DynamicTable.get` for compound type columns. @rly (#518)
- Fix and removed error "Field 'x' cannot be defined in y." when opening files with some extensions. @rly
  (#519)

## HDMF 2.3.0 (December 8, 2020)

### New features
- Add methods for automatic creation of `MultiContainerInterface` classes. @bendichter (#420, #425)
- Add ability to specify a custom class for new columns to a `DynamicTable` that are not `VectorData`,
  `DynamicTableRegion`, or `VocabData` using `DynamicTable.__columns__` or `DynamicTable.add_column(...)`. @rly (#436)
- Add support for creating and specifying multi-index columns in a `DynamicTable` using `add_column(...)`.
  @bendichter, @rly (#430)
- Add capability to add a row to a column after IO. @bendichter (#426)
- Add method `AbstractContainer.get_fields_conf`. @rly (#441)
- Add functionality for storing external resource references. @ajtritt (#442)
- Add method `hdmf.utils.get_docval_macro` to get a tuple of the current values for a docval_macro, e.g., 'array_data'
  and 'scalar_data'. @rly (#446)
- Add `SimpleMultiContainer`, a data_type for storing a `Container` and `Data` objects together. @ajtritt (#449)
- Support `pathlib.Path` paths in `HDMFIO.__init__`, `HDF5IO.__init__`, and `HDF5IO.load_namespaces`. @dsleiter (#450)
- Use hdmf-common-schema 1.2.1. See https://hdmf-common-schema.readthedocs.io/en/latest/format_release_notes.html for details.
- Block usage of h5py 3+. h5py>=2.9, <3 is supported. @rly (#461)
- Block usage of numpy>=1.19.4 due to a known issue with numpy on some Windows 10 systems. numpy>1.16, <1.19.4 is supported.
  @rly (#461)
- Add check for correct quantity during the build process in `ObjectMapper`. @rly (#463, #492)
- Allow passing `GroupSpec` and `DatasetSpec` objects for the 'target_type' argument of `LinkSpec.__init__(...)`.
  @rly (#468)
- Use hdmf-common-schema 1.3.0. @rly, @ajtritt (#486)
  - Changes from hdmf-common-schema 1.2.0:
    - Add data type ExternalResources for storing ontology information / external resource references. NOTE:
      this data type is in beta testing and is subject to change in a later version.
    - Fix missing data_type_inc and use dtype uint for CSRMatrix. It now has data_type_inc: Container.
    - Add hdmf-schema-language comment at the top of each yaml file.
    - Add SimpleMultiContainer, a Container for storing other Container and Data objects together.

### Internal improvements
- Drop support for Python 3.5. @ajtritt (#459)
- Improve warning about cached namespace when loading namespaces from file. @rly (#422)
- Refactor `HDF5IO.write_dataset` to be more readable. @rly (#428)
- Fix bug in slicing tables with DynamicTableRegions. @ajtritt (#449)
- Add testing for Python 3.9 and using pre-release packages. @ajtritt, @rly (#459, #472)
- Improve contributing guide. @rly (#474)
- Update CI. @rly, @dsleiter (#481, #493, #497)
- Add citation information to documentation and support for duecredit tool. @rly (#477, #488)
- Add type checking and conversion in `CSRMatrix`. @rly (#485)
- Clean up unreachable validator code. @rly (#483)
- Reformat imports. @bendichter (#469)
- Remove unused or refactored internal builder functions `GroupBuilder.add_group`, `GroupBuilder.add_dataset`,
  `GroupBuilder.add_link`, `GroupBuilder.set_builder`, `BaseBuilder.deep_update`, `GroupBuilder.deep_update`,
  `DatasetBuilder.deep_update`. Make `BaseBuilder` not instantiable and refactor builder code. @rly (#452)

### Bug fixes
- Fix development package dependency issues. @rly (#431)
- Fix handling of empty lists against a spec with text/bytes dtype. @rly (#434)
- Fix handling of 1-element datasets with compound dtype against a scalar spec with text/bytes dtype. @rly (#438)
- Fix convert dtype when writing numpy array from `h5py.Dataset`. @rly (#427)
- Fix inheritance when non-`AbstractContainer` is base class. @rly (#444)
- Fix use of `hdmf.testing.assertContainerEqual(...)` for `Data` objects. @rly (#445)
- Add missing support for data conversion against spec dtypes "bytes" and "short". @rly (#456)
- Clarify the validator error message when a named data type is missing. @dsleiter (#478)
- Update documentation on validation to indicate that the example command is not implemented @dsleiter (#482)
- Fix generated docval for classes with a LinkSpec. @rly (#487)
- Fix access of `DynamicTableRegion` of a `DynamicTable` with column of references. @rly (#491)
- Fix handling of `__fields__` for `Data` subclasses. @rly (#441)
- Fix `DynamicTableRegion` having duplicate fields conf 'table'. @rly (#441)
- Fix inefficient and sometimes inaccurate build process. @rly (#451)
- Fix garbage collection issue in Python 3.9. @rly (#496)

## HDMF 2.2.0 (August 14, 2020)

### New features
- Add ability to get list of tuples when indexing a `DynamicTable`. i.e. disable conversion to `pandas.DataFrame`.
  @ajtritt (#418)

### Internal improvements
- Improve documentation and index out of bounds error message for `DynamicTable`. @rly (#419)

### Bug fixes:
- Fix error when constructing `DynamicTable` with `DataChunkIterators` as columns. @ajtritt (#418)

## HDMF 2.1.0 (August 10, 2020)

### New features
- Users can now use the `MultiContainerInterface` class to generate custom API classes that contain collections of
  containers of a specified type. @bendichter @rly (#399)
  - See the user guide
    https://hdmf.readthedocs.io/en/stable/tutorials/multicontainerinterface.html for more information.

### Internal improvements
- Add ability to pass callable functions to run when adding or removing items from a ``LabelledDict``.
  An error is now raised when using unsupported functionality in ``LabelledDict``. @rly (#405)
- Raise a warning when building a container that is missing a required dataset. @rly (#413)

## HDMF 2.0.1 (July 22, 2020)

### Internal improvements
- Add tests for writing table columns with DataIO data, e.g., chunked, compressed data. @rly (#402)
- Add CI to check for breakpoints and print statements. @rly (#403)

### Bug fixes:
- Remove breakpoint. @rly (#403)
- Allow passing None for docval enum arguments with default value None. @rly (#409)
- If a file is written with an orphan container, e.g., a link to a container that is not written, then an
  `OrphanContainerBuildError` will be raised. This replaces the `OrphanContainerWarning` that was previously raised.
  @rly (#407)

## HDMF 2.0.0 (July 17, 2020)

### New features
- Users can now call `HDF5IO.export` and `HDF5IO.export_io` to write data that was read from one source to a new HDF5
  file. Developers can implement the `export` method in classes that extend `HDMFIO` to customize the export
  functionality. See https://hdmf.readthedocs.io/en/latest/export.html for more details. @rly (#388)
- Users can use the new export functionality to read data from one source, modify the data in-memory, and then write the
  modified data to a new file. Modifications can include additions and removals. To facilitate removals,
  `AbstractContainer` contains a new `_remove_child` method and `BuildManager` contains a new `purge_outdated` method.
  @rly (#388)
- Users can now call `Container.generate_new_id` to generate new object IDs for the container and all of its children.
  @rly (#401)
- Use hdmf-common-schema 1.2.0. @ajtritt @rly (#397)
  - `VectorIndex` now extends `VectorData` instead of `Index`. This change allows `VectorIndex` to index other `VectorIndex` types.
  - The `Index` data type is now unused and has been removed.
  - Fix missing dtype for `VectorIndex`.
  - Add new `VocabData` data type.

### Breaking changes
- `Builder` objects no longer have the `written` field which was used by `HDF5IO` to mark the object as written. This
  is replaced by `HDF5IO.get_written`. @rly (#381)
- `HDMFIO.write` and `HDMFIO.write_builder` no longer have the keyword argument `exhaust_dcis`. This remains present in
  `HDF5IO.write` and `HDF5IO.write_builder`. @rly (#388)
- The class method `HDF5IO.copy_file` is no longer supported and may be removed in a future version. Please use the
  `HDF5IO.export` method or `h5py.File.copy` method instead. @rly (#388)

## HDMF 1.6.4 (June 26, 2020)

### Internal improvements
- Add ability to close open links. @rly (#383)

### Bug fixes:
- Fix validation of empty arrays and scalar attributes. @rly (#377)
- Fix issue with constructing `DynamicTable` with empty array colnames. @rly (#379)
- Fix `TestCase.assertContainerEqual` passing wrong arguments. @rly (#385)
- Fix 'link_data' argument not being used when writing non-root level datasets. @rly (#384)
- Fix handling of ASCII numpy array. @rly (#387)
- Fix error when optional attribute reference is missing. @rly (#392)
- Improve testing for `get_data_shape` and fix issue with sets. @rly (#394)
- Fix inability to write references to HDF5 when the root builder is not named "root". @rly (#395)

## HDMF 1.6.3 (June 9, 2020)

### Internal improvements
- Improve documentation of `DynamicTable`. @rly (#371)
- Add user guide / tutorial for `DynamicTable`. @rly (#372)
- Improve logging of build and write processes. @rly (#373)

### Bug fixes:
- Fix adding of optional predefined columns to `DynamicTable`. @rly (#371)
- Use dtype from dataset data_type definition when extended spec lacks dtype. @rly (#364)

## HDMF 1.6.2 (May 26, 2020)

### Internal improvements:
- Update MacOS in CI. @rly (#310)
- Raise more informative error when adding column to `DynamicTable` w/ used name. @rly (#307)
- Refactor `_init_class_columns` for use by DynamicTable subclasses. @rly (#323)
- Add/fix docstrings for DynamicTable. @oruebel, @rly (#304, #353)
- Make docval-decorated functions more debuggable in pdb. @rly (#308)
- Change dtype conversion warning to include path to type. @rly (#311)
- Refactor `DynamicTable.add_column` to raise error when name is an optional column. @rly (#305)
- Improve unsupported filter error message. @bendichter (#329)
- Add functionality to validate a yaml file against a json schema file. @bendichter (#332)
- Update requirements-min.txt for yaml validator. @bendichter (#333)
- Add allowed value / enum validation in docval. @rly (#335)
- Add logging of build and hdf5 write process. @rly (#336, #349)
- Allow loading namespaces from h5py.File object not backed by file. @rly (#348)
- Add CHANGELOG.md. @rly (#352)
- Fix codecov reports. @rly (#362)
- Make `getargs` raise an error if the argument name is not found. @rly (#365)
- Improve `get_class` and `docval` support for uint. @rly (#361)

### Bug fixes:
- Register new child types before new parent type for dynamic class generation. @rly (#322)
- Raise warning not error when adding column with existing attr name. @rly (#324)
- Add `__version__`. @rly (#345)
- Only write a specific namespace version if it does not exist. @ajtritt (#346)
- Fix documentation formatting for DynamicTable. @rly (#353)


## HDMF 1.6.1 (Mar. 2, 2020)

### Internal improvements:
- Allow docval to warn about use of positional arguments. @rly (#293)
- Improve efficiency of writing chunks with `DataChunkIterator` and HDF5. @d-sot, @oruebel (#295)

### Bug fixes:
- Flake8 style fixes. @oruebel (#291)
- Handle missing namespace version. @rly (#292)
- Do not raise error when a numeric type with a higher precision is provided for a spec with a lower precision and different base type. Raise a warning when the base type of a given value is converted to the specified base type, regardless of precision level. Add missing support for boolean conversions. @rly (#298, #299)
- Add forgotten validation of links. @t-b, @ajtritt (#286)
- Improve message for "can't change container_source" error. @rly (#302)
- Fix setup.py development status. @rly (#303)
- Refactor missing namespace version handling. @rly, @ajtritt (#297)
- Add print function for `DynamicTableRegion`. @oruebel, @rly (#290)
- Fix writing of refined RefSpec attribute. @oruebel, @rly (#301)

## HDMF 1.6.0 (Jan. 31, 2020)

### Internal improvements:
- Allow extending/overwriting attributes on dataset builders. @rly, @ajtritt (#279)
- Allow ASCII data where UTF8 is specified. @rly (#282)
- Add function to convert `DynamicTableRegion` to a pandas dataframe. @oruebel (#239)
- Add "mode" property to HDF5IO. @t-b (#280)

### Bug fixes:
- Fix readthedocs config to include all submodules. @rly (#277)
- Fix test runner double printing in non-verbose mode. @rly (#278)

## HDMF 1.5.4 (Jan. 21, 2020)

### Bug fixes:
- Upgrade hdmf-common-schema 1.1.2 -> 1.1.3, which includes a bug fix for missing data and shape keys on `VectorData`, `VectorIndex`, and `DynamicTableRegion` data types. @rly (#272)
- Clean up documentation scripts. @rly (#268)
- Fix broken support for pytest testing framework. @rly (#274)
- Fix missing CI testing of minimum requirements on Windows and Mac. @rly (#270)
- Read 1-element datasets as scalar datasets when a scalar dataset is expected by the spec. @rly (#269)
- Fix bug where 'version' was not required for `SpecNamespace`. @bendichter (#276)

## HDMF 1.5.3 (Jan. 14, 2020)

### Minor improvements:
- Update and fix documentation. @rly (#267)

### Bug fixes:
- Fix ReadTheDocs integration. @rly (#263)
- Fix conda build. @rly (#266)

## HDMF 1.5.2 (Jan. 13, 2020)

### Minor improvements:
- Add support and testing for Python 3.8. @rly (#247)
- Remove code duplication and make Type/Value Error exceptions more informative. @yarikoptic (#243)
- Streamline CI and add testing of min requirements. @rly (#258)

### Bug fixes:
- Update hdmf-common-schema submodule to 1.1.2. @rly (#249, #252)
- Add support for `np.array(DataIO)` in py38. @rly (#248)
- Fix bug with latest version of coverage. @rly (#251)
- Stop running CI on latest and latest-tmp tags. @rly (#254)
- Remove lingering mentions of PyNWB. @rly (#257, #261)
- Fix and clean up documentation. @rly (#260)

## HDMF 1.5.1 (Jan. 8, 2020)

### Minor improvements:
- Allow passing HDF5 integer filter ID for dynamically loaded filters. @d-sot (#215)

### Bug fixes:
- Fix reference to hdmf-common-schema 1.1.0. @rly (#231)

## HDMF 1.5.0 (Jan. 6, 2020)

### Minor improvements:
- Improve CI for HDMF to test whether changes in HDMF break PyNWB. #207 (@rly)
- Improve and clean up unit tests. #211, #214, #217 (@rly)
- Refactor code to remove six dependency and separate ObjectMapper into its own file. #213, #221 (@rly)
- Output exception message in ObjectMapper.construct. #220 (@t-b)
- Improve docstrings for VectorData, VectorIndex, and DynamicTableRegion. #226, #227 (@bendichter)
- Remove unused "datetime64" from supported dtype strings. #230 (@bendichter)
- Cache builders by h5py object id not name. #235 (@oruebel)
- Update copyright date and add legal to source distribution. #232 (@rly)
- Allow access to export_spec function from hdmf.spec package. #233 (@bendichter)
- Make calls to docval functions more efficient, resulting in a ~20% overall speedup. #238 (@rly)

### Bug fixes:
- Fix wrong reference in ObjectMapper.get_carg_spec. #208 (@rly)
- Fix container source not being set for some references. #219 (@rly)

Python 2.7 is no longer supported.

## HDMF 1.4.0 and earlier

Please see the release notes on the [HDMF GitHub repo Releases page](https://github.com/hdmf-dev/hdmf/releases).
