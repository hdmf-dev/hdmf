# HDMF Changelog

## HDMF 2.0.0 (Upcoming)

### New features
- Users can now call `HDF5IO.export` and `HDF5IO.export_io` to write data that was read from one source to a new HDF5
  file. Developers can implement the `export` method in classes that extend `HDMFIO` to customize the export
  functionality. @rly (#388)
- Users can use the new export functionality to read data from one source, modify the data in-memory, and then write the
  modified data to a new file. Modifications can include additions and removals. To facilitate removals,
  `AbstractContainer` contains a new `_remove_child` method and `BuildManager` contains a new `purge_outdated` method.

### Breaking changes
- `Builder` objects no longer have the `written` field which was used by `HDF5IO` to mark the object as written. This
  is replaced by `HDF5IO.get_written`. @rly (#381)
- `HDMFIO.write` and `HDMFIO.write_builder` no longer have the keyword argument `exhaust_dcis`. This remains present in
  `HDF5IO.write` and `HDF5IO.write_builder`. @rly (#388)
- The class method `HDF5IO.copy_file` is no longer supported and may be removed in a future version. Please use the
  `HDF5IO.export` method or `h5py.File.copy` method instead.

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
