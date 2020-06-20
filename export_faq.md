## Export FAQ

### What happens if I export an HDF5 file to a new HDF5 file with soft or external links?

Situation 1
- File A contains a soft link to a group/dataset/link
- Read File A
- Export File A to File B
- File B should contain a soft link

Situation 2
- File A contains an external link to a group/link in File B
- Read File A
- Export File A to File C
- File C should contain an external link to the group/link in File B

Situation 3a
- File A contains an external link to a dataset in File B
- Read File A
- Export File A to File C with link_data=True (default)
- File C should contain an external link to the dataset in File B

Situation 3b
- File A contains an external link to a dataset in File B
- Read File A
- Export File A to File C with link_data=False
- File C should contain a copy of the dataset in File B
- **TODO**: is link_data=False respected in this case? I think when an external link is loaded, it is loaded as a LinkBuilder,
not a DatasetBuilder, so link_data is not relevant.

What happens if I load an HDF5 file, add a soft/external link to it, and export it to a new HDF5 file?

Situation 4
- Read File A
- Add a soft link in File A to a group/dataset/link
- Export modified File A to File B
- File B should contain a soft link

Situation 5
- Read File A
- Add an external link in File A to a group/link in File B
- Export modified File A to File C
- File C should contain an external link to the group/link in File B

Situation 6a
- Read File A
- Add an external link in File A to a dataset in File B
- Export modified File A to File C with link_data=True (default)
- File C should contain an external link to the dataset in File B

Situation 6b
- Read File A
- Add an external link in File A to a dataset in File B
- Export modified File A to File C with link_data=False
- File C should contain a copy of the dataset in File B
