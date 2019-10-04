# hdmf-common
Specifications for pre-defined data structures of HDMF

The HDMF common provides the following data structures:
- DynamicTable : a column-based table structure that stores one-to-one and one-to-many relationships
  - VectorData : a data structure for representing a column
  - VectorIndex : a data structure for indexing a VectorData. This is used to store one-to-many relationships
  - ElementIdentifiers : a 1D array for storing primary identifiers for elements of a table
- CSRMatrix : a compressed sparse row matrix
