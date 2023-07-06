.. _modifying_with_sidecar:

Modifying an HDMF File with a Sidecar JSON File
===============================================

Users may want to make small updates or corrections to an HDMF file without rewriting the entire file.
To do so, HDMF supports the use of a "sidecar" JSON file that lives adjacent to the HDMF file on disk and
specifies modifications to the HDMF file. Only a limited set of modifications are supported; for example, users can
hide a dataset or attribute so that it will not be read by HDMF, but cannot create a new dataset, attribute, group,
or link.
When HDMF reads an HDMF file, if the corresponding sidecar JSON file exists, it is
automatically read and the modifications that it specifies are automatically applied.

.. note::

  This default behavior can be changed such that the corresponding sidecar JSON file is ignored when the HDMF file
  is read by passing ``load_sidecar=False`` to ``HDMFIO.read()`` on the ``HDMFIO`` object used to read the HDMF file.

Allowed modifications
---------------------

Only the following modifications to an HDMF file are supported in the sidecar JSON file:

- Replace the values of a dataset or attribute with a scalar or 1-D array
- Hide a dataset or attribute

.. note::

  Replacing the values of a dataset or attribute with a very large 1-D array using the sidecar JSON file may not
  be efficient and is discouraged. Users should instead consider rewriting the HDMF file with the
  updated values.

Specification for the sidecar JSON file
---------------------------------------

The sidecar JSON file can be validated using the ``sidecar.schema.json`` JSON schema file
located at the root of the HDMF repository. When an HDMF file is read, if a sidecar JSON file
is present, it is automatically read and validated against this JSON schema file.

The sidecar JSON file must contain the following top-level keys:

- ``"description"``: A free-form string describing the modifications specified in this file.
- ``"author"``: A list of free-form strings containing the names of the people who created this file.
- ``"contact"``: A list of email addresses for the people who created this file. Each author listed in the "author" key
  *should* have a corresponding email address.
- ``"operations"``: A list of operations to perform on the data in the file, as specified below.
- ``"schema_version"``: The version of the sidecar JSON schema that the file conforms to, e.g., "0.1.0".
  View the current version of this file here:
  `sidecar.schema.json <https://github.com/hdmf-dev/hdmf/blob/dev/sidecar.schema.json>`_

Here is an example sidecar JSON file:

.. code:: javascript

    {
        "description": "Summary of changes",
        "author": [
            "The NWB Team"
        ],
        "contact": [
            "contact@nwb.org"
        ],
        "operations": [
            {
                "type": "replace",
                "description": "change foo1/my_data data from [1, 2, 3] to [4, 5] (int8)",
                "object_id": "e0449bb5-2b53-48c1-b04e-85a9a4631655",
                "relative_path": "my_data",
                "value": [
                    4,
                    5
                ],
                "dtype": "int8"
            },
            {
                "type": "delete",
                "description": "delete foo1/foo_holder/my_sub_data/attr6",
                "object_id": "993fef27-680c-457a-af4d-b1d2725fcca9",
                "relative_path": "foo_holder/my_sub_data/attr6"
            }
        ],
        "schema_version": "0.1.0"
    }

Specification for operations
----------------------------

All operations are required to have the following keys:

- ``"type"``: The type of modification to perform. Only "replace" and "delete" are supported currently.
- ``"description"``: A description of the specified modification.
- ``"object_id"``: The object ID (UUID) of the data type that is closest in the file hierarchy to the
  field being modified.
- ``"relative_path"``: The relative path from the data type with the given object ID to the field being modified.

.. warning:

    Modifying a file via a sidecar file can result in a file that is no longer compliant with the format
    specification of the file. For example, we may hide a required dataset via a sidecar operation, resulting
    in an invalid file that, in the worst case, may longer be readable because required arguments are missing.
    It is strongly recommended that the file is validated against the schema after creating, modifying,
    and loading the sidecar JSON file.


Replacing values of a dataset/attribute with a scalar or 1-D array
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Specify ``"type": "replace"`` to replace the values of a dataset/attribute from the associated HDMF file
as specified by the ``object_id`` and ``relative_path``.

The operation specification must have the following keys:

- ``"value"``: The new value for the dataset/attribute. Only scalar and 1-dimensional arrays can be
  specified as a replacement value.

The operation specification may also have the following keys:

- ``"dtype"``: String representing the dtype of the new value. If this key is not present, then the dtype of the
  existing value for the dataset/attribute is used. Allowed dtypes are listed in the
  `HDMF schema language docs for dtype <https://hdmf-schema-language.readthedocs.io/en/latest/description.html#dtype>`_.

In the example sidecar JSON file above, the first operation specifies that the value of dataset "my_data" in
group "foo1", which has the specified object ID, should be replaced with the 1-D array [4, 5] (dtype: int8).

.. note::

  Replacing the values of datasets or attributes with object references or a compound data type is not yet supported.

Deleting a dataset/attribute
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Specify ``"type": "delete"`` to delete (ignore) a dataset/attribute from the associated HDMF file
as specified by the ``object_id`` and ``relative_path``.

The operation specification does not use any additional keys.

In the example sidecar JSON file above, the second operation specifies that attribute "attr6"
at relative path "foo_holder/my_sub_data/attr6" from group "foo1", which has the specified object ID,
should be deleted.
If "attr6" is a required attribute, this is likely to result in an invalid file that cannot be read by HDMF.

Future changes
--------------

The HDMF team is considering supporting additional operations and expanding support for current operations
specified in the sidecar JSON file, such as:

- Add rows to a ``DynamicTable`` (column-based)
- Add rows to a ``Table`` (row-based)
- Add a new group
- Add a new dataset
- Add a new attribute
- Add a new link
- Replace a dataset or attribute with object references
- Replace a dataset or attribute with a compound data type
- Replace selected slices of a dataset or attribute
- Delete a group
- Delete a link

Please provide feedback on which operations are useful to you for HDMF to support in this
`issue ticket <https://github.com/hdmf-dev/hdmf/issues/676>`_.
