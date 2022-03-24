Export
======

Export is a new feature in HDMF 2.0. You can use export to take a container that was read from a file and write it to
a different file, with or without modifications to the container in memory.
The in-memory container being exported will be written to the exported file as if it was never read from a file.

To export a container, first read the container from a file, then create a new
:py:class:`~hdmf.backends.hdf5.h5tools.HDF5IO` object for exporting the data, then call
:py:meth:`~hdmf.backends.hdf5.h5tools.HDF5IO.export` on the
:py:class:`~hdmf.backends.hdf5.h5tools.HDF5IO` object, passing in the IO object used to read the container
and optionally, the container itself, which may be modified in memory between reading and exporting.

For example:

.. code-block:: python

   with HDF5IO(self.read_path, manager=manager, mode='r') as read_io:
       with HDF5IO(self.export_path, mode='w') as export_io:
           export_io.export(src_io=read_io)

FAQ
---

Can I read a container from disk, modify it, and then export the modified container?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Yes, you can export the in-memory container after modifying it in memory. The modifications will appear in the exported
file and not the read file.

- If the modifications are removals or additions of containers, then no special action must be taken, as long as the
  container hierarchy is updated correspondingly.
- If the modifications are changes to attributes, then
  :py:meth:`Container.set_modified() <hdmf.container.AbstractContainer.set_modified>` must be called
  on the container before exporting.

.. code-block:: python

   with HDF5IO(self.read_path, manager=manager, mode='r') as read_io:
       container = read_io.read()
       # ...  # modify container
       container.set_modified()  # this may be necessary if the modifications are changes to attributes
       with HDF5IO(self.export_path, mode='w') as export_io:
           export_io.export(src_io=read_io, container=container)

.. note::

  Modifications to :py:class:`h5py.Dataset <h5py.Dataset>` objects act *directly* on the read file on disk.
  Changes are applied immediately and do not require exporting or writing the file. If you want to modify a dataset
  only in the new file, than you should replace the whole object with a new array holding the modified data. To
  prevent unintentional changes to the source file, the source file should be opened with ``mode='r'``.

Can I export a newly instantiated container?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
No, you can only export containers that have been read from a file. The ``src_io`` argument is required in
:py:meth:`HDMFIO.export <hdmf.backends.io.HDMFIO.export>`.

Can I read a container from disk and export only part of the container?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
It depends. You can only export the root container from a file. To export the root container without certain other
sub-containers in the hierarchy, you can remove those other containers before exporting. However, you cannot export
only a sub-container of the container hierarchy.

Can I write a newly instantiated container to two different files?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
HDMF does not allow you to write a container that was not read from a file to two different files. For example, if you
instantiate container A and write it file 1 and then try to write it to file 2, an error will be raised. However, you
can read container A from file 1 and then export it to file 2, with or without modifications to container A in
memory.

What happens to links when I export?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The exported file will not contain any links to the original file.

All links (such as internal links (i.e., HDF5 soft links) and links to other files (i.e., HDF5 external links))
will be preserved in the exported file.

If a link to an :py:class:`h5py.Dataset <h5py.Dataset>` in another file is added to the in-memory container after
reading it from file and then exported, then by default, the export process will create an external link to the
existing :py:class:`h5py.Dataset <h5py.Dataset>` object. To instead copy the data from the
:py:class:`h5py.Dataset <h5py.Dataset>` in another
file to the exported file, pass the keyword argument ``write_args={'link_data': False}`` to
:py:meth:`HDF5IO.export <hdmf.backends.hdf5.h5tools.HDF5IO.export>`. This is similar to passing the keyword argument
``link_data=False`` to :py:meth:`HDF5IO.write <hdmf.backends.hdf5.h5tools.HDF5IO.write>` when writing a file with a
copy of externally linked datasets.

What happens to references when I export?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
References will be preserved in the exported file.
NOTE: Exporting a file involves loading into memory all datasets that contain references and attributes that are
references. The HDF5 reference IDs within an exported file may differ from the reference IDs in the original file.

What happens to object IDs when I export?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
After exporting a container, the object IDs of the container and its child containers will be identical to the object
IDs of the read container and its child containers. The object ID of a container uniquely identifies the container
within a file, but should *not* be used to distinguish between two different files.

If you would like all object IDs to change on export, then first call the method
:py:meth:`generate_new_id <hdmf.container.AbstractContainer.generate_new_id>` on the root container to generate
a new set of IDs for the root container and all of its children, recursively. Then export the container with its
new IDs. Note: calling the :py:meth:`generate_new_id <hdmf.container.AbstractContainer.generate_new_id>` method
changes the object IDs of the containers in memory. These changes are not reflected in the original file from
which the containers were read unless the :py:meth:`HDF5IO.write <hdmf.backends.hdf5.h5tools.HDF5IO.write>`
method is subsequently called.

.. code-block:: python

   with HDF5IO(self.read_path, manager=manager, mode='r') as read_io:
       container = read_io.read()
       container.generate_new_id()
       with HDF5IO(self.export_path, mode='w') as export_io:
           export_io.export(src_io=read_io, container=container)
