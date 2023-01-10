import numpy as np
import os
import re
import unittest
from abc import ABCMeta, abstractmethod

from .utils import remove_test_file
from ..backends.hdf5 import HDF5IO
from ..build import Builder
from ..common import validate as common_validate, get_manager
from ..container import AbstractContainer, Container, Data
from ..utils import get_docval_macro
from ..data_utils import AbstractDataChunkIterator


class TestCase(unittest.TestCase):
    """
    Extension of unittest's TestCase to add useful functions for unit testing in HDMF.
    """

    def assertRaisesWith(self, exc_type, exc_msg, *args, **kwargs):
        """
        Asserts the given invocation raises the expected exception. This is similar to unittest's assertRaises and
        assertRaisesRegex, but checks for an exact match.
        """

        return self.assertRaisesRegex(exc_type, '^%s$' % re.escape(exc_msg), *args, **kwargs)

    def assertWarnsWith(self, warn_type, exc_msg, *args, **kwargs):
        """
        Asserts the given invocation raises the expected warning. This is similar to unittest's assertWarns and
        assertWarnsRegex, but checks for an exact match.
        """

        return self.assertWarnsRegex(warn_type, '^%s$' % re.escape(exc_msg), *args, **kwargs)

    def assertContainerEqual(self,
                             container1,
                             container2,
                             ignore_name=False,
                             ignore_hdmf_attrs=False,
                             ignore_string_to_byte=False,
                             message=None):
        """
        Asserts that the two AbstractContainers have equal contents. This applies to both Container and Data types.

        :param container1: First container
        :type container1: AbstractContainer
        :param container2: Second container to compare with container 1
        :type container2: AbstractContainer
        :param ignore_name: whether to ignore testing equality of name of the top-level container
        :param ignore_hdmf_attrs: whether to ignore testing equality of HDMF container attributes, such as
                                  container_source and object_id
        :param ignore_string_to_byte: ignore conversion of str to bytes and compare as unicode instead
        :param message: custom additional message to show when assertions as part of this assert are failing
        """
        self.assertTrue(isinstance(container1, AbstractContainer), message)
        self.assertTrue(isinstance(container2, AbstractContainer), message)
        type1 = type(container1)
        type2 = type(container2)
        self.assertEqual(type1, type2, message)
        if not ignore_name:
            self.assertEqual(container1.name, container2.name, message)
        if not ignore_hdmf_attrs:
            self.assertEqual(container1.container_source, container2.container_source, message)
            self.assertEqual(container1.object_id, container2.object_id, message)
        # NOTE: parent is not tested because it can lead to infinite loops
        if isinstance(container1, Container):
            self.assertEqual(len(container1.children), len(container2.children), message)
        # do not actually check the children values here. all children *should* also be fields, which is checked below.
        # this is in case non-field children are added to one and not the other

        for field in getattr(container1, type1._fieldsname):
            with self.subTest(field=field, container_type=type1.__name__):
                f1 = getattr(container1, field)
                f2 = getattr(container2, field)
                self._assert_field_equal(f1, f2,
                                         ignore_hdmf_attrs=ignore_hdmf_attrs,
                                         ignore_string_to_byte=ignore_string_to_byte,
                                         message=message)

    def _assert_field_equal(self,
                            f1,
                            f2,
                            ignore_hdmf_attrs=False,
                            ignore_string_to_byte=False,
                            message=None):
        """
        Internal helper function used to compare two fields from Container objects

        :param f1: The first field
        :param f2: The second field
        :param ignore_hdmf_attrs: whether to ignore testing equality of HDMF container attributes, such as
                                  container_source and object_id
        :param ignore_string_to_byte: ignore conversion of str to bytes and compare as unicode instead
        :param message: custom additional message to show when assertions as part of this assert are failing
        """
        array_data_types = get_docval_macro('array_data')
        if (isinstance(f1, array_data_types) or isinstance(f2, array_data_types)):
            self._assert_array_equal(f1, f2,
                                     ignore_hdmf_attrs=ignore_hdmf_attrs,
                                     ignore_string_to_byte=ignore_string_to_byte,
                                     message=message)
        elif isinstance(f1, dict) and len(f1) and isinstance(f1.values()[0], Container):
            self.assertIsInstance(f2, dict, message)
            f1_keys = set(f1.keys())
            f2_keys = set(f2.keys())
            self.assertSetEqual(f1_keys, f2_keys, message)
            for k in f1_keys:
                with self.subTest(module_name=k):
                    self.assertContainerEqual(f1[k], f2[k],
                                              ignore_hdmf_attrs=ignore_hdmf_attrs,
                                              ignore_string_to_byte=ignore_string_to_byte,
                                              message=message)
        elif isinstance(f1, Container):
            self.assertContainerEqual(f1, f2,
                                      ignore_hdmf_attrs=ignore_hdmf_attrs,
                                      ignore_string_to_byte=ignore_string_to_byte,
                                      message=message)
        elif isinstance(f1, Data):
            self._assert_data_equal(f1, f2,
                                    ignore_hdmf_attrs=ignore_hdmf_attrs,
                                    ignore_string_to_byte=ignore_string_to_byte,
                                    message=message)
        elif isinstance(f1, (float, np.floating)):
            np.testing.assert_allclose(f1, f2, err_msg=message)
        else:
            self.assertEqual(f1, f2, message)

    def _assert_data_equal(self,
                           data1,
                           data2,
                           ignore_hdmf_attrs=False,
                           ignore_string_to_byte=False,
                           message=None):
        """
        Internal helper function used to compare two :py:class:`~hdmf.container.Data` objects

        :param data1: The first :py:class:`~hdmf.container.Data` object
        :type data1: :py:class:`hdmf.container.Data`
        :param data1: The second :py:class:`~hdmf.container.Data` object
        :type data1: :py:class:`hdmf.container.Data
        :param ignore_hdmf_attrs: whether to ignore testing equality of HDMF container attributes, such as
                                  container_source and object_id
        :param ignore_string_to_byte: ignore conversion of str to bytes and compare as unicode instead
        :param message: custom additional message to show when assertions as part of this assert are failing
        """
        self.assertTrue(isinstance(data1, Data), message)
        self.assertTrue(isinstance(data2, Data), message)
        self.assertEqual(len(data1), len(data2), message)
        self._assert_array_equal(data1.data, data2.data,
                                 ignore_hdmf_attrs=ignore_hdmf_attrs,
                                 ignore_string_to_byte=ignore_string_to_byte,
                                 message=message)
        self.assertContainerEqual(container1=data1,
                                  container2=data2,
                                  ignore_hdmf_attrs=ignore_hdmf_attrs,
                                  message=message)

    def _assert_array_equal(self,
                            arr1,
                            arr2,
                            ignore_hdmf_attrs=False,
                            ignore_string_to_byte=False,
                            message=None):
        """
        Internal helper function used to check whether two arrays are equal

        :param arr1: The first array
        :param arr2: The second array
        :param ignore_hdmf_attrs: whether to ignore testing equality of HDMF container attributes, such as
                                  container_source and object_id
        :param ignore_string_to_byte: ignore conversion of str to bytes and compare as unicode instead
        :param message: custom additional message to show when assertions as part of this assert are failing
        """
        array_data_types = tuple([i for i in get_docval_macro('array_data')
                                  if (i != list and i != tuple and i != AbstractDataChunkIterator)])
        # We construct array_data_types this way to avoid explicit dependency on h5py, Zarr and other
        # I/O backends. Only list and tuple do not support [()] slicing, and AbstractDataChunkIterator
        # should never occur here. The effective value of array_data_types is then:
        # array_data_types = (np.ndarray, h5py.Dataset, zarr.core.Array, hdmf.query.HDMFDataset)
        if isinstance(arr1, array_data_types):
            arr1 = arr1[()]
        if isinstance(arr2, array_data_types):
            arr2 = arr2[()]
        if not isinstance(arr1, (tuple, list, np.ndarray)) and not isinstance(arr2, (tuple, list, np.ndarray)):
            if isinstance(arr1, (float, np.floating)):
                np.testing.assert_allclose(arr1, arr2, err_msg=message)
            else:
                if ignore_string_to_byte:
                    if isinstance(arr1, bytes):
                        arr1 = arr1.decode('utf-8')
                    if isinstance(arr2, bytes):
                        arr2 = arr2.decode('utf-8')
                self.assertEqual(arr1, arr2, message)  # scalar
        else:
            self.assertEqual(len(arr1), len(arr2), message)
            if isinstance(arr1, np.ndarray) and len(arr1.dtype) > 1:  # compound type
                arr1 = arr1.tolist()
            if isinstance(arr2, np.ndarray) and len(arr2.dtype) > 1:  # compound type
                arr2 = arr2.tolist()
            if isinstance(arr1, np.ndarray) and isinstance(arr2, np.ndarray):
                if np.issubdtype(arr1.dtype, np.number):
                    np.testing.assert_allclose(arr1, arr2, err_msg=message)
                else:
                    np.testing.assert_array_equal(arr1, arr2, err_msg=message)
            else:
                for sub1, sub2 in zip(arr1, arr2):
                    if isinstance(sub1, Container):
                        self.assertContainerEqual(sub1, sub2,
                                                  ignore_hdmf_attrs=ignore_hdmf_attrs,
                                                  ignore_string_to_byte=ignore_string_to_byte,
                                                  message=message)
                    elif isinstance(sub1, Data):
                        self._assert_data_equal(sub1, sub2,
                                                ignore_hdmf_attrs=ignore_hdmf_attrs,
                                                ignore_string_to_byte=ignore_string_to_byte,
                                                message=message)
                    else:
                        self._assert_array_equal(sub1, sub2,
                                                 ignore_hdmf_attrs=ignore_hdmf_attrs,
                                                 ignore_string_to_byte=ignore_string_to_byte,
                                                 message=message)

    def assertBuilderEqual(self,
                           builder1,
                           builder2,
                           check_path=True,
                           check_source=True,
                           message=None):
        """
        Test whether two builders are equal. Like assertDictEqual but also checks type, name, path, and source.

        :param builder1: The first builder
        :type builder1: Builder
        :param builder2: The second builder
        :type builder2: Builder
        :param check_path: Check that the builder.path values are equal
        :type check_path: bool
        :param check_source: Check that the builder.source values are equal
        :type check_source: bool
        :param message: Custom message to add when any asserts as part of this assert are failing
        :type message: str or None (default=None)
        """
        self.assertTrue(isinstance(builder1, Builder), message)
        self.assertTrue(isinstance(builder2, Builder), message)
        self.assertEqual(type(builder1), type(builder2), message)
        self.assertEqual(builder1.name, builder2.name, message)
        if check_path:
            self.assertEqual(builder1.path, builder2.path, message)
        if check_source:
            self.assertEqual(builder1.source, builder2.source, message)
        self.assertDictEqual(builder1, builder2, message)


class H5RoundTripMixin(metaclass=ABCMeta):
    """
    Mixin class for methods to run a roundtrip test writing a container to and reading the container from an HDF5 file.
    The setUp, test_roundtrip, and tearDown methods will be run by unittest.

    The abstract method setUpContainer needs to be implemented by classes that include this mixin.

    Example::

        class TestMyContainerRoundTrip(H5RoundTripMixin, TestCase):
            def setUpContainer(self):
                # return the Container to read/write

    NOTE: This class is a mix-in and not a subclass of TestCase so that unittest does not discover it, try to run it,
    and skip it.
    """

    def setUp(self):
        self.__manager = get_manager()
        self.container = self.setUpContainer()
        self.container_type = self.container.__class__.__name__
        self.filename = 'test_%s.h5' % self.container_type
        self.export_filename = 'test_export_%s.h5' % self.container_type
        self.writer = None
        self.reader = None
        self.export_reader = None

    def tearDown(self):
        if self.writer is not None:
            self.writer.close()
        if self.reader is not None:
            self.reader.close()
        if self.export_reader is not None:
            self.export_reader.close()

        remove_test_file(self.filename)
        remove_test_file(self.export_filename)

    @abstractmethod
    def setUpContainer(self):
        """Return the Container to read/write."""
        raise NotImplementedError('Cannot run test unless setUpContainer is implemented')

    def test_roundtrip(self):
        """Test whether the container read from a written file is the same as the original file."""
        read_container = self.roundtripContainer()
        self._test_roundtrip(read_container, export=False)

    def test_roundtrip_export(self):
        """Test whether the container read from a written and then exported file is the same as the original file."""
        read_container = self.roundtripExportContainer()
        self._test_roundtrip(read_container, export=True)

    def _test_roundtrip(self, read_container, export=False):
        self.assertIsNotNone(str(self.container))  # added as a test to make sure printing works
        self.assertIsNotNone(str(read_container))
        # make sure we get a completely new object
        self.assertNotEqual(id(self.container), id(read_container))
        # the name of the root container of a file is always 'root' (see h5tools.py ROOT_NAME)
        # thus, ignore the name of the container when comparing original container vs read container
        if not export:
            self.assertContainerEqual(read_container, self.container, ignore_name=True)
        else:
            self.assertContainerEqual(read_container, self.container, ignore_name=True, ignore_hdmf_attrs=True)

        self.validate(read_container._experimental)

    def roundtripContainer(self, cache_spec=False):
        """Write the container to an HDF5 file, read the container from the file, and return it."""
        with HDF5IO(self.filename, manager=get_manager(), mode='w') as write_io:
            write_io.write(self.container, cache_spec=cache_spec)

        self.reader = HDF5IO(self.filename, manager=get_manager(), mode='r')
        return self.reader.read()

    def roundtripExportContainer(self, cache_spec=False):
        """Write the container to an HDF5 file, read it, export it to a new file, read that file, and return it."""
        self.roundtripContainer(cache_spec=cache_spec)

        HDF5IO.export_io(
            src_io=self.reader,
            path=self.export_filename,
            cache_spec=cache_spec,
        )

        self.export_reader = HDF5IO(self.export_filename, manager=get_manager(), mode='r')
        return self.export_reader.read()

    def validate(self, experimental=False):
        """Validate the written and exported files, if they exist."""
        if os.path.exists(self.filename):
            with HDF5IO(self.filename, manager=get_manager(), mode='r') as io:
                errors = common_validate(io, experimental=experimental)
                if errors:
                    for err in errors:
                        raise Exception(err)

        if os.path.exists(self.export_filename):
            with HDF5IO(self.filename, manager=get_manager(), mode='r') as io:
                errors = common_validate(io, experimental=experimental)
                if errors:
                    for err in errors:
                        raise Exception(err)
