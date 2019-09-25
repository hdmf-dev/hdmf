import unittest2 as unittest
import os
import numpy as np
import h5py
import numpy.testing as npt

from hdmf.common import validate as common_validate, get_manager
from hdmf.container import Container, Data
from hdmf.backends.hdf5 import HDF5IO


class HDMFTestCase(unittest.TestCase):

    def assertContainerEqual(self, container1, container2):           # noqa: C901
        '''
        container1 is what was read or generated
        container2 is what is hardcoded in the TestCase
        '''
        type1 = type(container1)
        type2 = type(container2)
        self.assertEqual(type1, type2)
        for nwbfield in container1.__fields__:
            with self.subTest(nwbfield=nwbfield, container_type=type1.__name__):
                f1 = getattr(container1, nwbfield)
                f2 = getattr(container2, nwbfield)
                if isinstance(f1, h5py.Dataset):
                    f1 = f1[()]
                if isinstance(f1, (tuple, list, np.ndarray)):
                    if len(f1) > 0:
                        if isinstance(f1[0], Container):
                            for sub1, sub2 in zip(f1, f2):
                                self.assertContainerEqual(sub1, sub2)
                        elif isinstance(f1[0], Data):
                            for sub1, sub2 in zip(f1, f2):
                                self.assertDataEqual(sub1, sub2)
                        continue
                    else:
                        self.assertEqual(len(f1), len(f2))
                        if len(f1) == 0:
                            continue
                        if isinstance(f1[0], float):
                            for v1, v2 in zip(f1, f2):
                                self.assertAlmostEqual(v1, v2, places=6)
                        else:
                            self.assertTrue(np.array_equal(f1, f2))
                elif isinstance(f1, dict) and len(f1) and isinstance(next(iter(f1.values())), Container):
                    f1_keys = set(f1.keys())
                    f2_keys = set(f2.keys())
                    self.assertSetEqual(f1_keys, f2_keys)
                    for k in f1_keys:
                        with self.subTest(module_name=k):
                            self.assertContainerEqual(f1[k], f2[k])
                elif isinstance(f1, Container):
                    self.assertContainerEqual(f1, f2)
                elif isinstance(f1, Data) or isinstance(f2, Data):
                    if isinstance(f1, Data) and isinstance(f2, Data):
                        self.assertDataEqual(f1, f2)
                    elif isinstance(f1, Data):
                        self.assertTrue(np.array_equal(f1.data, f2))
                    elif isinstance(f2, Data):
                        self.assertTrue(np.array_equal(f1.data, f2))
                else:
                    if isinstance(f1, (float, np.float32, np.float16)):
                        npt.assert_almost_equal(f1, f2)
                    else:
                        self.assertEqual(f1, f2)

    def assertDataEqual(self, data1, data2):
        self.assertEqual(type(data1), type(data2))
        self.assertEqual(len(data1), len(data2))


class TestMapRoundTrip(HDMFTestCase):

    def setUpContainer(self):
        ''' Should return the Container to build and read/write'''
        raise unittest.SkipTest('Cannot run test unless setUpContainer is implemented')

    def setUp(self):
        self.container = self.setUpContainer()
        self.object_id = self.container.object_id
        self.container_type = self.container.__class__.__name__
        self.filename = 'test_%s.h5' % self.container_type
        self.writer = None
        self.reader = None

    def tearDown(self):
        if self.writer is not None:
            self.writer.close()
        if self.reader is not None:
            self.reader.close()
        if os.path.exists(self.filename) and os.getenv("CLEAN_HDMF", '1') not in ('0', 'false', 'FALSE', 'False'):
            os.remove(self.filename)

    def roundtripContainer(self, cache_spec=False):
        self.writer = HDF5IO(self.filename, manager=get_manager(), mode='w')
        self.writer.write(self.container, cache_spec=cache_spec)
        self.writer.close()
        self.reader = HDF5IO(self.filename, manager=get_manager(), mode='r')
        try:
            return self.reader.read()
        except Exception as e:
            self.reader.close()
            self.reader = None
            raise e

    def test_roundtrip(self):
        self.read_container = self.roundtripContainer()
        # make sure we get a completely new object
        self.assertIsNotNone(str(self.container))  # added as a test to make sure printing works
        self.assertIsNotNone(str(self.read_container))
        self.assertNotEqual(id(self.container), id(self.read_container))
        self.assertContainerEqual(self.read_container, self.container)
        self.reader.close()
        self.validate()

    def validate(self):
        # validate created file
        if os.path.exists(self.filename):
            with HDF5IO(self.filename, manager=get_manager(), mode='r') as io:
                errors = common_validate(io)
                if errors:
                    for err in errors:
                        raise Exception(err)
