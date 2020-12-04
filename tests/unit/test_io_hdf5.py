import json
import os
from numbers import Number

import numpy as np
from h5py import File, Dataset, Reference
from hdmf.backends.hdf5 import HDF5IO
from hdmf.build import GroupBuilder, DatasetBuilder, LinkBuilder
from hdmf.testing import TestCase
from hdmf.utils import get_data_shape

from tests.unit.test_io_hdf5_h5tools import _get_manager
from tests.unit.utils import Foo


class HDF5Encoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Dataset):
            ret = None
            for t in (list, str):
                try:
                    ret = t(obj)
                    break
                except:  # noqa: E722
                    pass
            if ret is None:
                return obj
            else:
                return ret
        elif isinstance(obj, np.int64):
            return int(obj)
        elif isinstance(obj, bytes):
            return str(obj)
        return json.JSONEncoder.default(self, obj)


class GroupBuilderTestCase(TestCase):
    '''
    A TestCase class for comparing GroupBuilders.
    '''

    def __is_scalar(self, obj):
        if hasattr(obj, 'shape'):
            return len(obj.shape) == 0
        else:
            if any(isinstance(obj, t) for t in (int, str, float, bytes, str)):
                return True
        return False

    def __convert_h5_scalar(self, obj):
        if isinstance(obj, Dataset):
            return obj[...]
        return obj

    def __compare_attr_dicts(self, a, b):
        reasons = list()
        b_keys = set(b.keys())
        for k in a:
            if k not in b:
                reasons.append("'%s' attribute missing from second dataset" % k)
            else:
                if a[k] != b[k]:
                    reasons.append("'%s' attribute on datasets not equal" % k)
                b_keys.remove(k)
        for k in b_keys:
            reasons.append("'%s' attribute missing from first dataset" % k)
        return reasons

    def __compare_dataset(self, a, b):
        reasons = self.__compare_attr_dicts(a.attributes, b.attributes)
        if not self.__compare_data(a.data, b.data):
            reasons.append("dataset '%s' not equal" % a.name)
        return reasons

    def __compare_data(self, a, b):
        if isinstance(a, Number) and isinstance(b, Number):
            return a == b
        elif isinstance(a, Number) != isinstance(b, Number):
            return False
        else:
            a_scalar = self.__is_scalar(a)
            b_scalar = self.__is_scalar(b)
            if a_scalar and b_scalar:
                return self.__convert_h5_scalar(a_scalar) == self.__convert_h5_scalar(b_scalar)
            elif a_scalar != b_scalar:
                return False
            if len(a) == len(b):
                for i in range(len(a)):
                    if not self.__compare_data(a[i], b[i]):
                        return False
            else:
                return False
        return True

    def __fmt(self, val):
        return "%s (%s)" % (val, type(val))

    def __assert_helper(self, a, b):
        reasons = list()
        b_keys = set(b.keys())
        for k, a_sub in a.items():
            if k in b:
                b_sub = b[k]
                b_keys.remove(k)
                if isinstance(a_sub, LinkBuilder) and isinstance(a_sub, LinkBuilder):
                    a_sub = a_sub['builder']
                    b_sub = b_sub['builder']
                elif isinstance(a_sub, LinkBuilder) != isinstance(a_sub, LinkBuilder):
                    reasons.append('%s != %s' % (a_sub, b_sub))
                if isinstance(a_sub, DatasetBuilder) and isinstance(a_sub, DatasetBuilder):
                    # if not self.__compare_dataset(a_sub, b_sub):
                    #    reasons.append('%s != %s' % (a_sub, b_sub))
                    reasons.extend(self.__compare_dataset(a_sub, b_sub))
                elif isinstance(a_sub, GroupBuilder) and isinstance(a_sub, GroupBuilder):
                    reasons.extend(self.__assert_helper(a_sub, b_sub))
                else:
                    equal = None
                    a_array = isinstance(a_sub, np.ndarray)
                    b_array = isinstance(b_sub, np.ndarray)
                    if a_array and b_array:
                        equal = np.array_equal(a_sub, b_sub)
                    elif a_array or b_array:
                        # if strings, convert before comparing
                        if b_array:
                            if b_sub.dtype.char in ('S', 'U'):
                                a_sub = [np.string_(s) for s in a_sub]
                        else:
                            if a_sub.dtype.char in ('S', 'U'):
                                b_sub = [np.string_(s) for s in b_sub]
                        equal = np.array_equal(a_sub, b_sub)
                    else:
                        equal = a_sub == b_sub
                    if not equal:
                        reasons.append('%s != %s' % (self.__fmt(a_sub), self.__fmt(b_sub)))
            else:
                reasons.append("'%s' not in both" % k)
        for k in b_keys:
            reasons.append("'%s' not in both" % k)
        return reasons

    def assertBuilderEqual(self, a, b):
        ''' Tests that two GroupBuilders are equal '''
        reasons = self.__assert_helper(a, b)
        if len(reasons):
            raise AssertionError(', '.join(reasons))
        return True


class TestHDF5Writer(GroupBuilderTestCase):

    def setUp(self):
        self.manager = _get_manager()
        self.path = "test_io_hdf5.h5"

        self.foo_builder = GroupBuilder('foo1',
                                        attributes={'data_type': 'Foo',
                                                    'namespace': 'test_core',
                                                    'attr1': "bar",
                                                    'object_id': -1},
                                        datasets={'my_data': DatasetBuilder('my_data', list(range(100, 200, 10)),
                                                                            attributes={'attr2': 17})})
        self.foo = Foo('foo1', list(range(100, 200, 10)), attr1="bar", attr2=17, attr3=3.14)
        self.manager.prebuilt(self.foo, self.foo_builder)
        self.builder = GroupBuilder(
            'root',
            source=self.path,
            groups={'test_bucket':
                    GroupBuilder('test_bucket',
                                 groups={'foo_holder':
                                         GroupBuilder('foo_holder',
                                                      groups={'foo1': self.foo_builder})})},
            attributes={'data_type': 'FooFile'})

    def tearDown(self):
        if os.path.exists(self.path):
            os.remove(self.path)

    def check_fields(self):
        f = File(self.path, 'r')
        self.assertIn('test_bucket', f)
        bucket = f.get('test_bucket')
        self.assertIn('foo_holder', bucket)
        holder = bucket.get('foo_holder')
        self.assertIn('foo1', holder)
        return f

    def test_write_builder(self):
        writer = HDF5IO(self.path, manager=self.manager, mode='a')
        writer.write_builder(self.builder)
        writer.close()
        self.check_fields()

    def test_write_attribute_reference_container(self):
        writer = HDF5IO(self.path, manager=self.manager, mode='a')
        self.builder.set_attribute('ref_attribute', self.foo)
        writer.write_builder(self.builder)
        writer.close()
        f = self.check_fields()
        self.assertIsInstance(f.attrs['ref_attribute'], Reference)
        self.assertEqual(f['test_bucket/foo_holder/foo1'], f[f.attrs['ref_attribute']])

    def test_write_attribute_reference_builder(self):
        writer = HDF5IO(self.path, manager=self.manager, mode='a')
        self.builder.set_attribute('ref_attribute', self.foo_builder)
        writer.write_builder(self.builder)
        writer.close()
        f = self.check_fields()
        self.assertIsInstance(f.attrs['ref_attribute'], Reference)
        self.assertEqual(f['test_bucket/foo_holder/foo1'], f[f.attrs['ref_attribute']])

    def test_write_context_manager(self):
        with HDF5IO(self.path, manager=self.manager, mode='a') as writer:
            writer.write_builder(self.builder)
        self.check_fields()

    def test_read_builder(self):
        self.maxDiff = None
        io = HDF5IO(self.path, manager=self.manager, mode='a')
        io.write_builder(self.builder)
        builder = io.read_builder()
        self.assertBuilderEqual(builder, self.builder)
        io.close()

    def test_dataset_shape(self):
        self.maxDiff = None
        io = HDF5IO(self.path, manager=self.manager, mode='a')
        io.write_builder(self.builder)
        builder = io.read_builder()
        dset = builder['test_bucket']['foo_holder']['foo1']['my_data'].data
        self.assertEqual(get_data_shape(dset), (10,))
        io.close()
