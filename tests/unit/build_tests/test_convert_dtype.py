from datetime import datetime

import numpy as np
from hdmf.backends.hdf5 import H5DataIO
from hdmf.build import ObjectMapper
from hdmf.data_utils import DataChunkIterator
from hdmf.spec import DatasetSpec, RefSpec, DtypeSpec
from hdmf.testing import TestCase


class TestConvertDtype(TestCase):

    def test_value_none(self):
        spec = DatasetSpec('an example dataset', 'int', name='data')
        self.assertTupleEqual(ObjectMapper.convert_dtype(spec, None), (None, 'int'))

        spec = DatasetSpec('an example dataset', RefSpec(reftype='object', target_type='int'), name='data')
        self.assertTupleEqual(ObjectMapper.convert_dtype(spec, None), (None, 'object'))

    # do full matrix test of given value x and spec y, what does convert_dtype return?
    def test_convert_to_64bit_spec(self):
        """
        Test that if given any value for a spec with a 64-bit dtype, convert_dtype will convert to the spec type.
        Also test that if the given value is not the same as the spec, convert_dtype raises a warning.
        """
        spec_type = 'float64'
        value_types = ['double', 'float64']
        self._test_convert_alias(spec_type, value_types)

        spec_type = 'float64'
        value_types = ['float', 'float32', 'long', 'int64', 'int', 'int32', 'int16', 'short', 'int8', 'uint64', 'uint',
                       'uint32', 'uint16', 'uint8', 'bool']
        self._test_convert_higher_precision_helper(spec_type, value_types)

        spec_type = 'int64'
        value_types = ['long', 'int64']
        self._test_convert_alias(spec_type, value_types)

        spec_type = 'int64'
        value_types = ['double', 'float64', 'float', 'float32', 'int', 'int32', 'int16', 'short', 'int8', 'uint64',
                       'uint', 'uint32', 'uint16', 'uint8', 'bool']
        self._test_convert_higher_precision_helper(spec_type, value_types)

        spec_type = 'uint64'
        value_types = ['uint64']
        self._test_convert_alias(spec_type, value_types)

        spec_type = 'uint64'
        value_types = ['double', 'float64', 'float', 'float32', 'long', 'int64', 'int', 'int32', 'int16', 'short',
                       'int8', 'uint', 'uint32', 'uint16', 'uint8', 'bool']
        self._test_convert_higher_precision_helper(spec_type, value_types)

    def test_convert_to_float32_spec(self):
        """Test conversion of various types to float32.
        If given a value with precision > float32 and float base type, convert_dtype will keep the higher precision.
        If given a value with 64-bit precision and different base type, convert_dtype will convert to float64.
        If given a value that is float32, convert_dtype will convert to float32.
        If given a value with precision <= float32, convert_dtype will convert to float32 and raise a warning.
        """
        spec_type = 'float32'
        value_types = ['double', 'float64']
        self._test_keep_higher_precision_helper(spec_type, value_types)

        value_types = ['long', 'int64', 'uint64']
        expected_type = 'float64'
        self._test_change_basetype_helper(spec_type, value_types, expected_type)

        value_types = ['float', 'float32']
        self._test_convert_alias(spec_type, value_types)

        value_types = ['int', 'int32', 'int16', 'short', 'int8', 'uint', 'uint32', 'uint16', 'uint8', 'bool']
        self._test_convert_higher_precision_helper(spec_type, value_types)

    def test_convert_to_int32_spec(self):
        """Test conversion of various types to int32.
        If given a value with precision > int32 and int base type, convert_dtype will keep the higher precision.
        If given a value with 64-bit precision and different base type, convert_dtype will convert to int64.
        If given a value that is int32, convert_dtype will convert to int32.
        If given a value with precision <= int32, convert_dtype will convert to int32 and raise a warning.
        """
        spec_type = 'int32'
        value_types = ['int64', 'long']
        self._test_keep_higher_precision_helper(spec_type, value_types)

        value_types = ['double', 'float64', 'uint64']
        expected_type = 'int64'
        self._test_change_basetype_helper(spec_type, value_types, expected_type)

        value_types = ['int', 'int32']
        self._test_convert_alias(spec_type, value_types)

        value_types = ['float', 'float32', 'int16', 'short', 'int8', 'uint', 'uint32', 'uint16', 'uint8', 'bool']
        self._test_convert_higher_precision_helper(spec_type, value_types)

    def test_convert_to_uint32_spec(self):
        """Test conversion of various types to uint32.
        If given a value with precision > uint32 and uint base type, convert_dtype will keep the higher precision.
        If given a value with 64-bit precision and different base type, convert_dtype will convert to uint64.
        If given a value that is uint32, convert_dtype will convert to uint32.
        If given a value with precision <= uint32, convert_dtype will convert to uint32 and raise a warning.
        """
        spec_type = 'uint32'
        value_types = ['uint64']
        self._test_keep_higher_precision_helper(spec_type, value_types)

        value_types = ['double', 'float64', 'long', 'int64']
        expected_type = 'uint64'
        self._test_change_basetype_helper(spec_type, value_types, expected_type)

        value_types = ['uint', 'uint32']
        self._test_convert_alias(spec_type, value_types)

        value_types = ['float', 'float32', 'int', 'int32', 'int16', 'short', 'int8', 'uint16', 'uint8', 'bool']
        self._test_convert_higher_precision_helper(spec_type, value_types)

    def test_convert_to_int16_spec(self):
        """Test conversion of various types to int16.
        If given a value with precision > int16 and int base type, convert_dtype will keep the higher precision.
        If given a value with 64-bit precision and different base type, convert_dtype will convert to int64.
        If given a value with 32-bit precision and different base type, convert_dtype will convert to int32.
        If given a value that is int16, convert_dtype will convert to int16.
        If given a value with precision <= int16, convert_dtype will convert to int16 and raise a warning.
        """
        spec_type = 'int16'
        value_types = ['long', 'int64', 'int', 'int32']
        self._test_keep_higher_precision_helper(spec_type, value_types)

        value_types = ['double', 'float64', 'uint64']
        expected_type = 'int64'
        self._test_change_basetype_helper(spec_type, value_types, expected_type)

        value_types = ['float', 'float32', 'uint', 'uint32']
        expected_type = 'int32'
        self._test_change_basetype_helper(spec_type, value_types, expected_type)

        value_types = ['int16', 'short']
        self._test_convert_alias(spec_type, value_types)

        value_types = ['int8', 'uint16', 'uint8', 'bool']
        self._test_convert_higher_precision_helper(spec_type, value_types)

    def test_convert_to_uint16_spec(self):
        """Test conversion of various types to uint16.
        If given a value with precision > uint16 and uint base type, convert_dtype will keep the higher precision.
        If given a value with 64-bit precision and different base type, convert_dtype will convert to uint64.
        If given a value with 32-bit precision and different base type, convert_dtype will convert to uint32.
        If given a value that is uint16, convert_dtype will convert to uint16.
        If given a value with precision <= uint16, convert_dtype will convert to uint16 and raise a warning.
        """
        spec_type = 'uint16'
        value_types = ['uint64', 'uint', 'uint32']
        self._test_keep_higher_precision_helper(spec_type, value_types)

        value_types = ['double', 'float64', 'long', 'int64']
        expected_type = 'uint64'
        self._test_change_basetype_helper(spec_type, value_types, expected_type)

        value_types = ['float', 'float32', 'int', 'int32']
        expected_type = 'uint32'
        self._test_change_basetype_helper(spec_type, value_types, expected_type)

        value_types = ['uint16']
        self._test_convert_alias(spec_type, value_types)

        value_types = ['int16', 'short', 'int8', 'uint8', 'bool']
        self._test_convert_higher_precision_helper(spec_type, value_types)

    def test_convert_to_bool_spec(self):
        """Test conversion of various types to bool.
        If given a value with type bool, convert_dtype will convert to bool.
        If given a value with type int8/uint8, convert_dtype will convert to bool and raise a warning.
        Otherwise, convert_dtype will raise an error.
        """
        spec_type = 'bool'
        value_types = ['bool']
        self._test_convert_alias(spec_type, value_types)

        value_types = ['uint8', 'int8']
        self._test_convert_higher_precision_helper(spec_type, value_types)

        value_types = ['double', 'float64', 'float', 'float32', 'long', 'int64', 'int', 'int32', 'int16', 'short',
                       'uint64', 'uint', 'uint32', 'uint16']
        self._test_convert_mismatch_helper(spec_type, value_types)

    def _get_type(self, type_str):
        return ObjectMapper._ObjectMapper__dtypes[type_str]  # apply ObjectMapper mapping string to dtype

    def _test_convert_alias(self, spec_type, value_types):
        data = 1
        spec = DatasetSpec('an example dataset', spec_type, name='data')
        match = (self._get_type(spec_type)(data), self._get_type(spec_type))
        for dtype in value_types:
            value = self._get_type(dtype)(data)  # convert data to given dtype
            with self.subTest(dtype=dtype):
                ret = ObjectMapper.convert_dtype(spec, value)
                self.assertTupleEqual(ret, match)
                self.assertIs(ret[0].dtype.type, match[1])

    def _test_convert_higher_precision_helper(self, spec_type, value_types):
        data = 1
        spec = DatasetSpec('an example dataset', spec_type, name='data')
        match = (self._get_type(spec_type)(data), self._get_type(spec_type))
        for dtype in value_types:
            value = self._get_type(dtype)(data)  # convert data to given dtype
            with self.subTest(dtype=dtype):
                s = np.dtype(self._get_type(spec_type))
                g = np.dtype(self._get_type(dtype))
                msg = ("Spec 'data': Value with data type %s is being converted to data type %s as specified."
                       % (g.name, s.name))
                with self.assertWarnsWith(UserWarning, msg):
                    ret = ObjectMapper.convert_dtype(spec, value)
                self.assertTupleEqual(ret, match)
                self.assertIs(ret[0].dtype.type, match[1])

    def _test_keep_higher_precision_helper(self, spec_type, value_types):
        data = 1
        spec = DatasetSpec('an example dataset', spec_type, name='data')
        for dtype in value_types:
            value = self._get_type(dtype)(data)
            match = (value, self._get_type(dtype))
            with self.subTest(dtype=dtype):
                ret = ObjectMapper.convert_dtype(spec, value)
                self.assertTupleEqual(ret, match)
                self.assertIs(ret[0].dtype.type, match[1])

    def _test_change_basetype_helper(self, spec_type, value_types, exp_type):
        data = 1
        spec = DatasetSpec('an example dataset', spec_type, name='data')
        match = (self._get_type(exp_type)(data), self._get_type(exp_type))
        for dtype in value_types:
            value = self._get_type(dtype)(data)  # convert data to given dtype
            with self.subTest(dtype=dtype):
                s = np.dtype(self._get_type(spec_type))
                e = np.dtype(self._get_type(exp_type))
                g = np.dtype(self._get_type(dtype))
                msg = ("Spec 'data': Value with data type %s is being converted to data type %s "
                       "(min specification: %s)." % (g.name, e.name, s.name))
                with self.assertWarnsWith(UserWarning, msg):
                    ret = ObjectMapper.convert_dtype(spec, value)
                self.assertTupleEqual(ret, match)
                self.assertIs(ret[0].dtype.type, match[1])

    def _test_convert_mismatch_helper(self, spec_type, value_types):
        data = 1
        spec = DatasetSpec('an example dataset', spec_type, name='data')
        for dtype in value_types:
            value = self._get_type(dtype)(data)  # convert data to given dtype
            with self.subTest(dtype=dtype):
                s = np.dtype(self._get_type(spec_type))
                g = np.dtype(self._get_type(dtype))
                msg = "expected %s, received %s - must supply %s" % (s.name, g.name, s.name)
                with self.assertRaisesWith(ValueError, msg):
                    ObjectMapper.convert_dtype(spec, value)

    def test_dci_input(self):
        spec = DatasetSpec('an example dataset', 'int64', name='data')
        value = DataChunkIterator(np.array([1, 2, 3], dtype=np.int32))
        msg = "Spec 'data': Value with data type int32 is being converted to data type int64 as specified."
        with self.assertWarnsWith(UserWarning, msg):
            ret, ret_dtype = ObjectMapper.convert_dtype(spec, value)  # no conversion
        self.assertIs(ret, value)
        self.assertEqual(ret_dtype, np.int64)

        spec = DatasetSpec('an example dataset', 'int16', name='data')
        value = DataChunkIterator(np.array([1, 2, 3], dtype=np.int32))
        ret, ret_dtype = ObjectMapper.convert_dtype(spec, value)  # no conversion
        self.assertIs(ret, value)
        self.assertEqual(ret_dtype, np.int32)  # increase precision

    def test_text_spec(self):
        text_spec_types = ['text', 'utf', 'utf8', 'utf-8']
        for spec_type in text_spec_types:
            with self.subTest(spec_type=spec_type):
                spec = DatasetSpec('an example dataset', spec_type, name='data')

                value = 'a'
                ret, ret_dtype = ObjectMapper.convert_dtype(spec, value)
                self.assertEqual(ret, value)
                self.assertIs(type(ret), str)
                self.assertEqual(ret_dtype, 'utf8')

                value = b'a'
                ret, ret_dtype = ObjectMapper.convert_dtype(spec, value)
                self.assertEqual(ret, 'a')
                self.assertIs(type(ret), str)
                self.assertEqual(ret_dtype, 'utf8')

                value = ['a', 'b']
                ret, ret_dtype = ObjectMapper.convert_dtype(spec, value)
                self.assertListEqual(ret, value)
                self.assertIs(type(ret[0]), str)
                self.assertEqual(ret_dtype, 'utf8')

                value = np.array(['a', 'b'])
                ret, ret_dtype = ObjectMapper.convert_dtype(spec, value)
                np.testing.assert_array_equal(ret, value)
                self.assertEqual(ret_dtype, 'utf8')

                value = np.array(['a', 'b'], dtype='S1')
                ret, ret_dtype = ObjectMapper.convert_dtype(spec, value)
                np.testing.assert_array_equal(ret, np.array(['a', 'b'], dtype='U1'))
                self.assertEqual(ret_dtype, 'utf8')

                value = []
                ret, ret_dtype = ObjectMapper.convert_dtype(spec, value)
                self.assertListEqual(ret, value)
                self.assertEqual(ret_dtype, 'utf8')

                value = 1
                msg = "Expected unicode or ascii string, got <class 'int'>"
                with self.assertRaisesWith(ValueError, msg):
                    ObjectMapper.convert_dtype(spec, value)

                value = DataChunkIterator(np.array(['a', 'b']))
                ret, ret_dtype = ObjectMapper.convert_dtype(spec, value)  # no conversion
                self.assertIs(ret, value)
                self.assertEqual(ret_dtype, 'utf8')

                value = DataChunkIterator(np.array(['a', 'b'], dtype='S1'))
                ret, ret_dtype = ObjectMapper.convert_dtype(spec, value)  # no conversion
                self.assertIs(ret, value)
                self.assertEqual(ret_dtype, 'utf8')

    def test_ascii_spec(self):
        ascii_spec_types = ['ascii', 'bytes']
        for spec_type in ascii_spec_types:
            with self.subTest(spec_type=spec_type):
                spec = DatasetSpec('an example dataset', spec_type, name='data')

                value = 'a'
                ret, ret_dtype = ObjectMapper.convert_dtype(spec, value)
                self.assertEqual(ret, b'a')
                self.assertIs(type(ret), bytes)
                self.assertEqual(ret_dtype, 'ascii')

                value = b'a'
                ret, ret_dtype = ObjectMapper.convert_dtype(spec, value)
                self.assertEqual(ret, b'a')
                self.assertIs(type(ret), bytes)
                self.assertEqual(ret_dtype, 'ascii')

                value = ['a', 'b']
                ret, ret_dtype = ObjectMapper.convert_dtype(spec, value)
                self.assertListEqual(ret, [b'a', b'b'])
                self.assertIs(type(ret[0]), bytes)
                self.assertEqual(ret_dtype, 'ascii')

                value = np.array(['a', 'b'])
                ret, ret_dtype = ObjectMapper.convert_dtype(spec, value)
                np.testing.assert_array_equal(ret, np.array(['a', 'b'], dtype='S1'))
                self.assertEqual(ret_dtype, 'ascii')

                value = np.array(['a', 'b'], dtype='S1')
                ret, ret_dtype = ObjectMapper.convert_dtype(spec, value)
                np.testing.assert_array_equal(ret, value)
                self.assertEqual(ret_dtype, 'ascii')

                value = []
                ret, ret_dtype = ObjectMapper.convert_dtype(spec, value)
                self.assertListEqual(ret, value)
                self.assertEqual(ret_dtype, 'ascii')

                value = 1
                msg = "Expected unicode or ascii string, got <class 'int'>"
                with self.assertRaisesWith(ValueError, msg):
                    ObjectMapper.convert_dtype(spec, value)

                value = DataChunkIterator(np.array(['a', 'b']))
                ret, ret_dtype = ObjectMapper.convert_dtype(spec, value)  # no conversion
                self.assertIs(ret, value)
                self.assertEqual(ret_dtype, 'ascii')

                value = DataChunkIterator(np.array(['a', 'b'], dtype='S1'))
                ret, ret_dtype = ObjectMapper.convert_dtype(spec, value)  # no conversion
                self.assertIs(ret, value)
                self.assertEqual(ret_dtype, 'ascii')

    def test_no_spec(self):
        spec_type = None
        spec = DatasetSpec('an example dataset', spec_type, name='data')

        value = [1, 2, 3]
        ret, ret_dtype = ObjectMapper.convert_dtype(spec, value)
        self.assertListEqual(ret, value)
        self.assertIs(type(ret[0]), int)
        self.assertEqual(ret_dtype, int)

        value = np.uint64(4)
        ret, ret_dtype = ObjectMapper.convert_dtype(spec, value)
        self.assertEqual(ret, value)
        self.assertIs(type(ret), np.uint64)
        self.assertEqual(ret_dtype, np.uint64)

        value = 'hello'
        ret, ret_dtype = ObjectMapper.convert_dtype(spec, value)
        self.assertEqual(ret, value)
        self.assertIs(type(ret), str)
        self.assertEqual(ret_dtype, 'utf8')

        value = b'hello'
        ret, ret_dtype = ObjectMapper.convert_dtype(spec, value)
        self.assertEqual(ret, value)
        self.assertIs(type(ret), bytes)
        self.assertEqual(ret_dtype, 'ascii')

        value = np.array(['aa', 'bb'])
        ret, ret_dtype = ObjectMapper.convert_dtype(spec, value)
        np.testing.assert_array_equal(ret, value)
        self.assertEqual(ret_dtype, 'utf8')

        value = np.array(['aa', 'bb'], dtype='S2')
        ret, ret_dtype = ObjectMapper.convert_dtype(spec, value)
        np.testing.assert_array_equal(ret, value)
        self.assertEqual(ret_dtype, 'ascii')

        value = DataChunkIterator(data=[1, 2, 3])
        ret, ret_dtype = ObjectMapper.convert_dtype(spec, value)
        self.assertEqual(ret, value)
        self.assertIs(ret.dtype.type, np.dtype(int).type)
        self.assertIs(type(ret.data[0]), int)
        self.assertEqual(ret_dtype, np.dtype(int).type)

        value = DataChunkIterator(data=['a', 'b'])
        ret, ret_dtype = ObjectMapper.convert_dtype(spec, value)
        self.assertEqual(ret, value)
        self.assertIs(ret.dtype.type, np.str_)
        self.assertIs(type(ret.data[0]), str)
        self.assertEqual(ret_dtype, 'utf8')

        value = H5DataIO(np.arange(30).reshape(5, 2, 3))
        ret, ret_dtype = ObjectMapper.convert_dtype(spec, value)
        self.assertEqual(ret, value)
        self.assertIs(ret.data.dtype.type, np.dtype(int).type)
        self.assertEqual(ret_dtype, np.dtype(int).type)

        value = H5DataIO(['foo', 'bar'])
        ret, ret_dtype = ObjectMapper.convert_dtype(spec, value)
        self.assertEqual(ret, value)
        self.assertIs(type(ret.data[0]), str)
        self.assertEqual(ret_dtype, 'utf8')

        value = H5DataIO([b'foo', b'bar'])
        ret, ret_dtype = ObjectMapper.convert_dtype(spec, value)
        self.assertEqual(ret, value)
        self.assertIs(type(ret.data[0]), bytes)
        self.assertEqual(ret_dtype, 'ascii')

        value = []
        msg = "Cannot infer dtype of empty list or tuple. Please use numpy array with specified dtype."
        with self.assertRaisesWith(ValueError, msg):
            ObjectMapper.convert_dtype(spec, value)

    def test_numeric_spec(self):
        spec_type = 'numeric'
        spec = DatasetSpec('an example dataset', spec_type, name='data')

        value = np.uint64(4)
        ret, ret_dtype = ObjectMapper.convert_dtype(spec, value)
        self.assertEqual(ret, value)
        self.assertIs(type(ret), np.uint64)
        self.assertEqual(ret_dtype, np.uint64)

        value = DataChunkIterator(data=[1, 2, 3])
        ret, ret_dtype = ObjectMapper.convert_dtype(spec, value)
        self.assertEqual(ret, value)
        self.assertIs(ret.dtype.type, np.dtype(int).type)
        self.assertIs(type(ret.data[0]), int)
        self.assertEqual(ret_dtype, np.dtype(int).type)

        value = ['a', 'b']
        msg = "Cannot convert from <class 'str'> to 'numeric' specification dtype."
        with self.assertRaisesWith(ValueError, msg):
            ObjectMapper.convert_dtype(spec, value)

        value = np.array(['a', 'b'])
        msg = "Cannot convert from <class 'numpy.str_'> to 'numeric' specification dtype."
        with self.assertRaisesWith(ValueError, msg):
            ObjectMapper.convert_dtype(spec, value)

        value = []
        msg = "Cannot infer dtype of empty list or tuple. Please use numpy array with specified dtype."
        with self.assertRaisesWith(ValueError, msg):
            ObjectMapper.convert_dtype(spec, value)

    def test_bool_spec(self):
        spec_type = 'bool'
        spec = DatasetSpec('an example dataset', spec_type, name='data')

        value = np.bool_(True)
        ret, ret_dtype = ObjectMapper.convert_dtype(spec, value)
        self.assertEqual(ret, value)
        self.assertIs(type(ret), np.bool_)
        self.assertEqual(ret_dtype, np.bool_)

        value = True
        ret, ret_dtype = ObjectMapper.convert_dtype(spec, value)
        self.assertEqual(ret, value)
        self.assertIs(type(ret), np.bool_)
        self.assertEqual(ret_dtype, np.bool_)

    def test_override_type_int_restrict_precision(self):
        spec = DatasetSpec('an example dataset', 'int8', name='data')
        res = ObjectMapper.convert_dtype(spec, np.int64(1), 'int64')
        self.assertTupleEqual(res, (np.int64(1), np.int64))

    def test_override_type_numeric_to_uint(self):
        spec = DatasetSpec('an example dataset', 'numeric', name='data')
        res = ObjectMapper.convert_dtype(spec, np.uint32(1), 'uint8')
        self.assertTupleEqual(res, (np.uint32(1), np.uint32))

    def test_override_type_numeric_to_uint_list(self):
        spec = DatasetSpec('an example dataset', 'numeric', name='data')
        res = ObjectMapper.convert_dtype(spec, np.uint32((1, 2, 3)), 'uint8')
        np.testing.assert_array_equal(res[0], np.uint32((1, 2, 3)))
        self.assertEqual(res[1], np.uint32)

    def test_override_type_none_to_bool(self):
        spec = DatasetSpec('an example dataset', None, name='data')
        res = ObjectMapper.convert_dtype(spec, True, 'bool')
        self.assertTupleEqual(res, (True, np.bool_))

    def test_compound_type(self):
        """Test that convert_dtype passes through arguments if spec dtype is a list without any validation."""
        spec_type = [DtypeSpec('an int field', 'f1', 'int'), DtypeSpec('a float field', 'f2', 'float')]
        spec = DatasetSpec('an example dataset', spec_type, name='data')
        value = ['a', 1, 2.2]
        res, ret_dtype = ObjectMapper.convert_dtype(spec, value)
        self.assertListEqual(res, value)
        self.assertListEqual(ret_dtype, spec_type)

    def test_isodatetime_spec(self):
        spec_type = 'isodatetime'
        spec = DatasetSpec('an example dataset', spec_type, name='data')

        # NOTE: datetime.isoformat is called on all values with a datetime spec before conversion
        # see ObjectMapper.get_attr_value
        value = datetime.isoformat(datetime(2020, 11, 10))
        ret, ret_dtype = ObjectMapper.convert_dtype(spec, value)
        self.assertEqual(ret, b'2020-11-10T00:00:00')
        self.assertIs(type(ret), bytes)
        self.assertEqual(ret_dtype, 'ascii')
