import json

from hdmf.spec import GroupSpec, DatasetSpec, AttributeSpec, DtypeSpec, RefSpec
from hdmf.testing import TestCase


class DatasetSpecTests(TestCase):
    def setUp(self):
        self.attributes = [
            AttributeSpec('attribute1', 'my first attribute', 'text'),
            AttributeSpec('attribute2', 'my second attribute', 'text')
        ]

    def test_constructor(self):
        spec = DatasetSpec('my first dataset',
                           'int',
                           name='dataset1',
                           attributes=self.attributes)
        self.assertEqual(spec['dtype'], 'int')
        self.assertEqual(spec['name'], 'dataset1')
        self.assertEqual(spec['doc'], 'my first dataset')
        self.assertNotIn('linkable', spec)
        self.assertNotIn('data_type_def', spec)
        self.assertListEqual(spec['attributes'], self.attributes)
        self.assertIs(spec, self.attributes[0].parent)
        self.assertIs(spec, self.attributes[1].parent)
        json.dumps(spec)

    def test_constructor_datatype(self):
        spec = DatasetSpec('my first dataset',
                           'int',
                           name='dataset1',
                           attributes=self.attributes,
                           linkable=False,
                           data_type_def='EphysData')
        self.assertEqual(spec['dtype'], 'int')
        self.assertEqual(spec['name'], 'dataset1')
        self.assertEqual(spec['doc'], 'my first dataset')
        self.assertEqual(spec['data_type_def'], 'EphysData')
        self.assertFalse(spec['linkable'])
        self.assertListEqual(spec['attributes'], self.attributes)
        self.assertIs(spec, self.attributes[0].parent)
        self.assertIs(spec, self.attributes[1].parent)

    def test_constructor_shape(self):
        shape = [None, 2]
        spec = DatasetSpec(
            doc='my first dataset',
            dtype='int',
            name='dataset1',
            shape=shape,
        )
        self.assertEqual(spec['shape'], shape)
        self.assertEqual(spec.shape, shape)
        self.assertEqual(spec.dims, ('dim_0', 'dim_1'))

    def test_dims_without_shape(self):
        spec = DatasetSpec(
            doc='my first dataset',
            dtype='int',
            name='dataset1',
            dims=("test",),
        )
        self.assertEqual(spec.shape, (None, ))

    def test_constructor_invalidate_dtype(self):
        with self.assertRaises(ValueError):
            DatasetSpec(doc='my first dataset',
                        dtype='my bad dtype',     # <-- Expect AssertionError due to bad type
                        name='dataset1',
                        dims=(None, None),
                        attributes=self.attributes,
                        linkable=False,
                        data_type_def='EphysData')

    def test_constructor_ref_spec(self):
        dtype = RefSpec('TimeSeries', 'object')
        spec = DatasetSpec(doc='my first dataset',
                           dtype=dtype,
                           name='dataset1',
                           dims=(None, None),
                           attributes=self.attributes,
                           linkable=False,
                           data_type_def='EphysData')
        self.assertDictEqual(spec['dtype'], dtype)


    def test_constructor_table(self):
        dtype1 = DtypeSpec('column1', 'the first column', 'int')
        dtype2 = DtypeSpec('column2', 'the second column', 'float')
        spec = DatasetSpec('my first table',
                           [dtype1, dtype2],
                           name='table1',
                           attributes=self.attributes)
        self.assertEqual(spec['dtype'], [dtype1, dtype2])
        self.assertEqual(spec['name'], 'table1')
        self.assertEqual(spec['doc'], 'my first table')
        self.assertNotIn('linkable', spec)
        self.assertNotIn('data_type_def', spec)
        self.assertListEqual(spec['attributes'], self.attributes)
        self.assertIs(spec, self.attributes[0].parent)
        self.assertIs(spec, self.attributes[1].parent)
        json.dumps(spec)

    def test_constructor_invalid_table(self):
        with self.assertRaises(ValueError):
            DatasetSpec('my first table',
                        [DtypeSpec('column1', 'the first column', 'int'),
                         {}     # <--- Bad compound type spec must raise an error
                         ],
                        name='table1',
                        attributes=self.attributes)

    def test_constructor_default_value(self):
        spec = DatasetSpec(doc='test',
                           default_value=5,
                           dtype='int',
                           data_type_def='test')
        self.assertEqual(spec.default_value, 5)

    def test_name_with_incompatible_quantity(self):
        # Check that we raise an error when the quantity allows more than one instance with a fixed name
        with self.assertRaises(ValueError):
            DatasetSpec(doc='my first dataset',
                        dtype='int',
                        name='ds1',
                        quantity='zero_or_many')
        with self.assertRaises(ValueError):
            DatasetSpec(doc='my first dataset',
                        dtype='int',
                        name='ds1',
                        quantity='one_or_many')

    def test_name_with_compatible_quantity(self):
        # Make sure compatible quantity flags pass when name is fixed
        DatasetSpec(doc='my first dataset',
                    dtype='int',
                    name='ds1',
                    quantity='zero_or_one')
        DatasetSpec(doc='my first dataset',
                    dtype='int',
                    name='ds1',
                    quantity=1)

    def test_data_type_property_value(self):
        """Test that the property data_type has the expected value"""
        test_cases = {
            ('Foo', 'Bar'): 'Bar',
            ('Foo', None): 'Foo',
            (None, 'Bar'): 'Bar',
            (None, None): None,
        }
        for (data_type_inc, data_type_def), data_type in test_cases.items():
            with self.subTest(data_type_inc=data_type_inc,
                              data_type_def=data_type_def, data_type=data_type):
                group = GroupSpec('A group', name='group',
                                  data_type_inc=data_type_inc, data_type_def=data_type_def)
                self.assertEqual(group.data_type, data_type)

    def test_constructor_value(self):
        spec = DatasetSpec(doc='my first dataset', dtype='int', name='dataset1', value=42)
        assert spec.value == 42

    def test_build_warn_extra_args(self):
        spec_dict = {
            'name': 'dataset1',
            'doc': 'test dataset',
            'dtype': 'int',
            'required': True,
        }
        msg = ("Unexpected keys ['required'] in spec {'name': 'dataset1', 'doc': 'test dataset', "
               "'dtype': 'int', 'required': True}")
        with self.assertWarnsWith(UserWarning, msg):
            DatasetSpec.build_spec(spec_dict)

    def test_constructor_validates_name(self):
        with self.assertRaisesWith(
                ValueError,
                "Name 'one/two' is invalid. Names of Groups and Datasets cannot contain '/'",
        ):
            DatasetSpec(doc='my first dataset', dtype='int', name='one/two')

    def test_constructor_validates_default_name(self):
        with self.assertRaisesWith(
                ValueError,
                "Default name 'one/two' is invalid. Names of Groups and Datasets cannot contain '/'",
        ):
            DatasetSpec(doc='my first dataset', dtype='int', default_name='one/two', data_type_def='test')

    def test_constructor_value_default_value(self):
        msg = "cannot specify 'value' and 'default_value'"
        with self.assertRaisesWith(ValueError, msg):
            DatasetSpec(doc='my first dataset', dtype='int', name='dataset1', value=42, default_value=0)
