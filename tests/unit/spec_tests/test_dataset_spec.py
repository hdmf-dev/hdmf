from pydantic import ValidationError

from hdmf.spec import GroupSpec, DatasetSpec, AttributeSpec, DtypeSpec, RefSpec, QuantityEnum
from hdmf.testing import TestCase


class DatasetSpecTests(TestCase):
    def setUp(self):
        self.attributes = [
            AttributeSpec(name='attribute1', dtype='text', doc='my first attribute'),
            AttributeSpec(name='attribute2', dtype='text', doc='my second attribute'),
        ]

    def test_constructor(self):
        spec = DatasetSpec(
            name='dataset1',
            dtype='int',
            doc='my first dataset',
            attributes=self.attributes
        )
        self.assertEqual(spec.dtype, 'int')
        self.assertEqual(spec.name, 'dataset1')
        self.assertEqual(spec.doc, 'my first dataset')
        self.assertListEqual(spec.attributes, self.attributes)
        spec_dict = spec.model_dump(exclude_unset=True)
        expected = {
            'name': 'dataset1',
            'doc': 'my first dataset',
            'dtype': 'int',
            'attributes': [
                {
                    'name': 'attribute1',
                    'doc': 'my first attribute',
                    'dtype': 'text',
                },
                {
                    'name': 'attribute2',
                    'doc': 'my second attribute',
                    'dtype': 'text',
                },
            ],
        }
        self.assertDictEqual(spec_dict, expected)

    def test_constructor_data_type_def(self):
        spec = DatasetSpec(
            data_type_def='EphysData',
            name='dataset1',
            dtype='int',
            doc='my first dataset',
            attributes=self.attributes,
        )
        self.assertEqual(spec.data_type_def, 'EphysData')

    def test_constructor_shape(self):
        spec = DatasetSpec(
            name='dataset1',
            dtype='int',
            shape=[None, 2],
            doc='my first dataset',
        )
        self.assertEqual(spec.shape, (None, 2))
        self.assertEqual(spec.dims, ('dim_0', 'dim_1'))

    def test_dims_without_shape(self):
        spec = DatasetSpec(
            name='dataset1',
            dtype='int',
            dims=("test",),
            doc='my first dataset',
        )
        self.assertEqual(spec.shape, (None, ))

    def test_shape_and_dims_unequal_length(self):
        with self.assertRaisesRegex(ValueError, r"'dims' and 'shape' must have the same length\."):
            DatasetSpec(
                name='dataset1',
                dtype='int',
                dims=['test'],
                shape=[None, 2],
                doc='my first dataset',
            )

    def test_constructor_invalid_dtype(self):
        with self.assertRaisesRegex(ValueError, r"dtype 'my bad dtype' is not a valid primary data type\."):
            DatasetSpec(
                data_type_def='EphysData',
                name='dataset1',
                dtype='my bad dtype',
                dims=(None, None),
                doc='my first dataset',
                attributes=self.attributes,
            )

    def test_constructor_ref_spec(self):
        dtype = RefSpec(target_type='TimeSeries', reftype='object')
        spec = DatasetSpec(
            data_type_def='EphysData',
            name='dataset1',
            dtype=dtype,
            dims=(None, None),
            doc='my first dataset',
            attributes=self.attributes,
        )
        self.assertEqual(spec.dtype, dtype)

    def test_constructor_table(self):
        dtype1 = DtypeSpec(name='column1', dtype='int', doc='the first column')
        dtype2 = DtypeSpec(name='column2', dtype='float', doc='the second column')
        spec = DatasetSpec(
            name='table1',
            dtype=[dtype1, dtype2],
            doc='my first table',
            attributes=self.attributes,
        )
        self.assertEqual(spec.dtype, [dtype1, dtype2])

    def test_constructor_invalid_table(self):
        with self.assertRaisesRegex(ValidationError, r"5 validation errors for DatasetSpec"):
            DatasetSpec(
                name='table1',
                dtype=[
                    DtypeSpec(name='column1', dtype='int', doc='the first column'),
                    {}     # <--- Bad compound type spec must raise an error
                ],
                doc='my first table',
                attributes=self.attributes,
            )

    def test_constructor_default_value(self):
        spec = DatasetSpec(
            data_type_def='test',
            dtype='int',
            default_value=5,
            doc='test',
        )
        self.assertEqual(spec.default_value, 5)

    def test_name_with_incompatible_quantity(self):
        # Check that we raise an error when the quantity allows more than one instance with a fixed name
        with self.assertRaisesRegex(ValueError, r"Cannot specify 'name' on a spec that can exist multiple times\."):
            DatasetSpec(
                name='ds1',
                dtype='int',
                doc='my first dataset',
                quantity='*',
            )
        with self.assertRaisesRegex(ValueError, r"Cannot specify 'name' on a spec that can exist multiple times\."):
            DatasetSpec(
                name='ds1',
                dtype='int',
                doc='my first dataset',
                quantity='+',
            )

    def test_name_with_compatible_quantity(self):
        # Make sure compatible quantity flags pass when name is fixed
        spec = DatasetSpec(
            name='ds1',
            dtype='int',
            doc='my first dataset',
            quantity='?',
        )
        self.assertEqual(spec.quantity, QuantityEnum.ZERO_OR_ONE)
        spec = DatasetSpec(
            name='ds1',
            dtype='int',
            doc='my first dataset',
            quantity=1,
        )
        self.assertEqual(spec.quantity, 1)

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
                group = GroupSpec(data_type_def=data_type_def, data_type_inc=data_type_inc,
                                  name='group', doc='A group')
                self.assertEqual(group.data_type, data_type)

    def test_constructor_value(self):
        spec = DatasetSpec(
            name='dataset1',
            dtype='int',
            value=42,
            doc='my first dataset',
        )
        assert spec.value == 42

    def test_build_extra_args(self):
        spec_dict = {
            'name': 'dataset1',
            'doc': 'test dataset',
            'dtype': 'int',
            'required': True,
        }
        # TODO
        with self.assertRaisesRegex(ValidationError, r'required\s*Extra inputs are not permitted'):
            DatasetSpec.build_spec(spec_dict)

    def test_constructor_validates_name(self):
        with self.assertRaisesRegex(ValueError, r"Invalid character '/' in 'name'\."):
            DatasetSpec(
                name='one/two',
                dtype='int',
                doc='my first dataset',
            )

    def test_constructor_validates_default_name(self):
        with self.assertRaisesRegex(ValueError, r"Invalid character '/' in 'default_name'\."):
            DatasetSpec(
                data_type_def='test',
                default_name='one/two',
                dtype='int',
                doc='my first dataset',
            )
