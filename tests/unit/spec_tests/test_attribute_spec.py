from pydantic import ValidationError

from hdmf.spec import AttributeSpec, RefSpec
from hdmf.testing import TestCase


class AttributeSpecTests(TestCase):

    def test_constructor(self):
        spec = AttributeSpec(name='attribute1', doc='my first attribute', dtype='text')
        self.assertEqual(spec.name, 'attribute1')
        self.assertEqual(spec.dtype, 'text')
        self.assertEqual(spec.doc, 'my first attribute')
        self.assertIsNone(spec.parent)
        spec_dict = spec.model_dump(exclude_unset=True)
        expected = {
            'name': 'attribute1',
            'doc': 'my first attribute',
            'dtype': 'text',
        }
        self.assertDictEqual(spec_dict, expected)

    def test_invalid_dtype(self):
        with self.assertRaisesRegex(ValueError, r"dtype 'invalid' is not a valid primary data type\."):
            AttributeSpec(name='attribute1', doc='my first attribute', dtype='invalid')

    def test_both_value_and_default_value_set(self):
        with self.assertRaisesRegex(ValueError, r"Cannot specify both 'value' and 'default_value'\."):
            AttributeSpec(
                name='attribute1',
                doc='my first attribute',
                dtype='int',
                value=5,
                default_value=10,
            )

    def test_shape_and_dims_unequal_length(self):
        with self.assertRaisesRegex(ValueError, r"'dims' and 'shape' must have the same length\."):
            AttributeSpec(
                name='attribute1',
                doc='my first attribute',
                dtype='int',
                dims=['test'],
                shape=[None, 2],
            )

    def test_default_value(self):
        spec = AttributeSpec(name='attribute1', doc='my first attribute', dtype='text', default_value='some text')
        self.assertEqual(spec.default_value, 'some text')

    def test_shape(self):
        spec = AttributeSpec(name='attribute1', doc='my first attribute', dtype='text', shape=[None, 2])
        self.assertEqual(spec.shape, (None, 2))
        self.assertEqual(spec.dims, ('dim_0', 'dim_1'))

    def test_dims_without_shape(self):
        spec = AttributeSpec(name='attribute1', doc='my first attribute', dtype='text', dims=['test1', 'test2'])
        self.assertEqual(spec.shape, (None, None))
        self.assertEqual(spec.dims, ('test1', 'test2'))

    def test_shape_without_dims(self):
        spec = AttributeSpec(name='attribute1', doc='my first attribute', dtype='text', shape=(None, 3))
        self.assertEqual(spec.shape, (None, 3))
        self.assertEqual(spec.dims, ('dim_0', 'dim_1'))

    def test_build_spec(self):
        spec_dict = {
            'name': 'attribute1',
            'doc': 'my first attribute',
            'dtype': 'text',
            'shape': (None, ),
            'dims': ('dim1',),
            'value': ['a', 'b']
        }
        ret = AttributeSpec.build_spec(spec_dict)
        self.assertIsInstance(ret, AttributeSpec)
        ret_dict = ret.model_dump(exclude_unset=True)
        self.assertDictEqual(ret_dict, spec_dict)

    def test_build_spec_reftype(self):
        spec_dict = {
            'name': 'attribute1',
            'doc': 'my first attribute',
            'dtype': {'target_type': 'AnotherType', 'reftype': 'object'},
        }
        ret = AttributeSpec.build_spec(spec_dict)
        self.assertIsInstance(ret, AttributeSpec)
        self.assertEqual(ret.dtype, RefSpec(target_type='AnotherType', reftype='object'))
        ret_dict = ret.model_dump(exclude_unset=True)
        self.assertDictEqual(ret_dict, spec_dict)

    def test_build_spec_no_doc(self):
        spec_dict = {'name': 'attribute1', 'dtype': 'text'}
        with self.assertRaisesRegex(ValidationError, r'doc\s*Field required'):
            AttributeSpec.build_spec(spec_dict)

    def test_build_extra_args(self):
        spec_dict = {
            'name': 'attribute1',
            'doc': 'test attribute',
            'dtype': 'int',
            'quantity': '?',
        }
        with self.assertRaisesRegex(ValidationError, r'quantity\s*Extra inputs are not permitted'):
            AttributeSpec.build_spec(spec_dict)
