import json

from hdmf.spec import AttributeSpec, RefSpec
from hdmf.testing import TestCase


class AttributeSpecTests(TestCase):

    def test_constructor(self):
        spec = AttributeSpec('attribute1',
                             'my first attribute',
                             'text')
        self.assertEqual(spec['name'], 'attribute1')
        self.assertEqual(spec['dtype'], 'text')
        self.assertEqual(spec['doc'], 'my first attribute')
        self.assertIsNone(spec.parent)
        json.dumps(spec)  # to ensure there are no circular links

    def test_invalid_dtype(self):
        with self.assertRaises(ValueError):
            AttributeSpec(name='attribute1',
                          doc='my first attribute',
                          dtype='invalid'              # <-- Invalid dtype must raise a ValueError
                          )

    def test_both_value_and_default_value_set(self):
        with self.assertRaises(ValueError):
            AttributeSpec(name='attribute1',
                          doc='my first attribute',
                          dtype='int',
                          value=5,
                          default_value=10            # <-- Default_value and value can't be set at the same time
                          )

    def test_colliding_shape_and_dims(self):
        with self.assertRaises(ValueError):
            AttributeSpec(name='attribute1',
                          doc='my first attribute',
                          dtype='int',
                          dims=['test'],
                          shape=[None, 2]           # <-- Length of shape and dims do not match must raise a ValueError
                          )

    def test_default_value(self):
        spec = AttributeSpec('attribute1',
                             'my first attribute',
                             'text',
                             default_value='some text')
        self.assertEqual(spec['default_value'], 'some text')
        self.assertEqual(spec.default_value, 'some text')

    def test_shape(self):
        shape = [None, 2]
        spec = AttributeSpec('attribute1',
                             'my first attribute',
                             'text',
                             shape=shape)
        self.assertEqual(spec['shape'], shape)
        self.assertEqual(spec.shape, shape)

    def test_dims_without_shape(self):
        spec = AttributeSpec('attribute1',
                             'my first attribute',
                             'text',
                             dims=['test'])
        self.assertEqual(spec.shape, (None, ))

    def test_build_spec(self):
        spec_dict = {'name': 'attribute1',
                     'doc': 'my first attribute',
                     'dtype': 'text',
                     'shape': [None],
                     'dims': ['dim1'],
                     'value': ['a', 'b']}
        ret = AttributeSpec.build_spec(spec_dict)
        self.assertTrue(isinstance(ret, AttributeSpec))
        self.assertDictEqual(ret, spec_dict)

    def test_build_spec_reftype(self):
        spec_dict = {'name': 'attribute1',
                     'doc': 'my first attribute',
                     'dtype': {'target_type': 'AnotherType', 'reftype': 'object'}}
        expected = spec_dict.copy()
        expected['dtype'] = RefSpec(target_type='AnotherType', reftype='object')
        ret = AttributeSpec.build_spec(spec_dict)
        self.assertTrue(isinstance(ret, AttributeSpec))
        self.assertDictEqual(ret, expected)

    def test_build_spec_no_doc(self):
        spec_dict = {'name': 'attribute1', 'dtype': 'text'}
        msg = "AttributeSpec.__init__: missing argument 'doc'"
        with self.assertRaisesWith(TypeError, msg):
            AttributeSpec.build_spec(spec_dict)
