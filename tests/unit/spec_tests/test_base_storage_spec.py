from hdmf.spec import AttributeSpec
from hdmf.spec.spec import BaseStorageSpec
from hdmf.testing import TestCase


class FakeStorageSpec(BaseStorageSpec):
    """A fake class inheriting from BaseStorageSpec to be used for testing"""

    __type_key = 'foodata_type'
    __namespace_key = 'foo_namespace'
    __id_key = 'foo_id'
    __inc_key = 'foodata_type_inc'
    __def_key = 'foodata_type_def'

    @classmethod
    def type_key(cls):
        """Get the key used to store data type on an instance"""
        return cls.__type_key

    @classmethod
    def namespace_key(cls):
        """Get the key used to store namespace on an instance"""
        return cls.__namespace_key

    @classmethod
    def id_key(cls):
        """Get the key used to store data ID on an instance"""
        return cls.__id_key

    @classmethod
    def inc_key(cls):
        """Get the key used to define a data_type include."""
        return cls.__inc_key

    @classmethod
    def def_key(cls):
        """ Get the key used to define a data_type definition."""
        return cls.__def_key


class BaseStorageSpecInheritanceTests(TestCase):
    """Tests which verify that classes inheriting from BaseStorageSpec can override key values"""

    def test_reserved_attrs(self):
        """Test that reserved_attrs returns values defined by the child class"""
        self.assertEqual(FakeStorageSpec.reserved_attrs(), ('foo_namespace', 'foodata_type', 'foo_id'))

    def test_get_namespace_spec(self):
        """Test that get_namespace_spec return an AttributeSpec with a name
        matching the namespace_key defined by the child class
        """
        spec = FakeStorageSpec.get_namespace_spec()
        self.assertIsInstance(spec, AttributeSpec)
        self.assertEqual(spec.name, 'foo_namespace')

    def test_get_data_type_spec(self):
        """Test that get_data_type_spec returns an AttributeSpec with a name
        matching the data_type_key defined by the child class
        """
        spec = FakeStorageSpec.get_data_type_spec('Foo')
        self.assertIsInstance(spec, AttributeSpec)
        self.assertEqual(spec.name, 'foodata_type')

    def test_data_type_inc(self):
        """Test that data_type_inc looks up the attribute using the inc_key defined by the child class"""
        spec = FakeStorageSpec('A fake spec', 'foo')
        spec['foodata_type_inc'] = 'Foo'
        self.assertEqual(spec.data_type_inc, 'Foo')

    def test_data_type_def(self):
        """Test that data_type_def looks up the attribute using the def_key defined by the child class"""
        spec = FakeStorageSpec('A fake spec', 'bar')
        spec['foodata_type_def'] = 'Bar'
        self.assertEqual(spec.data_type_def, 'Bar')
