from hdmf.testing import TestCase
from hdmf import get_termset_config, load_termset_config, unload_termset_config
from hdmf.term_set import TermSetConfigurator

class TestConfig(TestCase):
    self.test_config = {}
    self.test_merged_extension_config = {}
    self.hdmf_config = {}

    def test_construct_config(self):
        # add asserts for self.path and self.config
        test_config = TermSetConfigurator(path='tests/unit/test_config.yaml')
        self.assertEqual(test_config.path, ['tests/unit/test_config.yaml'])
        # self.assertEqual(test_config.config, None)

    def test_load_termset_config(self):
        test_config = TermSetConfigurator(path='tests/unit/test_config.yaml')
        test_config.load_termset_config(path='tests/unit/test_extension_config.yaml')
        self.assertEqual(config.path, ['tests/unit/test_config.yaml', 'tests/unit/test_extension_config.yaml'])
        # self.assertEqual(config.config, None)

    def test_unload_termset_config(self):
        test_config = TermSetConfigurator(path='tests/unit/test_config.yaml')
        test_config.unload_termset_config()
        self.assertEqual(config.path, ['src/hdmf/hdmf_config.yaml'])
        self.assertEqual(config.config, None)

    def test_get_termset_config(self):
        config = get_termset_config()
        self.assertEqual(config.path, ['src/hdmf/hdmf_config.yaml'])
        # self.assertEqual(config.config, None)

    def test_unload_global_config(self):
        config = get_termset_config()
        unload_termset_config()
        self.assertEqual(config.path, ['src/hdmf/hdmf_config.yaml'])
        self.assertEqual(config.config, None)

    def test_load_global_config_reset(self):
        load_termset_config()
        self.assertEqual(config.path, ['src/hdmf/hdmf_config.yaml'])
        # self.assertEqual(config.config, None)

    def test_load_global_config_extension_config(self):
        load_termset_config()
        self.assertEqual(config.path, ['tests/unit/test_config.yaml', 'tests/unit/test_extension_config.yaml'])
        # self.assertEqual(config.config, None)

    def test_data(self):
        pass

    def test_dataset_not_in_spec(self):
        pass

    def test_attribute_not_in_spec(self):
        pass

    def test_attriute_in_spec(self):
        pass

    def test_dataset_in_spec(self):
        pass

    def test_data_type_not_in_namespace(self):
        pass

    def test_warn_not_wrapped(self):
        pass
