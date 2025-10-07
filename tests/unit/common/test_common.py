from hdmf import Data, Container
from hdmf.common import get_type_map, load_type_config, unload_type_config, available_namespaces, _get_resources
from hdmf.testing import TestCase


class TestCommonTypeMap(TestCase):

    def tearDown(self):
        unload_type_config()

    def test_base_types(self):
        tm = get_type_map()
        cls = tm.get_dt_container_cls('Container', 'hdmf-common')
        self.assertIs(cls, Container)
        cls = tm.get_dt_container_cls('Data', 'hdmf-common')
        self.assertIs(cls, Data)

    def test_copy_ts_config(self):
        path = 'tests/unit/hdmf_config.yaml'
        load_type_config(config_path=path)
        tm = get_type_map()
        config = {'namespaces': {'hdmf-common': {'version': '3.12.2',
                  'data_types': {'VectorData':
                 {'description': {'termset': 'example_test_term_set.yaml'}},
                  'VectorIndex': {'data': '...'}}}, 'foo_namespace':
                 {'version': '...', 'data_types':
                 {'ExtensionContainer': {'description': None}}}}}

        self.assertEqual(tm.type_config.config, config)
        self.assertEqual(tm.type_config.paths, [path])


class TestCommonInit(TestCase):
    def test_available_namespaces(self):
        ns = available_namespaces()
        self.assertIn('hdmf-common', ns)
        self.assertIn('hdmf-experimental', ns)

    def test_get_resources_legacy(self):
        """Test legacy _get_resources function."""
        result = _get_resources()
        self.assertIsInstance(result, dict)
