from hdmf import Data, Container
from hdmf.common import get_type_map
from hdmf.testing import TestCase


class TestCommonTypeMap(TestCase):

    def test_base_types(self):
        tm = get_type_map()
        cls = tm.get_container_cls('Container', 'hdmf-common')
        self.assertIs(cls, Container)
        cls = tm.get_container_cls('Data', 'hdmf-common')
        self.assertIs(cls, Data)
