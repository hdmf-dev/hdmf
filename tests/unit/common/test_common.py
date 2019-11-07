import unittest

from hdmf import Data, Container
from hdmf.common import get_type_map


class TestCommonTypeMap(unittest.TestCase):

    def test_base_types(self):
        tm = get_type_map()
        cls = tm.get_container_cls('hdmf-common', 'Container')
        self.assertIs(cls, Container)
        cls = tm.get_container_cls('hdmf-common', 'Data')
        self.assertIs(cls, Data)
