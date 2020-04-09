import unittest
from unittest import mock

from hdmf import Container
from hdmf.foreign import ForeignField


class SimpleContainer(Container):

    __fields__ = ('foo')

    def __init__(self, foo):
        self.foo = foo


def mock_requests_get(*args, **kwargs):
    class MockResponse:
        def __init__(self, json_data, status_code):
            self.json_data = json_data
            self.status_code = status_code

        def json(self):
            return self.json_data

    if args[0] == 'http://myurl.com/foo.json':
        return MockResponse({"foo_attr": 6.022}, 200)

    return MockResponse(None, 404)


class ForeignTests(unittest.TestCase):

    @mock.patch('hdmf.foreign.foreign.requests.get', side_effect=mock_requests_get)
    def test_resolve(self, get_mock):
        container = SimpleContainer(ForeignField(uri='http://myurl.com/foo.json'))
        self.assertIsInstance(container.foo, ForeignField)
        container.foo.resolve()
        self.assertEqual(container.foo, 6.022)
