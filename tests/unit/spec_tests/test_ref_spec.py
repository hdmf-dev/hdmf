import json

from hdmf.spec import RefSpec
from hdmf.testing import TestCase


class RefSpecTests(TestCase):

    def test_constructor(self):
        spec = RefSpec('TimeSeries', 'object')
        self.assertEqual(spec.target_type, 'TimeSeries')
        self.assertEqual(spec.reftype, 'object')
        json.dumps(spec)  # to ensure there are no circular links

    def test_wrong_reference_type(self):
        with self.assertRaises(ValueError):
            RefSpec('TimeSeries', 'unknownreftype')
