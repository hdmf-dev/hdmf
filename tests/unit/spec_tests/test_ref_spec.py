from pydantic import ValidationError

from hdmf.spec import RefSpec
from hdmf.testing import TestCase


class RefSpecTests(TestCase):

    def test_constructor(self):
        spec = RefSpec(target_type='TimeSeries', reftype='object')
        self.assertEqual(spec.target_type, 'TimeSeries')
        self.assertEqual(spec.reftype, 'object')
        spec_dict = spec.model_dump(exclude_unset=True)
        expected = {
            'target_type': 'TimeSeries',
            'reftype': 'object',
        }
        self.assertDictEqual(spec_dict, expected)

    def test_wrong_reference_type(self):
        with self.assertRaisesRegex(ValidationError, r"reftype\s*Input should be 'object'"):
            RefSpec(target_type='TimeSeries', reftype='unknownreftype')
