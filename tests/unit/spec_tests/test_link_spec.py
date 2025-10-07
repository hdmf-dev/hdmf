from pydantic import ValidationError

from hdmf.spec import LinkSpec, QuantityEnum
from hdmf.testing import TestCase


class LinkSpecTests(TestCase):

    def test_constructor(self):
        spec = LinkSpec(
            name='Link1',
            target_type='Group1',
            doc='A test link',
            quantity='?',
        )
        self.assertEqual(spec.name, 'Link1')
        self.assertEqual(spec.doc, 'A test link')
        self.assertEqual(spec.target_type, 'Group1')
        self.assertEqual(spec.quantity, QuantityEnum.ZERO_OR_ONE)
        spec_dict = spec.model_dump(exclude_unset=True)
        expected = {
            'name': 'Link1',
            'doc': 'A test link',
            'target_type': 'Group1',
            'quantity': '?',
        }
        self.assertDictEqual(spec_dict, expected)

    def test_constructor_defaults(self):
        spec = LinkSpec(
            target_type='Group1',
            doc='A test link',
        )
        self.assertEqual(spec.quantity, 1)
        self.assertIsNone(spec.name)
        model_dict = spec.model_dump(exclude_unset=True)
        expected = {
            "doc": "A test link",
            "target_type": "Group1",
        }
        self.assertDictEqual(model_dict, expected)

    def test_required_is_many(self):
        quantity_opts = ['?', 1, '*', '+']
        is_required = [False, True, False, True]
        is_many = [False, False, True, True]
        for (quantity, req, many) in zip(quantity_opts, is_required, is_many):
            with self.subTest(quantity=quantity):
                spec = LinkSpec(
                    target_type='Group1',
                    doc='A test link',
                    quantity=quantity,
                )
                self.assertEqual(spec.required, req)  # TODO
                self.assertEqual(spec.is_many(), many)

    def test_build_warn_extra_args(self):
        spec_dict = {
            'name': 'link1',
            'doc': 'test link',
            'target_type': 'TestType',
            'required': True,
        }
        # TODO
        with self.assertRaisesRegex(ValidationError, r"required\s*Extra inputs are not permitted"):
            LinkSpec(**spec_dict)
