import json

from hdmf.spec import GroupSpec, LinkSpec
from hdmf.testing import TestCase


class LinkSpecTests(TestCase):

    def test_constructor(self):
        spec = LinkSpec(
            doc='A test link',
            target_type='Group1',
            quantity='+',
            name='Link1',
        )
        self.assertEqual(spec.doc, 'A test link')
        self.assertEqual(spec.target_type, 'Group1')
        self.assertEqual(spec.data_type_inc, 'Group1')
        self.assertEqual(spec.quantity, '+')
        self.assertEqual(spec.name, 'Link1')
        json.dumps(spec)

    def test_constructor_target_spec_def(self):
        group_spec_def = GroupSpec(
            data_type_def='Group1',
            doc='A test group',
        )
        spec = LinkSpec(
            doc='A test link',
            target_type=group_spec_def,
        )
        self.assertEqual(spec.target_type, 'Group1')
        json.dumps(spec)

    def test_constructor_target_spec_inc(self):
        group_spec_inc = GroupSpec(
            data_type_inc='Group1',
            doc='A test group',
        )
        msg = "'target_type' must be a string or a GroupSpec or DatasetSpec with a 'data_type_def' key."
        with self.assertRaisesWith(ValueError, msg):
            LinkSpec(
                doc='A test link',
                target_type=group_spec_inc,
            )

    def test_constructor_defaults(self):
        spec = LinkSpec(
            doc='A test link',
            target_type='Group1',
        )
        self.assertEqual(spec.quantity, 1)
        self.assertIsNone(spec.name)
        json.dumps(spec)

    def test_required_is_many(self):
        quantity_opts = ['?', 1, '*', '+']
        is_required = [False, True, False, True]
        is_many = [False, False, True, True]
        for (quantity, req, many) in zip(quantity_opts, is_required, is_many):
            with self.subTest(quantity=quantity):
                spec = LinkSpec(
                    doc='A test link',
                    target_type='Group1',
                    quantity=quantity,
                    name='Link1',
                )
                self.assertEqual(spec.required, req)
                self.assertEqual(spec.is_many(), many)
