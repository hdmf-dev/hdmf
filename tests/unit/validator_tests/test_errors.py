from unittest import TestCase

from hdmf.validate.errors import Error


class TestErrorEquality(TestCase):
    def test_self_equality(self):
        """Verify that one error equals itself"""
        error = Error('foo', 'bad thing', 'a.b.c')
        self.assertEqual(error, error)

    def test_equality_with_same_field_values(self):
        """Verify that two errors with the same field values are equal"""
        err1 = Error('foo', 'bad thing', 'a.b.c')
        err2 = Error('foo', 'bad thing', 'a.b.c')
        self.assertEqual(err1, err2)

    def test_not_equal_with_different_reason(self):
        """Verify that two errors with a different reason are not equal"""
        err1 = Error('foo', 'bad thing', 'a.b.c')
        err2 = Error('foo', 'something else', 'a.b.c')
        self.assertNotEqual(err1, err2)

    def test_not_equal_with_different_name(self):
        """Verify that two errors with a different name are not equal"""
        err1 = Error('foo', 'bad thing', 'a.b.c')
        err2 = Error('bar', 'bad thing', 'a.b.c')
        self.assertNotEqual(err1, err2)

    def test_not_equal_with_different_location(self):
        """Verify that two errors with a different location are not equal"""
        err1 = Error('foo', 'bad thing', 'a.b.c')
        err2 = Error('foo', 'bad thing', 'd.e.f')
        self.assertNotEqual(err1, err2)

    def test_equal_with_no_location(self):
        """Verify that two errors with no location but the same name are equal"""
        err1 = Error('foo', 'bad thing')
        err2 = Error('foo', 'bad thing')
        self.assertEqual(err1, err2)

    def test_not_equal_with_overlapping_name_when_no_location(self):
        """Verify that two errors with an overlapping name but no location are
        not equal
        """
        err1 = Error('foo', 'bad thing')
        err2 = Error('x/y/foo', 'bad thing')
        self.assertNotEqual(err1, err2)

    def test_equal_with_overlapping_name_when_location_present(self):
        """Verify that two errors with an overlapping name and a location are equal"""
        err1 = Error('foo', 'bad thing', 'a.b.c')
        err2 = Error('x/y/foo', 'bad thing', 'a.b.c')
        self.assertEqual(err1, err2)
