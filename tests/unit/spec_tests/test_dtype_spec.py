from hdmf.spec import DtypeSpec, DtypeHelper, RefSpec
from hdmf.testing import TestCase


class DtypeSpecHelper(TestCase):

    def test_recommended_dtypes(self):
        self.assertListEqual(DtypeHelper.recommended_primary_dtypes,
                             list(DtypeHelper.primary_dtype_synonyms.keys()))

    def test_valid_primary_dtypes(self):
        a = set(list(DtypeHelper.primary_dtype_synonyms.keys()) +
                [vi for v in DtypeHelper.primary_dtype_synonyms.values() for vi in v])
        self.assertSetEqual(a, DtypeHelper.valid_primary_dtypes)

    def test_simplify_cpd_type(self):
        compound_type = [
            DtypeSpec(name='test', doc='test field', dtype='float'),
            DtypeSpec(name='test2', doc='test field2', dtype='int'),
        ]
        expected_result = ['float', 'int']
        result = DtypeHelper.simplify_cpd_type(compound_type)
        self.assertListEqual(result, expected_result)

    def test_simplify_cpd_type_ref(self):
        compound_type = [
            DtypeSpec(name='test', doc='test field', dtype='float'),
            DtypeSpec(name='test2', doc='test field2', dtype=RefSpec(target_type='MyType', reftype='object')),
        ]
        expected_result = ['float', 'object']
        result = DtypeHelper.simplify_cpd_type(compound_type)
        self.assertListEqual(result, expected_result)

    def test_check_dtype_ok(self):
        DtypeHelper.check_dtype(dtype='int')  # should not raise

    def test_check_dtype_bad(self):
        with self.assertRaisesRegex(ValueError, r"'bad dtype' is not a valid primary data type\."):
            DtypeHelper.check_dtype(dtype='bad dtype')

    def test_check_dtype_ref(self):
        refspec = RefSpec(target_type='target', reftype='object')
        DtypeHelper.check_dtype(dtype=refspec)  # should not raise

    def test_is_allowed(self):
        self.assertTrue(DtypeHelper.is_allowed_dtype(new='int32', orig='int'))
        self.assertTrue(DtypeHelper.is_allowed_dtype(new='float64', orig='float'))
        self.assertFalse(DtypeHelper.is_allowed_dtype(new='int32', orig='float'))
        self.assertFalse(DtypeHelper.is_allowed_dtype(new='string', orig='int'))
        self.assertTrue(DtypeHelper.is_allowed_dtype(new='object', orig='object'))
        self.assertTrue(DtypeHelper.is_allowed_dtype(new='int64', orig='numeric'))
        self.assertTrue(DtypeHelper.is_allowed_dtype(new='float32', orig='numeric'))
        self.assertFalse(DtypeHelper.is_allowed_dtype(new='string', orig='numeric'))
        self.assertTrue(DtypeHelper.is_allowed_dtype(new='numeric', orig='numeric'))

        with self.assertRaisesRegex(ValueError, r"Invalid dtype 'bad dtype'\."):
            DtypeHelper.is_allowed_dtype(new='int32', orig='bad dtype')


class DtypeSpecTests(TestCase):

    def test_constructor(self):
        spec = DtypeSpec(name='column1', doc='an example column', dtype='int')
        self.assertEqual(spec.doc, 'an example column')
        self.assertEqual(spec.name, 'column1')
        self.assertEqual(spec.dtype, 'int')

    def test_build_spec(self):
        spec = DtypeSpec.build_spec({'doc': 'an example column', 'name': 'column1', 'dtype': 'int'})
        self.assertEqual(spec.doc, 'an example column')
        self.assertEqual(spec.name, 'column1')
        self.assertEqual(spec.dtype, 'int')

    def test_refspec_dtype(self):
        # make sure this does not cause an error
        DtypeSpec(name='column1', doc='an example column', dtype=RefSpec(target_type='TimeSeries', reftype='object'))

    def test_invalid_dtype(self):
        with self.assertRaisesRegex(ValueError, r"dtype 'bad dtype' is not a valid primary data type\."):
            DtypeSpec(name='column1', doc='an example column', dtype='bad dtype')
