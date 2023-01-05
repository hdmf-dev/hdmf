from hdmf.spec import DtypeSpec, DtypeHelper, RefSpec
from hdmf.testing import TestCase


class DtypeSpecHelper(TestCase):
    def setUp(self):
        pass

    def test_recommended_dtypes(self):
        self.assertListEqual(DtypeHelper.recommended_primary_dtypes,
                             list(DtypeHelper.primary_dtype_synonyms.keys()))

    def test_valid_primary_dtypes(self):
        a = set(list(DtypeHelper.primary_dtype_synonyms.keys()) +
                [vi for v in DtypeHelper.primary_dtype_synonyms.values() for vi in v])
        self.assertSetEqual(a, DtypeHelper.valid_primary_dtypes)

    def test_simplify_cpd_type(self):
        compound_type = [DtypeSpec('test', 'test field', 'float'),
                         DtypeSpec('test2', 'test field2', 'int')]
        expected_result = ['float', 'int']
        result = DtypeHelper.simplify_cpd_type(compound_type)
        self.assertListEqual(result, expected_result)

    def test_simplify_cpd_type_ref(self):
        compound_type = [DtypeSpec('test', 'test field', 'float'),
                         DtypeSpec('test2', 'test field2', RefSpec(target_type='MyType', reftype='object'))]
        expected_result = ['float', 'object']
        result = DtypeHelper.simplify_cpd_type(compound_type)
        self.assertListEqual(result, expected_result)

    def test_check_dtype_ok(self):
        self.assertEqual('int', DtypeHelper.check_dtype('int'))

    def test_check_dtype_bad(self):
        msg = "dtype 'bad dtype' is not a valid primary data type."
        with self.assertRaisesRegex(ValueError, msg):
            DtypeHelper.check_dtype('bad dtype')

    def test_check_dtype_ref(self):
        refspec = RefSpec(target_type='target', reftype='object')
        self.assertIs(refspec, DtypeHelper.check_dtype(refspec))


class DtypeSpecTests(TestCase):
    def setUp(self):
        pass

    def test_constructor(self):
        spec = DtypeSpec('column1', 'an example column', 'int')
        self.assertEqual(spec.doc, 'an example column')
        self.assertEqual(spec.name, 'column1')
        self.assertEqual(spec.dtype, 'int')

    def test_build_spec(self):
        spec = DtypeSpec.build_spec({'doc': 'an example column', 'name': 'column1', 'dtype': 'int'})
        self.assertEqual(spec.doc, 'an example column')
        self.assertEqual(spec.name, 'column1')
        self.assertEqual(spec.dtype, 'int')

    def test_invalid_refspec_dict(self):
        """Test missing or bad target key for RefSpec."""
        msg = "'dtype' must have the key 'target_type'"
        with self.assertRaisesWith(ValueError, msg):
            DtypeSpec.assertValidDtype({'no target': 'test', 'reftype': 'object'})

    def test_refspec_dtype(self):
        # just making sure this does not cause an error
        DtypeSpec('column1', 'an example column', RefSpec('TimeSeries', 'object'))

    def test_invalid_dtype(self):
        msg = "dtype 'bad dtype' is not a valid primary data type."
        with self.assertRaisesRegex(ValueError, msg):
            DtypeSpec('column1', 'an example column', dtype='bad dtype')

    def test_is_ref(self):
        spec = DtypeSpec('column1', 'an example column', RefSpec('TimeSeries', 'object'))
        self.assertTrue(DtypeSpec.is_ref(spec))
        spec = DtypeSpec('column1', 'an example column', 'int')
        self.assertFalse(DtypeSpec.is_ref(spec))
