from abc import ABCMeta, abstractmethod
from datetime import datetime
from unittest import mock, skip

import numpy as np
from dateutil.tz import tzlocal
from hdmf.build import GroupBuilder, DatasetBuilder, LinkBuilder
from hdmf.spec import GroupSpec, AttributeSpec, DatasetSpec, SpecCatalog, SpecNamespace, LinkSpec
from hdmf.spec.spec import ONE_OR_MANY, ZERO_OR_MANY, ZERO_OR_ONE
from hdmf.testing import TestCase
from hdmf.validate import ValidatorMap
from hdmf.validate.errors import (DtypeError, MissingError, ExpectedArrayError, MissingDataType,
                                  IncorrectQuantityError, IllegalLinkError)

CORE_NAMESPACE = 'test_core'


class ValidatorTestBase(TestCase, metaclass=ABCMeta):

    def setUp(self):
        spec_catalog = SpecCatalog()
        for spec in self.getSpecs():
            spec_catalog.register_spec(spec, 'test.yaml')
        self.namespace = SpecNamespace(
            'a test namespace', CORE_NAMESPACE, [{'source': 'test.yaml'}], version='0.1.0', catalog=spec_catalog)
        self.vmap = ValidatorMap(self.namespace)

    @abstractmethod
    def getSpecs(self):
        pass

    def assertValidationError(self, error, type_, name=None, reason=None):
        """Assert that a validation Error matches expectations"""
        self.assertIsInstance(error, type_)
        if name is not None:
            self.assertEqual(error.name, name)
        if reason is not None:
            self.assertEqual(error.reason, reason)


class TestEmptySpec(ValidatorTestBase):

    def getSpecs(self):
        return (GroupSpec('A test group specification with a data type', data_type_def='Bar'),)

    def test_valid(self):
        builder = GroupBuilder('my_bar', attributes={'data_type': 'Bar'})
        validator = self.vmap.get_validator('Bar')
        result = validator.validate(builder)
        self.assertEqual(len(result), 0)

    def test_invalid_missing_req_type(self):
        builder = GroupBuilder('my_bar')
        err_msg = r"builder must have data type defined with attribute '[A-Za-z_]+'"
        with self.assertRaisesRegex(ValueError, err_msg):
            self.vmap.validate(builder)


class TestBasicSpec(ValidatorTestBase):

    def getSpecs(self):
        ret = GroupSpec('A test group specification with a data type',
                        data_type_def='Bar',
                        datasets=[DatasetSpec('an example dataset', 'int', name='data',
                                              attributes=[AttributeSpec(
                                                  'attr2', 'an example integer attribute', 'int')])],
                        attributes=[AttributeSpec('attr1', 'an example string attribute', 'text')])
        return (ret,)

    def test_invalid_missing(self):
        builder = GroupBuilder('my_bar', attributes={'data_type': 'Bar'})
        validator = self.vmap.get_validator('Bar')
        result = validator.validate(builder)
        self.assertEqual(len(result), 2)
        self.assertValidationError(result[0], MissingError, name='Bar/attr1')
        self.assertValidationError(result[1], MissingError, name='Bar/data')

    def test_invalid_incorrect_type_get_validator(self):
        builder = GroupBuilder('my_bar', attributes={'data_type': 'Bar', 'attr1': 10})
        validator = self.vmap.get_validator('Bar')
        result = validator.validate(builder)
        self.assertEqual(len(result), 2)
        self.assertValidationError(result[0], DtypeError, name='Bar/attr1')
        self.assertValidationError(result[1], MissingError, name='Bar/data')

    def test_invalid_incorrect_type_validate(self):
        builder = GroupBuilder('my_bar', attributes={'data_type': 'Bar', 'attr1': 10})
        result = self.vmap.validate(builder)
        self.assertEqual(len(result), 2)
        self.assertValidationError(result[0], DtypeError, name='Bar/attr1')
        self.assertValidationError(result[1], MissingError, name='Bar/data')

    def test_valid(self):
        builder = GroupBuilder('my_bar',
                               attributes={'data_type': 'Bar', 'attr1': 'a string attribute'},
                               datasets=[DatasetBuilder('data', 100, attributes={'attr2': 10})])
        validator = self.vmap.get_validator('Bar')
        result = validator.validate(builder)
        self.assertEqual(len(result), 0)


class TestDateTimeInSpec(ValidatorTestBase):

    def getSpecs(self):
        ret = GroupSpec('A test group specification with a data type',
                        data_type_def='Bar',
                        datasets=[DatasetSpec('an example dataset', 'int', name='data',
                                              attributes=[AttributeSpec(
                                                  'attr2', 'an example integer attribute', 'int')]),
                                  DatasetSpec('an example time dataset', 'isodatetime', name='time'),
                                  DatasetSpec('an array of times', 'isodatetime', name='time_array',
                                              dims=('num_times',), shape=(None,))],
                        attributes=[AttributeSpec('attr1', 'an example string attribute', 'text')])
        return (ret,)

    def test_valid_isodatetime(self):
        builder = GroupBuilder('my_bar',
                               attributes={'data_type': 'Bar', 'attr1': 'a string attribute'},
                               datasets=[DatasetBuilder('data', 100, attributes={'attr2': 10}),
                                         DatasetBuilder('time',
                                                        datetime(2017, 5, 1, 12, 0, 0, tzinfo=tzlocal())),
                                         DatasetBuilder('time_array',
                                                        [datetime(2017, 5, 1, 12, 0, 0, tzinfo=tzlocal())])])
        validator = self.vmap.get_validator('Bar')
        result = validator.validate(builder)
        self.assertEqual(len(result), 0)

    def test_invalid_isodatetime(self):
        builder = GroupBuilder('my_bar',
                               attributes={'data_type': 'Bar', 'attr1': 'a string attribute'},
                               datasets=[DatasetBuilder('data', 100, attributes={'attr2': 10}),
                                         DatasetBuilder('time', 100),
                                         DatasetBuilder('time_array',
                                                        [datetime(2017, 5, 1, 12, 0, 0, tzinfo=tzlocal())])])
        validator = self.vmap.get_validator('Bar')
        result = validator.validate(builder)
        self.assertEqual(len(result), 1)
        self.assertValidationError(result[0], DtypeError, name='Bar/time')

    def test_invalid_isodatetime_array(self):
        builder = GroupBuilder('my_bar',
                               attributes={'data_type': 'Bar', 'attr1': 'a string attribute'},
                               datasets=[DatasetBuilder('data', 100, attributes={'attr2': 10}),
                                         DatasetBuilder('time',
                                                        datetime(2017, 5, 1, 12, 0, 0, tzinfo=tzlocal())),
                                         DatasetBuilder('time_array',
                                                        datetime(2017, 5, 1, 12, 0, 0, tzinfo=tzlocal()))])
        validator = self.vmap.get_validator('Bar')
        result = validator.validate(builder)
        self.assertEqual(len(result), 1)
        self.assertValidationError(result[0], ExpectedArrayError, name='Bar/time_array')


class TestNestedTypes(ValidatorTestBase):

    def getSpecs(self):
        baz = DatasetSpec('A dataset with a data type', 'int', data_type_def='Baz',
                          attributes=[AttributeSpec('attr2', 'an example integer attribute', 'int')])
        bar = GroupSpec('A test group specification with a data type',
                        data_type_def='Bar',
                        datasets=[DatasetSpec('an example dataset', data_type_inc='Baz')],
                        attributes=[AttributeSpec('attr1', 'an example string attribute', 'text')])
        foo = GroupSpec('A test group that contains a data type',
                        data_type_def='Foo',
                        groups=[GroupSpec('A Bar group for Foos', name='my_bar', data_type_inc='Bar')],
                        attributes=[AttributeSpec('foo_attr', 'a string attribute specified as text', 'text',
                                                  required=False)])

        return (bar, foo, baz)

    def test_invalid_missing_named_req_group(self):
        """Test that a MissingDataType is returned when a required named nested data type is missing."""
        foo_builder = GroupBuilder('my_foo', attributes={'data_type': 'Foo',
                                                         'foo_attr': 'example Foo object'})
        results = self.vmap.validate(foo_builder)
        self.assertEqual(len(results), 1)
        self.assertValidationError(results[0], MissingDataType, name='Foo',
                                   reason='missing data type Bar (my_bar)')

    def test_invalid_wrong_name_req_type(self):
        """Test that a MissingDataType is returned when a required nested data type is given the wrong name."""
        bar_builder = GroupBuilder('bad_bar_name',
                                   attributes={'data_type': 'Bar', 'attr1': 'a string attribute'},
                                   datasets=[DatasetBuilder('data', 100, attributes={'attr2': 10})])

        foo_builder = GroupBuilder('my_foo',
                                   attributes={'data_type': 'Foo', 'foo_attr': 'example Foo object'},
                                   groups=[bar_builder])

        results = self.vmap.validate(foo_builder)
        self.assertEqual(len(results), 1)
        self.assertValidationError(results[0], MissingDataType, name='Foo')
        self.assertEqual(results[0].data_type, 'Bar')

    def test_invalid_missing_unnamed_req_group(self):
        """Test that a MissingDataType is returned when a required unnamed nested data type is missing."""
        bar_builder = GroupBuilder('my_bar',
                                   attributes={'data_type': 'Bar', 'attr1': 'a string attribute'})

        foo_builder = GroupBuilder('my_foo',
                                   attributes={'data_type': 'Foo', 'foo_attr': 'example Foo object'},
                                   groups=[bar_builder])

        results = self.vmap.validate(foo_builder)
        self.assertEqual(len(results), 1)
        self.assertValidationError(results[0], MissingDataType, name='Bar',
                                   reason='missing data type Baz')

    def test_valid(self):
        """Test that no errors are returned when nested data types are correctly built."""
        bar_builder = GroupBuilder('my_bar',
                                   attributes={'data_type': 'Bar', 'attr1': 'a string attribute'},
                                   datasets=[DatasetBuilder('data', 100, attributes={'data_type': 'Baz', 'attr2': 10})])

        foo_builder = GroupBuilder('my_foo',
                                   attributes={'data_type': 'Foo', 'foo_attr': 'example Foo object'},
                                   groups=[bar_builder])

        results = self.vmap.validate(foo_builder)
        self.assertEqual(len(results), 0)

    def test_valid_wo_opt_attr(self):
        """"Test that no errors are returned when an optional attribute is omitted from a group."""
        bar_builder = GroupBuilder('my_bar',
                                   attributes={'data_type': 'Bar', 'attr1': 'a string attribute'},
                                   datasets=[DatasetBuilder('data', 100, attributes={'data_type': 'Baz', 'attr2': 10})])
        foo_builder = GroupBuilder('my_foo',
                                   attributes={'data_type': 'Foo'},
                                   groups=[bar_builder])

        results = self.vmap.validate(foo_builder)
        self.assertEqual(len(results), 0)


class TestQuantityValidation(TestCase):

    def create_test_specs(self, q_groups, q_datasets, q_links):
        bar = GroupSpec('A test group', data_type_def='Bar')
        baz = DatasetSpec('A test dataset', 'int', data_type_def='Baz')
        qux = GroupSpec('A group to link', data_type_def='Qux')
        foo = GroupSpec('A group containing a quantity of tests and datasets',
                        data_type_def='Foo',
                        groups=[GroupSpec('A bar', data_type_inc='Bar', quantity=q_groups)],
                        datasets=[DatasetSpec('A baz', data_type_inc='Baz', quantity=q_datasets)],
                        links=[LinkSpec('A qux', target_type='Qux', quantity=q_links)],)
        return (bar, foo, baz, qux)

    def configure_specs(self, specs):
        spec_catalog = SpecCatalog()
        for spec in specs:
            spec_catalog.register_spec(spec, 'test.yaml')
        self.namespace = SpecNamespace(
            'a test namespace', CORE_NAMESPACE, [{'source': 'test.yaml'}], version='0.1.0', catalog=spec_catalog)
        self.vmap = ValidatorMap(self.namespace)

    def get_test_builder(self, n_groups, n_datasets, n_links):
        child_groups = [GroupBuilder(f'bar_{n}', attributes={'data_type': 'Bar'}) for n in range(n_groups)]
        child_datasets = [DatasetBuilder(f'baz_{n}', n, attributes={'data_type': 'Baz'}) for n in range(n_datasets)]
        child_links = [LinkBuilder(GroupBuilder(f'qux_{n}', attributes={'data_type': 'Qux'}), f'qux_{n}_link')
                       for n in range(n_links)]
        return GroupBuilder('my_foo', attributes={'data_type': 'Foo'},
                            groups=child_groups, datasets=child_datasets, links=child_links)

    def test_valid_zero_or_many(self):
        """"Verify that groups/datasets/links with ZERO_OR_MANY and a valid quantity correctly pass validation"""
        specs = self.create_test_specs(q_groups=ZERO_OR_MANY, q_datasets=ZERO_OR_MANY, q_links=ZERO_OR_MANY)
        self.configure_specs(specs)
        for n in [0, 1, 2, 5]:
            with self.subTest(quantity=n):
                builder = self.get_test_builder(n_groups=n, n_datasets=n, n_links=n)
                results = self.vmap.validate(builder)
                self.assertEqual(len(results), 0)

    def test_valid_one_or_many(self):
        """"Verify that groups/datasets/links with ONE_OR_MANY and a valid quantity correctly pass validation"""
        specs = self.create_test_specs(q_groups=ONE_OR_MANY, q_datasets=ONE_OR_MANY, q_links=ONE_OR_MANY)
        self.configure_specs(specs)
        for n in [1, 2, 5]:
            with self.subTest(quantity=n):
                builder = self.get_test_builder(n_groups=n, n_datasets=n, n_links=n)
                results = self.vmap.validate(builder)
                self.assertEqual(len(results), 0)

    def test_valid_zero_or_one(self):
        """"Verify that groups/datasets/links with ZERO_OR_ONE and a valid quantity correctly pass validation"""
        specs = self.create_test_specs(q_groups=ZERO_OR_ONE, q_datasets=ZERO_OR_ONE, q_links=ZERO_OR_ONE)
        self.configure_specs(specs)
        for n in [0, 1]:
            with self.subTest(quantity=n):
                builder = self.get_test_builder(n_groups=n, n_datasets=n, n_links=n)
                results = self.vmap.validate(builder)
                self.assertEqual(len(results), 0)

    def test_valid_fixed_quantity(self):
        """"Verify that groups/datasets/links with a correct fixed quantity correctly pass validation"""
        self.configure_specs(self.create_test_specs(q_groups=2, q_datasets=3, q_links=5))
        builder = self.get_test_builder(n_groups=2, n_datasets=3, n_links=5)
        results = self.vmap.validate(builder)
        self.assertEqual(len(results), 0)

    def test_missing_one_or_many_should_not_return_incorrect_quantity_error(self):
        """Verify that missing ONE_OR_MANY groups/datasets/links should not return an IncorrectQuantityError

        NOTE: a MissingDataType error should be returned instead
        """
        specs = self.create_test_specs(q_groups=ONE_OR_MANY, q_datasets=ONE_OR_MANY, q_links=ONE_OR_MANY)
        self.configure_specs(specs)
        builder = self.get_test_builder(n_groups=0, n_datasets=0, n_links=0)
        results = self.vmap.validate(builder)
        self.assertFalse(any(isinstance(e, IncorrectQuantityError) for e in results))

    def test_missing_fixed_quantity_should_not_return_incorrect_quantity_error(self):
        """Verify that missing groups/datasets/links should not return an IncorrectQuantityError"""
        self.configure_specs(self.create_test_specs(q_groups=5, q_datasets=3, q_links=2))
        builder = self.get_test_builder(0, 0, 0)
        results = self.vmap.validate(builder)
        self.assertFalse(any(isinstance(e, IncorrectQuantityError) for e in results))

    def test_incorrect_fixed_quantity_should_return_incorrect_quantity_error(self):
        """Verify that an incorrect quantity of groups/datasets/links should return an IncorrectQuantityError"""
        self.configure_specs(self.create_test_specs(q_groups=5, q_datasets=5, q_links=5))
        for n in [1, 2, 10]:
            with self.subTest(quantity=n):
                builder = self.get_test_builder(n_groups=n, n_datasets=n, n_links=n)
                results = self.vmap.validate(builder)
                self.assertEqual(len(results), 3)
                self.assertTrue(all(isinstance(e, IncorrectQuantityError) for e in results))

    def test_incorrect_zero_or_one_quantity_should_return_incorrect_quantity_error(self):
        """Verify that an incorrect ZERO_OR_ONE quantity of groups/datasets/links should return
        an IncorrectQuantityError
        """
        specs = self.create_test_specs(q_groups=ZERO_OR_ONE, q_datasets=ZERO_OR_ONE, q_links=ZERO_OR_ONE)
        self.configure_specs(specs)
        builder = self.get_test_builder(n_groups=2, n_datasets=2, n_links=2)
        results = self.vmap.validate(builder)
        self.assertEqual(len(results), 3)
        self.assertTrue(all(isinstance(e, IncorrectQuantityError) for e in results))

    def test_incorrect_quantity_error_message(self):
        """Verify that an IncorrectQuantityError includes the expected information in the message"""
        specs = self.create_test_specs(q_groups=2, q_datasets=ZERO_OR_MANY, q_links=ZERO_OR_MANY)
        self.configure_specs(specs)
        builder = self.get_test_builder(n_groups=7, n_datasets=0, n_links=0)
        results = self.vmap.validate(builder)
        self.assertEqual(len(results), 1)
        self.assertIsInstance(results[0], IncorrectQuantityError)
        message = str(results[0])
        self.assertTrue('expected a quantity of 2' in message)
        self.assertTrue('received 7' in message)


class TestDtypeValidation(TestCase):

    def set_up_spec(self, dtype):
        spec_catalog = SpecCatalog()
        spec = GroupSpec('A test group specification with a data type',
                         data_type_def='Bar',
                         datasets=[DatasetSpec('an example dataset', dtype, name='data')],
                         attributes=[AttributeSpec('attr1', 'an example attribute', dtype)])
        spec_catalog.register_spec(spec, 'test.yaml')
        self.namespace = SpecNamespace(
            'a test namespace', CORE_NAMESPACE, [{'source': 'test.yaml'}], version='0.1.0', catalog=spec_catalog)
        self.vmap = ValidatorMap(self.namespace)

    def test_ascii_for_utf8(self):
        """Test that validator allows ASCII data where UTF8 is specified."""
        self.set_up_spec('text')
        value = b'an ascii string'
        bar_builder = GroupBuilder('my_bar',
                                   attributes={'data_type': 'Bar', 'attr1': value},
                                   datasets=[DatasetBuilder('data', value)])
        results = self.vmap.validate(bar_builder)
        self.assertEqual(len(results), 0)

    def test_utf8_for_ascii(self):
        """Test that validator does not allow UTF8 where ASCII is specified."""
        self.set_up_spec('bytes')
        value = 'a utf8 string'
        bar_builder = GroupBuilder('my_bar',
                                   attributes={'data_type': 'Bar', 'attr1': value},
                                   datasets=[DatasetBuilder('data', value)])
        results = self.vmap.validate(bar_builder)
        result_strings = set([str(s) for s in results])
        expected_errors = {"Bar/attr1 (my_bar.attr1): incorrect type - expected 'bytes', got 'utf'",
                           "Bar/data (my_bar/data): incorrect type - expected 'bytes', got 'utf'"}
        self.assertEqual(result_strings, expected_errors)

    def test_int64_for_int8(self):
        """Test that validator allows int64 data where int8 is specified."""
        self.set_up_spec('int8')
        value = np.int64(1)
        bar_builder = GroupBuilder('my_bar',
                                   attributes={'data_type': 'Bar', 'attr1': value},
                                   datasets=[DatasetBuilder('data', value)])
        results = self.vmap.validate(bar_builder)
        self.assertEqual(len(results), 0)

    def test_int8_for_int64(self):
        """Test that validator does not allow int8 data where int64 is specified."""
        self.set_up_spec('int64')
        value = np.int8(1)
        bar_builder = GroupBuilder('my_bar',
                                   attributes={'data_type': 'Bar', 'attr1': value},
                                   datasets=[DatasetBuilder('data', value)])
        results = self.vmap.validate(bar_builder)
        result_strings = set([str(s) for s in results])
        expected_errors = {"Bar/attr1 (my_bar.attr1): incorrect type - expected 'int64', got 'int8'",
                           "Bar/data (my_bar/data): incorrect type - expected 'int64', got 'int8'"}
        self.assertEqual(result_strings, expected_errors)

    def test_int64_for_numeric(self):
        """Test that validator allows int64 data where numeric is specified."""
        self.set_up_spec('numeric')
        value = np.int64(1)
        bar_builder = GroupBuilder('my_bar',
                                   attributes={'data_type': 'Bar', 'attr1': value},
                                   datasets=[DatasetBuilder('data', value)])
        results = self.vmap.validate(bar_builder)
        self.assertEqual(len(results), 0)

    def test_bool_for_numeric(self):
        """Test that validator does not allow bool data where numeric is specified."""
        self.set_up_spec('numeric')
        value = np.bool(1)
        bar_builder = GroupBuilder('my_bar',
                                   attributes={'data_type': 'Bar', 'attr1': value},
                                   datasets=[DatasetBuilder('data', value)])
        results = self.vmap.validate(bar_builder)
        result_strings = set([str(s) for s in results])
        expected_errors = {"Bar/attr1 (my_bar.attr1): incorrect type - expected 'numeric', got 'bool'",
                           "Bar/data (my_bar/data): incorrect type - expected 'numeric', got 'bool'"}
        self.assertEqual(result_strings, expected_errors)

    def test_np_bool_for_bool(self):
        """Test that validator allows np.bool_ data where bool is specified."""
        self.set_up_spec('bool')
        value = np.bool_(True)
        bar_builder = GroupBuilder('my_bar',
                                   attributes={'data_type': 'Bar', 'attr1': value},
                                   datasets=[DatasetBuilder('data', value)])
        results = self.vmap.validate(bar_builder)
        self.assertEqual(len(results), 0)


class Test1DArrayValidation(TestCase):

    def set_up_spec(self, dtype):
        spec_catalog = SpecCatalog()
        spec = GroupSpec('A test group specification with a data type',
                         data_type_def='Bar',
                         datasets=[DatasetSpec('an example dataset', dtype, name='data', shape=(None, ))],
                         attributes=[AttributeSpec('attr1', 'an example attribute', dtype, shape=(None, ))])
        spec_catalog.register_spec(spec, 'test.yaml')
        self.namespace = SpecNamespace(
            'a test namespace', CORE_NAMESPACE, [{'source': 'test.yaml'}], version='0.1.0', catalog=spec_catalog)
        self.vmap = ValidatorMap(self.namespace)

    def test_scalar(self):
        """Test that validator does not allow a scalar where an array is specified."""
        self.set_up_spec('text')
        value = 'a string'
        bar_builder = GroupBuilder('my_bar',
                                   attributes={'data_type': 'Bar', 'attr1': value},
                                   datasets=[DatasetBuilder('data', value)])
        results = self.vmap.validate(bar_builder)
        result_strings = set([str(s) for s in results])
        expected_errors = {("Bar/attr1 (my_bar.attr1): incorrect shape - expected an array of shape '(None,)', "
                            "got non-array data 'a string'"),
                           ("Bar/data (my_bar/data): incorrect shape - expected an array of shape '(None,)', "
                            "got non-array data 'a string'")}
        self.assertEqual(result_strings, expected_errors)

    def test_empty_list(self):
        """Test that validator allows an empty list where an array is specified."""
        self.set_up_spec('text')
        value = []
        bar_builder = GroupBuilder('my_bar',
                                   attributes={'data_type': 'Bar', 'attr1': value},
                                   datasets=[DatasetBuilder('data', value)])
        results = self.vmap.validate(bar_builder)
        self.assertEqual(len(results), 0)

    def test_empty_nparray(self):
        """Test that validator allows an empty numpy array where an array is specified."""
        self.set_up_spec('text')
        value = np.array([])  # note: dtype is float64
        bar_builder = GroupBuilder('my_bar',
                                   attributes={'data_type': 'Bar', 'attr1': value},
                                   datasets=[DatasetBuilder('data', value)])
        results = self.vmap.validate(bar_builder)
        self.assertEqual(len(results), 0)

    # TODO test shape validation more completely


class TestLinkable(TestCase):

    def set_up_spec(self):
        spec_catalog = SpecCatalog()
        typed_dataset_spec = DatasetSpec('A typed dataset', data_type_def='Foo')
        typed_group_spec = GroupSpec('A typed group', data_type_def='Bar')
        spec = GroupSpec('A test group specification with a data type',
                         data_type_def='Baz',
                         datasets=[
                             DatasetSpec('A linkable child dataset', name='untyped_linkable_ds',
                                         linkable=True, quantity=ZERO_OR_ONE),
                             DatasetSpec('A non-linkable child dataset', name='untyped_nonlinkable_ds',
                                         linkable=False, quantity=ZERO_OR_ONE),
                             DatasetSpec('A linkable child dataset', data_type_inc='Foo',
                                         name='typed_linkable_ds', linkable=True, quantity=ZERO_OR_ONE),
                             DatasetSpec('A non-linkable child dataset', data_type_inc='Foo',
                                         name='typed_nonlinkable_ds', linkable=False, quantity=ZERO_OR_ONE),
                         ],
                         groups=[
                             GroupSpec('A linkable child group', name='untyped_linkable_group',
                                       linkable=True, quantity=ZERO_OR_ONE),
                             GroupSpec('A non-linkable child group', name='untyped_nonlinkable_group',
                                       linkable=False, quantity=ZERO_OR_ONE),
                             GroupSpec('A linkable child group', data_type_inc='Bar',
                                       name='typed_linkable_group', linkable=True, quantity=ZERO_OR_ONE),
                             GroupSpec('A non-linkable child group', data_type_inc='Bar',
                                       name='typed_nonlinkable_group', linkable=False, quantity=ZERO_OR_ONE),
                         ])
        spec_catalog.register_spec(spec, 'test.yaml')
        spec_catalog.register_spec(typed_dataset_spec, 'test.yaml')
        spec_catalog.register_spec(typed_group_spec, 'test.yaml')
        self.namespace = SpecNamespace(
            'a test namespace', CORE_NAMESPACE, [{'source': 'test.yaml'}], version='0.1.0', catalog=spec_catalog)
        self.vmap = ValidatorMap(self.namespace)

    def validate_linkability(self, link, expect_error):
        """Execute a linkability test and assert whether or not an IllegalLinkError is returned"""
        self.set_up_spec()
        builder = GroupBuilder('my_baz', attributes={'data_type': 'Baz'}, links=[link])
        result = self.vmap.validate(builder)
        if expect_error:
            self.assertEqual(len(result), 1)
            self.assertIsInstance(result[0], IllegalLinkError)
        else:
            self.assertEqual(len(result), 0)

    def test_untyped_linkable_dataset_accepts_link(self):
        """Test that the validator accepts a link when the spec has an untyped linkable dataset"""
        link = LinkBuilder(name='untyped_linkable_ds', builder=DatasetBuilder('foo'))
        self.validate_linkability(link, expect_error=False)

    def test_untyped_nonlinkable_dataset_does_not_accept_link(self):
        """Test that the validator returns an IllegalLinkError when the spec has an untyped non-linkable dataset"""
        link = LinkBuilder(name='untyped_nonlinkable_ds', builder=DatasetBuilder('foo'))
        self.validate_linkability(link, expect_error=True)

    def test_typed_linkable_dataset_accepts_link(self):
        """Test that the validator accepts a link when the spec has a typed linkable dataset"""
        link = LinkBuilder(name='typed_linkable_ds',
                           builder=DatasetBuilder('foo', attributes={'data_type': 'Foo'}))
        self.validate_linkability(link, expect_error=False)

    def test_typed_nonlinkable_dataset_does_not_accept_link(self):
        """Test that the validator returns an IllegalLinkError when the spec has a typed non-linkable dataset"""
        link = LinkBuilder(name='typed_nonlinkable_ds',
                           builder=DatasetBuilder('foo', attributes={'data_type': 'Foo'}))
        self.validate_linkability(link, expect_error=True)

    def test_untyped_linkable_group_accepts_link(self):
        """Test that the validator accepts a link when the spec has an untyped linkable group"""
        link = LinkBuilder(name='untyped_linkable_group', builder=GroupBuilder('foo'))
        self.validate_linkability(link, expect_error=False)

    def test_untyped_nonlinkable_group_does_not_accept_link(self):
        """Test that the validator returns an IllegalLinkError when the spec has an untyped non-linkable group"""
        link = LinkBuilder(name='untyped_nonlinkable_group', builder=GroupBuilder('foo'))
        self.validate_linkability(link, expect_error=True)

    def test_typed_linkable_group_accepts_link(self):
        """Test that the validator accepts a link when the spec has a typed linkable group"""
        link = LinkBuilder(name='typed_linkable_group',
                           builder=GroupBuilder('foo', attributes={'data_type': 'Bar'}))
        self.validate_linkability(link, expect_error=False)

    def test_typed_nonlinkable_group_does_not_accept_link(self):
        """Test that the validator returns an IllegalLinkError when the spec has a typed non-linkable group"""
        link = LinkBuilder(name='typed_nonlinkable_group',
                           builder=GroupBuilder('foo', attributes={'data_type': 'Bar'}))
        self.validate_linkability(link, expect_error=True)

    @mock.patch("hdmf.validate.validator.DatasetValidator.validate")
    def test_should_not_validate_illegally_linked_objects(self, mock_validator):
        """Test that an illegally linked child dataset is not validated

        Note: this behavior is expected to change in the future:
        https://github.com/hdmf-dev/hdmf/issues/516
        """
        self.set_up_spec()
        typed_link = LinkBuilder(name='typed_nonlinkable_ds',
                                 builder=DatasetBuilder('foo', attributes={'data_type': 'Foo'}))
        untyped_link = LinkBuilder(name='untyped_nonlinkable_ds', builder=DatasetBuilder('foo'))
        builder = GroupBuilder('my_baz', attributes={'data_type': 'Baz'}, links=[typed_link, untyped_link])
        _ = self.vmap.validate(builder)
        assert not mock_validator.called


class TestMultipleNamedChildrenOfSameType(TestCase):
    """When a group has multiple named children of the same type (such as X, Y,
    and Z VectorData), they all need to be validated.
    """

    def set_up_spec(self):
        spec_catalog = SpecCatalog()
        dataset_spec = DatasetSpec('A dataset', data_type_def='Foo')
        group_spec = GroupSpec('A group', data_type_def='Bar')
        spec = GroupSpec('A test group specification with a data type',
                         data_type_def='Baz',
                         datasets=[
                             DatasetSpec('Child Dataset A', name='a', data_type_inc='Foo'),
                             DatasetSpec('Child Dataset B', name='b', data_type_inc='Foo'),
                         ],
                         groups=[
                             GroupSpec('Child Group X', name='x', data_type_inc='Bar'),
                             GroupSpec('Child Group Y', name='y', data_type_inc='Bar'),
                         ])
        spec_catalog.register_spec(spec, 'test.yaml')
        spec_catalog.register_spec(dataset_spec, 'test.yaml')
        spec_catalog.register_spec(group_spec, 'test.yaml')
        self.namespace = SpecNamespace(
            'a test namespace', CORE_NAMESPACE, [{'source': 'test.yaml'}], version='0.1.0', catalog=spec_catalog)
        self.vmap = ValidatorMap(self.namespace)

    def validate_multiple_children(self, dataset_names, group_names):
        """Utility function to validate a builder with the specified named dataset and group children"""
        self.set_up_spec()
        datasets = [DatasetBuilder(ds, attributes={'data_type': 'Foo'}) for ds in dataset_names]
        groups = [GroupBuilder(gr, attributes={'data_type': 'Bar'}) for gr in group_names]
        builder = GroupBuilder('my_baz', attributes={'data_type': 'Baz'},
                               datasets=datasets, groups=groups)
        return self.vmap.validate(builder)

    def test_missing_first_dataset_should_return_error(self):
        """Test that the validator returns a MissingDataType error if the first dataset is missing"""
        result = self.validate_multiple_children(['b'], ['x', 'y'])
        self.assertEqual(len(result), 1)
        self.assertIsInstance(result[0], MissingDataType)

    def test_missing_last_dataset_should_return_error(self):
        """Test that the validator returns a MissingDataType error if the last dataset is missing"""
        result = self.validate_multiple_children(['a'], ['x', 'y'])
        self.assertEqual(len(result), 1)
        self.assertIsInstance(result[0], MissingDataType)

    def test_missing_first_group_should_return_error(self):
        """Test that the validator returns a MissingDataType error if the first group is missing"""
        result = self.validate_multiple_children(['a', 'b'], ['y'])
        self.assertEqual(len(result), 1)
        self.assertIsInstance(result[0], MissingDataType)

    def test_missing_last_group_should_return_error(self):
        """Test that the validator returns a MissingDataType error if the last group is missing"""
        result = self.validate_multiple_children(['a', 'b'], ['x'])
        self.assertEqual(len(result), 1)
        self.assertIsInstance(result[0], MissingDataType)

    def test_no_errors_when_all_children_satisfied(self):
        """Test that the validator does not return an error if all child specs are satisfied"""
        result = self.validate_multiple_children(['a', 'b'], ['x', 'y'])
        self.assertEqual(len(result), 0)


class TestLinkAndChildMatchingDataType(TestCase):
    """If a link and a child dataset/group have the same specified data type,
    both the link and the child need to be validated
    """

    def set_up_spec(self):
        spec_catalog = SpecCatalog()
        dataset_spec = DatasetSpec('A dataset', data_type_def='Foo')
        group_spec = GroupSpec('A group', data_type_def='Bar')
        spec = GroupSpec('A test group specification with a data type',
                         data_type_def='Baz',
                         datasets=[
                             DatasetSpec('Child Dataset', name='dataset', data_type_inc='Foo'),
                         ],
                         groups=[
                             GroupSpec('Child Group', name='group', data_type_inc='Bar'),
                         ],
                         links=[
                             LinkSpec('Linked Dataset', name='dataset_link', target_type='Foo'),
                             LinkSpec('Linked Dataset', name='group_link', target_type='Bar')
                         ])
        spec_catalog.register_spec(spec, 'test.yaml')
        spec_catalog.register_spec(dataset_spec, 'test.yaml')
        spec_catalog.register_spec(group_spec, 'test.yaml')
        self.namespace = SpecNamespace(
            'a test namespace', CORE_NAMESPACE, [{'source': 'test.yaml'}], version='0.1.0', catalog=spec_catalog)
        self.vmap = ValidatorMap(self.namespace)

    def validate_matching_link_data_type_case(self, datasets, groups, links):
        """Execute validation against a group builder using the provided group
        children and verify that a MissingDataType error is returned
        """
        self.set_up_spec()
        builder = GroupBuilder('my_baz', attributes={'data_type': 'Baz'},
                               datasets=datasets, groups=groups, links=links)
        result = self.vmap.validate(builder)
        self.assertEqual(len(result), 1)
        self.assertIsInstance(result[0], MissingDataType)

    def test_error_on_missing_child_dataset(self):
        """Test that a MissingDataType is returned when the child dataset is missing"""
        datasets = []
        groups = [GroupBuilder('group', attributes={'data_type': 'Bar'})]
        links = [
            LinkBuilder(name='dataset_link', builder=DatasetBuilder('foo', attributes={'data_type': 'Foo'})),
            LinkBuilder(name='group_link', builder=GroupBuilder('bar', attributes={'data_type': 'Bar'}))
        ]
        self.validate_matching_link_data_type_case(datasets, groups, links)

    def test_error_on_missing_linked_dataset(self):
        """Test that a MissingDataType is returned when the linked dataset is missing"""
        datasets = [DatasetBuilder('dataset', attributes={'data_type': 'Foo'})]
        groups = [GroupBuilder('group', attributes={'data_type': 'Bar'})]
        links = [
            LinkBuilder(name='group_link', builder=GroupBuilder('bar', attributes={'data_type': 'Bar'}))
        ]
        self.validate_matching_link_data_type_case(datasets, groups, links)

    def test_error_on_missing_group(self):
        """Test that a MissingDataType is returned when the child group is missing"""
        self.set_up_spec()
        datasets = [DatasetBuilder('dataset', attributes={'data_type': 'Foo'})]
        groups = []
        links = [
            LinkBuilder(name='dataset_link', builder=DatasetBuilder('foo', attributes={'data_type': 'Foo'})),
            LinkBuilder(name='group_link', builder=GroupBuilder('bar', attributes={'data_type': 'Bar'}))
        ]
        self.validate_matching_link_data_type_case(datasets, groups, links)

    def test_error_on_missing_linked_group(self):
        """Test that a MissingDataType is returned when the linked group is missing"""
        self.set_up_spec()
        datasets = [DatasetBuilder('dataset', attributes={'data_type': 'Foo'})]
        groups = [GroupBuilder('group', attributes={'data_type': 'Bar'})]
        links = [
            LinkBuilder(name='dataset_link', builder=DatasetBuilder('foo', attributes={'data_type': 'Foo'}))
        ]
        self.validate_matching_link_data_type_case(datasets, groups, links)


class TestMultipleChildrenAtDifferentLevelsOfInheritance(TestCase):
    """When multiple children can satisfy multiple specs due to data_type
    inheritance, the validation needs to carefully match builders against specs
    """

    def set_up_spec(self):
        spec_catalog = SpecCatalog()
        dataset_spec = DatasetSpec('A dataset', data_type_def='Foo')
        sub_dataset_spec = DatasetSpec('An Inheriting Dataset',
                                       data_type_def='Bar', data_type_inc='Foo')
        spec = GroupSpec('A test group specification with a data type',
                         data_type_def='Baz',
                         datasets=[
                             DatasetSpec('Child Dataset', data_type_inc='Foo'),
                             DatasetSpec('Child Dataset', data_type_inc='Bar'),
                         ])
        spec_catalog.register_spec(spec, 'test.yaml')
        spec_catalog.register_spec(dataset_spec, 'test.yaml')
        spec_catalog.register_spec(sub_dataset_spec, 'test.yaml')
        self.namespace = SpecNamespace(
            'a test namespace', CORE_NAMESPACE, [{'source': 'test.yaml'}], version='0.1.0', catalog=spec_catalog)
        self.vmap = ValidatorMap(self.namespace)

    def test_error_returned_when_child_at_highest_level_missing(self):
        """Test that a MissingDataType error is returned when the dataset at
        the highest level of the inheritance hierarchy is missing
        """
        self.set_up_spec()
        datasets = [
            DatasetBuilder('bar', attributes={'data_type': 'Bar'})
        ]
        builder = GroupBuilder('my_baz', attributes={'data_type': 'Baz'}, datasets=datasets)
        result = self.vmap.validate(builder)
        self.assertEqual(len(result), 1)
        self.assertIsInstance(result[0], MissingDataType)

    def test_error_returned_when_child_at_lowest_level_missing(self):
        """Test that a MissingDataType error is returned when the dataset at
        the lowest level of the inheritance hierarchy is missing
        """
        self.set_up_spec()
        datasets = [
            DatasetBuilder('foo', attributes={'data_type': 'Foo'})
        ]
        builder = GroupBuilder('my_baz', attributes={'data_type': 'Baz'}, datasets=datasets)
        result = self.vmap.validate(builder)
        self.assertEqual(len(result), 1)
        self.assertIsInstance(result[0], MissingDataType)

    def test_both_levels_of_hierarchy_validated(self):
        """Test that when both required children at separate levels of
        inheritance hierarchy are present, both child specs are satisfied
        """
        self.set_up_spec()
        datasets = [
            DatasetBuilder('foo', attributes={'data_type': 'Foo'}),
            DatasetBuilder('bar', attributes={'data_type': 'Bar'})
        ]
        builder = GroupBuilder('my_baz', attributes={'data_type': 'Baz'}, datasets=datasets)
        result = self.vmap.validate(builder)
        self.assertEqual(len(result), 0)

    @skip("Functionality not yet supported")
    def test_both_levels_of_hierarchy_validated_inverted_order(self):
        """Test that when both required children at separate levels of
        inheritance hierarchy are present, both child specs are satisfied.
        This should work no matter what the order of the builders.
        """
        self.set_up_spec()
        datasets = [
            DatasetBuilder('bar', attributes={'data_type': 'Bar'}),
            DatasetBuilder('foo', attributes={'data_type': 'Foo'})
        ]
        builder = GroupBuilder('my_baz', attributes={'data_type': 'Baz'}, datasets=datasets)
        result = self.vmap.validate(builder)
        self.assertEqual(len(result), 0)
