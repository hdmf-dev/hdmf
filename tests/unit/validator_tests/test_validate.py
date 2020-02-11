from abc import ABCMeta, abstractmethod
from datetime import datetime
from dateutil.tz import tzlocal
import numpy as np

from hdmf.spec import GroupSpec, AttributeSpec, DatasetSpec, SpecCatalog, SpecNamespace
from hdmf.build import GroupBuilder, DatasetBuilder
from hdmf.validate import ValidatorMap
from hdmf.validate.errors import *  # noqa: F403
from hdmf.testing import TestCase

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
        self.assertIsInstance(result[0], MissingError)  # noqa: F405
        self.assertEqual(result[0].name, 'Bar/attr1')
        self.assertIsInstance(result[1], MissingError)  # noqa: F405
        self.assertEqual(result[1].name, 'Bar/data')

    def test_invalid_incorrect_type_get_validator(self):
        builder = GroupBuilder('my_bar', attributes={'data_type': 'Bar', 'attr1': 10})
        validator = self.vmap.get_validator('Bar')
        result = validator.validate(builder)
        self.assertEqual(len(result), 2)
        self.assertIsInstance(result[0], DtypeError)  # noqa: F405
        self.assertEqual(result[0].name, 'Bar/attr1')
        self.assertIsInstance(result[1], MissingError)  # noqa: F405
        self.assertEqual(result[1].name, 'Bar/data')

    def test_invalid_incorrect_type_validate(self):
        builder = GroupBuilder('my_bar', attributes={'data_type': 'Bar', 'attr1': 10})
        result = self.vmap.validate(builder)
        self.assertEqual(len(result), 2)
        self.assertIsInstance(result[0], DtypeError)  # noqa: F405
        self.assertEqual(result[0].name, 'Bar/attr1')
        self.assertIsInstance(result[1], MissingError)  # noqa: F405
        self.assertEqual(result[1].name, 'Bar/data')

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
        self.assertIsInstance(result[0], DtypeError)  # noqa: F405
        self.assertEqual(result[0].name, 'Bar/time')

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
        self.assertIsInstance(result[0], ExpectedArrayError)  # noqa: F405
        self.assertEqual(result[0].name, 'Bar/time_array')


class TestNestedTypes(ValidatorTestBase):

    def getSpecs(self):
        bar = GroupSpec('A test group specification with a data type',
                        data_type_def='Bar',
                        datasets=[DatasetSpec('an example dataset', 'int', name='data',
                                              attributes=[AttributeSpec('attr2', 'an example integer attribute',
                                                                        'int')])],
                        attributes=[AttributeSpec('attr1', 'an example string attribute', 'text')])
        foo = GroupSpec('A test group that contains a data type',
                        data_type_def='Foo',
                        groups=[GroupSpec('A Bar group for Foos', name='my_bar', data_type_inc='Bar')],
                        attributes=[AttributeSpec('foo_attr', 'a string attribute specified as text', 'text',
                                                  required=False)])

        return (bar, foo)

    def test_invalid_missing_req_group(self):
        foo_builder = GroupBuilder('my_foo', attributes={'data_type': 'Foo',
                                                         'foo_attr': 'example Foo object'})
        results = self.vmap.validate(foo_builder)
        self.assertIsInstance(results[0], MissingDataType)  # noqa: F405
        self.assertEqual(results[0].name, 'Foo')
        self.assertEqual(results[0].reason, 'missing data type Bar')

    def test_invalid_wrong_name_req_type(self):
        bar_builder = GroupBuilder('bad_bar_name',
                                   attributes={'data_type': 'Bar', 'attr1': 'a string attribute'},
                                   datasets=[DatasetBuilder('data', 100, attributes={'attr2': 10})])

        foo_builder = GroupBuilder('my_foo',
                                   attributes={'data_type': 'Foo', 'foo_attr': 'example Foo object'},
                                   groups=[bar_builder])

        results = self.vmap.validate(foo_builder)
        self.assertEqual(len(results), 1)
        self.assertIsInstance(results[0], MissingDataType)  # noqa: F405
        self.assertEqual(results[0].data_type, 'Bar')

    def test_valid(self):
        bar_builder = GroupBuilder('my_bar',
                                   attributes={'data_type': 'Bar', 'attr1': 'a string attribute'},
                                   datasets=[DatasetBuilder('data', 100, attributes={'attr2': 10})])

        foo_builder = GroupBuilder('my_foo',
                                   attributes={'data_type': 'Foo', 'foo_attr': 'example Foo object'},
                                   groups=[bar_builder])

        results = self.vmap.validate(foo_builder)
        self.assertEqual(len(results), 0)

    def test_valid_wo_opt_attr(self):
        bar_builder = GroupBuilder('my_bar',
                                   attributes={'data_type': 'Bar', 'attr1': 'a string attribute'},
                                   datasets=[DatasetBuilder('data', 100, attributes={'attr2': 10})])
        foo_builder = GroupBuilder('my_foo',
                                   attributes={'data_type': 'Foo'},
                                   groups=[bar_builder])

        results = self.vmap.validate(foo_builder)
        self.assertEqual(len(results), 0)


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
