from abc import ABCMeta, abstractmethod

import numpy as np
from hdmf import Container, Data, TermSet, TermSetWrapper
from hdmf.common import VectorData, get_type_map
from hdmf.build import ObjectMapper, BuildManager, GroupBuilder, DatasetBuilder, ReferenceBuilder
from hdmf.build.warnings import DtypeConversionWarning, IncorrectDatasetShapeBuildWarning
from hdmf.spec import GroupSpec, AttributeSpec, DatasetSpec, RefSpec, Spec
from hdmf.testing import TestCase
from hdmf.utils import docval, getargs

from tests.unit.helpers.utils import CORE_NAMESPACE, create_test_type_map

try:
    import linkml_runtime  # noqa: F401
    LINKML_INSTALLED = True
except ImportError:
    LINKML_INSTALLED = False


class TestUnwrapTermSetWrapperBuild(TestCase):
    """
    Test the unwrapping of TermSetWrapper on regular datasets within build.
    """
    def setUp(self):
        if not LINKML_INSTALLED:
            self.skipTest("optional LinkML module is not installed")

    def test_unwrap(self):
        manager = BuildManager(get_type_map())
        terms = TermSet(term_schema_path='tests/unit/example_test_term_set.yaml')
        build = manager.build(VectorData(name='test_data',
                                         description='description',
                                         data=TermSetWrapper(value=['Homo sapiens'], termset= terms)))

        self.assertEqual(build.data, ['Homo sapiens'])


class TestBuildNoDtypeSpec(TestCase):
    def test_structured_array_without_dtype_raises(self):
        # Create a structured (compound) array
        compound_data = np.array(
            [(1.0, True), (2.0, False)],
            dtype=[('volume', 'f4'), ('autorewarded', 'bool')]
        )

        # Set up build manager with core type map
        manager = BuildManager(get_type_map())

        # Expect ValueError due to compound dtype with no declared dtype in the spec
        with self.assertRaises(Exception):
            manager.build(VectorData(
                name='test_data',
                description='description',
                data=compound_data
            ))
# TODO: test build of extended group/dataset that modifies an attribute dtype (commented out below), shape, value, etc.
# by restriction. also check that attributes cannot be deleted or scope expanded.
# TODO: test build of extended dataset that modifies shape by restriction.

class Bar(Container):

    @docval({'name': 'name', 'type': str, 'doc': 'the name of this Bar'},
            {'name': 'attr1', 'type': str, 'doc': 'a string attribute'},
            {'name': 'attr2', 'type': 'int', 'doc': 'an int attribute', 'default': None},
            {'name': 'ext_attr', 'type': bool, 'doc': 'a boolean attribute', 'default': True})
    def __init__(self, **kwargs):
        name, attr1, attr2, ext_attr = getargs('name', 'attr1', 'attr2', 'ext_attr', kwargs)
        super().__init__(name=name)
        self.__attr1 = attr1
        self.__attr2 = attr2
        self.__ext_attr = kwargs['ext_attr']

    @property
    def data_type(self):
        return 'Bar'

    @property
    def attr1(self):
        return self.__attr1

    @property
    def attr2(self):
        return self.__attr2

    @property
    def ext_attr(self):
        return self.__ext_attr


class BarHolder(Container):

    @docval({'name': 'name', 'type': str, 'doc': 'the name of this BarHolder'},
            {'name': 'bars', 'type': ('data', 'array_data'), 'doc': 'bars', 'default': list()})
    def __init__(self, **kwargs):
        name, bars = getargs('name', 'bars', kwargs)
        super().__init__(name=name)
        self.__bars = bars
        for b in bars:
            if b is not None and b.parent is None:
                b.parent = self

    @property
    def data_type(self):
        return 'BarHolder'

    @property
    def bars(self):
        return self.__bars


class ExtBarMapper(ObjectMapper):

    @docval({"name": "spec", "type": Spec, "doc": "the spec to get the attribute value for"},
            {"name": "container", "type": Bar, "doc": "the container to get the attribute value from"},
            {"name": "manager", "type": BuildManager, "doc": "the BuildManager used for managing this build"},
            returns='the value of the attribute')
    def get_attr_value(self, **kwargs):
        ''' Get the value of the attribute corresponding to this spec from the given container '''
        spec, container, manager = getargs('spec', 'container', 'manager', kwargs)
        # handle custom mapping of field 'ext_attr' within container BarHolder/Bar -> spec BarHolder/Bar.ext_attr
        if isinstance(container.parent, BarHolder):
            if spec.name == 'ext_attr':
                return container.ext_attr
        return super().get_attr_value(**kwargs)

    @ObjectMapper.constructor_arg('ext_attr')
    def ext_attr_carg(self, builder, manager):
        # ext_attr lives on the inc-site spec, so it is not in BarMapper's def-site spec. Read it
        # straight from the builder's attributes when constructing a Bar inside an extended context.
        return builder.attributes.get('ext_attr')


class BuildGroupExtAttrsMixin(TestCase, metaclass=ABCMeta):

    def setUp(self):
        self.setUpBarSpec()
        self.setUpBarHolderSpec()
        type_map = create_test_type_map(
            specs=[self.bar_spec, self.bar_holder_spec],
            container_classes={'Bar': Bar, 'BarHolder': BarHolder},
            mappers={'Bar': ExtBarMapper},
        )
        self.manager = BuildManager(type_map)

    def setUpBarSpec(self):
        attr1_attr = AttributeSpec(
            name='attr1',
            dtype='text',
            doc='an example string attribute',
        )
        attr2_attr = AttributeSpec(
            name='attr2',
            dtype='int',
            doc='an example int attribute',
        )
        self.bar_spec = GroupSpec(
            doc='A test group specification with a data type',
            data_type_def='Bar',
            attributes=[attr1_attr, attr2_attr],
        )

    @abstractmethod
    def setUpBarHolderSpec(self):
        pass


class TestBuildGroupAddedAttr(BuildGroupExtAttrsMixin, TestCase):
    """
    If the spec defines a group data_type A (Bar) using 'data_type_def' and defines another data_type B (BarHolder)
    that includes A using 'data_type_inc', then the included A spec is an extended (or refined) spec of A - call it A'.
    The spec of A' can refine or add attributes to the spec of A. This test ensures that *added attributes* in A' are
    handled properly.
    """

    def setUpBarHolderSpec(self):
        ext_attr = AttributeSpec(
            name='ext_attr',
            dtype='bool',
            doc='A boolean attribute',
        )
        bar_ext_no_name_spec = GroupSpec(
            doc='A Bar extended with attribute ext_attr',
            data_type_inc='Bar',
            quantity='*',
            attributes=[ext_attr],
        )
        self.bar_holder_spec = GroupSpec(
            doc='A container of multiple extended Bar objects',
            data_type_def='BarHolder',
            groups=[bar_ext_no_name_spec],
        )

    def test_build_added_attr(self):
        """
        Test build of BarHolder which can contain multiple extended Bar objects, which have a new attribute.
        """
        ext_bar_inst = Bar(
            name='my_bar',
            attr1='a string',
            attr2=10,
            ext_attr=False,
        )
        bar_holder_inst = BarHolder(
            name='my_bar_holder',
            bars=[ext_bar_inst],
        )

        expected_inner = GroupBuilder(
            name='my_bar',
            attributes={
                'attr1': 'a string',
                'attr2': 10,
                'data_type': 'Bar',
                'ext_attr': False,
                'namespace': CORE_NAMESPACE,
                'object_id': ext_bar_inst.object_id,
            },
        )
        expected = GroupBuilder(
            name='my_bar_holder',
            groups={'my_bar': expected_inner},
            attributes={
                'data_type': 'BarHolder',
                'namespace': CORE_NAMESPACE,
                'object_id': bar_holder_inst.object_id,
            },
        )

        # the object mapper automatically maps the spec of extended Bars to the 'BarMapper.bars' field
        builder = self.manager.build(bar_holder_inst, source='test.h5')
        self.assertDictEqual(builder, expected)

    def test_construct_added_attr(self):
        """
        Test construct of BarHolder containing an extended Bar with the inc-site ext_attr set.
        """
        ext_bar_inst = Bar(
            name='my_bar',
            attr1='a string',
            attr2=10,
            ext_attr=False,
        )
        bar_holder_inst = BarHolder(
            name='my_bar_holder',
            bars=[ext_bar_inst],
        )

        # build then construct to round-trip through builders
        builder = self.manager.build(bar_holder_inst, source='test.h5')
        self.manager.clear_cache()
        constructed = self.manager.construct(builder)

        self.assertEqual(constructed.name, 'my_bar_holder')
        self.assertEqual(len(constructed.bars), 1)
        constructed_bar = constructed.bars[0]
        self.assertEqual(constructed_bar.name, 'my_bar')
        self.assertEqual(constructed_bar.attr1, 'a string')
        self.assertEqual(constructed_bar.attr2, 10)
        self.assertEqual(constructed_bar.ext_attr, False)


class TestBuildGroupRefinedAttr(BuildGroupExtAttrsMixin, TestCase):
    """
    If the spec defines a group data_type A (Bar) using 'data_type_def' and defines another data_type B (BarHolder)
    that includes A using 'data_type_inc', then the included A spec is an extended (or refined) spec of A - call it A'.
    The spec of A' can refine or add attributes to the spec of A. This test ensures that *refine attributes* in A' are
    handled properly.
    """

    def setUpBarHolderSpec(self):
        int_attr2 = AttributeSpec(
            name='attr2',
            dtype='int64',
            doc='Refine Bar spec from int to int64',
        )
        bar_ext_no_name_spec = GroupSpec(
            doc='A Bar extended with modified attribute attr2',
            data_type_inc='Bar',
            quantity='*',
            attributes=[int_attr2],
        )
        self.bar_holder_spec = GroupSpec(
            doc='A container of multiple extended Bar objects',
            data_type_def='BarHolder',
            groups=[bar_ext_no_name_spec],
        )

    def test_build_refined_attr(self):
        """
        Test build of BarHolder which can contain multiple extended Bar objects, which have a modified attr2.
        """
        ext_bar_inst = Bar(
            name='my_bar',
            attr1='a string',
            attr2=np.int64(10),
        )
        bar_holder_inst = BarHolder(
            name='my_bar_holder',
            bars=[ext_bar_inst],
        )

        expected_inner = GroupBuilder(
            name='my_bar',
            attributes={
                'attr1': 'a string',
                'attr2': np.int64(10),
                'data_type': 'Bar',
                'namespace': CORE_NAMESPACE,
                'object_id': ext_bar_inst.object_id,
            }
        )
        expected = GroupBuilder(
            name='my_bar_holder',
            groups={'my_bar': expected_inner},
            attributes={
                'data_type': 'BarHolder',
                'namespace': CORE_NAMESPACE,
                'object_id': bar_holder_inst.object_id,
            },
        )

        # the object mapper automatically maps the spec of extended Bars to the 'BarMapper.bars' field
        builder = self.manager.build(bar_holder_inst, source='test.h5')
        self.assertDictEqual(builder, expected)

    def test_build_refined_attr_wrong_type(self):
        """
        Test build of BarHolder which contains a Bar that has the wrong dtype for an attr.
        """
        ext_bar_inst = Bar(
            name='my_bar',
            attr1='a string',
            attr2=10,  # spec specifies attr2 should be an int64 for Bars within BarHolder
        )
        bar_holder_inst = BarHolder(
            name='my_bar_holder',
            bars=[ext_bar_inst],
        )

        expected_inner = GroupBuilder(
            name='my_bar',
            attributes={
                'attr1': 'a string',
                'attr2': np.int64(10),
                'data_type': 'Bar',
                'namespace': CORE_NAMESPACE,
                'object_id': ext_bar_inst.object_id,
            }
        )
        expected = GroupBuilder(
            name='my_bar_holder',
            groups={'my_bar': expected_inner},
            attributes={
                'data_type': 'BarHolder',
                'namespace': CORE_NAMESPACE,
                'object_id': bar_holder_inst.object_id,
            },
        )

        # TODO build should raise a conversion warning for converting 10 (int32) to np.int64
        builder = self.manager.build(bar_holder_inst, source='test.h5')
        self.assertDictEqual(builder, expected)


class BarData(Data):

    @docval({'name': 'name', 'type': str, 'doc': 'the name of this BarData'},
            {'name': 'data', 'type': ('data', 'array_data'), 'doc': 'the data'},
            {'name': 'attr1', 'type': str, 'doc': 'a string attribute'},
            {'name': 'attr2', 'type': 'int', 'doc': 'an int attribute', 'default': None},
            {'name': 'ext_attr', 'type': bool, 'doc': 'a boolean attribute', 'default': True})
    def __init__(self, **kwargs):
        name, data, attr1, attr2, ext_attr = getargs('name', 'data', 'attr1', 'attr2', 'ext_attr', kwargs)
        super().__init__(name=name, data=data)
        self.__attr1 = attr1
        self.__attr2 = attr2
        self.__ext_attr = ext_attr

    @property
    def data_type(self):
        return 'BarData'

    @property
    def attr1(self):
        return self.__attr1

    @property
    def attr2(self):
        return self.__attr2

    @property
    def ext_attr(self):
        return self.__ext_attr


class BarDataHolder(Container):

    @docval({'name': 'name', 'type': str, 'doc': 'the name of this BarDataHolder'},
            {'name': 'bar_datas', 'type': ('data', 'array_data'), 'doc': 'bar_datas', 'default': list()})
    def __init__(self, **kwargs):
        name, bar_datas = getargs('name', 'bar_datas', kwargs)
        super().__init__(name=name)
        self.__bar_datas = bar_datas
        for b in bar_datas:
            if b is not None and b.parent is None:
                b.parent = self

    @property
    def data_type(self):
        return 'BarDataHolder'

    @property
    def bar_datas(self):
        return self.__bar_datas


class ExtBarDataMapper(ObjectMapper):

    @docval({"name": "spec", "type": Spec, "doc": "the spec to get the attribute value for"},
            {"name": "container", "type": BarData, "doc": "the container to get the attribute value from"},
            {"name": "manager", "type": BuildManager, "doc": "the BuildManager used for managing this build"},
            returns='the value of the attribute')
    def get_attr_value(self, **kwargs):
        ''' Get the value of the attribute corresponding to this spec from the given container '''
        spec, container, manager = getargs('spec', 'container', 'manager', kwargs)
        # handle custom mapping of field 'ext_attr' within container
        # BardataHolder/BarData -> spec BarDataHolder/BarData.ext_attr
        if isinstance(container.parent, BarDataHolder):
            if spec.name == 'ext_attr':
                return container.ext_attr
        return super().get_attr_value(**kwargs)

    @ObjectMapper.constructor_arg('ext_attr')
    def ext_attr_carg(self, builder, manager):
        # ext_attr lives on the inc-site spec, so it is not in BarDataMapper's def-site spec. Read it
        # straight from the builder's attributes when constructing a BarData inside an extended context.
        return builder.attributes.get('ext_attr')


class BuildDatasetExtAttrsMixin(TestCase, metaclass=ABCMeta):

    def setUp(self):
        self.set_up_specs()
        type_map = create_test_type_map(
            specs=[self.bar_data_spec, self.bar_data_holder_spec],
            container_classes={'BarData': BarData, 'BarDataHolder': BarDataHolder},
            mappers={'BarData': ExtBarDataMapper},
        )
        self.manager = BuildManager(type_map)

    def set_up_specs(self):
        attr1_attr = AttributeSpec(
            name='attr1',
            dtype='text',
            doc='an example string attribute',
        )
        attr2_attr = AttributeSpec(
            name='attr2',
            dtype='int',
            doc='an example int attribute',
        )
        self.bar_data_spec = DatasetSpec(
            doc='A test dataset specification with a data type',
            data_type_def='BarData',
            dtype='int',
            shape=[[None], [None, None]],
            attributes=[attr1_attr, attr2_attr],
        )
        self.bar_data_holder_spec = GroupSpec(
            doc='A container of multiple extended BarData objects',
            data_type_def='BarDataHolder',
            datasets=[self.get_refined_bar_data_spec()],
        )

    @abstractmethod
    def get_refined_bar_data_spec(self):
        pass


class TestBuildDatasetAddedAttrs(BuildDatasetExtAttrsMixin, TestCase):
    """
    If the spec defines a dataset data_type A (BarData) using 'data_type_def' and defines another data_type B
    (BarHolder) that includes A using 'data_type_inc', then the included A spec is an extended (or refined) spec of A -
    call it A'. The spec of A' can refine or add attributes, refine the dtype, refine the shape, or set a fixed value
    to the spec of A. This test ensures that *added attributes* in A' are handled properly. This is similar to how the
    spec for a subtype of DynamicTable can contain a VectorData that has a new attribute.
    """

    def get_refined_bar_data_spec(self):
        ext_attr = AttributeSpec(
            name='ext_attr',
            dtype='bool',
            doc='A boolean attribute',
        )
        refined_spec = DatasetSpec(
            doc='A BarData extended with attribute ext_attr',
            data_type_inc='BarData',
            quantity='*',
            attributes=[ext_attr],
        )
        return refined_spec

    def test_build_added_attr(self):
        """
        Test build of BarHolder which can contain multiple extended BarData objects, which have a new attribute.
        """
        ext_bar_data_inst = BarData(
            name='my_bar',
            data=list(range(10)),
            attr1='a string',
            attr2=10,
            ext_attr=False,
        )
        bar_data_holder_inst = BarDataHolder(
            name='my_bar_holder',
            bar_datas=[ext_bar_data_inst],
        )

        expected_inner = DatasetBuilder(
            name='my_bar',
            data=list(range(10)),
            attributes={
                'attr1': 'a string',
                'attr2': 10,
                'data_type': 'BarData',
                'ext_attr': False,
                'namespace': CORE_NAMESPACE,
                'object_id': ext_bar_data_inst.object_id,
            },
        )
        expected = GroupBuilder(
            name='my_bar_holder',
            datasets={'my_bar': expected_inner},
            attributes={
                'data_type': 'BarDataHolder',
                'namespace': CORE_NAMESPACE,
                'object_id': bar_data_holder_inst.object_id,
            },
        )

        # the object mapper automatically maps the spec of extended Bars to the 'BarMapper.bars' field
        builder = self.manager.build(bar_data_holder_inst, source='test.h5')
        self.assertDictEqual(builder, expected)

    def test_construct_added_attr(self):
        """
        Test construct of BarDataHolder containing an extended BarData with the inc-site ext_attr set.
        """
        ext_bar_data_inst = BarData(
            name='my_bar',
            data=list(range(10)),
            attr1='a string',
            attr2=10,
            ext_attr=False,
        )
        bar_data_holder_inst = BarDataHolder(
            name='my_bar_holder',
            bar_datas=[ext_bar_data_inst],
        )

        # build then construct to round-trip through builders
        builder = self.manager.build(bar_data_holder_inst, source='test.h5')
        self.manager.clear_cache()
        constructed = self.manager.construct(builder)

        self.assertEqual(constructed.name, 'my_bar_holder')
        self.assertEqual(len(constructed.bar_datas), 1)
        constructed_bar = constructed.bar_datas[0]
        self.assertEqual(constructed_bar.name, 'my_bar')
        self.assertEqual(list(constructed_bar.data), list(range(10)))
        self.assertEqual(constructed_bar.attr1, 'a string')
        self.assertEqual(constructed_bar.attr2, 10)
        self.assertEqual(constructed_bar.ext_attr, False)


class TestBuildDatasetRefinedDtype(BuildDatasetExtAttrsMixin, TestCase):
    """
    If the spec defines a dataset data_type A (BarData) using 'data_type_def' and defines another data_type B
    (BarHolder) that includes A using 'data_type_inc', then the included A spec is an extended (or refined) spec of A -
    call it A'. The spec of A' can refine or add attributes, refine the dtype, refine the shape, or set a fixed value
    to the spec of A. This test ensures that if A' refines the dtype of A, the build process uses the correct dtype for
    conversion.
    """

    def get_refined_bar_data_spec(self):
        refined_spec = DatasetSpec(
            doc='A BarData with refined int64 dtype',
            data_type_inc='BarData',
            dtype='int64',
            quantity='*',
        )
        return refined_spec

    def test_build_refined_dtype_convert(self):
        """
        Test build of BarDataHolder which contains a BarData with data that needs to be converted to the refined dtype.
        """
        ext_bar_data_inst = BarData(
            name='my_bar',
            data=np.array([1, 2], dtype=np.int32),  # the refined spec says data should be int64s
            attr1='a string',
            attr2=10,
        )
        bar_data_holder_inst = BarDataHolder(
            name='my_bar_holder',
            bar_datas=[ext_bar_data_inst],
        )

        expected_inner = DatasetBuilder(
            name='my_bar',
            data=np.array([1, 2], dtype=np.int64),  # the objectmapper should convert the given data to int64s
            attributes={
                'attr1': 'a string',
                'attr2': 10,
                'data_type': 'BarData',
                'namespace': CORE_NAMESPACE,
                'object_id': ext_bar_data_inst.object_id,
            },
        )
        expected = GroupBuilder(
            name='my_bar_holder',
            datasets={'my_bar': expected_inner},
            attributes={
                'data_type': 'BarDataHolder',
                'namespace': CORE_NAMESPACE,
                'object_id': bar_data_holder_inst.object_id,
            },
        )

        # the object mapper automatically maps the spec of extended Bars to the 'BarMapper.bars' field
        msg = ("Spec 'BarDataHolder/BarData': Value with data type int32 is being converted to data type int64 "
               "as specified.")
        with self.assertWarnsWith(DtypeConversionWarning, msg):
            builder = self.manager.build(bar_data_holder_inst, source='test.h5')
        np.testing.assert_array_equal(builder.datasets['my_bar'].data, expected.datasets['my_bar'].data)
        self.assertEqual(builder.datasets['my_bar'].data.dtype, np.int64)


class TestBuildDatasetNotRefinedDtype(BuildDatasetExtAttrsMixin, TestCase):
    """
    If the spec defines a dataset data_type A (BarData) using 'data_type_def' and defines another data_type B
    (BarHolder) that includes A using 'data_type_inc', then the included A spec is an extended (or refined) spec of A -
    call it A'. The spec of A' can refine or add attributes, refine the dtype, refine the shape, or set a fixed value
    to the spec of A. This test ensures that if A' does not refine the dtype of A, the build process uses the correct
    dtype for conversion.
    """

    def get_refined_bar_data_spec(self):
        refined_spec = DatasetSpec(
            doc='A BarData',
            data_type_inc='BarData',
            quantity='*',
        )
        return refined_spec

    def test_build_correct_dtype(self):
        """
        Test build of BarDataHolder which contains a BarData.
        """
        ext_bar_data_inst = BarData(
            name='my_bar',
            data=[1, 2],
            attr1='a string',
            attr2=10,
        )
        bar_data_holder_inst = BarDataHolder(
            name='my_bar_holder',
            bar_datas=[ext_bar_data_inst],
        )

        expected_inner = DatasetBuilder(
            name='my_bar',
            data=[1, 2],
            attributes={
                'attr1': 'a string',
                'attr2': 10,
                'data_type': 'BarData',
                'namespace': CORE_NAMESPACE,
                'object_id': ext_bar_data_inst.object_id,
            },
        )
        expected = GroupBuilder(
            name='my_bar_holder',
            datasets={'my_bar': expected_inner},
            attributes={
                'data_type': 'BarDataHolder',
                'namespace': CORE_NAMESPACE,
                'object_id': bar_data_holder_inst.object_id,
            },
        )

        # the object mapper automatically maps the spec of extended Bars to the 'BarMapper.bars' field
        builder = self.manager.build(bar_data_holder_inst, source='test.h5')
        self.assertDictEqual(builder, expected)

    def test_build_incorrect_dtype(self):
        """
        Test build of BarDataHolder which contains a BarData
        """
        ext_bar_data_inst = BarData(
            name='my_bar',
            data=['a', 'b'],
            attr1='a string',
            attr2=10,
        )
        bar_data_holder_inst = BarDataHolder(
            name='my_bar_holder',
            bar_datas=[ext_bar_data_inst],
        )

        # the object mapper automatically maps the spec of extended Bars to the 'BarMapper.bars' field
        msg = "could not resolve dtype for BarData 'my_bar': invalid literal for int() with base 10: 'a'"
        with self.assertRaisesWith(Exception, msg):
            self.manager.build(bar_data_holder_inst, source='test.h5')


class BarRefDataset(Data):
    """A Data subclass whose data is a list of BarData references."""

    @docval({'name': 'name', 'type': str, 'doc': 'the name of this BarRefDataset'},
            {'name': 'data', 'type': ('data', 'array_data'), 'doc': 'a list of BarData references'})
    def __init__(self, **kwargs):
        super().__init__(**kwargs)


class BarDataRefHolder(Container):
    """A holder with both a direct extended BarData and a typed reference dataset to BarData."""

    @docval({'name': 'name', 'type': str, 'doc': 'the name of this BarDataRefHolder'},
            {'name': 'bar_data', 'type': BarData, 'doc': 'a BarData', 'default': None},
            {'name': 'bar_data_ref', 'type': BarRefDataset,
             'doc': 'a typed reference dataset pointing to BarData', 'default': None})
    def __init__(self, **kwargs):
        name, bar_data, bar_data_ref = getargs('name', 'bar_data', 'bar_data_ref', kwargs)
        super().__init__(name=name)
        self.__bar_data = bar_data
        self.__bar_data_ref = bar_data_ref
        if bar_data is not None and bar_data.parent is None:
            bar_data.parent = self
        if bar_data_ref is not None and bar_data_ref.parent is None:
            bar_data_ref.parent = self

    @property
    def data_type(self):
        return 'BarDataRefHolder'

    @property
    def bar_data(self):
        return self.__bar_data

    @property
    def bar_data_ref(self):
        return self.__bar_data_ref


class ExtBarDataRefHolderMapper(ObjectMapper):
    """Mapper for BarData in BarDataRefHolder context. Pulls inc-site ext_attr from the container."""

    @docval({"name": "spec", "type": Spec, "doc": "the spec to get the attribute value for"},
            {"name": "container", "type": BarData, "doc": "the container to get the attribute value from"},
            {"name": "manager", "type": BuildManager, "doc": "the BuildManager used for managing this build"},
            returns='the value of the attribute')
    def get_attr_value(self, **kwargs):
        spec, container, manager = getargs('spec', 'container', 'manager', kwargs)
        if isinstance(container.parent, BarDataRefHolder) and spec.name == 'ext_attr':
            return container.ext_attr
        return super().get_attr_value(**kwargs)

    @ObjectMapper.constructor_arg('ext_attr')
    def ext_attr_carg(self, builder, manager):
        return builder.attributes.get('ext_attr')


class TestBuildDatasetAddedAttrsWithRef(TestCase):
    """
    Inc-site attribute extension must hold up when the same dataset container is reachable through a
    separate reference field. Mirrors PR #283's TestExtendDatasetAttrsWithRef: the holder's spec has both
    a direct named BarData (with inc-site ext_attr) and a sibling typed reference dataset that lists the
    same BarData. Build must apply ext_attr to the BarData builder, and the reference dataset must point
    at that same memoized builder.
    """

    def setUp(self):
        attr1_attr = AttributeSpec(name='attr1', dtype='text', doc='an example string attribute')
        attr2_attr = AttributeSpec(name='attr2', dtype='int', doc='an example int attribute')
        self.bar_data_spec = DatasetSpec(
            doc='A test dataset specification with a data type',
            data_type_def='BarData',
            dtype='int',
            shape=[None],
            attributes=[attr1_attr, attr2_attr],
        )
        self.bar_ref_dataset_spec = DatasetSpec(
            doc='A typed dataset of references to BarData',
            data_type_def='BarRefDataset',
            dtype=RefSpec(target_type='BarData', reftype='object'),
            shape=[None],
        )
        ext_attr_spec = AttributeSpec(name='ext_attr', dtype='bool', doc='a boolean attribute')
        ext_bar_data_spec = DatasetSpec(
            doc='An extended BarData with inc-site ext_attr',
            data_type_inc='BarData',
            quantity='?',
            name='bar_data',
            attributes=[ext_attr_spec],
        )
        # Reference dataset is listed before the direct dataset so the BarData container is reached first
        # via the reference path; the queued reference resolution then needs the BarData builder to have
        # ext_attr applied even though it was first encountered through the reference field.
        ref_inc_spec = DatasetSpec(
            doc='An included BarRefDataset',
            data_type_inc='BarRefDataset',
            quantity='?',
            name='bar_data_ref',
        )
        self.bar_data_ref_holder_spec = GroupSpec(
            doc='A holder with a direct extended BarData and a reference dataset',
            data_type_def='BarDataRefHolder',
            datasets=[ref_inc_spec, ext_bar_data_spec],
        )

        type_map = create_test_type_map(
            specs=[self.bar_data_spec, self.bar_ref_dataset_spec, self.bar_data_ref_holder_spec],
            container_classes={
                'BarData': BarData,
                'BarRefDataset': BarRefDataset,
                'BarDataRefHolder': BarDataRefHolder,
            },
            mappers={'BarData': ExtBarDataRefHolderMapper},
        )
        self.manager = BuildManager(type_map)

    def test_build_added_attr_with_ref(self):
        bar_data_inst = BarData(
            name='bar_data',
            data=[1, 2, 3],
            attr1='a string',
            attr2=10,
            ext_attr=False,
        )
        bar_ref_inst = BarRefDataset(
            name='bar_data_ref',
            data=[bar_data_inst],
        )
        holder_inst = BarDataRefHolder(
            name='my_holder',
            bar_data=bar_data_inst,
            bar_data_ref=bar_ref_inst,
        )

        builder = self.manager.build(holder_inst, source='test.h5', root=True)

        # The direct bar_data builder must carry the inc-site ext_attr
        self.assertIn('bar_data', builder.datasets)
        bar_data_builder = builder.datasets['bar_data']
        self.assertEqual(bar_data_builder.attributes['ext_attr'], False)
        self.assertEqual(bar_data_builder.attributes['attr1'], 'a string')
        self.assertEqual(bar_data_builder.attributes['attr2'], 10)

        # The reference dataset must point to the same memoized bar_data builder
        self.assertIn('bar_data_ref', builder.datasets)
        ref_builder = builder.datasets['bar_data_ref']
        self.assertEqual(len(ref_builder.data), 1)
        self.assertIsInstance(ref_builder.data[0], ReferenceBuilder)
        self.assertIs(ref_builder.data[0].builder, bar_data_builder)


class BuildDatasetShapeMixin(TestCase, metaclass=ABCMeta):

    def setUp(self):
        self.set_up_specs()
        type_map = create_test_type_map(
            specs=[self.bar_data_spec, self.bar_data_holder_spec],
            container_classes={'BarData': BarData, 'BarDataHolder': BarDataHolder},
            mappers={'BarData': ExtBarDataMapper},
        )
        self.manager = BuildManager(type_map)

    def set_up_specs(self):
        shape, dims = self.get_base_shape_dims()
        self.bar_data_spec = DatasetSpec(
            doc='A test dataset specification with a data type',
            data_type_def='BarData',
            dtype='int',
            shape=shape,
            dims=dims,
        )
        self.bar_data_holder_spec = GroupSpec(
            doc='A container of multiple extended BarData objects',
            data_type_def='BarDataHolder',
            datasets=[self.get_dataset_inc_spec()],
        )

    @abstractmethod
    def get_base_shape_dims(self):
        pass

    @abstractmethod
    def get_dataset_inc_spec(self):
        pass


class TestBuildDatasetOneOptionBadShapeUnspecified1(BuildDatasetShapeMixin):
    """Test dataset spec shape = 2D any length, data = 1D. Should raise warning and set dimension_labels to None."""

    def get_base_shape_dims(self):
        return [None, None], ['a', 'b']

    def get_dataset_inc_spec(self):
        dataset_inc_spec = DatasetSpec(
            doc='A BarData',
            data_type_inc='BarData',
            quantity='*',
        )
        return dataset_inc_spec

    def test_build(self):
        """
        Test build of BarDataHolder which contains a BarData.
        """
        # NOTE: attr1 doesn't map to anything but is required in the test container class
        bar_data_inst = BarData(name='my_bar', data=[1, 2, 3], attr1='a string')
        bar_data_holder_inst = BarDataHolder(
            name='my_bar_holder',
            bar_datas=[bar_data_inst],
        )

        msg = "Shape of data does not match shape in spec 'BarData'"
        with self.assertWarnsWith(IncorrectDatasetShapeBuildWarning, msg):
            builder = self.manager.build(bar_data_holder_inst, source='test.h5')
        assert builder.datasets['my_bar'].dimension_labels is None


class TestBuildDatasetOneOptionBadShapeUnspecified2(BuildDatasetShapeMixin):
    """Test dataset spec shape = (any, 2), data = (3, 1). Should raise warning and set dimension_labels to None."""

    def get_base_shape_dims(self):
        return [None, 2], ['a', 'b']

    def get_dataset_inc_spec(self):
        dataset_inc_spec = DatasetSpec(
            doc='A BarData',
            data_type_inc='BarData',
            quantity='*',
        )
        return dataset_inc_spec

    def test_build(self):
        """
        Test build of BarDataHolder which contains a BarData.
        """
        # NOTE: attr1 doesn't map to anything but is required in the test container class
        bar_data_inst = BarData(name='my_bar', data=[[1], [2], [3]], attr1='a string')
        bar_data_holder_inst = BarDataHolder(
            name='my_bar_holder',
            bar_datas=[bar_data_inst],
        )

        msg = "Shape of data does not match shape in spec 'BarData'"
        with self.assertWarnsWith(IncorrectDatasetShapeBuildWarning, msg):
            builder = self.manager.build(bar_data_holder_inst, source='test.h5')
        assert builder.datasets['my_bar'].dimension_labels is None


class TestBuildDatasetTwoOptionsBadShapeUnspecified(BuildDatasetShapeMixin):
    """Test dataset spec shape = (any, 2) or (any, 3), data = (3, 1).
    Should raise warning and set dimension_labels to None.
    """

    def get_base_shape_dims(self):
        return [[None, 2], [None, 3]], [['a', 'b1'], ['a', 'b2']]

    def get_dataset_inc_spec(self):
        dataset_inc_spec = DatasetSpec(
            doc='A BarData',
            data_type_inc='BarData',
            quantity='*',
        )
        return dataset_inc_spec

    def test_build(self):
        """
        Test build of BarDataHolder which contains a BarData.
        """
        # NOTE: attr1 doesn't map to anything but is required in the test container class
        bar_data_inst = BarData(name='my_bar', data=[[1], [2], [3]], attr1='a string')
        bar_data_holder_inst = BarDataHolder(
            name='my_bar_holder',
            bar_datas=[bar_data_inst],
        )

        msg = "Shape of data does not match any allowed shapes in spec 'BarData'"
        with self.assertWarnsWith(IncorrectDatasetShapeBuildWarning, msg):
            builder = self.manager.build(bar_data_holder_inst, source='test.h5')
        assert builder.datasets['my_bar'].dimension_labels is None


class TestBuildDatasetDimensionLabelsUnspecified(BuildDatasetShapeMixin):

    def get_base_shape_dims(self):
        return None, None

    def get_dataset_inc_spec(self):
        dataset_inc_spec = DatasetSpec(
            doc='A BarData',
            data_type_inc='BarData',
            quantity='*',
        )
        return dataset_inc_spec

    def test_build(self):
        """
        Test build of BarDataHolder which contains a BarData.
        """
        # NOTE: attr1 doesn't map to anything but is required in the test container class
        bar_data_inst = BarData(name='my_bar', data=[[1, 2, 3], [4, 5, 6]], attr1='a string')
        bar_data_holder_inst = BarDataHolder(
            name='my_bar_holder',
            bar_datas=[bar_data_inst],
        )

        builder = self.manager.build(bar_data_holder_inst, source='test.h5')
        assert builder.datasets['my_bar'].dimension_labels is None


class TestBuildDatasetDimensionLabelsOneOption(BuildDatasetShapeMixin):

    def get_base_shape_dims(self):
        return [None, None], ['a', 'b']

    def get_dataset_inc_spec(self):
        dataset_inc_spec = DatasetSpec(
            doc='A BarData',
            data_type_inc='BarData',
            quantity='*',
        )
        return dataset_inc_spec

    def test_build(self):
        """
        Test build of BarDataHolder which contains a BarData.
        """
        # NOTE: attr1 doesn't map to anything but is required in the test container class
        bar_data_inst = BarData(name='my_bar', data=[[1, 2, 3], [4, 5, 6]], attr1='a string')
        bar_data_holder_inst = BarDataHolder(
            name='my_bar_holder',
            bar_datas=[bar_data_inst],
        )

        builder = self.manager.build(bar_data_holder_inst, source='test.h5')
        assert builder.datasets['my_bar'].dimension_labels == ('a', 'b')


class TestBuildDatasetDimensionLabelsTwoOptionsOneMatch(BuildDatasetShapeMixin):

    def get_base_shape_dims(self):
        return [[None], [None, None]], [['a'], ['a', 'b']]

    def get_dataset_inc_spec(self):
        dataset_inc_spec = DatasetSpec(
            doc='A BarData',
            data_type_inc='BarData',
            quantity='*',
        )
        return dataset_inc_spec

    def test_build(self):
        """
        Test build of BarDataHolder which contains a BarData.
        """
        # NOTE: attr1 doesn't map to anything but is required in the test container class
        bar_data_inst = BarData(name='my_bar', data=[[1, 2, 3], [4, 5, 6]], attr1='a string')
        bar_data_holder_inst = BarDataHolder(
            name='my_bar_holder',
            bar_datas=[bar_data_inst],
        )

        builder = self.manager.build(bar_data_holder_inst, source='test.h5')
        assert builder.datasets['my_bar'].dimension_labels == ('a', 'b')


class TestBuildDatasetDimensionLabelsTwoOptionsTwoMatches(BuildDatasetShapeMixin):

    def get_base_shape_dims(self):
        return [[None, None], [None, 3]], [['a', 'b1'], ['a', 'b2']]

    def get_dataset_inc_spec(self):
        dataset_inc_spec = DatasetSpec(
            doc='A BarData',
            data_type_inc='BarData',
            quantity='*',
        )
        return dataset_inc_spec

    def test_build(self):
        """
        Test build of BarDataHolder which contains a BarData.
        """
        # NOTE: attr1 doesn't map to anything but is required in the test container class
        bar_data_inst = BarData(name='my_bar', data=[[1, 2, 3], [4, 5, 6]], attr1='a string')
        bar_data_holder_inst = BarDataHolder(
            name='my_bar_holder',
            bar_datas=[bar_data_inst],
        )

        builder = self.manager.build(bar_data_holder_inst, source='test.h5')
        assert builder.datasets['my_bar'].dimension_labels == ('a', 'b2')


class TestBuildDatasetDimensionLabelsOneOptionRefined(BuildDatasetShapeMixin):

    def get_base_shape_dims(self):
        return [None, None], ['a', 'b1']

    def get_dataset_inc_spec(self):
        dataset_inc_spec = DatasetSpec(
            doc='A BarData',
            data_type_inc='BarData',
            quantity='*',
            shape=[None, 3],
            dims=['a', 'b2'],
        )
        return dataset_inc_spec

    def test_build(self):
        """
        Test build of BarDataHolder which contains a BarData.
        """
        # NOTE: attr1 doesn't map to anything but is required in the test container class
        bar_data_inst = BarData(name='my_bar', data=[[1, 2, 3], [4, 5, 6]], attr1='a string')
        bar_data_holder_inst = BarDataHolder(
            name='my_bar_holder',
            bar_datas=[bar_data_inst],
        )

        builder = self.manager.build(bar_data_holder_inst, source='test.h5')
        assert builder.datasets['my_bar'].dimension_labels == ('a', 'b2')
