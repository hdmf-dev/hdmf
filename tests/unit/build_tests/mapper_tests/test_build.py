from abc import ABCMeta, abstractmethod

import numpy as np
from hdmf import Container, Data
from hdmf.build import ObjectMapper, BuildManager, TypeMap, GroupBuilder, DatasetBuilder
from hdmf.build.warnings import DtypeConversionWarning
from hdmf.spec import GroupSpec, AttributeSpec, DatasetSpec, SpecCatalog, SpecNamespace, NamespaceCatalog, Spec
from hdmf.testing import TestCase
from hdmf.utils import docval, getargs

from tests.unit.utils import CORE_NAMESPACE


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


class BuildGroupExtAttrsMixin(TestCase, metaclass=ABCMeta):

    def setUp(self):
        self.setUpBarSpec()
        self.setUpBarHolderSpec()
        spec_catalog = SpecCatalog()
        spec_catalog.register_spec(self.bar_spec, 'test.yaml')
        spec_catalog.register_spec(self.bar_holder_spec, 'test.yaml')
        namespace = SpecNamespace(
            doc='a test namespace',
            name=CORE_NAMESPACE,
            schema=[{'source': 'test.yaml'}],
            version='0.1.0',
            catalog=spec_catalog
        )
        namespace_catalog = NamespaceCatalog()
        namespace_catalog.add_namespace(CORE_NAMESPACE, namespace)
        type_map = TypeMap(namespace_catalog)
        type_map.register_container_type(CORE_NAMESPACE, 'Bar', Bar)
        type_map.register_container_type(CORE_NAMESPACE, 'BarHolder', BarHolder)
        type_map.register_map(Bar, ExtBarMapper)
        type_map.register_map(BarHolder, ObjectMapper)
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

    # def test_build_refined_attr_wrong_type(self):
    #     """
    #     Test build of BarHolder which contains a Bar that has the wrong dtype for an attr.
    #     """
    #     ext_bar_inst = Bar(
    #         name='my_bar',
    #         attr1='a string',
    #         attr2=10,  # spec specifies attr2 should be an int64 for Bars within BarHolder
    #     )
    #     bar_holder_inst = BarHolder(
    #         name='my_bar_holder',
    #         bars=[ext_bar_inst],
    #     )
    #
    #     expected_inner = GroupBuilder(
    #         name='my_bar',
    #         attributes={
    #             'attr1': 'a string',
    #             'attr2': np.int64(10),
    #             'data_type': 'Bar',
    #             'namespace': CORE_NAMESPACE,
    #             'object_id': ext_bar_inst.object_id,
    #         }
    #     )
    #     expected = GroupBuilder(
    #         name='my_bar_holder',
    #         groups={'my_bar': expected_inner},
    #         attributes={
    #             'data_type': 'BarHolder',
    #             'namespace': CORE_NAMESPACE,
    #             'object_id': bar_holder_inst.object_id,
    #         },
    #     )
    #
    #     # the object mapper automatically maps the spec of extended Bars to the 'BarMapper.bars' field
    #
    #     # TODO build should raise a conversion warning for converting 10 (int32) to np.int64
    #     builder = self.manager.build(bar_holder_inst, source='test.h5')
    #     self.assertDictEqual(builder, expected)


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
        self.__ext_attr = kwargs['ext_attr']

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


class BuildDatasetExtAttrsMixin(TestCase, metaclass=ABCMeta):

    def setUp(self):
        self.set_up_specs()
        spec_catalog = SpecCatalog()
        spec_catalog.register_spec(self.bar_data_spec, 'test.yaml')
        spec_catalog.register_spec(self.bar_data_holder_spec, 'test.yaml')
        namespace = SpecNamespace(
            doc='a test namespace',
            name=CORE_NAMESPACE,
            schema=[{'source': 'test.yaml'}],
            version='0.1.0',
            catalog=spec_catalog
        )
        namespace_catalog = NamespaceCatalog()
        namespace_catalog.add_namespace(CORE_NAMESPACE, namespace)
        type_map = TypeMap(namespace_catalog)
        type_map.register_container_type(CORE_NAMESPACE, 'BarData', BarData)
        type_map.register_container_type(CORE_NAMESPACE, 'BarDataHolder', BarDataHolder)
        type_map.register_map(BarData, ExtBarDataMapper)
        type_map.register_map(BarDataHolder, ObjectMapper)
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
        msg = "could not resolve dtype for BarData 'my_bar'"
        with self.assertRaisesWith(Exception, msg):
            self.manager.build(bar_data_holder_inst, source='test.h5')
