from abc import ABCMeta, abstractmethod
import numpy as np

from hdmf import Container
from hdmf.build import ObjectMapper, BuildManager, TypeMap, GroupBuilder, DatasetBuilder
from hdmf.utils import docval, getargs, get_docval
from hdmf.spec import GroupSpec, AttributeSpec, DatasetSpec, SpecCatalog, SpecNamespace, NamespaceCatalog, Spec
from hdmf.testing import TestCase

from tests.unit.utils import CORE_NAMESPACE


# TODO: test build of extended group with attr that modifies dtype (commented out below), shape, value, etc.

class Bar(Container):

    @docval({'name': 'name', 'type': str, 'doc': 'the name of this Bar'},
            {'name': 'attr1', 'type': str, 'doc': 'a string attribute'},
            {'name': 'attr2', 'type': ('int', 'float', 'uint'), 'doc': 'a numeric attribute', 'default': None},
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

    @docval({'name': 'name', 'type': str, 'doc': 'the name of this Bar'},
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


class BuildExtAttrsMixin(TestCase, metaclass=ABCMeta):

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
            dtype='numeric',
            doc='an example numeric attribute',
        )
        self.bar_spec = GroupSpec(
            doc='A test group specification with a data type',
            data_type_def='Bar',
            attributes=[attr1_attr, attr2_attr],
        )

    @abstractmethod
    def setUpBarHolderSpec(self):
        pass


class TestBuildNewExtAttrs(BuildExtAttrsMixin, TestCase):
    """
    If the spec defines data_type A (Bar) using 'data_type_def' and defines another data_type B (BarHolder) that
    includes A using 'data_type_inc', then the included A spec is an extended (or refined) spec of A - call it A'.
    The spec of A' can change or add attributes to the spec of A. This test ensures that *new attributes* in A' are
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

    def test_build_new_attr(self):
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


class TestBuildModExtAttrs(BuildExtAttrsMixin, TestCase):
    """
    If the spec defines data_type A (Bar) using 'data_type_def' and defines another data_type B (BarHolder) that
    includes A using 'data_type_inc', then the included A spec is an extended (or refined) spec of A - call it A'.
    The spec of A' can change or add attributes to the spec of A. This test ensures that *modified attributes* in A' are
    handled properly.
    """

    def setUpBarHolderSpec(self):
        int_attr2 = AttributeSpec(
            name='attr2',
            dtype='int',
            doc='Refine Bar spec from numeric to int',
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

    def test_build_mod_attr(self):
        """
        Test build of BarHolder which can contain multiple extended Bar objects, which have a modified attr2.
        """
        ext_bar_inst = Bar(
            name='my_bar',
            attr1='a string',
            attr2=10,
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

    # def test_build_mod_attr_wrong_type(self):
    #     """
    #     Test build of BarHolder which contains a Bar that has the wrong dtype for an attr.
    #     """
    #     ext_bar_inst = Bar(
    #         name='my_bar',
    #         attr1='a string',
    #         attr2=10.1,  # spec specifies attr2 should be an int for Bars within BarHolder
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
    #             'attr2': 10,
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
    #     # TODO build should raise a conversion warning for converting 10.1 (float64) to np.int64
    #     builder = self.manager.build(bar_holder_inst, source='test.h5')
    #     self.assertDictEqual(builder, expected)
