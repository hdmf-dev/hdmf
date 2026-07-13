import pytest

from hdmf.build.builders import DatasetBuilder, GroupBuilder
from hdmf.container import Container
from hdmf.spec.spec import AttributeSpec, DatasetSpec, GroupSpec
from hdmf.utils import docval, getargs
from hdmf.build import ObjectMapper
from uuid import uuid4

from ...helpers.utils import create_test_type_map, CORE_NAMESPACE


class Bar(Container):
    @docval(
        {"name": "name", "type": str, "doc": "the name of this Foo"},
        {"name": "my_data", "type": ("array_data", "data"), "doc": "some data"},
        {"name": "attr1", "type": str, "doc": "an attribute", "default": None},
    )
    def __init__(self, **kwargs):
        name, my_data, attr1 = getargs("name", "my_data", "attr1", kwargs)
        super().__init__(name=name)
        self.__data = my_data
        self.__attr1 = attr1

    @property
    def my_data(self):
        return self.__data

    @property
    def attr1(self):
        return self.__attr1


def test_carg_override():
    """Test that a constructor argument can be overridden by a custom mapper."""

    class CustomBarMapper(ObjectMapper):

        @ObjectMapper.constructor_arg("attr1")
        def attr1_carg(self, builder, manager):
            """When constructing a Bar object, use "custom" as the value for the "attr1" argument."""
            return "custom"

    bar_spec = GroupSpec(
        "A test group specification with a data type",
        data_type_def="Bar",
        datasets=[
            DatasetSpec(
                name="my_data",
                doc="an example 1D int dataset",
                dtype="int",
                shape=[None],
            )
        ],
        attributes=[
            AttributeSpec(name="attr1", doc="an example string attribute", dtype="text"),
        ],
    )
    specs = [bar_spec]
    container_classes = {"Bar": Bar}
    mappers = {"Bar": CustomBarMapper}
    type_map = create_test_type_map(specs, container_classes, mappers)

    bar_builder = GroupBuilder(
        name='my_bar',
        datasets=[DatasetBuilder(name='my_data', data=[1, 2, 3])],
        attributes={'attr1': 'value1', 'namespace': CORE_NAMESPACE, 'object_id': str(uuid4()), 'data_type': 'Bar'}
    )
    bar = type_map.construct(bar_builder)
    assert bar.attr1 == 'custom'


def test_carg_override_no_override_sentinel():
    """Test that returning NO_OVERRIDE from a constructor_arg override falls through to the built value."""

    class CustomBarMapper(ObjectMapper):

        @ObjectMapper.constructor_arg("attr1")
        def attr1_carg(self, builder, manager):
            """Signal no override for "attr1" so the value built from the file is used."""
            return ObjectMapper.NO_OVERRIDE

    bar_spec = GroupSpec(
        "A test group specification with a data type",
        data_type_def="Bar",
        datasets=[
            DatasetSpec(
                name="my_data",
                doc="an example 1D int dataset",
                dtype="int",
                shape=[None],
            )
        ],
        attributes=[
            AttributeSpec(name="attr1", doc="an example string attribute", dtype="text"),
        ],
    )
    specs = [bar_spec]
    container_classes = {"Bar": Bar}
    mappers = {"Bar": CustomBarMapper}
    type_map = create_test_type_map(specs, container_classes, mappers)

    bar_builder = GroupBuilder(
        name='my_bar',
        datasets=[DatasetBuilder(name='my_data', data=[1, 2, 3])],
        attributes={'attr1': 'value1', 'namespace': CORE_NAMESPACE, 'object_id': str(uuid4()), 'data_type': 'Bar'}
    )
    bar = type_map.construct(bar_builder)
    assert bar.attr1 == 'value1'


def test_carg_override_none_deprecated():
    """Test that returning None from a constructor_arg override falls through and warns.

    Returning None to signal "no override" is deprecated. In HDMF 8.0 a None return will set the
    argument to None; NO_OVERRIDE should be returned to fall through to the built value.
    """

    class CustomBarMapper(ObjectMapper):

        @ObjectMapper.constructor_arg("attr1")
        def attr1_carg(self, builder, manager):
            """Return None, which currently falls through to the value built from the file."""
            return None

    bar_spec = GroupSpec(
        "A test group specification with a data type",
        data_type_def="Bar",
        datasets=[
            DatasetSpec(
                name="my_data",
                doc="an example 1D int dataset",
                dtype="int",
                shape=[None],
            )
        ],
        attributes=[
            AttributeSpec(name="attr1", doc="an example string attribute", dtype="text"),
        ],
    )
    specs = [bar_spec]
    container_classes = {"Bar": Bar}
    mappers = {"Bar": CustomBarMapper}
    type_map = create_test_type_map(specs, container_classes, mappers)

    bar_builder = GroupBuilder(
        name='my_bar',
        datasets=[DatasetBuilder(name='my_data', data=[1, 2, 3])],
        attributes={'attr1': 'value1', 'namespace': CORE_NAMESPACE, 'object_id': str(uuid4()), 'data_type': 'Bar'}
    )
    with pytest.warns(DeprecationWarning, match="returned None"):
        bar = type_map.construct(bar_builder)
    assert bar.attr1 == 'value1'


def test_object_attr_override():
    """Test that an object attribute can be overridden by a custom mapper."""

    class CustomBarMapper(ObjectMapper):

        @ObjectMapper.object_attr("attr1")
        def attr1_attr(self, container, manager):
            """When building a Bar object, use "custom" as the value for the "attr1" attribute."""
            return "custom"

    bar_spec = GroupSpec(
        "A test group specification with a data type",
        data_type_def="Bar",
        datasets=[
            DatasetSpec(
                name="my_data",
                doc="an example 1D int dataset",
                dtype="int",
                shape=[None],
            )
        ],
        attributes=[
            AttributeSpec(name="attr1", doc="an example string attribute", dtype="text"),
        ],
    )
    specs = [bar_spec]
    container_classes = {"Bar": Bar}
    mappers = {"Bar": CustomBarMapper}
    type_map = create_test_type_map(specs, container_classes, mappers)

    bar = Bar(name='my_bar', my_data=[1, 2, 3], attr1='value1')
    bar_builder = type_map.build(bar)
    assert bar_builder.attributes['attr1'] == 'custom'


def test_object_attr_override_no_override_sentinel():
    """Test that returning NO_OVERRIDE from an object_attr override falls through to the container attribute."""

    class CustomBarMapper(ObjectMapper):

        @ObjectMapper.object_attr("attr1")
        def attr1_attr(self, container, manager):
            """Signal no override for "attr1" so the container's attribute value is used."""
            return ObjectMapper.NO_OVERRIDE

    bar_spec = GroupSpec(
        "A test group specification with a data type",
        data_type_def="Bar",
        datasets=[
            DatasetSpec(
                name="my_data",
                doc="an example 1D int dataset",
                dtype="int",
                shape=[None],
            )
        ],
        attributes=[
            AttributeSpec(name="attr1", doc="an example string attribute", dtype="text"),
        ],
    )
    specs = [bar_spec]
    container_classes = {"Bar": Bar}
    mappers = {"Bar": CustomBarMapper}
    type_map = create_test_type_map(specs, container_classes, mappers)

    bar = Bar(name='my_bar', my_data=[1, 2, 3], attr1='value1')
    bar_builder = type_map.build(bar)
    assert bar_builder.attributes['attr1'] == 'value1'


def test_object_attr_override_none_deprecated():
    """Test that returning None from an object_attr override falls through and warns.

    Returning None to signal "no override" is deprecated. In HDMF 8.0 a None return will set the
    attribute to None; NO_OVERRIDE should be returned to fall through to the container's value.
    """

    class CustomBarMapper(ObjectMapper):

        @ObjectMapper.object_attr("attr1")
        def attr1_attr(self, container, manager):
            """Return None, which currently falls through to the container's attribute value."""
            return None

    bar_spec = GroupSpec(
        "A test group specification with a data type",
        data_type_def="Bar",
        datasets=[
            DatasetSpec(
                name="my_data",
                doc="an example 1D int dataset",
                dtype="int",
                shape=[None],
            )
        ],
        attributes=[
            AttributeSpec(name="attr1", doc="an example string attribute", dtype="text"),
        ],
    )
    specs = [bar_spec]
    container_classes = {"Bar": Bar}
    mappers = {"Bar": CustomBarMapper}
    type_map = create_test_type_map(specs, container_classes, mappers)

    bar = Bar(name='my_bar', my_data=[1, 2, 3], attr1='value1')
    with pytest.warns(DeprecationWarning, match="returned None"):
        bar_builder = type_map.build(bar)
    assert bar_builder.attributes['attr1'] == 'value1'
