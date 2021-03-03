from .. import register_map
from ..table import DynamicTable, VectorData, VectorIndex
from ...build import ObjectMapper, BuildManager, CustomClassGenerator
from ...spec import Spec
from ...spec.spec import BaseStorageSpec
from ...utils import docval, getargs, get_docval


@register_map(DynamicTable)
class DynamicTableMap(ObjectMapper):

    def __init__(self, spec):
        super().__init__(spec)
        vector_data_spec = spec.get_data_type('VectorData')
        self.map_spec('columns', vector_data_spec)

    @ObjectMapper.object_attr('colnames')
    def attr_columns(self, container, manager):
        if all(not col for col in container.columns):
            return tuple()
        return container.colnames

    @docval({"name": "spec", "type": Spec, "doc": "the spec to get the attribute value for"},
            {"name": "container", "type": DynamicTable, "doc": "the container to get the attribute value from"},
            {"name": "manager", "type": BuildManager, "doc": "the BuildManager used for managing this build"},
            returns='the value of the attribute')
    def get_attr_value(self, **kwargs):
        ''' Get the value of the attribute corresponding to this spec from the given container '''
        spec, container, manager = getargs('spec', 'container', 'manager', kwargs)
        attr_value = super().get_attr_value(spec, container, manager)
        if attr_value is None and spec.name in container:
            if spec.data_type_inc == 'VectorData':
                attr_value = container[spec.name]
                if isinstance(attr_value, VectorIndex):
                    attr_value = attr_value.target
            elif spec.data_type_inc == 'DynamicTableRegion':
                attr_value = container[spec.name]
                if isinstance(attr_value, VectorIndex):
                    attr_value = attr_value.target
                if attr_value.table is None:
                    msg = "empty or missing table for DynamicTableRegion '%s' in DynamicTable '%s'" % \
                          (attr_value.name, container.name)
                    raise ValueError(msg)
            elif spec.data_type_inc == 'VectorIndex':
                attr_value = container[spec.name]
        return attr_value


class DynamicTableGenerator(CustomClassGenerator):

    def update_cls_args(self, classdict, bases, not_inherited_fields, name, default_name):
        """Update the given class dict and base classes if there is a DynamicTable
        :param classdict: The dict to update with __clsconf__ if applicable
        :param bases: The list of base classes to update if applicable
        :param not_inherited_fields: Dict of additional fields that are not in the base class
        :param name: Fixed name of instances of this class, or None if name is not fixed to a particular value
        :param default_name: Default name of instances of this class, or None if not specified
        """
        cols = list()
        for f, field_spec in not_inherited_fields.items():
            # check for columns of a dynamic table
            # TODO is there a smarter check using type hierarchy than comparing spec inc key names?
            if (any([b.__name__ == 'DynamicTable' for b in bases])
                    and getattr(field_spec, field_spec.inc_key(), None) in ('VectorData', 'DynamicTableRegion')):
                cols.append(self.__make_dynamic_table_column(f, field_spec, not_inherited_fields))
        if len(cols):
            classdict.update(__columns__=tuple(cols))

            # alter __init__ docval to remove columns
            old_dv = list(get_docval(classdict['__init__']))
            new_dv = [arg for arg in old_dv if not isinstance(arg['type'], VectorData)]

            # TODO improve this override build_docval??? add a hook in there?
            @docval(*new_dv)
            def __init__(self, **kwargs):
                # TODO: improve this
                classdict['__init__'](**kwargs)  # this still requires some columns

            classdict.update(__init__=__init__)

    @staticmethod
    def __make_dynamic_table_column(f: str, field_spec: BaseStorageSpec, not_inherited_fields: dict) -> dict:
        """Make single column for __columns__ configuration for auto-generated DynamicTable API class.
        :param f: field name
        :param field_spec:
        :param not_inherited_fields: d
        :return: dict
        """
        col_spec = dict(name=f, description=field_spec['doc'])
        if getattr(field_spec, 'quantity', None) == '?':
            col_spec.update(required=False)
        if field_spec[field_spec.inc_key()] == 'DynamicTableRegion':
            col_spec.update(table=True)
        if '{}_index'.format(f) in not_inherited_fields:
            counter = 0
            index_name = f
            while '{}_index'.format(index_name) in not_inherited_fields:
                index_name = '{}_index'
                counter += 1
            if counter == 1:
                col_spec.update(index=True)
            else:
                col_spec.update(index=counter)
        return col_spec
