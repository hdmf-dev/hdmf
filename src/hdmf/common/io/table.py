from .. import register_map
from ..table import DynamicTable, VectorData, VectorIndex, DynamicTableRegion
from ...build import ObjectMapper, BuildManager, CustomClassGenerator
from ...spec import Spec
from ...utils import docval, getargs


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

    @classmethod
    def apply_generator_to_field(cls, field_spec, bases, type_map):
        """Return True if this is a DynamicTable and the field spec is a column."""
        dtype = cls._get_type(field_spec, type_map)
        return DynamicTable in bases and issubclass(dtype, VectorData)

    @classmethod
    def process_field_spec(cls, classdict, docval_args, parent_cls, attr_name, not_inherited_fields, type_map):
        """Add __columns__ to the classdict and update the docval args for the field spec with the given attribute name.
        :param classdict: The dict to update with __columns__.
        :param docval_args: The list of docval arguments.
        :param parent_cls: The parent class.
        :param attr_name: The attribute name of the field spec for the container class to generate.
        :param spec: The spec for the container class to generate.
        :param type_map: The type map to use.
        """
        if attr_name.endswith('_index'):  # do not add index columns to __columns__
            return
        field_spec = not_inherited_fields[attr_name]
        column_conf = dict(name=attr_name, description=field_spec['doc'])
        if not field_spec.required:
            column_conf['required'] = False
        dtype = cls._get_type(field_spec, type_map)
        if issubclass(dtype, DynamicTableRegion):
            column_conf['table'] = True

        index_counter = 0
        index_name = attr_name
        while '{}_index'.format(index_name) in not_inherited_fields:  # an index column exists for this column
            index_name = '{}_index'.format(index_name)
            index_counter += 1
        if index_counter == 1:
            column_conf['index'] = True
        elif index_counter > 1:
            column_conf['index'] = index_counter

        classdict.setdefault('__columns__', list()).append(column_conf)

        # do not add DynamicTable columns to init docval

    @classmethod
    def post_process(cls, classdict, bases, docval_args, spec):
        """Convert classdict['__columns__'] to tuple.
        :param classdict: The class dictionary.
        :param bases: The list of base classes.
        :param docval_args: The dict of docval arguments.
        :param spec: The spec for the container class to generate.
        """
        # convert classdict['__columns__'] from list to tuple if present
        columns = classdict.get('__columns__')
        if columns is not None:
            classdict['__columns__'] = tuple(columns)
