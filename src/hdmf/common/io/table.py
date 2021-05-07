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
        for b in bases:
            if issubclass(b, DynamicTable):
                break
        else:  # return False if no base is a subclass of DynamicTable
            return False
        dtype = cls._get_type(field_spec, type_map)
        return isinstance(dtype, type) and issubclass(dtype, VectorData)

    @classmethod
    def process_field_spec(cls, classdict, docval_args, parent_cls, attr_name, not_inherited_fields, type_map, spec):
        """Add __columns__ to the classdict and update the docval args for the field spec with the given attribute name.
        :param classdict: The dict to update with __columns__.
        :param docval_args: The list of docval arguments.
        :param parent_cls: The parent class.
        :param attr_name: The attribute name of the field spec for the container class to generate.
        :param not_inherited_fields: Dictionary of fields not inherited from the parent class.
        :param type_map: The type map to use.
        :param spec: The spec for the container class to generate.
        """
        if attr_name.endswith('_index'):  # do not add index columns to __columns__
            return
        field_spec = not_inherited_fields[attr_name]
        column_conf = dict(
            name=attr_name,
            description=field_spec['doc'],
            required=field_spec.required
        )
        dtype = cls._get_type(field_spec, type_map)
        if issubclass(dtype, DynamicTableRegion):
            # the spec does not know which table this DTR points to
            # the user must specify the table attribute on the DTR after it is generated
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

        # add a specialized docval arg for __init__ for specifying targets for DTRs
        target_tables_dvarg = dict(
            name='target_tables',
            doc=('dict mapping DynamicTableRegion column name to the table that the DTR points to. The column is '
                 'added to the table if it is not already present (i.e., when it is optional).'),
            type=dict,
            default=None
        )
        cls._add_to_docval_args(docval_args, target_tables_dvarg)

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

    @classmethod
    def set_init(cls, classdict, bases, docval_args, not_inherited_fields, name):
        if '__columns__' not in classdict:
            return

        base_init = classdict.get('__init__')
        if base_init is None:  # pragma: no cover
            raise ValueError("Generated class dictionary is missing base __init__ method.")

        @docval(*docval_args)
        def __init__(self, **kwargs):
            base_init(self, **kwargs)

            # set target attribute on DTR
            target_tables = kwargs.get('target_tables')
            if target_tables:
                for colname, table in target_tables.items():
                    if colname not in self:  # column has not yet been added (it is optional)
                        column_conf = None
                        for conf in self.__columns__:
                            if conf['name'] == colname:
                                column_conf = conf
                                break
                        if column_conf is None:
                            raise ValueError("'%s' is not the name of a predefined column of table %s."
                                             % (colname, self))
                        if not column_conf.get('table', False):
                            raise ValueError("Column '%s' must be a DynamicTableRegion to have a target table."
                                             % colname)
                        self.add_column(name=column_conf['name'],
                                        description=column_conf['description'],
                                        index=column_conf.get('index', False),
                                        table=True)
                    if isinstance(self[colname], VectorIndex):
                        col = self[colname].target
                    else:
                        col = self[colname]
                    col.table = table

        classdict['__init__'] = __init__
