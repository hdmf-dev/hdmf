from copy import deepcopy
from datetime import datetime, date

import numpy as np

from ..container import Container, Data, DataRegion, MultiContainerInterface
from ..spec import AttributeSpec, LinkSpec, RefSpec, GroupSpec
from ..spec.spec import BaseStorageSpec, ZERO_OR_MANY, ONE_OR_MANY
from ..utils import docval, getargs, ExtenderMeta, get_docval, popargs, AllowPositional


class ClassGenerator:

    def __init__(self):
        self.__custom_generators = []

    @property
    def custom_generators(self):
        return self.__custom_generators

    @docval({'name': 'generator', 'type': type, 'doc': 'the CustomClassGenerator class to register'})
    def register_generator(self, **kwargs):
        """Add a custom class generator to this ClassGenerator.

        Generators added later are run first. Duplicates are moved to the top of the list.
        """
        generator = getargs('generator', kwargs)
        if not issubclass(generator, CustomClassGenerator):
            raise ValueError('Generator %s must be a subclass of CustomClassGenerator.' % generator)
        if generator in self.__custom_generators:
            self.__custom_generators.remove(generator)
        self.__custom_generators.insert(0, generator)

    @docval({'name': 'data_type', 'type': str, 'doc': 'the data type to create a AbstractContainer class for'},
            {'name': 'spec', 'type': BaseStorageSpec, 'doc': ''},
            {'name': 'parent_cls', 'type': type, 'doc': ''},
            {'name': 'attr_names', 'type': dict, 'doc': ''},
            {'name': 'type_map', 'type': 'TypeMap', 'doc': ''},
            returns='the class for the given namespace and data_type', rtype=type)
    def generate_class(self, **kwargs):
        """Get the container class from data type specification.
        If no class has been associated with the ``data_type`` from ``namespace``, a class will be dynamically
        created and returned.
        """
        data_type, spec, parent_cls, attr_names, type_map = getargs('data_type', 'spec', 'parent_cls', 'attr_names',
                                                                    'type_map', kwargs)

        not_inherited_fields = dict()
        for k, field_spec in attr_names.items():
            if k == 'help':  # pragma: no cover
                # (legacy) do not add field named 'help' to any part of class object
                continue
            if isinstance(field_spec, GroupSpec) and field_spec.data_type is None:  # skip named, untyped groups
                continue
            if not spec.is_inherited_spec(field_spec):
                not_inherited_fields[k] = field_spec
        try:
            classdict = dict()
            bases = [parent_cls]
            docval_args = list(deepcopy(get_docval(parent_cls.__init__)))
            for attr_name, field_spec in not_inherited_fields.items():
                for class_generator in self.__custom_generators:  # pragma: no branch
                    # each generator can update classdict and docval_args
                    if class_generator.apply_generator_to_field(field_spec, bases, type_map):
                        class_generator.process_field_spec(classdict, docval_args, parent_cls, attr_name,
                                                           not_inherited_fields, type_map, spec)
                        break  # each field_spec should be processed by only one generator

            for class_generator in self.__custom_generators:
                class_generator.post_process(classdict, bases, docval_args, spec)

            for class_generator in reversed(self.__custom_generators):
                # go in reverse order so that base init is added first and
                # later class generators can modify or overwrite __init__ set by an earlier class generator
                class_generator.set_init(classdict, bases, docval_args, not_inherited_fields, spec.name)
        except TypeDoesNotExistError as e:  # pragma: no cover
            # this error should never happen after hdmf#322
            name = spec.data_type_def
            if name is None:
                name = 'Unknown'
            raise ValueError("Cannot dynamically generate class for type '%s'. " % name
                             + str(e)
                             + " Please define that type before defining '%s'." % name)
        cls = ExtenderMeta(data_type, tuple(bases), classdict)
        return cls


class TypeDoesNotExistError(Exception):  # pragma: no cover
    pass


class CustomClassGenerator:
    """Subclass this class and register an instance to alter how classes are auto-generated."""

    def __new__(cls, *args, **kwargs):  # pragma: no cover
        raise TypeError('Cannot instantiate class %s' % cls.__name__)

    # mapping from spec types to allowable python types for docval for fields during dynamic class generation
    # e.g., if a dataset/attribute spec has dtype int32, then get_class should generate a docval for the class'
    # __init__ method that allows the types (int, np.int32, np.int64) for the corresponding field.
    # passing an np.int16 would raise a docval error.
    # passing an int64 to __init__ would result in the field storing the value as an int64 (and subsequently written
    # as an int64). no upconversion or downconversion happens as a result of this map
    _spec_dtype_map = {
        'float32': (float, np.float32, np.float64),
        'float': (float, np.float32, np.float64),
        'float64': (float, np.float64),
        'double': (float, np.float64),
        'int8': (np.int8, np.int16, np.int32, np.int64, int),
        'int16': (np.int16, np.int32, np.int64, int),
        'short': (np.int16, np.int32, np.int64, int),
        'int32': (int, np.int32, np.int64),
        'int': (int, np.int32, np.int64),
        'int64': np.int64,
        'long': np.int64,
        'uint8': (np.uint8, np.uint16, np.uint32, np.uint64),
        'uint16': (np.uint16, np.uint32, np.uint64),
        'uint32': (np.uint32, np.uint64),
        'uint64': np.uint64,
        'numeric': (float, np.float32, np.float64, np.int8, np.int16, np.int32, np.int64, int, np.uint8, np.uint16,
                    np.uint32, np.uint64),
        'text': str,
        'utf': str,
        'utf8': str,
        'utf-8': str,
        'ascii': bytes,
        'bytes': bytes,
        'bool': (bool, np.bool_),
        'isodatetime': (datetime, date),
        'datetime': (datetime, date)
    }

    @classmethod
    def _get_type_from_spec_dtype(cls, spec_dtype):
        """Get the Python type associated with the given spec dtype string.
        Raises ValueError if the given dtype has no mapping to a Python type.
        """
        dtype = cls._spec_dtype_map.get(spec_dtype)
        if dtype is None:  # pragma: no cover
            # this should not happen as long as _spec_dtype_map is kept up to date with
            # hdmf.spec.spec.DtypeHelper.valid_primary_dtypes
            raise ValueError("Spec dtype '%s' cannot be mapped to a Python type." % spec_dtype)
        return dtype

    @classmethod
    def _get_container_type(cls, type_name, type_map):
        """Search all namespaces for the container class associated with the given data type.
        Raises TypeDoesNotExistError if type is not found in any namespace.
        """
        container_type = type_map.get_dt_container_cls(type_name)
        if container_type is None:  # pragma: no cover
            # this should never happen after hdmf#322
            raise TypeDoesNotExistError("Type '%s' does not exist." % type_name)
        return container_type

    @classmethod
    def _get_type(cls, spec, type_map):
        """Get the type of a spec for use in docval.
        Returns a container class, a type, a tuple of types, ('array_data', 'data') for specs with
        non-scalar shape, or (Data, Container) when an attribute reference target has not been mapped to a container
        class.
        """
        if isinstance(spec, AttributeSpec):
            if isinstance(spec.dtype, RefSpec):
                try:
                    container_type = cls._get_container_type(spec.dtype.target_type, type_map)
                    return container_type
                except TypeDoesNotExistError:
                    # TODO what happens when the attribute ref target is not (or not yet) mapped to a container class?
                    # returning Data, Container works as a generic fallback for now but should be more specific
                    return Data, Container
            elif spec.shape is None and spec.dims is None:
                return cls._get_type_from_spec_dtype(spec.dtype)
            else:
                return 'array_data', 'data'
        if isinstance(spec, LinkSpec):
            return cls._get_container_type(spec.target_type, type_map)
        if spec.data_type is not None:
            return cls._get_container_type(spec.data_type, type_map)
        if spec.shape is None and spec.dims is None:
            return cls._get_type_from_spec_dtype(spec.dtype)
        return 'array_data', 'data'

    @classmethod
    def _ischild(cls, dtype):
        """Check if dtype represents a type that is a child."""
        ret = False
        if isinstance(dtype, tuple):
            for sub in dtype:
                ret = ret or cls._ischild(sub)
        elif isinstance(dtype, type) and issubclass(dtype, (Container, Data, DataRegion)):
            ret = True
        return ret

    @staticmethod
    def _set_default_name(docval_args, default_name):
        """Set the default value for the name docval argument."""
        if default_name is not None:
            for x in docval_args:
                if x['name'] == 'name':
                    x['default'] = default_name

    @classmethod
    def apply_generator_to_field(cls, field_spec, bases, type_map):
        """Return True to signal that this generator should return on all fields not yet processed."""
        return True

    @classmethod
    def process_field_spec(cls, classdict, docval_args, parent_cls, attr_name, not_inherited_fields, type_map, spec):
        """Add __fields__ to the classdict and update the docval args for the field spec with the given attribute name.
        :param classdict: The dict to update with __fields__ (or a different parent_cls._fieldsname).
        :param docval_args: The list of docval arguments.
        :param parent_cls: The parent class.
        :param attr_name: The attribute name of the field spec for the container class to generate.
        :param not_inherited_fields: Dictionary of fields not inherited from the parent class.
        :param type_map: The type map to use.
        :param spec: The spec for the container class to generate.
        """
        field_spec = not_inherited_fields[attr_name]
        dtype = cls._get_type(field_spec, type_map)
        fields_conf = {'name': attr_name,
                       'doc': field_spec['doc']}
        if cls._ischild(dtype) and issubclass(parent_cls, Container) and not isinstance(field_spec, LinkSpec):
            fields_conf['child'] = True
        fixed_value = getattr(field_spec, 'value', None)
        if fixed_value is not None:
            fields_conf['settable'] = False
        if isinstance(field_spec, (BaseStorageSpec, LinkSpec)) and field_spec.data_type is not None:
            # subgroups, datasets, and links with data types can have fixed names
            fixed_name = getattr(field_spec, 'name', None)
            if fixed_name is not None:
                fields_conf['required_name'] = fixed_name
        classdict.setdefault(parent_cls._fieldsname, list()).append(fields_conf)

        if fixed_value is not None:  # field has fixed value - do not create arg on __init__
            return

        docval_arg = dict(
            name=attr_name,
            doc=field_spec.doc,
            type=cls._get_type(field_spec, type_map)
        )
        shape = getattr(field_spec, 'shape', None)
        if shape is not None:
            docval_arg['shape'] = shape
        if cls._check_spec_optional(field_spec, spec):
            docval_arg['default'] = getattr(field_spec, 'default_value', None)
        cls._add_to_docval_args(docval_args, docval_arg)

    @classmethod
    def _check_spec_optional(cls, field_spec, spec):
        """Returns True if the spec or any of its parents (up to the parent type spec) are optional."""
        if not field_spec.required:
            return True
        if field_spec == spec:
            return False
        if field_spec.parent is not None:
            return cls._check_spec_optional(field_spec.parent, spec)

    @classmethod
    def _add_to_docval_args(cls, docval_args, arg, err_if_present=False):
        """Add the docval arg to the list if not present. If present, overwrite it in place or raise an error."""
        inserted = False
        for i, x in enumerate(docval_args):
            if x['name'] == arg['name']:
                if err_if_present:
                    raise ValueError("Argument %s already exists in docval args." % arg["name"])
                docval_args[i] = arg
                inserted = True
        if not inserted:
            docval_args.append(arg)

    @classmethod
    def post_process(cls, classdict, bases, docval_args, spec):
        """Convert classdict['__fields__'] to tuple and update docval args for a fixed name and default name.
        :param classdict: The class dictionary to convert with '__fields__' key (or a different bases[0]._fieldsname)
        :param bases: The list of base classes.
        :param docval_args: The dict of docval arguments.
        :param spec: The spec for the container class to generate.
        """
        # convert classdict['__fields__'] from list to tuple if present
        for b in bases:
            fields = classdict.get(b._fieldsname)
            if fields is not None and not isinstance(fields, tuple):
                classdict[b._fieldsname] = tuple(fields)

        # if spec provides a fixed name for this type, remove the 'name' arg from docval_args so that values cannot
        # be passed for a name positional or keyword arg
        if spec.name is not None:
            for arg in list(docval_args):
                if arg['name'] == 'name':
                    docval_args.remove(arg)

        # set default name in docval args if provided
        cls._set_default_name(docval_args, spec.default_name)

    @classmethod
    def _get_attrs_not_to_set_init(cls, classdict, parent_docval_args):
        return parent_docval_args

    @classmethod
    def set_init(cls, classdict, bases, docval_args, not_inherited_fields, name):
        # get docval arg names from superclass
        base = bases[0]
        parent_docval_args = set(arg['name'] for arg in get_docval(base.__init__))
        attrs_to_set = list()
        fixed_value_attrs_to_set = list()
        attrs_not_to_set = cls._get_attrs_not_to_set_init(classdict, parent_docval_args)
        for attr_name, field_spec in not_inherited_fields.items():
            # store arguments for fields that are not in the superclass and not in the superclass __init__ docval
            # so that they are set after calling base.__init__
            # except for fields that have fixed values -- these are set at the class level
            fixed_value = getattr(field_spec, 'value', None)
            if fixed_value is not None:
                fixed_value_attrs_to_set.append(attr_name)
            elif attr_name not in attrs_not_to_set:
                attrs_to_set.append(attr_name)

        @docval(*docval_args, allow_positional=AllowPositional.WARNING)
        def __init__(self, **kwargs):
            if name is not None:  # force container name to be the fixed name in the spec
                kwargs.update(name=name)

            # remove arguments from kwargs that correspond to fields that are new (not inherited)
            # set these arguments after calling base.__init__
            new_kwargs = dict()
            for f in attrs_to_set:
                new_kwargs[f] = popargs(f, kwargs) if f in kwargs else None

            # NOTE: the docval of some constructors do not include all of the fields. the constructor may set
            # some fields to fixed values. so only keep the kwargs that are used in the constructor docval
            kwargs_to_pass = {k: v for k, v in kwargs.items() if k in parent_docval_args}

            base.__init__(self, **kwargs_to_pass)  # special case: need to pass self to __init__
            # TODO should super() be used above instead of base?

            # set the fields that are new to this class (not inherited)
            for f, arg_val in new_kwargs.items():
                setattr(self, f, arg_val)

            # set the fields that have fixed values using the fields dict directly
            # because the setters do not allow setting the value
            for f in fixed_value_attrs_to_set:
                self.fields[f] = getattr(not_inherited_fields[f], 'value')

        classdict['__init__'] = __init__


class MCIClassGenerator(CustomClassGenerator):

    @classmethod
    def apply_generator_to_field(cls, field_spec, bases, type_map):
        """Return True if the field spec has quantity * or +, False otherwise."""
        return getattr(field_spec, 'quantity', None) in (ZERO_OR_MANY, ONE_OR_MANY)

    @classmethod
    def process_field_spec(cls, classdict, docval_args, parent_cls, attr_name, not_inherited_fields, type_map, spec):
        """Add __clsconf__ to the classdict and update the docval args for the field spec with the given attribute name.
        :param classdict: The dict to update with __clsconf__.
        :param docval_args: The list of docval arguments.
        :param parent_cls: The parent class.
        :param attr_name: The attribute name of the field spec for the container class to generate.
        :param not_inherited_fields: Dictionary of fields not inherited from the parent class.
        :param type_map: The type map to use.
        :param spec: The spec for the container class to generate.
        """
        field_spec = not_inherited_fields[attr_name]
        field_clsconf = dict(
            attr=attr_name,
            type=cls._get_type(field_spec, type_map),
            add='add_{}'.format(attr_name),
            get='get_{}'.format(attr_name),
            create='create_{}'.format(attr_name)
        )
        classdict.setdefault('__clsconf__', list()).append(field_clsconf)

        # add a specialized docval arg for __init__
        docval_arg = dict(
            name=attr_name,
            doc=field_spec.doc,
            type=(list, tuple, dict, cls._get_type(field_spec, type_map))
        )
        if cls._check_spec_optional(field_spec, spec):
            docval_arg['default'] = getattr(field_spec, 'default_value', None)
        cls._add_to_docval_args(docval_args, docval_arg)

    @classmethod
    def post_process(cls, classdict, bases, docval_args, spec):
        """Add MultiContainerInterface to the list of base classes.
        :param classdict: The class dictionary.
        :param bases: The list of base classes.
        :param docval_args: The dict of docval arguments.
        :param spec: The spec for the container class to generate.
        """
        if '__clsconf__' in classdict:
            # do not add MCI as a base if a base is already a subclass of MultiContainerInterface
            for b in bases:
                if issubclass(b, MultiContainerInterface):
                    break
            else:
                if issubclass(MultiContainerInterface, bases[0]):
                    # if bases[0] is Container or another superclass of MCI, then make sure MCI goes first
                    # otherwise, MRO is ambiguous
                    bases.insert(0, MultiContainerInterface)
                else:
                    # bases[0] is not a subclass of MCI and not a superclass of MCI. place that class first
                    # before MCI. that class __init__ should call super().__init__ which will call the
                    # MCI init
                    bases.insert(1, MultiContainerInterface)

    @classmethod
    def set_init(cls, classdict, bases, docval_args, not_inherited_fields, name):
        if '__clsconf__' in classdict:
            previous_init = classdict['__init__']

            @docval(*docval_args, allow_positional=AllowPositional.WARNING)
            def __init__(self, **kwargs):
                # store the values passed to init for each MCI attribute so that they can be added
                # after calling __init__
                new_kwargs = list()
                for field_clsconf in classdict['__clsconf__']:
                    attr_name = field_clsconf['attr']
                    # do not store the value if it is None or not present
                    if attr_name not in kwargs or kwargs[attr_name] is None:
                        continue
                    add_method_name = field_clsconf['add']
                    new_kwarg = dict(
                        attr_name=attr_name,
                        value=popargs(attr_name, kwargs),
                        add_method_name=add_method_name
                    )
                    new_kwargs.append(new_kwarg)

                    # pass an empty list to previous_init in case attr_name field is required
                    # (one or many). we do not want previous_init to set the attribute directly.
                    # instead, we will use the add_method after previous_init is finished.
                    kwargs[attr_name] = list()

                # call the parent class init without the MCI attribute
                previous_init(self, **kwargs)

                # call the add method for each MCI attribute
                for new_kwarg in new_kwargs:
                    add_method = getattr(self, new_kwarg['add_method_name'])
                    add_method(new_kwarg['value'])

            # override __init__
            classdict['__init__'] = __init__
