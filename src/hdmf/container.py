from abc import abstractmethod
from uuid import uuid4
from six import with_metaclass
from .utils import docval, getargs, ExtenderMeta
from warnings import warn


class Container(with_metaclass(ExtenderMeta, object)):

    _fieldsname = '__fields__'

    __fields__ = tuple()

    def __new__(cls, *args, **kwargs):
        inst = super(Container, cls).__new__(cls)
        inst.__container_source = kwargs.pop('container_source', None)
        inst.__parent = None
        inst.__children = list()
        inst.__modified = True
        inst.__object_id = kwargs.pop('object_id', str(uuid4()))
        inst.parent = kwargs.pop('parent', None)
        return inst

    @docval({'name': 'name', 'type': str, 'doc': 'the name of this container'})
    def __init__(self, **kwargs):
        name = getargs('name', kwargs)
        if '/' in name:
            raise ValueError("name '" + name + "' cannot contain '/'")
        self.__name = name
        self.__fields = dict()

    @property
    def object_id(self):
        if self.__object_id is None:
            self.__object_id = str(uuid4())
        return self.__object_id

    @property
    def modified(self):
        return self.__modified

    @docval({'name': 'modified', 'type': bool,
             'doc': 'whether or not this Container has been modified', 'default': True})
    def set_modified(self, **kwargs):
        modified = getargs('modified', kwargs)
        self.__modified = modified
        if modified and isinstance(self.parent, Container):
            self.parent.set_modified()

    @property
    def children(self):
        return tuple(self.__children)

    @docval({'name': 'child', 'type': 'Container',
             'doc': 'the child Container for this Container', 'default': None})
    def add_child(self, **kwargs):
        warn(DeprecationWarning('add_child is deprecated. Set the parent attribute instead.'))
        child = getargs('child', kwargs)
        if child is not None:
            # if child.parent is a Container, then the mismatch between child.parent and parent
            # is used to make a soft/external link from the parent to a child elsewhere
            # if child.parent is not a Container, it is either None or a Proxy and should be set to self
            if not isinstance(child.parent, Container):
                # actually add the child to the parent in parent setter
                child.parent = self
        else:
            warn('Cannot add None as child to a container %s' % self.name)

    @classmethod
    def type_hierarchy(cls):
        return cls.__mro__

    @property
    def name(self):
        '''
        The name of this Container
        '''
        return self.__name

    @property
    def container_source(self):
        '''
        The source of this Container
        '''
        return self.__container_source

    @container_source.setter
    def container_source(self, source):
        if self.__container_source is not None:
            raise Exception('cannot reassign container_source')
        self.__container_source = source

    @property
    def parent(self):
        '''
        The parent Container of this Container
        '''
        # do it this way because __parent may not exist yet (not set in constructor)
        return getattr(self, '_Container__parent', None)

    @parent.setter
    def parent(self, parent_container):
        if self.parent is parent_container:
            return

        if self.parent is not None:
            if isinstance(self.parent, Container):
                raise ValueError(('Cannot reassign parent to Container: %s. '
                                  'Parent is already: %s.' % (repr(self), repr(self.parent))))
            else:
                if parent_container is None:
                    raise ValueError("Got None for parent of '%s' - cannot overwrite Proxy with NoneType" % repr(self))
                # TODO this assumes isinstance(parent_container, Proxy) but
                # circular import if we try to do that. Proxy would need to move
                # or Container extended with this functionality in build/map.py
                if self.parent.matches(parent_container):
                    self.__parent = parent_container
                    parent_container.__children.append(self)
                    parent_container.set_modified()
                else:
                    self.__parent.add_candidate(parent_container, self)
        else:
            self.__parent = parent_container
            if isinstance(parent_container, Container):
                parent_container.__children.append(self)
                parent_container.set_modified()

    @docval({'name': 'data_type', 'type': str, 'doc': 'the data_type to search for', 'default': None})
    def get_ancestor(self, **kwargs):
        """
        Traverse parent hierarchy and return first instance of the specified data_type
        """
        data_type = getargs('data_type', kwargs)
        if data_type is None:
            return self.parent
        p = self.parent
        while p is not None:
            if p.data_type == data_type:
                return p
            p = p.parent
        return None

    @property
    def fields(self):
        return self.__fields

    @staticmethod
    def _transform_arg(field):
        tmp = field
        if isinstance(tmp, dict):
            if 'name' not in tmp:
                raise ValueError("must specify 'name' if using dict in __fields__")
        else:
            tmp = {'name': tmp}
        return tmp

    @classmethod
    def _getter(cls, field):
        doc = field.get('doc')
        name = field['name']

        def getter(self):
            return self.fields.get(name)

        setattr(getter, '__doc__', doc)
        return getter

    @classmethod
    def _setter(cls, field):
        name = field['name']

        if not field.get('settable', True):
            return None

        def setter(self, val):
            if val is None:
                return
            if name in self.fields:
                msg = "can't set attribute '%s' -- already set" % name
                raise AttributeError(msg)
            self.fields[name] = val

        return setter

    @ExtenderMeta.pre_init
    def __gather_fields(cls, name, bases, classdict):
        '''
        This classmethod will be called during class declaration in the metaclass to automatically
        create setters and getters for fields that need to be exported
        '''
        if not isinstance(cls.__fields__, tuple):
            raise TypeError("'__fields__' must be of type tuple")

        if len(bases) and 'Container' in globals() and issubclass(bases[-1], Container) \
                and bases[-1].__fields__ is not cls.__fields__:
            new_fields = list(cls.__fields__)
            new_fields[0:0] = bases[-1].__fields__
            cls.__fields__ = tuple(new_fields)
        new_fields = list()
        docs = {dv['name']: dv['doc'] for dv in get_docval(cls.__init__)}
        for f in cls.__fields__:
            pconf = cls._transform_arg(f)
            pname = pconf['name']
            pconf.setdefault('doc', docs.get(pname))
            if not hasattr(cls, pname):
                setattr(cls, pname, property(cls._getter(pconf), cls._setter(pconf)))
            new_fields.append(pname)
        cls.__fields__ = tuple(new_fields)

    def __repr__(self):
        template = "\n{} {}\nFields:\n""".format(getattr(self, 'name'), type(self))
        for k in sorted(self.fields):  # sorted to enable tests
            v = self.fields[k]
            if isinstance(v, DataIO) or not hasattr(v, '__len__') or len(v) > 0:
                template += "  {}: {}\n".format(k, self.__smart_str(v, 1))
        return template

    @staticmethod
    def __smart_str(v, num_indent):
        """
        Print compact string representation of data.

        If v is a list, try to print it using numpy. This will condense the string
        representation of datasets with many elements. If that doesn't work, just print the list.

        If v is a dictionary, print the name and type of each element

        If v is a set, print it sorted

        If v is a neurodata_type, print the name of type

        Otherwise, use the built-in str()
        Parameters
        ----------
        v

        Returns
        -------
        str

        """

        if isinstance(v, list) or isinstance(v, tuple):
            if len(v) and isinstance(v[0], Container):
                return Container.__smart_str_list(v, num_indent, '(')
            try:
                return str(np.asarray(v))
            except ValueError:
                return Container.__smart_str_list(v, num_indent, '(')
        elif isinstance(v, dict):
            return Container.__smart_str_dict(v, num_indent)
        elif isinstance(v, set):
            return Container.__smart_str_list(sorted(list(v)), num_indent, '{')
        elif isinstance(v, Container):
            return "{} {}".format(getattr(v, 'name'), type(v))
        else:
            return str(v)

    @staticmethod
    def __smart_str_list(l, num_indent, left_br):
        if left_br == '(':
            right_br = ')'
        if left_br == '{':
            right_br = '}'
        if len(l) == 0:
            return left_br + ' ' + right_br
        indent = num_indent * 2 * ' '
        indent_in = (num_indent + 1) * 2 * ' '
        out = left_br
        for v in l[:-1]:
            out += '\n' + indent_in + Container.__smart_str(v, num_indent + 1) + ','
        if l:
            out += '\n' + indent_in + Container.__smart_str(l[-1], num_indent + 1)
        out += '\n' + indent + right_br
        return out

    @staticmethod
    def __smart_str_dict(d, num_indent):
        left_br = '{'
        right_br = '}'
        if len(d) == 0:
            return left_br + ' ' + right_br
        indent = num_indent * 2 * ' '
        indent_in = (num_indent + 1) * 2 * ' '
        out = left_br
        keys = sorted(list(d.keys()))
        for k in keys[:-1]:
            out += '\n' + indent_in + Container.__smart_str(k, num_indent + 1) + ' ' + str(type(d[k])) + ','
        if keys:
            out += '\n' + indent_in + Container.__smart_str(keys[-1], num_indent + 1) + ' ' + str(type(d[keys[-1]]))
        out += '\n' + indent + right_br
        return out


class Data(Container):

    @docval({'name': 'name', 'type': str, 'doc': 'the name of this container'},
            {'name': 'data', 'type': ('array_data', 'data'), 'doc': 'the source of the data'})
    def __init__(self, **kwargs):
        call_docval_func(super(Data, self).__init__, kwargs)
        self.__data = getargs('data', kwargs)

    @property
    def data(self):
        return self.__data

    def __bool__(self):
        if not hasattr(self.data, '__len__'):
            raise NotImplementedError('__bool__ must be implemented when data has no __len__')
        return len(self.data) != 0

    def __len__(self):
        return len(self.__data)

    def __getitem__(self, args):
        if isinstance(self.data, (tuple, list)) and isinstance(args, (tuple, list)):
            return [self.data[i] for i in args]
        return self.data[args]

    def append(self, arg):
        if isinstance(self.data, list):
            self.data.append(arg)
        elif isinstance(self.data, np.ndarray):
            self.__data = np.append(self.__data, [arg])
        else:
            msg = "NWBData cannot append to object of type '%s'" % type(self.__data)
            raise ValueError(msg)

    def extend(self, arg):
        if isinstance(self.data, list):
            self.data.extend(arg)
        elif isinstance(self.data, np.ndarray):
            self.__data = np.append(self.__data, [arg])
        else:
            msg = "NWBData cannot extend object of type '%s'" % type(self.__data)
            raise ValueError(msg)


class DataRegion(Data):

    @property
    @abstractmethod
    def data(self):
        '''
        The target data that this region applies to
        '''
        pass

    @property
    @abstractmethod
    def region(self):
        '''
        The region that indexes into data e.g. slice or list of indices
        '''
        pass
