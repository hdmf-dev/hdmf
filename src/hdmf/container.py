import abc
from six import with_metaclass
from .utils import docval, getargs, ExtenderMeta, get_docval, fmt_docval_args, popargs, call_docval_func
from warnings import warn


class Container(with_metaclass(ExtenderMeta, object)):

    _fieldsname = '__fields__'

    @docval({'name': 'name', 'type': str, 'doc': 'the name of this container'},
            {'name': 'parent', 'type': 'Container', 'doc': 'the Container that holds this Container', 'default': None},
            {'name': 'container_source', 'type': str, 'doc': 'the source of this container', 'default': None})
    def __init__(self, **kwargs):
        name = getargs('name', kwargs)
        if '/' in name:
            raise ValueError("name '" + name + "' cannot contain '/'")
        self.__name = name
        self.__parent = getargs('parent', kwargs)
        self.__container_source = getargs('container_source', kwargs)
        self.__children = list()
        self.__modified = True

    def __repr__(self):
        return "<%s '%s' at 0x%d>" % (self.__class__.__name__, self.name, id(self))

    @property
    def modified(self):
        return self.__modified

    @docval({'name': 'modified', 'type': bool,
             'doc': 'whether or not this Container has been modified', 'default': True})
    def set_modified(self, **kwargs):
        modified = getargs('modified', kwargs)
        self.__modified = modified
        if modified and self.parent is not None:
            self.parent.set_modified()

    @property
    def children(self):
        return tuple(self.__children)

    @docval({'name': 'child', 'type': 'Container',
             'doc': 'the child Container for this Container', 'default': None})
    def add_child(self, **kwargs):
        child = getargs('child', kwargs)
        if child is not None:
            # if child.parent is a Container, then the mismatch between child.parent and parent
            # is used to make a soft/external link from the parent to a child elsewhere
            # if child.parent is not a Container, it is either None or a Proxy and should be set to self
            if not isinstance(child.parent, Container):
                self.__children.append(child)
                self.set_modified()
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
        return self.__parent

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
                else:
                    self.__parent.add_candidate(parent_container)
        else:
            self.__parent = parent_container


class Data(Container):

    @abc.abstractproperty
    def data(self):
        '''
        The data that is held by this Container
        '''
        pass

    def __nonzero__(self):
        return len(self.data) != 0


class DataRegion(Data):

    @abc.abstractproperty
    def data(self):
        '''
        The target data that this region applies to
        '''
        pass

    @abc.abstractproperty
    def region(self):
        '''
        The region that indexes into data e.g. slice or list of indices
        '''
        pass

class LabelledDict(dict):
    '''
    A dict wrapper class for aggregating Timeseries
    from the standard locations
    '''

    @docval({'name': 'label', 'type': str, 'doc': 'the TimeSeries type ('})
    def __init__(self, **kwargs):
        label = getargs('label', kwargs)
        self.__label = label

    @property
    def label(self):
        return self.__label

    def __getitem__(self, args):
        key = args
        if '==' in args:
            key, val = args.split("==")
            key = key.strip()
            val = val.strip()
            if key != 'name':
                ret = list()
                for item in self.values():
                    if getattr(item, key, None) == val:
                        ret.append(item)
                return ret if len(ret) else None
            key = val
        return super(LabelledDict, self).__getitem__(key)


class MultiContainerInterface(Container):
    '''
    A class for dynamically defining a API classes that
    represent NWBDataInterfaces that contain multiple Containers
    of the same type

    To use, extend this class, and create a dictionary as a class
    attribute with the following keys:

    * 'add' to name the method for adding Container instances

    * 'create' to name the method fo creating Container instances

    * 'get' to name the method for getting Container instances

    * 'attr' to name the attribute that stores the Container instances

    * 'type' to provide the Container object type

    See LFP or Position for an example of how to use this.
    '''

    @docval(*get_docval(Container.__init__))
    def __init__(self, **kwargs):
        call_docval_func(super(MultiContainerInterface, self).__init__, kwargs)
        if isinstance(self.__clsconf__, dict):
            attr_name = self.__clsconf__['attr']
            self.fields[attr_name] = LabelledDict(attr_name)
        else:
            for d in self.__clsconf__:
                attr_name = d['attr']
                self.fields[attr_name] = LabelledDict(attr_name)

    @staticmethod
    def __add_article(noun):
        if noun[0] in ('aeiouAEIOU'):
            return 'an %s' % noun
        return 'a %s' % noun

    @classmethod
    def __make_get(cls, func_name, attr_name, container_type):
        doc = "Get %s from this %s" % (cls.__add_article(container_type.__name__), cls.__name__)

        @docval({'name': 'name', 'type': str, 'doc': 'the name of the %s' % container_type.__name__,
                 'default': None}, rtype=container_type, returns='the %s with the given name' % container_type.__name__,
                func_name=func_name, doc=doc)
        def _func(self, **kwargs):
            name = getargs('name', kwargs)
            d = getattr(self, attr_name)
            ret = None
            if name is None:
                if len(d) > 1:
                    msg = "more than one element in %s of %s '%s' -- must specify a name" % \
                          (attr_name, cls.__name__, self.name)
                    raise ValueError(msg)
                elif len(d) == 0:
                    msg = "%s of %s '%s' is empty" % (attr_name, cls.__name__, self.name)
                    raise ValueError(msg)
                elif len(d) == 1:
                    for v in d.values():
                        ret = v
            else:
                ret = d.get(name)
                if ret is None:
                    msg = "'%s' not found in %s of %s '%s'" % (name, attr_name, cls.__name__, self.name)
                    raise KeyError(msg)
            return ret

        return _func

    @classmethod
    def __make_add(cls, func_name, attr_name, container_type):
        doc = "Add %s to this %s" % (cls.__add_article(container_type.__name__), cls.__name__)

        @docval({'name': attr_name, 'type': (list, tuple, dict, container_type),
                 'doc': 'the %s to add' % container_type.__name__},
                func_name=func_name, doc=doc)
        def _func(self, **kwargs):
            container = getargs(attr_name, kwargs)
            if isinstance(container, container_type):
                containers = [container]
            elif isinstance(container, dict):
                containers = container.values()
            else:
                containers = container
            d = getattr(self, attr_name)
            for tmp in containers:
                self.add_child(tmp)
                if tmp.name in d:
                    msg = "'%s' already exists in '%s'" % (tmp.name, self.name)
                    raise ValueError(msg)
                d[tmp.name] = tmp
            return container
        return _func

    @classmethod
    def __make_create(cls, func_name, add_name, container_type):
        doc = "Create %s and add it to this %s" % \
                       (cls.__add_article(container_type.__name__), cls.__name__)

        @docval(*filter(lambda x: x['name'] != 'parent',
                        get_docval(container_type.__init__)),
                func_name=func_name, doc=doc,
                returns="the %s object that was created" % container_type.__name__, rtype=container_type)
        def _func(self, **kwargs):
            cargs, ckwargs = fmt_docval_args(container_type.__init__, kwargs)
            ret = container_type(*cargs, **ckwargs)
            getattr(self, add_name)(ret)
            return ret
        return _func

    @classmethod
    def __make_constructor(cls, clsconf):
        args = list()
        for conf in clsconf:
            attr_name = conf['attr']
            container_type = conf['type']
            args.append({'name': attr_name, 'type': (list, tuple, dict, container_type),
                         'doc': '%s to store in this interface' % container_type.__name__, 'default': dict()})

        args.append({'name': 'name', 'type': str, 'doc': 'the name of this container', 'default': cls.__name__})

        @docval(*args, func_name='__init__')
        def _func(self, **kwargs):
            call_docval_func(super(cls, self).__init__, kwargs)
            for conf in clsconf:
                attr_name = conf['attr']
                add_name = conf['add']
                container = popargs(attr_name, kwargs)
                add = getattr(self, add_name)
                add(container)
        return _func

    @classmethod
    def __make_getitem(cls, attr_name, container_type):
        doc = "Get %s from this %s" % (cls.__add_article(container_type.__name__), cls.__name__)

        @docval({'name': 'name', 'type': str, 'doc': 'the name of the %s' % container_type.__name__,
                 'default': None}, rtype=container_type, returns='the %s with the given name' % container_type.__name__,
                func_name='__getitem__', doc=doc)
        def _func(self, **kwargs):
            name = getargs('name', kwargs)
            d = getattr(self, attr_name)
            if len(d) == 0:
                msg = "%s '%s' is empty" % (cls.__name__, self.name)
                raise ValueError(msg)
            if len(d) > 1 and name is None:
                msg = "more than one %s in this %s -- must specify a name" % container_type.__name__, cls.__name__
                raise ValueError(msg)
            ret = None
            if len(d) == 1:
                for v in d.values():
                    ret = v
            else:
                ret = d.get(name)
                if ret is None:
                    msg = "'%s' not found in %s '%s'" % (name, cls.__name__, self.name)
                    raise KeyError(msg)
            return ret

        return _func

    @classmethod
    def __make_setter(cls, nwbfield, add_name):

        @docval({'name': 'val', 'type': (list, tuple, dict), 'doc': 'the sub items to add', 'default': None})
        def nwbbt_setter(self, **kwargs):
            val = getargs('val', kwargs)
            if val is None:
                return
            getattr(self, add_name)(val)

        return nwbbt_setter

    @ExtenderMeta.pre_init
    def __build_class(cls, name, bases, classdict):
        '''
        This classmethod will be called during class declaration in the metaclass to automatically
        create setters and getters for NWB fields that need to be exported
        '''
        if not hasattr(cls, '__clsconf__'):
            return
        multi = False
        if isinstance(cls.__clsconf__, dict):
            clsconf = [cls.__clsconf__]
        elif isinstance(cls.__clsconf__, list):
            multi = True
            clsconf = cls.__clsconf__
        else:
            raise TypeError("'__clsconf__' must be a dict or a list of dicts")

        for i, d in enumerate(clsconf):
            # get add method name
            add = d.get('add')
            if add is None:
                msg = "MultiContainerInterface subclass '%s' is missing 'add' key in __clsconf__" % cls.__name__
                if multi:
                    msg += " at element %d" % i
                raise ValueError(msg)

            # get container attribute name
            attr = d.get('attr')
            if attr is None:
                msg = "MultiContainerInterface subclass '%s' is missing 'attr' key in __clsconf__" % cls.__name__
                if multi:
                    msg += " at element %d" % i
                raise ValueError(msg)

            # get container type
            container_type = d.get('type')
            if container_type is None:
                msg = "MultiContainerInterface subclass '%s' is missing 'type' key in __clsconf__" % cls.__name__
                if multi:
                    msg += " at element %d" % i
                raise ValueError(msg)

            # create property with the name given in 'attr'
            if not hasattr(cls, attr):
                aconf = cls._transform_arg(attr)
                getter = cls._getter(aconf)
                doc = "a dictionary containing the %s in this %s container" % (container_type.__name__, cls.__name__)
                setattr(cls, attr, property(getter, cls.__make_setter(aconf, add), None, doc))

            # create the add method
            setattr(cls, add, cls.__make_add(add, attr, container_type))

            # get create method name
            create = d.get('create')
            if create is not None:
                setattr(cls, create, cls.__make_create(create, add, container_type))

            get = d.get('get')
            if get is not None:
                setattr(cls, get, cls.__make_get(get, attr, container_type))

        if len(clsconf) == 1:
            setattr(cls, '__getitem__', cls.__make_getitem(attr, container_type))

        # create the constructor, only if it has not been overridden
        # i.e. it is the same method as the parent class constructor
        if cls.__init__ == MultiContainerInterface.__init__:
            setattr(cls, '__init__', cls.__make_constructor(clsconf))
