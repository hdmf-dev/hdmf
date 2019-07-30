import abc
from uuid import uuid4
from six import with_metaclass
from .utils import docval, getargs, ExtenderMeta
from warnings import warn


class Container(with_metaclass(ExtenderMeta, object)):

    _fieldsname = '__fields__'

    # @docval({'name': 'container_source', 'type': str, 'doc': 'source of this Container', 'default': None},
    #         {'name': 'object_id', 'type': str, 'doc': 'UUID4 unique identifier for this Container', 'default': None},
    #         {'name': 'parent', 'type': str, 'doc': 'parent Container for this Container', 'default': None})
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

    def __repr__(self):
        return "<%s '%s' at 0x%d>" % (self.__class__.__name__, self.name, id(self))

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
