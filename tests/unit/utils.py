import tempfile
from copy import copy, deepcopy

from hdmf.container import Container
from hdmf.spec import GroupSpec, DatasetSpec, SpecNamespace
from hdmf.utils import docval, getargs, get_docval

CORE_NAMESPACE = 'test_core'


class Foo(Container):

    @docval({'name': 'name', 'type': str, 'doc': 'the name of this Foo'},
            {'name': 'my_data', 'type': ('array_data', 'data'), 'doc': 'some data'},
            {'name': 'attr1', 'type': str, 'doc': 'an attribute'},
            {'name': 'attr2', 'type': int, 'doc': 'another attribute'},
            {'name': 'attr3', 'type': float, 'doc': 'a third attribute', 'default': 3.14})
    def __init__(self, **kwargs):
        name, my_data, attr1, attr2, attr3 = getargs('name', 'my_data', 'attr1', 'attr2', 'attr3', kwargs)
        super().__init__(name=name)
        self.__data = my_data
        self.__attr1 = attr1
        self.__attr2 = attr2
        self.__attr3 = attr3

    def __eq__(self, other):
        attrs = ('name', 'my_data', 'attr1', 'attr2', 'attr3')
        return all(getattr(self, a) == getattr(other, a) for a in attrs)

    def __str__(self):
        attrs = ('name', 'my_data', 'attr1', 'attr2', 'attr3')
        return '<' + ','.join('%s=%s' % (a, getattr(self, a)) for a in attrs) + '>'

    @property
    def my_data(self):
        return self.__data

    @property
    def attr1(self):
        return self.__attr1

    @property
    def attr2(self):
        return self.__attr2

    @property
    def attr3(self):
        return self.__attr3

    def __hash__(self):
        return hash(self.name)


class FooBucket(Container):

    @docval({'name': 'name', 'type': str, 'doc': 'the name of this bucket'},
            {'name': 'foos', 'type': list, 'doc': 'the Foo objects in this bucket', 'default': list()})
    def __init__(self, **kwargs):
        name, foos = getargs('name', 'foos', kwargs)
        super().__init__(name=name)
        self.__foos = {f.name: f for f in foos}  # note: collections of groups are unordered in HDF5
        for f in foos:
            f.parent = self

    def __eq__(self, other):
        return self.name == other.name and self.foos == other.foos

    def __str__(self):
        return 'name=%s, foos=%s' % (self.name, self.foos)

    @property
    def foos(self):
        return self.__foos

    def remove_foo(self, foo_name):
        foo = self.__foos.pop(foo_name)
        if foo.parent is self:
            self._remove_child(foo)
        return foo


def get_temp_filepath():
    # On Windows, h5py cannot truncate an open file in write mode.
    # The temp file will be closed before h5py truncates it and will be removed during the tearDown step.
    temp_file = tempfile.NamedTemporaryFile()
    temp_file.close()
    return temp_file.name


# ##### custom spec classes #####

def swap_inc_def(cls, custom_cls):
    args = get_docval(cls.__init__)
    ret = list()
    for arg in args:
        if arg['name'] == 'data_type_def':
            ret.append({'name': 'my_data_type_def', 'type': str,
                        'doc': 'the NWB data type this spec defines', 'default': None})
        elif arg['name'] == 'data_type_inc':
            ret.append({'name': 'my_data_type_inc', 'type': (custom_cls, str),
                        'doc': 'the NWB data type this spec includes', 'default': None})
        else:
            ret.append(copy(arg))
    return ret


class BaseStorageOverride:
    __type_key = 'my_data_type'
    __inc_key = 'my_data_type_inc'
    __def_key = 'my_data_type_def'

    @classmethod
    def type_key(cls):
        ''' Get the key used to store data type on an instance'''
        return cls.__type_key

    @classmethod
    def inc_key(cls):
        ''' Get the key used to define a data_type include.'''
        return cls.__inc_key

    @classmethod
    def def_key(cls):
        ''' Get the key used to define a data_type definition.'''
        return cls.__def_key

    @classmethod
    def build_const_args(cls, spec_dict):
        """Extend base functionality to remap data_type_def and data_type_inc keys"""
        spec_dict = copy(spec_dict)
        proxy = super(BaseStorageOverride, cls)
        if proxy.inc_key() in spec_dict:
            spec_dict[cls.inc_key()] = spec_dict.pop(proxy.inc_key())
        if proxy.def_key() in spec_dict:
            spec_dict[cls.def_key()] = spec_dict.pop(proxy.def_key())
        ret = proxy.build_const_args(spec_dict)
        return ret

    @classmethod
    def _translate_kwargs(cls, kwargs):
        """Swap mydata_type_def and mydata_type_inc for data_type_def and data_type_inc, respectively"""
        proxy = super(BaseStorageOverride, cls)
        kwargs[proxy.def_key()] = kwargs.pop(cls.def_key())
        kwargs[proxy.inc_key()] = kwargs.pop(cls.inc_key())
        return kwargs


class CustomGroupSpec(BaseStorageOverride, GroupSpec):

    @docval(*deepcopy(swap_inc_def(GroupSpec, 'CustomGroupSpec')))
    def __init__(self, **kwargs):
        kwargs = self._translate_kwargs(kwargs)
        super().__init__(**kwargs)

    @classmethod
    def dataset_spec_cls(cls):
        return CustomDatasetSpec

    @docval(*deepcopy(swap_inc_def(GroupSpec, 'CustomGroupSpec')))
    def add_group(self, **kwargs):
        spec = CustomGroupSpec(**kwargs)
        self.set_group(spec)
        return spec

    @docval(*deepcopy(swap_inc_def(DatasetSpec, 'CustomDatasetSpec')))
    def add_dataset(self, **kwargs):
        ''' Add a new specification for a subgroup to this group specification '''
        spec = CustomDatasetSpec(**kwargs)
        self.set_dataset(spec)
        return spec


class CustomDatasetSpec(BaseStorageOverride, DatasetSpec):

    @docval(*deepcopy(swap_inc_def(DatasetSpec, 'CustomDatasetSpec')))
    def __init__(self, **kwargs):
        kwargs = self._translate_kwargs(kwargs)
        super().__init__(**kwargs)


class CustomSpecNamespace(SpecNamespace):
    __types_key = 'my_data_types'

    @classmethod
    def types_key(cls):
        return cls.__types_key
