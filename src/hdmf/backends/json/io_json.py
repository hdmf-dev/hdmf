from ..io import HDMFIO
from hdmf.build import BuildManager, GroupBuilder, DatasetBuilder, TypeMap
from hdmf.common import get_type_map
from hdmf.common.resources import *
from ...utils import docval, getargs, popargs, get_docval
import numpy as np
import json

class JSONIO(HDMFIO):
    @docval(*get_docval(HDMFIO.__init__, 'source'),
            {'name': 'type_map', 'type': TypeMap, 'default': None,
             'doc': 'The type map. If None is provided, the HDMF-common type map will be used.'})
    def __init__(self, **kwargs):
        if kwargs['type_map'] is not None:
            self.type_map =  popargs('type_map', kwargs)
        else:
            self.type_map = get_type_map()
        # super().__init__(**kwargs)
        path = kwargs['source']
        manager = BuildManager(self.type_map)
        super().__init__(manager, source=path)


    @docval(returns='a GroupBuilder representing the read data', rtype='GroupBuilder')
    def read_builder(self, **kwargs):
        ''' Read data and return the GroupBuilder representing it '''


    @docval({'name': 'builder', 'type': GroupBuilder, 'doc': 'the GroupBuilder object representing the Container'},
            allow_extra=True)
    def write_builder(self, **kwargs):
        ''' Write a GroupBuilder representing an Container object '''
        builder = kwargs['builder']
        with open('ER.json', 'w') as f:
            json.dump(builder, f)

    def open(self):
        ''' Open this HDMFIO object for writing of the builder '''
        pass

    def close(self):
        ''' Close this HDMFIO object to further reading/writing'''
        pass

    def read_to_dict(self, **kwargs):
        path = kwargs['path']
        file = open(path)
        loaded_dict = json.load(file)
        return loaded_dict

    def create_er(self, **kwargs):
        path = kwargs['path']
        name = kwargs['name']
        loaded_dict=self.read_to_dict(path=path)

        keys = KeyTable().from_dataframe(pd.DataFrame(loaded_dict['keys']['data'], columns=KeyTable().columns))
        resources = ResourceTable().from_dataframe(pd.DataFrame(loaded_dict['resources']['data'], columns=ResourceTable().columns))
        entities = EntityTable().from_dataframe(pd.DataFrame(loaded_dict['entities']['data'], columns=EntityTable().columns))
        objects = ObjectTable().from_dataframe(pd.DataFrame(loaded_dict['objects']['data'], columns=ObjectTable().columns))
        object_keys = ObjectKeyTable().from_dataframe(pd.DataFrame(loaded_dict['object_keys']['data'], columns=ObjectKeyTable().columns))

        er = ExternalResources(name=name, keys=keys, resources=resources, objects=objects, object_keys=object_keys, entities=entities)

        return er
