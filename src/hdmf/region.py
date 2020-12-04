from abc import ABCMeta, abstractmethod
from operator import itemgetter

from .container import Data, DataRegion
from .utils import docval, getargs


class RegionSlicer(DataRegion, metaclass=ABCMeta):
    '''
    A abstract base class to control getting using a region

    Subclasses must implement `__getitem__` and `__len__`
    '''

    @docval({'name': 'target', 'type': None, 'doc': 'the target to slice'},
            {'name': 'slice', 'type': None, 'doc': 'the region to slice'})
    def __init__(self, **kwargs):
        self.__target = getargs('target', kwargs)
        self.__slice = getargs('slice', kwargs)

    @property
    def data(self):
        """The target data. Same as self.target"""
        return self.target

    @property
    def region(self):
        """The selected region. Same as self.slice"""
        return self.slice

    @property
    def target(self):
        """The target data"""
        return self.__target

    @property
    def slice(self):
        """The selected slice"""
        return self.__slice

    @property
    @abstractmethod
    def __getitem__(self, idx):
        """Must be implemented by subclasses"""
        pass

    @property
    @abstractmethod
    def __len__(self):
        """Must be implemented by subclasses"""
        pass


class ListSlicer(RegionSlicer):
    """Implementation of RegionSlicer for slicing Lists and Data"""

    @docval({'name': 'dataset', 'type': (list, tuple, Data), 'doc': 'the dataset to slice'},
            {'name': 'region', 'type': (list, tuple, slice), 'doc': 'the region reference to use to slice'})
    def __init__(self, **kwargs):
        self.__dataset, self.__region = getargs('dataset', 'region', kwargs)
        super().__init__(self.__dataset, self.__region)
        if isinstance(self.__region, slice):
            self.__getter = itemgetter(self.__region)
            self.__len = len(range(*self.__region.indices(len(self.__dataset))))
        else:
            self.__getter = itemgetter(*self.__region)
            self.__len = len(self.__region)

    def __read_region(self):
        """
        Internal helper function used to define self._read
        """
        if not hasattr(self, '_read'):
            self._read = self.__getter(self.__dataset)
            del self.__getter

    def __getitem__(self, idx):
        """
        Get data values from selected data
        """
        self.__read_region()
        getter = None
        if isinstance(idx, (list, tuple)):
            getter = itemgetter(*idx)
        else:
            getter = itemgetter(idx)
        return getter(self._read)

    def __len__(self):
        """Number of values in the slice/region"""
        return self.__len
