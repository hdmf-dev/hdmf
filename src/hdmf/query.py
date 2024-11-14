from abc import ABCMeta, abstractmethod

import numpy as np

from .utils import ExtenderMeta, docval_macro, docval, getargs


@docval_macro('array_data')
class HDMFDataset(metaclass=ExtenderMeta):
    def __evaluate_key(self, key):
        if isinstance(key, tuple) and len(key) == 0:
            return key
        if isinstance(key, (tuple, list, np.ndarray)):
            return list(map(self.__evaluate_key, key))
        else:
            return key

    def __getitem__(self, key):
        idx = self.__evaluate_key(key)
        return self.dataset[idx]

    @docval({'name': 'dataset', 'type': 'array_data', 'doc': 'the HDF5 file lazily evaluate'})
    def __init__(self, **kwargs):
        super().__init__()
        self.__dataset = getargs('dataset', kwargs)

    @property
    def dataset(self):
        return self.__dataset

    @property
    def dtype(self):
        return self.__dataset.dtype

    def __len__(self):
        return len(self.__dataset)

    def __iter__(self):
        return iter(self.dataset)

    def __next__(self):
        return next(self.dataset)

    def next(self):
        return self.dataset.next()

    def append(self, arg):
        """
        Override this method to support appending to backend-specific datasets
        """
        pass # pragma: no cover


class ReferenceResolver(metaclass=ABCMeta):
    """
    A base class for classes that resolve references
    """

    @classmethod
    @abstractmethod
    def get_inverse_class(cls):
        """
        Return the class the represents the ReferenceResolver
        that resolves references to the opposite type.

        BuilderResolver.get_inverse_class should return a class
        that subclasses ContainerResolver.

        ContainerResolver.get_inverse_class should return a class
        that subclasses BuilderResolver.
        """
        pass

    @abstractmethod
    def invert(self):
        """
        Return an object that defers reference resolution
        but in the opposite direction.
        """
        pass


class BuilderResolver(ReferenceResolver):
    """
    A reference resolver that resolves references to Builders

    Subclasses should implement the invert method and the get_inverse_class
    classmethod

    BuilderResolver.get_inverse_class should return a class that subclasses
    ContainerResolver.
    """

    pass


class ContainerResolver(ReferenceResolver):
    """
    A reference resolver that resolves references to Containers

    Subclasses should implement the invert method and the get_inverse_class
    classmethod

    ContainerResolver.get_inverse_class should return a class that subclasses
    BuilderResolver.
    """

    pass
