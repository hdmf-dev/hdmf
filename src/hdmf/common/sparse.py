import scipy.sparse as sps
import numpy as np
import h5py

from ..container import Container
from ..utils import docval, getargs, call_docval_func

from . import register_class


@register_class('CSRMatrix')
class CSRMatrix(Container):

    __fields__ = ({'name': 'data', 'settable': False},
                  {'name': 'indices', 'settable': False},
                  {'name': 'indptr', 'settable': False},
                  {'name': 'shape', 'settable': False})

    @docval({'name': 'data', 'type': (sps.csr_matrix, np.ndarray, h5py.Dataset),
             'doc': 'the data to use for this CSRMatrix or CSR data array.'
                    'If passing CSR data array, *indices*, *indptr*, and *shape* must also be provided'},
            {'name': 'indices', 'type': (np.ndarray, h5py.Dataset), 'doc': 'CSR index array', 'default': None},
            {'name': 'indptr', 'type': (np.ndarray, h5py.Dataset), 'doc': 'CSR index pointer array', 'default': None},
            {'name': 'shape', 'type': (list, tuple, np.ndarray), 'doc': 'the shape of the matrix', 'default': None},
            {'name': 'name', 'type': str, 'doc': 'the name to use for this when storing', 'default': 'csr_matrix'})
    def __init__(self, **kwargs):
        call_docval_func(super().__init__, kwargs)
        data = getargs('data', kwargs)
        if isinstance(data, (np.ndarray, h5py.Dataset)):
            if data.ndim == 2:
                data = sps.csr_matrix(self.data)
            elif data.ndim == 1:
                indptr, indices, shape = getargs('indptr', 'indices', 'shape', kwargs)
                if any(_ is None for _ in (indptr, indices, shape)):
                    raise ValueError("must specify indptr, indices, and shape when passing data array")
                self.__check_ind(indptr, 'indptr')
                self.__check_ind(indices, 'indices')
                if len(shape) != 2:
                    raise ValueError('shape must specify two and only two dimensions')
                data = sps.csr_matrix((data, indices, indptr), shape=shape)
            else:
                raise ValueError("cannot use ndarray of dimensionality > 2")
        self.fields['data'] = data
        self.fields['indices'] = data.indices
        self.fields['indptr'] = data.indptr
        self.fields['shape'] = data.shape

    @staticmethod
    def __check_ind(ar, arg):
        if not (ar.ndim == 1 or np.issubdtype(ar.dtype, int)):
            raise ValueError('%s must be a 1D array of integers' % arg)

    @property
    def data(self):
        # override auto-generated getter
        return self.fields['data'].data

    def to_spmat(self):
        return self.fields['data']

    def __repr__(self):
        cls = self.__class__
        template = "%s %s.%s at 0x%d" % (self.name, cls.__module__, cls.__name__, id(self))
        if len(self.fields):
            template += "\nFields:\n"
        for k in sorted(self.fields):  # sorted to enable tests
            v = self.fields[k]
            if k == 'data':
                template += "  {}:\n{}\n".format(k, v)
            else:
                template += "  {}: {}\n".format(k, v)
        return template
