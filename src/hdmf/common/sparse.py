import h5py
import numpy as np
import scipy.sparse as sps

from . import register_class
from ..container import Container
from ..utils import docval, getargs, call_docval_func, to_uint_array


@register_class('CSRMatrix')
class CSRMatrix(Container):

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
                data = sps.csr_matrix(data)
            elif data.ndim < 2:
                indptr, indices, shape = getargs('indptr', 'indices', 'shape', kwargs)
                if any(_ is None for _ in (indptr, indices, shape)):
                    raise ValueError("Must specify 'indptr', 'indices', and 'shape' arguments when passing data array.")
                indptr = self.__check_arr(indptr, 'indptr')
                indices = self.__check_arr(indices, 'indices')
                shape = self.__check_arr(shape, 'shape')
                if len(shape) != 2:
                    raise ValueError("'shape' argument must specify two and only two dimensions.")
                data = sps.csr_matrix((data, indices, indptr), shape=shape)
            else:
                raise ValueError("'data' argument cannot be ndarray of dimensionality > 2.")
        self.__data = data

    @staticmethod
    def __check_arr(ar, arg):
        try:
            ar = to_uint_array(ar)
        except ValueError as ve:
            raise ValueError("Cannot convert '%s' to an array of unsigned integers." % arg) from ve
        if ar.ndim != 1:
            raise ValueError("'%s' must be a 1D array of unsigned integers." % arg)
        return ar

    def __getattr__(self, val):
        # NOTE: this provides access to self.data, self.indices, self.indptr, self.shape
        attr = getattr(self.__data, val)
        if val in ('indices', 'indptr', 'shape'):  # needed because sps.csr_matrix may contain int arrays for these
            attr = to_uint_array(attr)
        return attr

    def to_spmat(self):
        return self.__data
