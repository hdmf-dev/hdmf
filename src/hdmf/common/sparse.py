import scipy.sparse as sps
import numpy as np

from ..container import Container
from ..utils import docval, getargs, call_docval_func, get_data_shape

from . import register_class


@register_class('CSRMatrix')
class CSRMatrix(Container):

    @docval({'name': 'data', 'type': (sps.csr_matrix, 'array_data'),
             'doc': 'the data to use for this CSRMatrix or CSR data array.'
                    'If passing CSR data array, *indices*, *indptr*, and *shape* must also be provided'},
            {'name': 'indices', 'type': 'array_data', 'doc': 'CSR index array', 'default': None},
            {'name': 'indptr', 'type': 'array_data', 'doc': 'CSR index pointer array', 'default': None},
            {'name': 'shape', 'type': 'array_data', 'doc': 'the shape of the matrix', 'default': None},
            {'name': 'name', 'type': str, 'doc': 'the name to use for this when storing', 'default': 'csr_matrix'})
    def __init__(self, **kwargs):
        call_docval_func(super().__init__, kwargs)
        data = getargs('data', kwargs)
        if not isinstance(data, sps.csr_matrix):
            temp_shape = get_data_shape(data)
            temp_ndim = len(temp_shape)
            if temp_ndim == 2:
                data = sps.csr_matrix(data)
            elif temp_ndim == 1:
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
        self.__data = data
        self.__shape = data.shape

    @staticmethod
    def __check_ind(ar, arg):
        temp_shape = get_data_shape(ar)
        temp_ndim = len(temp_shape)
        if not temp_ndim == 1:
            raise ValueError('%s must be a 1D array of integers. Found %iD array' % (arg, temp_ndim))
        elif temp_shape[0] > 1:
            temp_dtype = ar.dtype if hasattr(ar, 'dtype') else np.asarray(ar[0]).dtype
            if not (np.issubdtype(temp_dtype, np.signedinteger) or np.issubdtype(temp_dtype, np.unsignedinteger)):
                raise ValueError('%s must be a 1D array of integers. Found 1D array of %s' % (arg, str(temp_dtype)))

    def __getattr__(self, val):
        return getattr(self.__data, val)

    @property
    def shape(self):
        return self.__shape

    def to_spmat(self):
        return self.__data
