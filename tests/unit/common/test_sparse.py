from hdmf.common import CSRMatrix
from hdmf.testing import TestCase, H5RoundTripMixin

import scipy.sparse as sps
import numpy as np
# TODO more unit tests are needed for CSRMatrix


class TestCSRMatrix(TestCase):

    def test_from_sparse_matrix(self):
        data = np.array([1, 2, 3, 4, 5, 6])
        indices = np.array([0, 2, 2, 0, 1, 2])
        indptr = np.array([0, 2, 3, 6])
        expected = CSRMatrix(data, indices, indptr, (3, 3))

        sps_mat = sps.csr_matrix((data, indices, indptr), shape=(3, 3))
        received = CSRMatrix(sps_mat)
        self.assertContainerEqual(received, expected, ignore_hdmf_attrs=True)

    def test_to_spmat(self):
        data = np.array([1, 2, 3, 4, 5, 6])
        indices = np.array([0, 2, 2, 0, 1, 2])
        indptr = np.array([0, 2, 3, 6])
        csr_mat = CSRMatrix(data, indices, indptr, (3, 3))
        spmat_array = csr_mat.to_spmat().toarray()

        expected = np.asarray([[1, 0, 2], [0, 0, 3], [4, 5, 6]])
        np.testing.assert_array_equal(spmat_array, expected)

    def test_from_dense_2D_array(self):
        data = np.array([[1, 0, 2], [0, 0, 3], [4, 5, 6]])
        csr_mat = CSRMatrix(data=data)
        spmat_array = csr_mat.to_spmat().toarray()
        np.testing.assert_array_equal(spmat_array, data)

    def test_valueerror_from_ndarray(self):
        msg = "cannot use ndarray of dimensionality > 2"
        with self.assertRaisesWith(ValueError, msg):
            data = np.arange(27).reshape((3, 3, 3))
            _ = CSRMatrix(data=data)

    def test_valueerror_missing_indptr_indices_or_shape(self):
        data = np.arange(10)
        msg = "must specify indptr, indices, and shape when passing data array"
        with self.assertRaisesWith(ValueError, msg):
            _ = CSRMatrix(data=data)  # indptr, indices, and shape are missing
        with self.assertRaisesWith(ValueError, msg):
            _ = CSRMatrix(data=data, indptr=data, indices=data)  # shape missing
        with self.assertRaisesWith(ValueError, msg):
            _ = CSRMatrix(data=data, indptr=data, shape=(2, 5))  # indices missing
        with self.assertRaisesWith(ValueError, msg):
            _ = CSRMatrix(data=data, indices=data, shape=(2, 5))  # indptr missing

    def test_valueerror_non_2D_shape(self):
        data = np.array([1, 2, 3, 4, 5, 6])
        indices = np.array([0, 2, 2, 0, 1, 2])
        indptr = np.array([0, 2, 3, 6])
        with self.assertRaisesWith(ValueError, 'shape must specify two and only two dimensions'):
            _ = CSRMatrix(data, indices, indptr, (3, 3, 1))
        with self.assertRaisesWith(ValueError, 'shape must specify two and only two dimensions'):
            _ = CSRMatrix(data, indices, indptr, (9, ))

    def test_valueerror_non_1d_indptr_or_indicies(self):
        data = np.array([1, 2, 3, 4, 5, 6])
        indices = np.array([0, 2, 2, 0, 1, 2])
        indptr = np.array([0, 2, 3, 6])
        with self.assertRaisesWith(ValueError, 'indices must be a 1D array of integers. Found 2D array'):
            _ = CSRMatrix(data, indices.reshape((3, 2)), indptr, (3, 3))
        with self.assertRaisesWith(ValueError, 'indptr must be a 1D array of integers. Found 2D array'):
            _ = CSRMatrix(data, indices, indptr.reshape((2, 2)), (3, 3))

    def test_valueerror_non_int_indptr_or_indicies(self):
        data = np.array([1, 2, 3, 4, 5, 6])
        indices = np.array([0, 2, 2, 0, 1, 2])
        indptr = np.array([0, 2, 3, 6])
        # test indices numpy array of floats
        with self.assertRaisesWith(ValueError, 'indices must be a 1D array of integers. Found 1D array of float64'):
            _ = CSRMatrix(data, indices.astype(float), indptr, (3, 3))
        # test indptr numpy array of floats
        with self.assertRaisesWith(ValueError, 'indptr must be a 1D array of integers. Found 1D array of float64'):
            _ = CSRMatrix(data, indices, indptr.astype(float), (3, 3))
        # test indices list of floats
        with self.assertRaisesWith(ValueError, 'indices must be a 1D array of integers. Found 1D array of float64'):
            _ = CSRMatrix(data, indices.astype(float).tolist(), indptr, (3, 3))
        # test indptr list of floats
        with self.assertRaisesWith(ValueError, 'indptr must be a 1D array of integers. Found 1D array of float64'):
            _ = CSRMatrix(data, indices, indptr.astype(float).tolist(), (3, 3))


class TestCSRMatrixRoundTrip(H5RoundTripMixin, TestCase):

    def setUpContainer(self):
        data = np.array([1, 2, 3, 4, 5, 6])
        indices = np.array([0, 2, 2, 0, 1, 2])
        indptr = np.array([0, 2, 3, 6])
        return CSRMatrix(data, indices, indptr, (3, 3))


class TestCSRMatrixRoundTripFromLists(H5RoundTripMixin, TestCase):
    """Test that CSRMatrix works with lists as well"""

    def setUpContainer(self):
        data = [1, 2, 3, 4, 5, 6]
        indices = [0, 2, 2, 0, 1, 2]
        indptr = [0, 2, 3, 6]
        return CSRMatrix(data, indices, indptr, (3, 3))
