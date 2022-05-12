from hdmf.common import CSRMatrix
from hdmf.testing import TestCase, H5RoundTripMixin

import scipy.sparse as sps
import numpy as np


class TestCSRMatrix(TestCase):

    def test_from_sparse_matrix(self):
        data = np.array([1, 2, 3, 4, 5, 6])
        indices = np.array([0, 2, 2, 0, 1, 2])
        indptr = np.array([0, 2, 3, 6])
        shape = (3, 3)
        expected = CSRMatrix(data=data, indices=indices, indptr=indptr, shape=shape)

        sps_mat = sps.csr_matrix((data, indices, indptr), shape=shape)
        received = CSRMatrix(data=sps_mat)
        self.assertContainerEqual(received, expected, ignore_hdmf_attrs=True)

    def test_2d_data(self):
        data = np.array([[1, 0, 2], [0, 0, 3], [4, 5, 6]])
        csr_mat = CSRMatrix(data=data)
        sps_mat = sps.csr_matrix(data)
        np.testing.assert_array_equal(csr_mat.data, sps_mat.data)

    def test_getattrs(self):
        data = np.array([1, 2, 3, 4, 5, 6])
        indices = np.array([0, 2, 2, 0, 1, 2], dtype=np.int32)
        indptr = np.array([0, 2, 3, 6], dtype=np.int32)
        shape = (3, 3)
        csr_mat = CSRMatrix(data=data, indices=indices, indptr=indptr, shape=shape)

        np.testing.assert_array_equal(data, csr_mat.data)
        np.testing.assert_array_equal(indices, csr_mat.indices)
        np.testing.assert_array_equal(indptr, csr_mat.indptr)
        np.testing.assert_array_equal(shape, csr_mat.shape)
        self.assertEqual(csr_mat.indices.dtype.type, np.uint32)
        self.assertEqual(csr_mat.indptr.dtype.type, np.uint32)
        # NOTE: shape is stored internally in scipy.sparse.spmat as a tuple of ints. this is then converted to ndarray
        # but precision differs by OS
        self.assertTrue(np.issubdtype(csr_mat.shape.dtype.type, np.unsignedinteger))

    def test_to_spmat(self):
        data = np.array([1, 2, 3, 4, 5, 6])
        indices = np.array([0, 2, 2, 0, 1, 2])
        indptr = np.array([0, 2, 3, 6])
        shape = (3, 3)
        csr_mat = CSRMatrix(data=data, indices=indices, indptr=indptr, shape=shape)
        spmat_array = csr_mat.to_spmat().toarray()

        expected = np.asarray([[1, 0, 2], [0, 0, 3], [4, 5, 6]])
        np.testing.assert_array_equal(spmat_array, expected)

    def test_from_dense_2D_array(self):
        data = np.array([[1, 0, 2], [0, 0, 3], [4, 5, 6]])
        csr_mat = CSRMatrix(data=data)
        spmat_array = csr_mat.to_spmat().toarray()
        np.testing.assert_array_equal(spmat_array, data)

    def test_valueerror_from_ndarray(self):
        msg = "'data' argument cannot be ndarray of dimensionality > 2."
        with self.assertRaisesWith(ValueError, msg):
            data = np.arange(27).reshape((3, 3, 3))
            _ = CSRMatrix(data=data)

    def test_valueerror_missing_indptr_indices_or_shape(self):
        data = np.arange(10)
        msg = "Must specify 'indptr', 'indices', and 'shape' arguments when passing data array."
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
        with self.assertRaisesWith(ValueError, "'shape' argument must specify two and only two dimensions."):
            _ = CSRMatrix(data=data, indices=indices, indptr=indptr, shape=(3, 3, 1))
        with self.assertRaisesWith(ValueError, "'shape' argument must specify two and only two dimensions."):
            _ = CSRMatrix(data=data, indices=indices, indptr=indptr, shape=(9, ))

    def test_valueerror_non_1d_indptr_or_indicies(self):
        data = np.array([1, 2, 3, 4, 5, 6])
        indices = np.array([0, 2, 2, 0, 1, 2])
        indptr = np.array([0, 2, 3, 6])
        with self.assertRaisesWith(ValueError,  "'indices' must be a 1D array of unsigned integers."):
            _ = CSRMatrix(data=data, indices=indices.reshape((3, 2)), indptr=indptr, shape=(3, 3))
        with self.assertRaisesWith(ValueError,  "'indptr' must be a 1D array of unsigned integers."):
            _ = CSRMatrix(data=data, indices=indices, indptr=indptr.reshape((2, 2)), shape=(3, 3))

    def test_valueerror_non_int_indptr_or_indicies(self):
        data = np.array([1, 2, 3, 4, 5, 6])
        indices = np.array([0, 2, 2, 0, 1, 2])
        indptr = np.array([0, 2, 3, 6])
        # test indices numpy array of floats
        with self.assertRaisesWith(ValueError, "Cannot convert 'indices' to an array of unsigned integers."):
            _ = CSRMatrix(data=data, indices=indices.astype(float), indptr=indptr, shape=(3, 3))
        # test indptr numpy array of floats
        with self.assertRaisesWith(ValueError, "Cannot convert 'indptr' to an array of unsigned integers."):
            _ = CSRMatrix(data=data, indices=indices, indptr=indptr.astype(float), shape=(3, 3))
        # test indices list of floats
        with self.assertRaisesWith(ValueError, "Cannot convert 'indices' to an array of unsigned integers."):
            _ = CSRMatrix(data=data, indices=indices.astype(float).tolist(), indptr=indptr, shape=(3, 3))
        # test indptr list of floats
        with self.assertRaisesWith(ValueError, "Cannot convert 'indptr' to an array of unsigned integers."):
            _ = CSRMatrix(data=data, indices=indices, indptr=indptr.astype(float).tolist(), shape=(3, 3))

    def test_constructor_indices_missing(self):
        data = np.array([1, 2, 3, 4, 5, 6])
        msg = "Must specify 'indptr', 'indices', and 'shape' arguments when passing data array."
        with self.assertRaisesWith(ValueError, msg):
            CSRMatrix(data=data)

    def test_constructor_bad_indices(self):
        data = np.array([1, 2, 3, 4, 5, 6])
        indices = np.array([0, -2, 2, 0, 1, 2])
        indptr = np.array([0, 2, 3, 6])
        shape = (3, 3)
        msg = "Cannot convert 'indices' to an array of unsigned integers."
        with self.assertRaisesWith(ValueError, msg):
            CSRMatrix(data=data, indices=indices, indptr=indptr, shape=shape)

    def test_constructor_bad_indices_dim(self):
        data = np.array([1, 2, 3, 4, 5, 6])
        indices = np.array([[0, 2, 2, 0, 1, 2]])
        indptr = np.array([0, 2, 3, 6])
        shape = (3, 3)
        msg = "'indices' must be a 1D array of unsigned integers."
        with self.assertRaisesWith(ValueError, msg):
            CSRMatrix(data=data, indices=indices, indptr=indptr, shape=shape)

    def test_constructor_bad_shape(self):
        data = np.array([1, 2, 3, 4, 5, 6])
        indices = np.array([0, 2, 2, 0, 1, 2])
        indptr = np.array([0, 2, 3, 6])
        shape = (3, )
        msg = "'shape' argument must specify two and only two dimensions."
        with self.assertRaisesWith(ValueError, msg):
            CSRMatrix(data=data, indices=indices, indptr=indptr, shape=shape)

    def test_array_bad_dim(self):
        data = np.array([[[1, 2], [3, 4], [5, 6]]])
        indices = np.array([0, 2, 2, 0, 1, 2])
        indptr = np.array([0, 2, 3, 6])
        shape = (3, 3)
        msg = "'data' argument cannot be ndarray of dimensionality > 2."
        with self.assertRaisesWith(ValueError, msg):
            CSRMatrix(data=data, indices=indices, indptr=indptr, shape=shape)


class TestCSRMatrixRoundTrip(H5RoundTripMixin, TestCase):

    def setUpContainer(self):
        data = np.array([1, 2, 3, 4, 5, 6])
        indices = np.array([0, 2, 2, 0, 1, 2])
        indptr = np.array([0, 2, 3, 6])
        shape = (3, 3)
        return CSRMatrix(data=data, indices=indices, indptr=indptr, shape=shape)


class TestCSRMatrixRoundTripFromLists(H5RoundTripMixin, TestCase):
    """Test that CSRMatrix works with lists as well"""

    def setUpContainer(self):
        data = [1, 2, 3, 4, 5, 6]
        indices = [0, 2, 2, 0, 1, 2]
        indptr = [0, 2, 3, 6]
        shape = (3, 3)
        return CSRMatrix(data=data, indices=indices, indptr=indptr, shape=shape)
