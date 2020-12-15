import numpy as np
import scipy.sparse as sps
from hdmf.common import CSRMatrix
from hdmf.testing import TestCase, H5RoundTripMixin


class TestCSRMatrix(TestCase):

    def test_from_sparse_matrix(self):
        data = np.array([1, 2, 3, 4, 5, 6])
        indices = np.array([0, 2, 2, 0, 1, 2])
        indptr = np.array([0, 2, 3, 6])
        shape = (3, 3)
        expected = CSRMatrix(data, indices, indptr, shape)

        sps_mat = sps.csr_matrix((data, indices, indptr), shape=shape)
        received = CSRMatrix(sps_mat)
        self.assertContainerEqual(received, expected, ignore_hdmf_attrs=True)

    def test_2d_data(self):
        data = np.array([[1, 0, 2], [0, 0, 3], [4, 5, 6]])
        csr_mat = CSRMatrix(data)
        sps_mat = sps.csr_matrix(data)
        np.testing.assert_array_equal(csr_mat.data, sps_mat.data)

    def test_getattrs(self):
        data = np.array([1, 2, 3, 4, 5, 6])
        indices = np.array([0, 2, 2, 0, 1, 2], dtype=np.int32)
        indptr = np.array([0, 2, 3, 6], dtype=np.int32)
        shape = (3, 3)
        csr_mat = CSRMatrix(data, indices, indptr, shape)

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
        csr_mat = CSRMatrix(data, indices, indptr, shape)
        spmat_array = csr_mat.to_spmat().toarray()

        expected = np.asarray([[1, 0, 2], [0, 0, 3], [4, 5, 6]])
        np.testing.assert_array_equal(spmat_array, expected)

    def test_constructor_indices_missing(self):
        data = np.array([1, 2, 3, 4, 5, 6])
        msg = "Must specify 'indptr', 'indices', and 'shape' arguments when passing data array."
        with self.assertRaisesWith(ValueError, msg):
            CSRMatrix(data)

    def test_constructor_bad_indices(self):
        data = np.array([1, 2, 3, 4, 5, 6])
        indices = np.array([0, -2, 2, 0, 1, 2])
        indptr = np.array([0, 2, 3, 6])
        shape = (3, 3)
        msg = "Cannot convert 'indices' to an array of unsigned integers."
        with self.assertRaisesWith(ValueError, msg):
            CSRMatrix(data, indices, indptr, shape)

    def test_constructor_bad_indices_dim(self):
        data = np.array([1, 2, 3, 4, 5, 6])
        indices = np.array([[0, 2, 2, 0, 1, 2]])
        indptr = np.array([0, 2, 3, 6])
        shape = (3, 3)
        msg = "'indices' must be a 1D array of unsigned integers."
        with self.assertRaisesWith(ValueError, msg):
            CSRMatrix(data, indices, indptr, shape)

    def test_constructor_bad_shape(self):
        data = np.array([1, 2, 3, 4, 5, 6])
        indices = np.array([0, 2, 2, 0, 1, 2])
        indptr = np.array([0, 2, 3, 6])
        shape = (3, )
        msg = "'shape' argument must specify two and only two dimensions."
        with self.assertRaisesWith(ValueError, msg):
            CSRMatrix(data, indices, indptr, shape)

    def test_array_bad_dim(self):
        data = np.array([[[1, 2], [3, 4], [5, 6]]])
        indices = np.array([0, 2, 2, 0, 1, 2])
        indptr = np.array([0, 2, 3, 6])
        msg = "'data' argument cannot be ndarray of dimensionality > 2."
        with self.assertRaisesWith(ValueError, msg):
            CSRMatrix(data, indices, indptr, (3, 3))


class TestCSRMatrixRoundTrip(H5RoundTripMixin, TestCase):

    def setUpContainer(self):
        data = np.array([1, 2, 3, 4, 5, 6])
        indices = np.array([0, 2, 2, 0, 1, 2])
        indptr = np.array([0, 2, 3, 6])
        return CSRMatrix(data, indices, indptr, (3, 3))
