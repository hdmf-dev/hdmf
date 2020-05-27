from hdmf.common import CSRMatrix
from hdmf.testing import TestCase, H5RoundTripMixin

import scipy.sparse as sps
import numpy as np


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

    # TODO more unit tests are needed for CSRMatrix


class TestCSRMatrixRoundTrip(H5RoundTripMixin, TestCase):

    def setUpContainer(self):
        data = np.array([1, 2, 3, 4, 5, 6])
        indices = np.array([0, 2, 2, 0, 1, 2])
        indptr = np.array([0, 2, 3, 6])
        return CSRMatrix(data, indices, indptr, (3, 3))
