from hdmf.common import CSRMatrix
from hdmf.testing import TestMapH5RoundTrip

import scipy.sparse as sps
import numpy as np


class TestCSRMatrix(TestMapH5RoundTrip):

    def setUp(self):
        self.data = np.array([1, 2, 3, 4, 5, 6])
        self.indices = np.array([0, 2, 2, 0, 1, 2])
        self.indptr = np.array([0, 2, 3, 6])
        super().setUp()

    def setUpContainer(self):
        return CSRMatrix(self.data, self.indices, self.indptr, (3, 3))

    def test_from_sparse_matrix(self):
        sps_mat = sps.csr_matrix((self.data, self.indices, self.indptr), shape=(3, 3))
        csr_container = CSRMatrix(sps_mat)
        self.assertContainerEqual(csr_container, self.container)
