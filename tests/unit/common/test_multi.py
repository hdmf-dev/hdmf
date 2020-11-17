from hdmf.common import SimpleMultiContainer
from hdmf.container import Container, Data
from hdmf.testing import TestCase, H5RoundTripMixin


class SimpleMultiContainerRoundTrip(H5RoundTripMixin, TestCase):

    def setUpContainer(self):
        containers = [
            Container('container1'),
            Container('container2'),
            Data('data1', [0, 1, 2, 3, 4]),
            Data('data2', [0.0, 1.0, 2.0, 3.0, 4.0]),
        ]
        multi_container = SimpleMultiContainer('multi', containers)
        return multi_container
