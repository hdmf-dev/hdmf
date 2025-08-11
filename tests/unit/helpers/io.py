from hdmf.backends.io import HDMFIO

class DoNothingIO(HDMFIO):

    @staticmethod
    def can_read(path):
        pass

    def read_builder(self):
        pass

    def write_builder(self, **kwargs):
        pass

    def open(self):
        pass

    def close(self):
        pass

    @classmethod
    def load_namespaces(cls, namespace_catalog, path, namespaces):
        pass

    def load_namespaces_io(self, namespace_catalog, namespaces):
        pass
