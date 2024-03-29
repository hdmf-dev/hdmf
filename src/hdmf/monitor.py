from abc import ABCMeta, abstractmethod

from .data_utils import AbstractDataChunkIterator, DataChunkIterator, DataChunk
from .utils import docval, getargs


class NotYetExhausted(Exception):
    pass


class DataChunkProcessor(AbstractDataChunkIterator, metaclass=ABCMeta):

    @docval({'name': 'data', 'type': DataChunkIterator, 'doc': 'the DataChunkIterator to analyze'})
    def __init__(self, **kwargs):
        """Initialize the DataChunkIterator"""
        # Get the user parameters
        self.__dci = getargs('data', kwargs)

    def __next__(self):
        try:
            dc = self.__dci.__next__()
        except StopIteration as e:
            self.__done = True
            raise e
        self.process_data_chunk(dc)
        return dc

    def __iter__(self):
        return iter(self.__dci)

    def recommended_chunk_shape(self):
        return self.__dci.recommended_chunk_shape()

    def recommended_data_shape(self):
        return self.__dci.recommended_data_shape()

    def get_final_result(self, **kwargs):
        ''' Return the result of processing data fed by this DataChunkIterator '''
        if not self.__done:
            raise NotYetExhausted()
        return self.compute_final_result()

    @abstractmethod
    @docval({'name': 'data_chunk', 'type': DataChunk, 'doc': 'a chunk to process'})
    def process_data_chunk(self, **kwargs):
        ''' This method should take in a DataChunk,
            and process it.
        '''
        pass

    @abstractmethod
    @docval(returns='the result of processing this stream')
    def compute_final_result(self, **kwargs):
        ''' Return the result of processing this stream
            Should raise NotYetExhaused exception
        '''
        pass


class NumSampleCounter(DataChunkProcessor):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.__sample_count = 0

    @docval({'name': 'data_chunk', 'type': DataChunk, 'doc': 'a chunk to process'})
    def process_data_chunk(self, **kwargs):
        dc = getargs('data_chunk', kwargs)
        self.__sample_count += len(dc)

    @docval(returns='the result of processing this stream')
    def compute_final_result(self, **kwargs):
        return self.__sample_count
