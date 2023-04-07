from hdmf.common import ExternalResources
from hdmf.common import DynamicTable, VectorData
from hdmf.container import AbstractContainer, Container, Data, ExternalResourcesManager
from hdmf.utils import docval,call_docval_func

from pynwb.core import NWBContainer, NWBDataInterface, MultiContainerInterface
from pynert import TermSet

import numpy as np
from pynwb import NWBFile, TimeSeries, NWBHDF5IO
from pynwb import get_type_map as tm
from pynwb.epoch import TimeIntervals
from pynwb.file import Subject
from pynwb.behavior import SpatialSeries, Position
from datetime import datetime
from dateutil import tz

terms = TermSet(name='Species_TermSet', term_schema_path='/Users/mavaylon/Research/NWB/species_term_set.yaml')

col1 = VectorData(
    name='NCBI',
    description='...',
    data=['Homo sapiens'],
    term_set=terms,
    validate=True
)
col2 = VectorData(
    name='Ensembl',
    description='...',
    data=['Mus musculus'],
    # term_set=terms,
    # validate=True
)
species = DynamicTable(name='species', description='My species', columns=[col1,col2],)

species.add_row(NCBI='Mus mrusculus', Ensembl='rat')
