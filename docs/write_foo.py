from datetime import datetime
from uuid import uuid4

import numpy as np
from dateutil.tz import tzlocal

from pynwb import NWBHDF5IO, NWBFile
from pynwb.ecephys import LFP, ElectricalSeries

from hdmf import TermSetWrapper as tw
from hdmf.common import DynamicTable
from hdmf import TermSet
from pynwb.resources import HERD

terms = TermSet(term_schema_path='/Users/mavaylon/Research/NWB/hdmf2/hdmf/docs/gallery/example_term_set.yaml')

import numpy as np

from pynwb import TimeSeries

data = np.arange(100, 200, 10)
timestamps = np.arange(10)

from hdmf.backends.hdf5.h5_utils import H5DataIO

table = DynamicTable(name='table', description='table')
table.add_column(name='col1', description="column")
table.add_row(id=0, col1='data')

test_ts = TimeSeries(
    name="test_compressed_timeseries",
    data=data,
    unit=tw(value="Homo sapiens", field_name='unit', termset=terms),
    timestamps=timestamps,
)
# breakpoint()
nwbfile = NWBFile(
    session_description="my first synthetic recording",
    identifier=str(uuid4()),
    session_start_time=datetime.now(tzlocal()),
    experimenter=tw(value=["Mus musculus"], field_name='experimenter', termset=terms),
    lab="Bag End Laboratory",
    institution="University of Middle Earth at the Shire",
    experiment_description="I went on an adventure to reclaim vast treasures.",
    session_id="LONELYMTN001",
)
nwbfile.add_acquisition(test_ts)
nwbfile.add_acquisition(table)


filename = "nwbfile_test.nwb"
er = HERD()
with NWBHDF5IO(filename, "w") as io:
    io.write(nwbfile, herd=er)

# open the NWB file in r+ mode
with NWBHDF5IO(filename, "r+") as io:
    read_nwbfile = io.read()
breakpoint()
