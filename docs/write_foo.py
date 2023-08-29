from datetime import datetime
from uuid import uuid4

import numpy as np
from dateutil.tz import tzlocal

from pynwb import NWBHDF5IO, NWBFile
from pynwb.ecephys import LFP, ElectricalSeries

from hdmf import TermSetWrapper as tw
from hdmf import Data
from hdmf import TermSet
terms = TermSet(term_schema_path='/Users/mavaylon/Research/NWB/hdmf2/hdmf/docs/gallery/example_term_set.yaml')

import numpy as np

from pynwb import TimeSeries

data = np.arange(100, 200, 10)
timestamps = np.arange(10)

from hdmf.backends.hdf5.h5_utils import H5DataIO

test_ts = TimeSeries(
    name="test_compressed_timeseries",
    data=H5DataIO(data=data, compression=True),
    unit=tw(item="SIunit", termset=terms),
    timestamps=timestamps,
)

nwbfile = NWBFile(
    session_description="my first synthetic recording",
    identifier=str(uuid4()),
    session_start_time=datetime.now(tzlocal()),
    experimenter=[
        "Baggins, Bilbo",
    ],
    lab="Bag End Laboratory",
    institution="University of Middle Earth at the Shire",
    experiment_description="I went on an adventure to reclaim vast treasures.",
    session_id="LONELYMTN001",
)
nwbfile.add_acquisition(test_ts)

filename = "nwbfile_test.nwb"
with NWBHDF5IO(filename, "w") as io:
    io.write(nwbfile)

# open the NWB file in r+ mode
with NWBHDF5IO(filename, "r+") as io:
    read_nwbfile = io.read()
breakpoint()
