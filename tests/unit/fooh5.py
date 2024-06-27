# create mock NWB file
from pynwb.testing.mock.file import mock_NWBFile
from pynwb.file import Device
from pynwb.ecephys import ElectrodeGroup
from pynwb import NWBHDF5IO
from hdmf.backends.hdf5.h5_utils import H5DataIO
import numpy as np

from pynwb import TimeSeries

nwbfile = mock_NWBFile()

data = np.arange(10000).reshape((1000, 10))
wrapped_data = H5DataIO(
    data=data,
    chunks=True,  # <---- Enable chunking
    maxshape=(None, 10),  # <---- Make the time dimension unlimited and hence resizable
)
test_ts = TimeSeries(
    name="test_chunked_timeseries",
    data=wrapped_data,  # <----
    unit="SIunit",
    starting_time=0.0,
    rate=10.0,
)
nwbfile.add_acquisition(test_ts)


# save to Zarr (same error in "a" mode)
with NWBHDF5IO("testh5.nwb", mode="w") as io:
    io.write(nwbfile)

# now reload and try to add some more electrodes
io = NWBHDF5IO("testh5.nwb", mode="a")
nwbfile_read = io.read()

# if isinstance(dataset, h5py.Dataset):
#     question='yes'
# else:
#     question='no'
breakpoint()
