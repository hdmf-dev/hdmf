# create mock NWB file
from pynwb.testing.mock.file import mock_NWBFile
from pynwb.file import Device
from pynwb.ecephys import ElectrodeGroup
from hdmf_zarr.nwb import NWBZarrIO

nwbfile = mock_NWBFile()

# add device
device = Device(name="my_device", description="my device")
_ = nwbfile.add_device(device)
# add electrode group
eg = ElectrodeGroup(name="tetrode", description="my_tetrode", location="unknown", device=device)
_ = nwbfile.add_electrode_group(eg)

nwbfile.add_electrode_column(name="source", description="1st or 2nd round")

# add a few electrodes
rel_xs = [0., 10., 20., 30.]
rel_ys = [0., 0., 0., 0.]

for x, y in zip(rel_xs, rel_ys):
    for x, y in zip(rel_xs, rel_ys):
        nwbfile.add_electrode(
            rel_x=x,
            rel_y=y,
            enforce_unique_id=True,
            source="first",
            group=eg,
            location="unknown"
        )


# save to Zarr (same error in "a" mode)
with NWBZarrIO("test.nwb", mode="w") as io:
    io.write(nwbfile)

# now reload and try to add some more electrodes
io = NWBZarrIO("test.nwb", mode="r+d")
nwbfile_read = io.read()

rel_xs = [50., 60., 70., 80.]
rel_ys = [0., 0., 0., 0.]

for x, y in zip(rel_xs, rel_ys):
    nwbfile_read.add_electrode(
        rel_x=x,
        rel_y=y,
        enforce_unique_id=True,
        source="second",
        group=eg,
        location="unknown"
    )
