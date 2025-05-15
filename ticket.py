from pynwb import NWBFile, NWBHDF5IO
from pynwb.base import TimeSeries
from datetime import datetime
import numpy as np
import pandas as pd

result_nwb_path = "example_hdf5.nwb"

# Dummy rewards data
rewards = pd.DataFrame({
    'volume': [5.0, 10.0, 7.5, 0.0],
    'autorewarded': [0, 1, 0, 1],
    'timestamps': [1.2, 3.5, 7.8, 9.0]
})

# Create new NWB file
nwbfile = NWBFile(
    session_description='Test session',
    identifier='TEST123',
    session_start_time=datetime.now()
)

with NWBHDF5IO(result_nwb_path, 'w') as io:
    io.write(nwbfile)

# Reopen and add reward TimeSeries
with NWBHDF5IO(result_nwb_path, "r+") as io:
    nwbfile = io.read()

    reward_data = np.array(
        list(zip(rewards['volume'], rewards['autorewarded'].astype(bool))),
        dtype=[('volume', 'f4'), ('autorewarded', 'bool')]
    )

    timestamps = rewards.timestamps.to_numpy()
    rewards_ts = TimeSeries(
        name='rewards_combined',
        data=reward_data,
        unit='',
        timestamps=timestamps,
        description='Reward events with volume and autorewarded flag'
    )

    nwbfile.add_acquisition(rewards_ts)
    io.write(nwbfile)
