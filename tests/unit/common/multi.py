from pynwb.resources import ExternalResources
from pynwb import NWBFile, TimeSeries, NWBHDF5IO


e1='/Users/mavaylon/Downloads/sub-Rat203_ecephys.nwb'
io=NWBHDF5IO(e1, "r")
read_nwbfile_e1 = io.read()

e2='/Users/mavaylon/Downloads/sub-EE_ses-EE-042_ecephys.nwb'
io=NWBHDF5IO(e2, "r")
read_nwbfile_e2 = io.read()

e3 = '/Users/mavaylon/Downloads/sub-BH243.nwb'
io=NWBHDF5IO(e3, "r")
read_nwbfile_e3 = io.read()

er = ExternalResources()

er.add_ref(
    file=read_nwbfile_e1,
    container=read_nwbfile_e1.subject,
    attribute='species',
    key='rat',
    entity_id='NCBI_TAXON:10116',
    entity_uri='https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=10116'
)

er.add_ref(
    file=read_nwbfile_e2,
    container=read_nwbfile_e2.subject,
    attribute='species',
    key='Rattus norvegicus domestica',
    entity_id='NCBI_TAXON:10116',
    entity_uri='https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=10116'
)

er.add_ref(
    file=read_nwbfile_e3,
    container=read_nwbfile_e3.subject,
    attribute='species',
    key='rattus norvegicus',
    entity_id='NCBI_TAXON:10116',
    entity_uri='https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id=10116'
)
er.to_dataframe()
