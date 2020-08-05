from . import hdf5
# Import the Zarr backend if possible, but issue only a warning if it fails (e.g., if Zarr is not installed)
try:
    from . import zarr
except ImportError as e:
    import warnings
    warnings.warn("Import of the zarr backend failed due to " + str(e))