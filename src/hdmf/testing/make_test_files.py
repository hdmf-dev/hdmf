"""Script to generate test files for backward compatibility testing."""
import os
from pathlib import Path

from hdmf import Data, HERDManager, __version__
from hdmf.common import SimpleMultiContainer, get_hdf5io
from hdmf.common.resources import HERD


class _HERDManagerContainer(SimpleMultiContainer, HERDManager):
    """A SimpleMultiContainer that also implements HERDManager, for testing."""

    pass


def make_herd_1_8_0_file(outdir):
    """Generate an HDF5 file using hdmf-common-schema 1.8.0.

    In schema 1.8.0, HERD was defined in the hdmf-experimental namespace.
    This creates test files to verify backward compatibility when reading
    files created with the old namespace.

    Before running this, pip install hdmf==4.3.1 which includes hdmf-common-schema 1.8.0.

    Parameters
    ----------
    outdir : Path
        Directory where the test files will be written.
    """
    species = Data(name="species", data=["Homo sapiens", "Mus musculus"])
    container = _HERDManagerContainer(name="root", containers=[species])

    er = HERD()
    er.add_ref(
        file=container,
        container=species,
        key="Homo sapiens",
        entity_id="NCBI_TAXON:9606",
        entity_uri="https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=Info&id=9606",
    )

    h5_path = outdir / "herd_1_8_0.h5"
    with get_hdf5io(path=str(h5_path), mode="w") as io:
        io.write(container)

    old_cwd = os.getcwd()
    os.chdir(str(outdir))
    er.to_zip(path="herd_1_8_0.zip")
    os.chdir(old_cwd)

    print(f"Created {h5_path}")


if __name__ == "__main__":
    # Install these versions of HDMF and run this script to generate new files:
    #   python src/hdmf/testing/make_test_files.py
    # Files will be made in tests/back_compat/

    if __version__ == '4.3.1':
        outdir = Path(__file__).resolve().parent.parent.parent.parent / "tests" / "unit" / "back_compat_tests"
        make_herd_1_8_0_file(outdir)
