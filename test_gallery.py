"""Test that the Sphinx Gallery files run without warnings or errors.

See tox.ini for usage.
"""

import importlib.util
import logging
import os
import os.path
import sys
import traceback
import warnings

TOTAL = 0
FAILURES = 0
ERRORS = 0


def _import_from_file(script):
    modname = os.path.basename(script)
    spec = importlib.util.spec_from_file_location(os.path.basename(script), script)
    module = importlib.util.module_from_spec(spec)
    sys.modules[modname] = module
    spec.loader.exec_module(module)


_numpy_warning_re = "numpy.ufunc size changed, may indicate binary incompatibility. Expected 216, got 192"

_experimental_warning_re = (
    "[a-zA-Z0-9]+ is experimental -- it may be removed in the future "
    "and is not guaranteed to maintain backward compatibility"
)

def run_gallery_tests():
    global TOTAL, FAILURES, ERRORS
    logging.info("Testing execution of Sphinx Gallery files")

    # get all python file names in docs/gallery
    gallery_file_names = list()
    for root, _, files in os.walk(os.path.join(os.path.dirname(__file__), "docs", "gallery")):
        for f in files:
            if f.endswith(".py"):
                gallery_file_names.append(os.path.join(root, f))

    warnings.simplefilter("error")
    warnings.filterwarnings(
        "ignore",
        category=DeprecationWarning,  # these can be triggered by downstream packages. ignore for these tests
    )

    TOTAL += len(gallery_file_names)
    for script in gallery_file_names:
        logging.info("Executing %s" % script)
        try:
            with warnings.catch_warnings(record=True):
                warnings.filterwarnings(
                    "ignore",
                    message=_experimental_warning_re,
                    category=UserWarning,
                )
                warnings.filterwarnings(
                    # this warning is triggered when some numpy extension code in an upstream package was compiled
                    # against a different version of numpy than the one installed
                    "ignore",
                    message=_numpy_warning_re,
                    category=RuntimeWarning,
                )
                _import_from_file(script)
        except (ImportError, ValueError) as e:
            if "linkml" in str(e):
                pass  # this is OK because linkml is not always installed
            else:
                raise e
        except Exception:
            print(traceback.format_exc())
            FAILURES += 1
            ERRORS += 1


def main():
    logging_format = (
        "======================================================================\n"
        "%(asctime)s - %(levelname)s - %(message)s"
    )
    logging.basicConfig(format=logging_format, level=logging.INFO)

    run_gallery_tests()

    final_message = "Ran %s tests" % TOTAL
    exitcode = 0
    if ERRORS > 0 or FAILURES > 0:
        exitcode = 1
        _list = list()
        if ERRORS > 0:
            _list.append("errors=%d" % ERRORS)
        if FAILURES > 0:
            _list.append("failures=%d" % FAILURES)
        final_message = "%s - FAILED (%s)" % (final_message, ",".join(_list))
    else:
        final_message = "%s - OK" % final_message

    logging.info(final_message)

    return exitcode


if __name__ == "__main__":
    sys.exit(main())
