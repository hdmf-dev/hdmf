#!/usr/bin/env python

from __future__ import print_function

import warnings
import re
import argparse
import logging
import os.path
import os
import sys
import traceback
import unittest

flags = {'hdmf': 1, 'example': 4}

TOTAL = 0
FAILURES = 0
ERRORS = 0


class SuccessRecordingResult(unittest.TextTestResult):
    '''A unittest test result class that stores successful test cases as well
    as failures and skips.
    '''

    def addSuccess(self, test):
        if not hasattr(self, 'successes'):
            self.successes = [test]
        else:
            self.successes.append(test)

    def get_all_cases_run(self):
        '''Return a list of each test case which failed or succeeded
        '''
        cases = []

        if hasattr(self, 'successes'):
            cases.extend(self.successes)
        cases.extend([failure[0] for failure in self.failures])

        return cases


def run_test_suite(directory, description="", verbose=True):
    global TOTAL, FAILURES, ERRORS
    logging.info("running %s" % description)
    directory = os.path.join(os.path.dirname(__file__), directory)
    runner = unittest.TextTestRunner(verbosity=verbose, resultclass=SuccessRecordingResult)
    test_result = runner.run(unittest.TestLoader().discover(directory))

    TOTAL += test_result.testsRun
    FAILURES += len(test_result.failures)
    ERRORS += len(test_result.errors)

    return test_result


def _import_from_file(script):
    import imp
    return imp.load_source(os.path.basename(script), script)


warning_re = re.compile("Parent module '[a-zA-Z0-9]+' not found while handling absolute import")


def run_example_tests():
    global TOTAL, FAILURES, ERRORS
    logging.info('running example tests')
    examples_scripts = list()
    for root, dirs, files in os.walk(os.path.join(os.path.dirname(__file__), "docs", "gallery")):
        for f in files:
            if f.endswith(".py"):
                examples_scripts.append(os.path.join(root, f))

    TOTAL += len(examples_scripts)
    for script in examples_scripts:
        try:
            logging.info("Executing %s" % script)
            ws = list()
            with warnings.catch_warnings(record=True) as tmp:
                _import_from_file(script)
                for w in tmp:  # ignore RunTimeWarnings about importing
                    if isinstance(w.message, RuntimeWarning) and not warning_re.match(str(w.message)):
                        ws.append(w)
            for w in ws:
                warnings.showwarning(w.message, w.category, w.filename, w.lineno, w.line)
        except Exception:
            print(traceback.format_exc())
            FAILURES += 1
            ERRORS += 1


def main():
    # setup and parse arguments
    parser = argparse.ArgumentParser('python test.py [options]')
    parser.set_defaults(verbosity=1, suites=[])
    parser.add_argument('-v', '--verbose', const=2, dest='verbosity', action='store_const', help='run in verbose mode')
    parser.add_argument('-q', '--quiet', const=0, dest='verbosity', action='store_const', help='run disabling output')
    parser.add_argument('-u', '--unit', action='append_const', const=flags['hdmf'], dest='suites',
                        help='run unit tests for hdmf package')
    parser.add_argument('-e', '--example', action='append_const', const=flags['example'], dest='suites',
                        help='run example tests')
    args = parser.parse_args()
    if not args.suites:
        args.suites = list(flags.values())
        args.suites.pop(args.suites.index(flags['example']))  # remove example as a suite run by default

    # set up logger
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter('======================================================================\n'
                                  '%(asctime)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    root.addHandler(ch)

    warnings.simplefilter('always')

    # many tests use NamespaceCatalog.add_namespace, which is deprecated, to set up tests.
    # ignore these warnings for now.
    warnings.filterwarnings("ignore", category=DeprecationWarning, module="hdmf.spec.namespace",
                            message=("NamespaceCatalog.add_namespace has been deprecated. "
                                     "SpecNamespaces should be added with load_namespaces."))

    # Run unit tests for hdmf package
    if flags['hdmf'] in args.suites:
        run_test_suite("tests/unit", "hdmf unit tests", verbose=args.verbosity)

    # Run example tests
    if flags['example'] in args.suites:
        run_example_tests()

    final_message = 'Ran %s tests' % TOTAL
    exitcode = 0
    if ERRORS > 0 or FAILURES > 0:
        exitcode = 1
        _list = list()
        if ERRORS > 0:
            _list.append('errors=%d' % ERRORS)
        if FAILURES > 0:
            _list.append('failures=%d' % FAILURES)
        final_message = '%s - FAILED (%s)' % (final_message, ','.join(_list))
    else:
        final_message = '%s - OK' % final_message

    logging.info(final_message)

    return exitcode


if __name__ == "__main__":
    sys.exit(main())
