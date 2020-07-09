#!/bin/env/python

from subprocess import call
import time
import os

if __name__ == '__main__':
    max_tries = 10
    num_tries = 1
    cmd = 'codecov --required'
    rc = call(cmd, shell=True)
    while rc != 0 and num_tries < max_tries:
        time.sleep(5)
        os.remove(os.path.join(os.getcwd(), "coverage.xml"))
        rc = call(cmd, shell=True)
        num_tries += 1

    raise SystemExit(rc)
