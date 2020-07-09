#!/bin/env/python

from subprocess import call
import time
import os

if __name__ == '__main__':
    max_tries = 3
    num_tries = 1
    cmd = 'codecov --required'
    rc = call(cmd, shell=True)
    while rc != 0 and num_tries < max_tries:
        print("Try #%d/%d failed. Sleeping for 60 seconds and trying again..." % (num_tries, max_tries))
        time.sleep(60)
        os.remove(os.path.join(os.getcwd(), "coverage.xml"))
        rc = call(cmd, shell=True)
        num_tries += 1

    raise SystemExit(rc)
