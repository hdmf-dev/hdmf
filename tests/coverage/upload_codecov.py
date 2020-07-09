#!/bin/env/python

from subprocess import call
import time

if __name__ == '__main__':
    max_tries = 10
    num_tries = 1
    cmd = 'codecov --required'
    rc = call(cmd)
    while rc != 0 and num_tries < max_tries:
        time.sleep(5)
        rc = call(cmd)
        num_tries += 1

    raise SystemExit(rc)
