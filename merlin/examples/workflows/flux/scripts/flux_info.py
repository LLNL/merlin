#!/usr/bin/env python2
"""This module will collect information on flux jobs from the live kvs
store and output times for each phase.

create: The time flux registerd the job
starting: The time the job was created
running: The time the job was running
completing: The time the job started its' completion pahse.
complete: The time the job was complete
walltime: ? Seems to be 0.
"""
import os

import flux
from flux import kvs


f = flux.Flux()

fs = "FLUX_START_SECONDS"
if fs in os.environ:
    print("flux start: {0}".format(os.environ[fs]))

for d in kvs.walk("lwj", flux_handle=f):
    try:
        # print(type(d))
        fdir = "lwj.{0}".format(d[0])

        qcreate = "{0}.create-time".format(fdir)
        create_time = kvs.get(f, qcreate)

        qstart = "{0}.starting-time".format(fdir)
        start_time = kvs.get(f, qstart)

        qrun = "{0}.running-time".format(fdir)
        start_time = kvs.get(f, qrun)

        qcomplete = "{0}.complete-time".format(fdir)
        complete_time = kvs.get(f, qcomplete)

        qcompleting = "{0}.completing-time".format(fdir)
        completing_time = kvs.get(f, qcompleting)

        qwall = "{0}.walltime".format(fdir)
        wall_time = kvs.get(f, qwall)

        proto = "Job {0}: create: {1} start {1} run {2} completing {3} complete {4} wall {5}"
        print(
            proto.format(
                d[0], create_time, start_time, completing_time, complete_time, wall_time
            )
        )
    except BaseException:
        pass


# vi: ts=4 sw=4 expandtab
