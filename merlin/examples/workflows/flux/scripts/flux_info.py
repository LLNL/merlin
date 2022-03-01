#!/usr/bin/env python3
"""This module will collect information on flux jobs from the live kvs
store and output times for each phase.

old:
create: The time flux registerd the job
starting: The time the job was created
running: The time the job was running
completing: The time the job started its' completion pahse.
complete: The time the job was complete
walltime: ? Seems to be 0.

new:
init:
starting:
shell.init:
shell.start:
complete:
cleanup.start:
cleanup.finish:
done:
"""
import json
import os
import subprocess
from typing import IO, Dict, Union

import flux
from flux import kvs


f = flux.Flux()

fs = "FLUX_START_SECONDS"
if fs in os.environ:
    print("flux start: {0}".format(os.environ[fs]))

try:
    kvs.get(f, "lwj")

    for d in kvs.walk("lwj", flux_handle=f):
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

        print(
            f"Job {d[0]}: create: {create_time} start {start_time} run {start_time} completing {completing_time} complete {complete_time} wall {wall_time}"
        )

except KeyError:
    top_dir = "job"

    def get_data_dict(key: str) -> Dict:
        kwargs: Dict[str, Union[str, bool, os.Environ]] = {
            "env": os.environ,
            "shell": True,
            "universal_newlines": True,
            "stdout": subprocess.PIPE,
            "stderr": subprocess.PIPE,
        }
        flux_com: str = f"flux kvs get {key}"
        p: subprocess.Popen = subprocess.Popen(flux_com, **kwargs)
        stdout: IO[str]
        stderr: IO[str]
        stdout, stderr = p.communicate()

        data: Dict = {}
        line: str
        for line in stdout.split("/n"):
            token: str
            for token in line.strip().split():
                if "timestamp" in token:
                    jstring: str = token.replace("'", '"')
                    d: Dict = json.loads(jstring)
                    data[d["name"]] = d["timestamp"]

        return data

    for d in kvs.walk(top_dir, flux_handle=f):
        if "exec" in d[0]:
            for e in d[2]:
                key = ".".join([top_dir, d[0], e])

                # This is currently not working gives
                # json.decoder.JSONDecodeError
                # data = kvs.get(f, key)

                data = get_data_dict(key)

                print(
                    f"Job {d[0]}: init: {data['init']} start {data['shell.start']} complete {data['complete']} done {data['done']} "
                )


# vi: ts=4 sw=4 expandtab
