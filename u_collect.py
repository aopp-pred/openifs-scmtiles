#!/usr/bin/env python2.7
"""Collect the results of an scmtiles array job."""
from __future__ import print_function

from argparse import ArgumentParser
from datetime import datetime, timedelta
import os.path
import sys


class JobInfo(object):

    def __init__(self, name, jid, run=None, pp_generate=None, pp_cleanup=None):
        self.name = name
        self.id = jid
        self.run = run
        self.pp_generate = pp_generate
        self.pp_cleanup = pp_cleanup
        self.date = JobInfo.date_from_job_id(jid)

    @staticmethod
    def date_from_job_id(job_id):
        reference_time = datetime(2009, 4, 6, 1)
        offset_minutes = 15 * (job_id - 1)
        return reference_time + timedelta(minutes=offset_minutes)

    @classmethod
    def from_name_and_id(cls, name, jid, pp_only=False):
        job_info = cls(name, jid)

        def lsb_run_complete(log_file):
            lsb_complete = 'Successfully completed.'
            try:
                with open(log_file, 'r') as f:
                    completed = any([line.startswith(lsb_complete)
                                 for line in f.readlines()])
            except IOError as e:
                raise Error(str(e))
            return completed

        if pp_only:
            job_info.run = True
        else:
            run_log = os.path.join('logs', '{!s}_run.{:d}.log'.format(name, jid))
            if not os.path.exists(run_log):
                return None
            if not lsb_run_complete(run_log):
                job_info.run = False
                return job_info
            job_info.run = True

        pp_log = os.path.join('logs', '{!s}_pp.{:d}.log'.format(name, jid))
        if not os.path.exists(pp_log):
            return job_info
        if not lsb_run_complete(pp_log):
            # Run didn't complete, but the output may have been written
            # successfully and the cleanup timed-out:
            pp_output = 'scm_out.{:s}.nc'.format(job_info.date.strftime('%Y%m%d_%H%M%S'))
            if os.path.exists(os.path.join('../../..', 'data','scm_out', pp_output)):
                # Output file exists, but cleanup not finished
                job_info.pp_generate = True
                job_info.pp_cleanup = False
            else:
                job_info.pp_generate = False
        else:
            job_info.pp_generate = True
            job_info.pp_cleanup = True

        return job_info


class Error(Exception):
    pass


class Usage(Exception):
    pass


def parse_array_spec(array_spec):
    job_ids = []
    for sub_spec in array_spec.split(','):
        items = [int(x) for x in sub_spec.split('-')]
        if len(items) == 1:
            result = [items[0]]
        elif len(items) == 2:
            result = list(range(items[0], items[1] + 1))
        else:
            msg = 'invalid array specification: "{!s}" in "{!s}'
            raise Usage(msg.format(sub_spec, array_spec))
        job_ids.append(result)
    return sorted(set([x for y in job_ids for x in y]))


def job_info_to_csv(job_info):
    translate_opt = {None: None, True: 'yes', False: 'no'}
    fields = [
        str(job_info.id),
        job_info.date.strftime('%F %T'),
        translate_opt[job_info.run],
        translate_opt[job_info.pp_generate],
        translate_opt[job_info.pp_cleanup],
    ]
    return ','.join(filter(None, fields))


def collect_jobs(job_name, job_ids, pp_only=False):
    info = filter(None, [JobInfo.from_name_and_id(job_name, job_id, pp_only)
                         for job_id in job_ids])
    for job_info in info:
        print(job_info_to_csv(job_info))


def main(argv=None):
    if argv is None:
        argv = sys.argv
    ap = ArgumentParser()
    ap.add_argument('-p', '--pp', action='store_true', default=False,
                    help='collect post-processing data, assume the model ran')
    ap.add_argument('job_name', type=str, help="name of the array job")
    ap.add_argument('array_spec', type=str, help="job array specification")
    argns = ap.parse_args(argv[1:])
    try:
        job_ids = parse_array_spec(argns.array_spec)
        collect_jobs(argns.job_name, job_ids, pp_only=argns.pp)
    except Usage as e:
        print("error: {!s}".format(e), file=sys.stderr)
        print("  use -h or --help for help", file=sys.stderr)
        return 1
    except Error as e:
        print("error: {!s}".format(e), file=sys.stderr)
        return 2
    return 0


if __name__ == '__main__':
    sys.exit(main())
