from __future__ import division
from past.utils import old_div
import os
import re
import math
import itertools as it
import csv
import logging
import time
from datetime import datetime

import attr
import sh

import drm.base as base


@base.none_guard_filters
def make_environment(template_dict):
    def format_timedelta(time):
        hours = time.days * 24 + old_div(time.seconds, 3600)
        minutes = time.seconds % 3600 / 60
        return '{0:02.0f}:{1:02.0f}:00'.format(hours, minutes)

    def format_logDir(logDir, script_name):
        return os.path.join(logDir, script_name)

    def format_ntasks(workers, ppn):
        ppn = ppn or 1
        ntasks = int(workers / ppn)
        return str(max(1, ntasks))

    env = base.make_jinja_env(template_dict)
    env.filters['format_memory'] = lambda x: int(math.ceil(x * 1000))
    env.filters['format_jid_list'] = lambda x: ':'.join(x)
    env.filters['format_timedelta'] = format_timedelta
    env.filters['format_logDir'] = format_logDir
    env.filters['format_ntasks'] = format_ntasks

    env.tests['array'] = lambda x: isinstance(x, JobArray)
    return env


template_dict = {}

template_dict['resource'] = '''\
#SBATCH -t {{ time|format_timedelta }}
#SBATCH --mem={{ memInGB|format_memory }}
#SBATCH -c {{ workers }}
{{ constraint }}
'''

template_dict['mpi_resource'] = '''\
#SBATCH -t {{ time|format_timedelta }}
#SBATCH --mem={{ memInGB|format_memory }}
#SBATCH --ntasks {{ workers|format_ntasks(ppn) }}
{% if ppn is not none %}
#SBATCH --cpus-per-task={{ ppn }}
#SBATCH --ntasks-per-node=1
{% endif %}
{{ constraint }}
'''

template_dict['array'] = '''\
{% for job in jobs %}
if [ $SLURM_ARRAY_TASK_ID == {{ loop.index - 1 }} ]
  {{ job }}
fi
{% endfor %}'''

template_dict['job'] = '''#!{{ shell }}
#SBATCH --parsable
#SBATCH --export=ALL

{% if job is array %} 
#SBATCH --array=0-{{ job|length - 1}}
#SBATCH -o {{ logDir| format_logDir(script_name) }}.o%A_%a
#SBATCH -e {{ logDir| format_logDir(script_name) }}.e%A_%a
{% else %}
#SBATCH -o {{ logDir| format_logDir(script_name) }}.o%j
#SBATCH -e {{ logDir| format_logDir(script_name) }}.e%j
{% endif %}

#SBATCH -D {{ workDir }}

{% if name is not none %}
#SBATCH -J {{ name }}
{% endif %}

{% if jid_list is not none %}
#SBATCH -d afterok:{{ jid_list|format_jid_list }}
{% endif %}

{{ resource }}

{{ job }}
'''

_ENV = make_environment(template_dict)


def remove_lines_ending_in_none(_str):
    lines = _str.split(os.linesep)
    return os.linesep.join(e for e in lines if not re.search(r'None\s*$', e))


class Constraint(base.Constraint):
    _template = '#SBATCH --constraint {}'

    def __str__(self):
        if not self:
            return ''
        else:
            return self._template.format(self.features)


class Resource(base.Resource):
    _template = _ENV.get_template('resource')

    def __str__(self):
        return remove_lines_ending_in_none(
            self._template.render(**attr.asdict(self, recurse=False)))


class MpiResource(base.MpiResource):
    _template = _ENV.get_template('mpi_resource')

    def __str__(self):
        return remove_lines_ending_in_none(
            self._template.render(**attr.asdict(self, recurse=False)))


class JobArray(base.JobArray):
    _template = _ENV.get_template('array')

    def add_job(self, job):
        self.jobs.append(job)

    def __str__(self):
        return self._template.render(jobs=self.jobs)


class Submitter(base.Submitter):
    template = _ENV.get_template('job')
    submit_cmd = 'sbatch'

    def get_jobid_from_submit(self, stdout):
        m = re.search(r'\d+', stdout)
        if m is None:
            return None
        else:
            return m.group(0)


@attr.s
class Waiter(base.Waiter):
    '''
    Class for waiting on jobids using sacct
    No test for running jobs because it creates a race condition

    Note that duplicate jobids can exist. By default only the most recent
    job with the given id will be returned. We ask for duplicates so we
    can check for issues. We ignore any job that was submitted 
    before this module is imported. We log a message if there is more than one such job.

    The main issue is that if there is a duplicate that has completed it will cause
    wait() to return immediately if we call query() before our new job has registered
    with sacct.

    JobID entry is mangled in the case of array jobs with a _\d+
    JobIDRaw is a job's true jobid
    '''
    _MODULE_START_TIME = datetime.utcnow()
    _time_format = '%Y-%m-%dT%H:%M:%S'

    _header = ['jobidraw', 'state', 'exitcode', 'submit']
    try:
        _cmd = sh.Command('sacct').bake(
            '-nDP', '--format={}'.format(','.join(_header)), '-j')
    except sh.CommandNotFound:
        _cmd = None

    _info = attr.ib(default=attr.Factory(list))

    def wait(self):
        _timeout = self.timeout
        N = len(self.jobid_lst)
        while (True):
            self.query()
            if N > len(self.successful_jobs() + self.unsuccessful_jobs()):
                time.sleep(self.interval)
            else:
                return self
            if _timeout is not None:
                _timeout -= self.interval
                if _timeout <= 0:
                    return self

    def query(self):
        # sacct returns all jobs if -j is empty string, avoid this
        if not self.jobid_lst:
            self._info = []
        jobs = ','.join(self.jobid_lst)
        raw_data = self._cmd(jobs).stdout

        _iter = csv.DictReader(
            raw_data.split('\n'), delimiter='|', fieldnames=self._header)

        self._info = [
            line for line in _iter
            if not self._is_entry_batch(line['jobidraw'])
            and self._is_submit_in_range(line['submit'])
        ]
        self._check_for_duplicates()
        return self

    def unsuccessful_jobs(self):
        def cond(s):
            return any(st in s for st in ['FAIL', 'CANCELLED', 'TIMEOUT'])

        return [job['jobidraw'] for job in self._info if cond(job['state'])]

    def successful_jobs(self):
        def cond(s):
            return 'COMPLETED' in s

        return [job['jobidraw'] for job in self._info if cond(job['state'])]

    def _is_entry_batch(self, jobid):
        return 'batch' in jobid

    def _is_submit_in_range(self, submit_time):
        submit = datetime.strptime(submit_time, self._time_format)
        return submit >= self._MODULE_START_TIME

    def _check_for_duplicates(self):
        _ids = sorted(e['jobidraw'] for e in self._info)
        for k, grp in it.groupby(_ids):
            if len(list(grp)) > 1:
                logging.critical('JobIDRaw %s has multiple entries', k)
