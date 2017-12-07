from __future__ import division
from past.utils import old_div
import re
import math
import attr

import drm.base as base


@base.none_guard_filters
def make_environment(template_dict):
    def format_concurrent(workers, ppn='', constraint=''):
        ppn = (ppn or 1)
        nodes = int(math.ceil(old_div(workers, ppn)))
        if constraint:
            return 'nodes={nodes}:ppn={ppn}:{constraint}'.format(**locals())
        else:
            return 'nodes={nodes}:ppn={ppn}'.format(**locals())

    def format_name(name):
        '''
        pbs require a job name to begin with alphabetic character and
        have no whitespace
        '''
        valid_name = name
        if not re.match(r'[a-z]', valid_name, re.I):
            valid_name = 'job-' + valid_name

        valid_name = re.sub(r'\s+', '_', valid_name)

        return valid_name

    def format_timedelta(time):
        hours = time.days * 24 + old_div(time.seconds, 3600)
        minutes = time.seconds % 3600 / 60
        return '{0:02.0f}:{1:02.0f}:00'.format(hours, minutes)

    env = base.make_jinja_env(template_dict)
    env.filters['format_memory'] = lambda x: '{0:.0f}gb'.format(math.ceil(x))
    env.filters['format_jid_list'] = lambda x: ':'.join(x)
    env.filters['format_timedelta'] = format_timedelta
    env.filters['format_concurrent'] = format_concurrent
    env.filters['format_name'] = format_name

    env.tests['array'] = lambda x: isinstance(x, JobArray)
    return env


template_dict = {}

template_dict['resource'] = '''
#PBS -l {{ workers| format_concurrent(workers, constraint) }},\
vmem={{ memInGB| format_memory }},walltime={{ time|format_timedelta }}
'''

template_dict['mpi_resource'] = '''
#PBS -l {{ workers|format_concurrent(ppn, constraint) }},\
vmem={{ memInGB| format_memory }},walltime={{ time|format_timedelta }}
'''

template_dict['array'] = '''
{% for job in jobs %}
if [ ${PBS_ARRAYID} == {{ loop.index - 1 }} ]
  {{ job }}
fi
{% endfor %}
'''

template_dict['job'] = '''#!{{ shell }}
#PBS -V
#PBS -o {{ logDir }}
#PBS -e {{ logDir }}
#PBS -d {{ workDir }}
{% if job is array %} 
#PBS -t 0-{{ job|length - 1 }}
{% endif %}
{% if name is not none %}
#PBS -N {{ name }}
{% endif %}

{% if jid_list is not none %}
#PBS -W depend=afterok:{{ jid_list|format_jid_list }}
{% endif %}

{{ resource }}

{{ job }}
'''

_ENV = make_environment(template_dict)


def remove_empty_resources(_str):
    tag, flag, raw_items = _str.split(' ', 2)
    items = raw_items.split(',')
    new_items = [e for e in items if 'None' not in e]
    if not new_items:
        return ''
    else:
        return ' '.join([tag, flag, ','.join(new_items)])


class Constraint(base.Constraint):
    def __str__(self):
        return '{}'.format(self.features)


class Resource(base.Resource):
    _template = _ENV.get_template('resource')

    def __str__(self):
        return remove_empty_resources(
            self._template.render(
                **attr.asdict(
                    self, recurse=False)))


class MpiResource(base.MpiResource):
    _template = _ENV.get_template('mpi_resource')

    def __str__(self):
        return remove_empty_resources(
            self._template.render(
                **attr.asdict(
                    self, recurse=False)))


class JobArray(base.JobArray):
    _template = _ENV.get_template('array')

    def add_job(self, job):
        self.jobs.append(job)

    def __str__(self):
        return self._template.render(jobs=self.jobs)


class Submitter(base.Submitter):
    template = _ENV.get_template('job')
    submit_cmd = 'qsub'

    def get_jobid_from_submit(self, stdout):
        return stdout.strip()
