from past.utils import old_div
import os.path
import os
import re

import attr

import drm.base as base

PE_NAME = os.environ.get('DRM_SGE_PE', 'smp')


@base.none_guard_filters
def make_environment(template_dict):
    def format_timedelta(time):
        hours = time.days * 24 + old_div(time.seconds, 3600)
        minutes = time.seconds % 3600 / 60
        return '{0:02.0f}:{1:02.0f}:00'.format(hours, minutes)

    def format_parallel(workers):
        if workers <= 1:
            return None
        else:
            return '{} {}'.format(PE_NAME, workers)

    def format_memory(memInGB, workers):
        mem = memInGB / float(max(1, workers))
        return '{:.2f}'.format(max(mem, 0.01))

    env = base.make_jinja_env(template_dict)
    env.filters['format_jid_list'] = lambda x: ','.join(x)
    env.filters['format_memory'] = format_memory
    env.filters['format_timedelta'] = format_timedelta
    env.filters['format_parallel'] = format_parallel
    return env


template_dict = {}

template_dict['resource'] = '''\
#$ -l h_rt={{ time|format_timedelta }},\
h_vmem={{ memInGB|format_memory }}G,mem_free={{ memInGB|format_memory }}G
#$ -pe {{ workers|format_parallel(workers) }}
{{ constraint }}
'''

template_dict['job'] = '''#!{{ shell }}
#$ -wd {{ workDir }}
#$ -V
#$ -S {{ shell }}
#$ -o {{ logDir }}
#$ -e {{ logDir }}
#$ -N {{ name }}
#$ -hold_jid {{ jid_list|format_jid_list }}

{{ resource }}

{{ job }}
'''

_ENV = make_environment(template_dict)


def remove_lines_ending_in_none(_str):
    lines = _str.split(os.linesep)
    return os.linesep.join(e for e in lines if not re.search(r'None\s*$', e))


class Constraint(base.Constraint):
    def __str__(self):
        return '{}'.format(self.features)


class Resource(base.Resource):
    _template = _ENV.get_template('resource')

    def __str__(self):
        return remove_lines_ending_in_none(
            self._template.render(**attr.asdict(self, recurse=False)))


class Submitter(base.Submitter):
    template = _ENV.get_template('job')
    submit_cmd = 'qsub'

    def get_jobid_from_submit(self, stdout):
        m = re.search(r'\d+', stdout)
        if m is None:
            return None
        else:
            return m.group(0)
