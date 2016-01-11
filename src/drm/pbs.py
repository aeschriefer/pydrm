import os.path
import re
import math
from datetime import timedelta

from . import base


class Resource(base.BaseResource):

    def format_timedelta(self, time):
        hours = time.days * 24 + time.seconds / 3600
        minutes = time.seconds % 3600 / 60
        return 'walltime={0:02}:{1:02}:00'.format(hours, minutes)

    def format_memory(self, memInGB):
        return 'vmem={0:.0f}gb'.format(math.ceil(memInGB))

    def format_concurrent(self, workers):
        return 'nodes=1:ppn={workers}'.format(workers=workers)

    def build(self, time=timedelta(minutes=59), workers=1, memInGB=1, **kwargs):
        items = [
            self.format_concurrent(workers),
            self.format_memory(memInGB),
            self.format_timedelta(time),
        ]

        return '#PBS -l ' + ','.join(i for i in items if i is not None)


class MpiResource(Resource):

    def _format_concurrent(self, workers, ppn, processor):
        if processor:
            return 'nodes={ppn}:ppn={workers}:{processor}'.format(**locals())
        else:
            return 'nodes={ppn}:ppn={workers}'.format(**locals())

    def build(self,
              time=timedelta(minutes=59),
              workers=1,
              memInGB=1,
              ppn=1,
              processor=None,
              **kwargs):
        items = [
            self._format_concurrent(workers, ppn, processor),
            self.format_memory(memInGB),
            self.format_timedelta(time),
        ]

        return '-l ' + ','.join(i for i in items if i is not None)


class Submitter(base.BaseSubmitter):

    drm_flag = '#PBS'

    ## def __init__(self, script=None, log=None, resource=PbsResource()):
    ##     super(PbsSubmit, self).__init__(script, log, resource)

    def format_hold(self, jid_list):
        return '-W depend=afterany:' + ':'.join(map(str, jid_list))

    def format_copyEnv(self):
        return '-V'

    def format_env(self, env):
        return '-v ' + ','.join('='.join(e) for e in env.iteritems())

    def format_workDir(self, workDir):
        return '-d %s' % os.path.abspath(workDir)

    def format_logDir(self, logDir):
        return ['%s %s' % (x, logDir) for x in ['-e', '-o']]

    def format_name(self, name):
        '''
        pbs require a job name to begin with alphabetic character and
        have no whitespace
        '''
        valid_name = name
        if not re.match(r'[a-z]', valid_name, re.I):
            valid_name = 'job-' + valid_name

        valid_name = re.sub(r'\s+', '_', valid_name)

        return '-N %s' % valid_name

    def get_jobid_from_submit(self, stdout):
        return stdout.strip()
