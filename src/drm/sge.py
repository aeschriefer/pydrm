import os.path
import re
import math
from datetime import timedelta

from . import base

'''
This module is specifically for the cgs, not for sge in general
'''

class Resource(base.BaseResource):
    drm_flag = '#$'

    def format_timedelta(self, time):
        if time > timedelta(minutes=59):
            return '-P long'
        else:
            return None

    def format_memory(self, memInGB):
        return '-l h_vmem={0:.0f}gb'.format(math.ceil(memInGB))

    def format_concurrent(self, workers):
        return '-pe shared {workers}'.format(workers=workers)

    def build(self, time=timedelta(minutes=59), workers=1, memInGB=1, **kwargs):
        items = [
            self.format_concurrent(workers),
            self.format_memory(memInGB),
            self.format_timedelta(time),
            ]
            
        return self.make_header(items)


class Submitter(base.BaseSubmitter):
    
    drm_flag = '#?'

    def format_hold(self, jid_list):
        return '-hold_jid' + ','.join(map(str, jid_list))

    def format_copyEnv(self):
        return '-V'

    def format_env(self, env):
        return '-v ' + ','.join('='.join(e) for e in env.iteritems())

    def format_workDir(self, workDir):
        return '-wd %s' % os.path.abspath(workDir)

    def format_logDir(self, logDir):
        return ['%s %s' % (x, logDir) for x in ['-e', '-o']]

    def format_name(self, name):
        return '-N %s' % name

    def get_jobid_from_submit(self, stdout):
        return re.search(r'\d+', stdout).group(0)

