import math
import os
import sh
import re
from datetime import timedelta

from . import base

    
class Resource(base.FormatterObject):
    drm_flag = '#SBATCH'
    
    def format_timedelta(self, time):
        hours = time.days * 24 + time.seconds / 3600
        minutes = time.seconds % 3600 / 60
        return '-t {0:02}:{1:02}:00'.format(hours, minutes)


    def format_memory(self, memInGB):
        #slurm specifies memory in megabytes
        return '--mem={0:.0f}'.format(math.ceil(memInGB * 1000))


    def format_concurrent(self, workers):
        return '-c {workers}'.format(workers=workers)


    def build(self, time=timedelta(minutes=59), workers=1, memInGB=1, **kwargs):
        return self.make_header([
            self.format_concurrent(workers),
            self.format_memory(memInGB),
            self.format_timedelta(time),
        ])

    
    
class MpiResource(Resource):
    def format_concurrent(self, workers):
        return '--ntasks=%s' % workers

    def format_processor(self, processor):
        return '--constraint=%s' % processor

    def format_mpi_ppn(self, ppn):
        return '--ntasks-per-node=%s' % ppn
    
    def build(self, time=timedelta(minutes=59), workers=1, memInGB=1, ppn=1, processor=None, **kwargs):
        first_header = super(MpiResource, self).build(time=time, workers=workers, memInGB=memInGB)
        second_header = self.make_header([
            self.format_mpi_ppn(ppn),
            self.format_processor(processor),
            ])
        
        return os.linesep.join([first_header, second_header])
    

class Submitter(base.BaseSubmitter):

    drm_flag = '#SBATCH'
    submit_cmd = 'sbatch'

        
    def format_hold(self, jid_list):
        return '-d afterok:' + ':'.join(jid_list)


    def format_copyEnv(self):
        return '--export=ALL'

        
    def format_env(self, env):
        return '--export=' + ','.join('='.join(e) for e in env.iteritems())


    def format_workDir(self, workDir):
        return '-D %s' % os.path.abspath(workDir)

        
    def format_logDir(self, logDir, name):
        out = '-o {0}.o%j'.format(os.path.join(logDir, name))
        err = '-e {0}.e%j'.format(os.path.join(logDir, name))
        return [out, err]


    def format_name(self, name):
        return '-J %s' % name


    def _add_logDir(self, fh, **kwargs):
        name = (kwargs.get('name') or 'sbatch')
        logdir = (kwargs.get('logDir') or self.logDir)
        self._BaseSubmitter__run_format_method(fh, self.format_logDir, logdir, name)


    def get_jobid_from_submit(self, stdout):
        return re.search(r'\d+', stdout).group(0)
