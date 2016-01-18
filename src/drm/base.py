import uuid
import threading
import sh
import itertools as it
import os
import pdb
import functools
from path import path


def handle_formatter(func):

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        #handle None arguments by returning None
        if not any(x is None for x in it.chain(args[1:], kwargs.itervalues())):
            return func(*args, **kwargs)
        else:
            return None

    return wrapper


class FormatterType(type):

    def __new__(cls, name, parents, attrs):
        newattrs = attrs.copy()
        for name, value in attrs.iteritems():
            if 'format' in name:
                newattrs[name] = handle_formatter(value)

        return super(FormatterType, cls).__new__(cls, name, parents, newattrs)


class FormatterObject(object):
    __metaclass__ = FormatterType

    def make_header(self, flags):
        '''
        flags is a list of strings each representing a line in the header
        Ignore any entries that are None
        Call str() on any scalar values
        Flatten any iterables and call str() on all values
        '''
        lst = []
        for each in flags:
            if each is None:
                pass
            elif hasattr(each, '__iter__'):
                lst.extend([str(f) for f in each])
            else:
                lst.append(str(each))

        return os.linesep.join(self.drm_flag + ' ' + f for f in lst)


class BaseResource(FormatterObject):
    pass


class BaseSubmitter(FormatterObject):

    drm_flag = '#?'
    submit_cmd = 'qsub'

    script_name_join = '-'
    uid_length = 8

    _LOCK = threading.Lock()
    _JOB_NAME_TO_ID = {}

    def __init__(self, script=None, log=None):
        '''
        By default batch scripts and job streams are written to working directory
        '''
        self.scriptDir = path(script or path.getcwd()).abspath()
        self.logDir = path(log or path.getcwd()).abspath()

        self.scriptDir.mkdir_p()
        self.logDir.mkdir_p()

        self.uid = uuid.uuid4().get_hex()[:self.uid_length]

    def get_jobid_from_submit(self, stdout):
        raise NotImplementedError()

    def submit_job(self,
                   cmd,
                   name=None,
                   hold=None,
                   env=None,
                   copyEnv=True,
                   workDir=None,
                   logDir=None,
                   shell='/bin/sh',
                   resource='',):

        '''
        This function writes a formatted job script to the self.script dir 
        and then calls self.submit_cmd on the script.

        Parameters
        ==========

        cmd : string
              Represents a script in some language (generally bash or sh).
  	      Should be properly formatted with newlines and such as it 
              will be written to file raw.
              cmd script must not take any commandline arguments, 
              They must be hardcoded in.
        name : string. 
               Gives a name to the job and acts as the job ID for pydrm. 
               name must be unique for each invocation of submit_job().
        -hold is a list of names. The job will not start until all hold jobs are finished. 
        names that never existed are ignored.
        -env is a dictionary with string keys that represent environmental variables that are passed to the job.
        -copyEnv is a boolean determining if all current env variables should be passed. This overwrites env.
        -workDir is a path to the directory the job should execute in. Defaults to cwd.
        -logDir is a path to the directory that the job will write its stdout and stderr. 
        Defaults to value from __init__.
        -shell is the #! that is written at the top of the job script.
        -resource is a string generally created by a Resource.build() call.
	This string is written to file raw so must be properly formatted as a DRM script header.
        '''
        script_name = (name or 'job')

        #should do something here to ensure unique
        script_fp = self.scriptDir.joinpath(self.script_name_join.join(
            [script_name, self.uid]))

        kwargs = locals().copy()
        kwargs.pop('self')
        header = self.__make_submit_header(**kwargs)

        with open(script_fp, 'w') as fh:
            shebang = '#!' + shell
            fh.write(os.linesep.join([shebang, header, resource, cmd, '']))

        self.__submit_and_validate(script_fp, name)
        return script_fp

    def __make_submit_header(self, **kwargs):
        '''
        if workDir is None, defaults to cwd
        fh is actually a list
        '''
        flag_list = list()

        self._add_hold(flag_list, **kwargs)
        self._add_env(flag_list, **kwargs)  #handles env and copyEnv
        self._add_workDir(flag_list, **kwargs)
        self._add_logDir(flag_list, **kwargs)
        self._add_name(flag_list, **kwargs)

        return self.make_header(flag_list)

    def _add_env(self, flag_list, **kwargs):
        '''
        if env is a non-empty dict copyEnv is ignored
        all keys and values in env have str() called on them
        '''
        env = kwargs.get('env')
        copyEnv = kwargs.get('copyEnv')
        if isinstance(env, dict) and env:
            string_env = {str(k): str(v) for k, v in env.iteritems()}
            self.__run_format_method(flag_list, self.format_env, string_env)
        elif copyEnv:
            self.__run_format_method(flag_list, self.format_copyEnv)

    def _add_hold(self, flag_list, **kwargs):
        '''
        All job_ids passed to self.format_hold are guaranteed to be strings
        '''
        self.__run_format_method(flag_list, self.format_hold,
                                 self.__map_name_to_jid(kwargs.get('hold')))

    def _add_logDir(self, flag_list, **kwargs):
        '''
        Defaults to the directory optionally passed at construction
        '''
        self.__run_format_method(flag_list, self.format_logDir,
                                 (kwargs.get('logDir') or self.logDir))

    def _add_name(self, flag_list, **kwargs):
        self.__run_format_method(flag_list, self.format_name,
                                 kwargs.get('name'))

    def _add_workDir(self, flag_list, **kwargs):
        '''
        Defaults to current working directory
        '''
        self.__run_format_method(flag_list, self.format_workDir,
                                 (kwargs.get('workDir') or path.getcwd()))

    def __run_format_method(self, flag_list, method, *args, **kwargs):
        '''
        Does not add a line if any of the args/kwargs are None
        I think this wont work for map_name_to_jid, maybe change to all?
        '''
        flag_list.append(method(*args, **kwargs))

    def __map_name_to_jid(self, name):
        '''
        If name is scalar, cast it has a list
        Remove any name entries that evaluate to false or
        do not have an entry in _JOB_NAME_TO_ID
        '''
        if hasattr(name, '__iter__'):
            job_ids = [self._JOB_NAME_TO_ID.get(n) for n in name if n]
        else:
            job_ids = [self._JOB_NAME_TO_ID.get(name)]

        final_job_ids = [x for x in job_ids if x is not None]

        return final_job_ids if final_job_ids else None

    def _submit(self, script_fp):
        return sh.Command(self.submit_cmd)(script_fp)

    def __submit_and_validate(self, script_fp, name=None):
        '''
        Makes sure that within a single process we are not submitting
        two jobs with the same name.  We do this because we use names for
        holdjid in the main api.  We convert these names into a job id in
        the holdjid flag for the drm system. If there are more than one
        job with the same name this mapping is ambiguous.
        '''
        p = self._submit(script_fp)

        if name in self._JOB_NAME_TO_ID:
            message = 'Name {0} already in _JOB_NAME_TO_ID with value {1}'.format(
                name, self._JOB_NAME_TO_ID.get(name))
            raise RuntimeError(message)

        if name is not None:
            self._JOB_NAME_TO_ID[name] = self.get_jobid_from_submit(p.stdout)

        return p
