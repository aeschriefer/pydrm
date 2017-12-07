from builtins import object
import uuid
import logging
import threading
import sh
import functools
from past.builtins import basestring
from datetime import timedelta
from path import Path
import attr

logger = logging.getLogger(__name__)


def make_jinja_env(template_dict):
    from jinja2 import Environment, DictLoader
    env = Environment(
        loader=DictLoader(template_dict),
        trim_blocks=True,
        lstrip_blocks=True,
        keep_trailing_newline=True)
    return env


def none_guard_filters(func):
    '''
    Only guards against the first argument being None
    '''

    def guard(f):
        @functools.wraps(f)
        def subwrapper(*args, **kwargs):
            if args[0] is None:
                return None
            else:
                return f(*args, **kwargs)

        return subwrapper

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        ret = func(*args, **kwargs)
        for k, v in ret.filters.items():
            if 'format_' in k:
                ret.filters[k] = guard(v)
        return ret

    return wrapper


class JobInfo(object):
    def __init__(self, id, script):
        self.id = id
        self.script = script


@attr.s
class JobArray(object):
    jobs = attr.ib(default=attr.Factory(list))

    def add_job(self, job):
        raise NotImplementedError()

    def __str__(self):
        raise NotImplementedError()

    def __len__(self):
        return len(self.jobs)

    def __iter__(self):
        return iter(self.jobs)

    def __bool__(self):
        return bool(self.jobs)


def _scalar_to_iter(arg):
    return [arg] if isinstance(arg, basestring) else arg


@attr.s
class Constraint(object):
    features = attr.ib()

    def __str__(self):
        raise NotImplementedError()

    def __bool__(self):
        return bool(self.features)


@attr.s
class Resource(object):
    memInGB = attr.ib(default=1)
    workers = attr.ib(default=1)
    time = attr.ib(default=timedelta(minutes=59))
    constraint = attr.ib(default=None)

    def __str__(self):
        raise NotImplementedError()


@attr.s
class MpiResource(object):
    memInGB = attr.ib(default=1)
    workers = attr.ib(default=1)
    ppn = attr.ib(default=1)
    time = attr.ib(default=timedelta(minutes=59))
    constraint = attr.ib(default=None)

    def __str__(self):
        raise NotImplementedError()


class Submitter(object):
    submit_cmd = None  # String or sh.Command
    template = None  # Jinja2 template

    shell = '/bin/bash'
    script_name_join = '-'
    uid_length = 8

    _LOCK = threading.Lock()
    _JOB_NAME_TO_ID = {}
    _NO_NAME_JOBS = set()

    def __init__(self, script=None, log=None):
        '''
        By default batch scripts and job streams are written to working directory
        '''
        self.scriptDir = Path(script or Path.getcwd()).abspath()
        self.logDir = Path(log or Path.getcwd()).abspath()

        self.scriptDir.mkdir_p()
        self.logDir.mkdir_p()

        self.uid = uuid.uuid4().hex[:self.uid_length]

    def get_jobid_from_submit(self, stdout):
        raise NotImplementedError()

    def submit_job(
            self,
            job,
            name=None,
            hold=None,
            workDir=None,
            resource='', ):

        if not job:
            return None

        workDir = Path(workDir or Path.getcwd()).abspath()
        logDir = self.logDir
        jid_list = self._map_name_to_jid(hold)

        script_name = (name or 'job')
        #should do something here to ensure unique
        script_fp = self.scriptDir.joinpath(
            self.script_name_join.join([script_name, self.uid]))

        with open(script_fp, 'w') as fh:
            kwargs = locals()
            kwargs.pop('self')
            fh.write(self.template.render(shell=self.shell, **kwargs))

        job_info = self._submit_and_validate(script_fp, name)
        return job_info

    @property
    def jobs(self):
        return list(self._JOB_NAME_TO_ID.values()) + list(self._NO_NAME_JOBS)

    def _map_name_to_jid(self, name):
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

    def _submit_and_validate(self, script_fp, name=None):
        '''
        Makes sure that within a single process we are not submitting
        two jobs with the same name.  We do this because we use names for
        holdjid in the main api.  We convert these names into a job id in
        the holdjid flag for the drm system. If there are more than one
        job with the same name this mapping is ambiguous.
        '''
        try:
            p = self._submit(script_fp)
        except sh.CommandNotFound as err:
            logger.warning(str(err))
            jobid = None
        else:
            jobid = self.get_jobid_from_submit(p.stdout)

        if name in self._JOB_NAME_TO_ID:
            message = 'Name {0} already in _JOB_NAME_TO_ID with value {1}'.format(
                name, self._JOB_NAME_TO_ID.get(name))
            raise RuntimeError(message)

        if name is not None:
            self._JOB_NAME_TO_ID[name] = jobid
        elif jobid:
            self._NO_NAME_JOBS.add(jobid)

        return JobInfo(jobid, script_fp)


@attr.s
class Waiter(object):
    jobid_lst = attr.ib(convert=_scalar_to_iter)
    interval = attr.ib(default=60)
    timeout = attr.ib(default=None)

    def wait(self):
        raise NotImplementedError()
