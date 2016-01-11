import pytest
import re
import os
from path import path
from datetime import timedelta
from drm import pbs, base, slurm


class FakeProcess(object):

    def __init__(self, stdout):
        self.stdout = str(stdout)


#I think this runs for every test so jid should be new everytime
@pytest.fixture(autouse=True)
def no_submit(monkeypatch):

    jid = iter(xrange(1, 100))

    def mocksubmit(self, fp):
        return FakeProcess(next(jid))

    for module in [pbs, slurm]:
        monkeypatch.setattr(module.Submitter, '_submit', mocksubmit)
        #reset the job_id dict between module tests
        monkeypatch.setattr(module.Submitter, '_JOB_NAME_TO_ID', {})


@pytest.fixture
def tmpdirs(tmpdir_factory):
    return tmpdir_factory.mktemp('scripts'), tmpdir_factory.mktemp('log')


@pytest.mark.parametrize('module,kwargs,expected',
                         [
                             (pbs, {'time': timedelta(days=2,
                                                      minutes=6),
                                    'workers': 4,
                                    'memInGB': 2.345},
                              '-l nodes=1:ppn=4,vmem=3gb,walltime=48:06:00'),
                             (pbs, {},
                              '-l nodes=1:ppn=1,vmem=1gb,walltime=00:59:00'),
                         ])
def test_resource_string(tmpdirs, module, kwargs, expected):

    resource = module.Resource()
    str_ = resource.build(**kwargs)

    script_dir, log_dir = tmpdirs
    submit = module.Submitter(script=script_dir, log=log_dir)

    script = submit.submit_job('', resource=str_).text()

    assert expected + os.linesep in script


@pytest.mark.parametrize('module,kwargs,expected',
                         [
                             (slurm, {'time': timedelta(days=2,
                                                        minutes=6),
                                      'workers': 4,
                                      'memInGB': 2.345},
                              ['-t 48:06:00', '-c 4', '--mem=2345']),
                             (slurm, {}, ['-t 00:59:00', '-c 1', '--mem=1000']),
                         ])
def test_resource_list(tmpdirs, module, kwargs, expected):

    resource = module.Resource()
    str_ = resource.build(**kwargs)

    script_dir, log_dir = tmpdirs
    submit = module.Submitter(script=script_dir, log=log_dir)

    script = submit.submit_job('', resource=str_).text()

    for ex in expected:
        assert ex + os.linesep in script


def test_slurm_submit(tmpdirs):
    script_dir, log_dir = tmpdirs
    submit = slurm.Submitter(script=script_dir, log=log_dir)
    fp = submit.submit_job('ls', name='test', hold='testA')
    script = open(fp, 'r').read()

    assert fp == path(script_dir).joinpath(submit.script_name_join.join(
        ['test', submit.uid]))
    assert '#!/bin/sh' + os.linesep in script


def test_pbs_submit(tmpdirs):
    script_dir, log_dir = tmpdirs
    submit = pbs.Submitter(script=script_dir, log=log_dir)
    fp = submit.submit_job('ls', name='test', hold='testA')
    script = open(fp, 'r').read()

    assert fp == path(script_dir).joinpath(submit.script_name_join.join(
        ['test', submit.uid]))

    assert re.match('^#!/bin/sh$', script, re.MULTILINE)
    assert re.search(r'^#PBS -N test$', script, re.MULTILINE)
    #workdir should default to current working directory
    assert re.search('^#PBS -d {0}$'.format(path.getcwd()), script,
                     re.MULTILINE)
    assert re.search('^#PBS -e %s$' % log_dir, script, re.MULTILINE)
    assert re.search('^#PBS -o %s$' % log_dir, script, re.MULTILINE)
    assert re.search('^#PBS -V$', script, re.MULTILINE)
    assert re.search(r'^ls$', script, re.MULTILINE)

    #job named testA has not be submitted so we shouldn't hold on it
    assert not re.search(r'^#PBS -W$', script, re.MULTILINE)

    #what happens when we run with all defaults
    #what happens when name is ''
    #what happens when name is numeric
    #what happens when resource returns '-l '


@pytest.mark.parametrize('module,flag,expected', [
    (pbs, '-W', ['-W depend=afterany:1', '-W depend=afterany:1:2']),
    (slurm, '-d', ['-d afterok:1', '-d afterok:1:2']),
])
def test_hold_jobs(tmpdirs, module, flag, expected):
    script_dir, log_dir = tmpdirs

    submit = module.Submitter(script=script_dir, log=log_dir)
    fp = submit.submit_job('', name='first')
    fp2 = submit.submit_job('', name='second', hold=['first', ''])
    fp3 = submit.submit_job('', name='third', hold=['first', 'second', 'fake'])
    fp4 = submit.submit_job('', name='fourth', hold=['fake'])

    assert flag not in fp.text()
    assert expected[0] in fp2.text()
    assert expected[1] in fp3.text()
    assert flag not in fp4.text()

    #test that we cant re-use a name
    with pytest.raises(RuntimeError):
        submit.submit_job('', name='first')

    #test that we cant re-use a name across a new submitter object
    with pytest.raises(RuntimeError):
        new_submit = module.Submitter(script=script_dir, log=log_dir)
        new_submit.submit_job('', name='first')


@pytest.mark.parametrize('module,copy_flag,env_string', [
    (pbs, '-V', '-v A=1,B=2'),
    (slurm, '--export=ALL', '--export=A=1,B=2'),
])
def test_env(tmpdirs, module, copy_flag, env_string):
    script_dir, log_dir = tmpdirs

    submit = module.Submitter(script=script_dir, log=log_dir)
    s1 = submit.submit_job('', copyEnv=True).text()
    s2 = submit.submit_job('', copyEnv=False, env={'A': 1, 'B': 2}).text()
    s3 = submit.submit_job('', copyEnv=True, env={'A': 1, 'B': 2}).text()
    s4 = submit.submit_job('', copyEnv=True, env={}).text()
    s5 = submit.submit_job('', copyEnv=False, env={}).text()

    assert copy_flag in s1

    assert env_string in s2 and copy_flag not in s2

    assert env_string in s3 and copy_flag not in s3  #env dict should overwrite copyEnv

    assert env_string not in s4 and copy_flag in s4  #if env dict is empty, format using copyEnv

    assert env_string not in s5 and copy_flag not in s5


@pytest.mark.parametrize('module,flag', [(pbs, 'walltime'), (slurm, '-t'),])
def test_empty_resources(tmpdirs, module, flag):
    resource = module.Resource()
    assert flag in resource.build()
    assert flag not in resource.build(time=None)
