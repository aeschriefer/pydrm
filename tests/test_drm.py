from future import standard_library
standard_library.install_aliases()
from builtins import str
from builtins import map
from builtins import next
from builtins import range
from builtins import object
import pytest
import re
import os
import time
import pickle
from io import BytesIO
from datetime import timedelta, datetime
from path import Path
from drm import pbs, base, slurm, bash

SHELL = base.Submitter.shell


class FakeProcess(object):
    def __init__(self, stdout):
        self.stdout = str(stdout)


#I think this runs for every test so jid should be new everytime
@pytest.fixture(autouse=True)
def no_submit(monkeypatch):

    jid = iter(range(1, 100))

    def mocksubmit(self, fp):
        return FakeProcess(next(jid))

    for module in [pbs, slurm]:
        monkeypatch.setattr(module.Submitter, '_submit', mocksubmit)
        #reset the job_id dict between module tests
        monkeypatch.setattr(module.Submitter, '_JOB_NAME_TO_ID', {})
        monkeypatch.setattr(module.Submitter, '_NO_NAME_JOBS', set())


@pytest.fixture
def no_query(monkeypatch):
    sacct_data = '''1|COMPLETED|0:0|2016-02-13T11:32:47
2|CANCELLED|0:0|{0}
3|COMPLETED|0:15|{0}
3.batch|COMPLETED|0:15|{0}
4|PENDING|0:0|{0}
5|FAILED|1:0|{0}'''

    time_string = datetime.utcnow().strftime(slurm.Waiter._time_format)
    sacct_data = sacct_data.format(time_string)

    def mockcmd(self, jobs):
        # only take lines that have jobids actually mentioned unless
        # no jobids passed then return everything
        if not jobs:
            return FakeProcess(sacct_data)

        cond = lambda line: any(re.match(jid, line) for jid in jobs)
        filtered = '\n'.join(
            [line for line in sacct_data.split('\n') if cond(line)])
        return FakeProcess(filtered)

    monkeypatch.setattr(slurm.Waiter, '_cmd', mockcmd)


@pytest.fixture
def tmpdirs(tmpdir_factory):
    return tmpdir_factory.mktemp('scripts'), tmpdir_factory.mktemp('log')


# yapf: disable
@pytest.mark.parametrize('module,kwargs,expected', [
    (pbs, {
    'time':
    timedelta(days=2, minutes=6),
    'workers':
    4,
    'memInGB':
    2.345
}, ['-l nodes=1:ppn=4,vmem=3gb,walltime=48:06:00']),
    (pbs, {}, [
    '-l nodes=1:ppn=1,vmem=1gb,walltime=00:59:00'
]),
    (pbs, {
    'constraint': pbs.Constraint('intel')
}, ['-l nodes=1:ppn=1:intel,vmem=1gb,walltime=00:59:00']),
    (slurm, {
    'time':
    timedelta(days=2, minutes=6),
    'workers':
    4,
    'memInGB':
    2.345
}, ['-t 48:06:00', '-c 4', '--mem=2345']),
    (slurm, {}, [
    '-t 00:59:00', '-c 1', '--mem=1000'
]), (slurm, {
    'constraint': slurm.Constraint('intel')
}, ['-t 00:59:00', '-c 1', '--mem=1000', '--constraint intel'])])
# yapf: enable
def test_resource(tmpdirs, module, kwargs, expected):
    resource = module.Resource(**kwargs)
    script_dir, log_dir = tmpdirs
    submit = module.Submitter(script=script_dir, log=log_dir)

    script = submit.submit_job('ls', resource=resource).script.text()

    for ex in expected:
        assert ex + os.linesep in script


# yapf: disable
@pytest.mark.parametrize('module,kwargs,expected', [
    (pbs, {
    'time':
    timedelta(days=2, minutes=6),
    'workers':
    4,
    'memInGB':
    2.345
}, ['-l nodes=4:ppn=1,vmem=3gb,walltime=48:06:00']),
    (pbs, {}, [
    '-l nodes=1:ppn=1,vmem=1gb,walltime=00:59:00'
]),
    (pbs, {
    'constraint': pbs.Constraint('intel')
}, ['-l nodes=1:ppn=1:intel,vmem=1gb,walltime=00:59:00']),
    (slurm, {
    'time':
    timedelta(days=2, minutes=6),
    'workers':
    4,
    'memInGB':
    2.345
}, ['-t 48:06:00', '--ntasks 4', '--cpus-per-task=1', '--ntasks-per-node=1',
    '--mem=2345']),
    (slurm, {}, [
        '-t 00:59:00', '--mem=1000', '--ntasks 1', '--cpus-per-task=1',
        '--ntasks-per-node=1'
    ]),
    (slurm, {'ppn': None}, [
        '-t 00:59:00', '--mem=1000', '--ntasks 1'
    ]),
    (slurm, {
        'constraint': slurm.Constraint('intel'), 'workers': 3, 'ppn': 4,
    }, [
        '-t 00:59:00', '--mem=1000', '--ntasks 1', '--cpus-per-task=4',
        '--ntasks-per-node=1', '--constraint intel'
    ])])
# yapf: enable
def test_mpi_resource(tmpdirs, module, kwargs, expected):
    resource = module.MpiResource(**kwargs)
    script_dir, log_dir = tmpdirs
    submit = module.Submitter(script=script_dir, log=log_dir)

    script = submit.submit_job('ls', resource=resource).script.text()
    for ex in expected:
        assert ex + os.linesep in script


def test_bash_submit(tmpdirs):
    script_dir, log_dir = tmpdirs
    submit = bash.Submitter(script=script_dir, log=log_dir)
    # Test that nothing is done for empty job
    assert submit.submit_job('', name='test', hold='testA') is None
    fp = submit.submit_job('echo hello', name='test', hold='testA').script
    script = open(fp, 'r').read()

    assert fp == Path(script_dir).joinpath(
        submit.script_name_join.join(['test', submit.uid]))
    assert '#!{}'.format(SHELL) + os.linesep in script

    output = open(os.path.join(str(log_dir), 'test.stdout'), 'r').read()
    assert re.search(r'^hello$', output)


def test_slurm_submit(tmpdirs):
    script_dir, log_dir = tmpdirs
    submit = slurm.Submitter(script=script_dir, log=log_dir)
    # Test that nothing is done for empty job
    assert submit.submit_job('', name='test', hold='testA') is None

    fp = submit.submit_job('ls', name='test', hold='testA').script
    script = open(fp, 'r').read()

    assert fp == Path(script_dir).joinpath(
        submit.script_name_join.join(['test', submit.uid]))
    assert '#!{}'.format(SHELL) + os.linesep in script


def test_pbs_submit(tmpdirs):
    script_dir, log_dir = tmpdirs
    submit = pbs.Submitter(script=script_dir, log=log_dir)
    # Test that nothing is done for empty job
    assert submit.submit_job('', name='test', hold='testA') is None

    fp = submit.submit_job('ls', name='test', hold='testA').script
    script = open(fp, 'r').read()

    assert fp == Path(script_dir).joinpath(
        submit.script_name_join.join(['test', submit.uid]))

    assert re.match('^#!{}$'.format(SHELL), script, re.MULTILINE)
    assert re.search(r'^#PBS -N test$', script, re.MULTILINE)
    #workdir should default to current working directory
    assert re.search('^#PBS -d {0}$'.format(Path.getcwd()), script,
                     re.MULTILINE)
    assert re.search('^#PBS -e %s$' % log_dir, script, re.MULTILINE)
    assert re.search('^#PBS -o %s$' % log_dir, script, re.MULTILINE)
    assert re.search('^#PBS -V$', script, re.MULTILINE)
    assert re.search(r'^ls$', script, re.MULTILINE)

    #job named testA has not be submitted so we shouldn't hold on it
    assert not re.search(r'^#PBS -W$', script, re.MULTILINE)


@pytest.mark.parametrize('module,flag,expected', [
    (pbs, '-W', ['-W depend=afterok:1', '-W depend=afterok:1:2']),
    (slurm, '-d', ['-d afterok:1', '-d afterok:1:2']),
])
def test_hold_jobs(tmpdirs, module, flag, expected):
    script_dir, log_dir = tmpdirs

    submit = module.Submitter(script=script_dir, log=log_dir)
    fp = submit.submit_job('ls', name='first').script
    fp2 = submit.submit_job('ls', name='second', hold=['first', '']).script
    fp3 = submit.submit_job(
        'ls', name='third', hold=['first', 'second', 'fake']).script
    fp4 = submit.submit_job('ls', name='fourth', hold=['fake']).script

    assert flag not in fp.text()
    assert expected[0] in fp2.text()
    assert expected[1] in fp3.text()
    assert flag not in fp4.text()

    #test that we cant re-use a name
    with pytest.raises(RuntimeError):
        submit.submit_job('ls', name='first')

    #test that we cant re-use a name across a new submitter object
    with pytest.raises(RuntimeError):
        new_submit = module.Submitter(script=script_dir, log=log_dir)
        new_submit.submit_job('ls', name='first')


@pytest.mark.parametrize('module,copy_flag', [
    (pbs, '-V'),
    (slurm, '--export=ALL'),
])
def test_env(tmpdirs, module, copy_flag):
    script_dir, log_dir = tmpdirs

    submit = module.Submitter(script=script_dir, log=log_dir)
    s1 = submit.submit_job('ls').script.text()
    assert copy_flag in s1


@pytest.mark.parametrize('module,expected', [
    (slurm, ['--array=0-0', '--array=0-1']),
    (pbs, ['-t 0-0', '-t 0-1']),
])
def test_array_submit(tmpdirs, module, expected):
    def search(lst, text):
        for x in lst:
            if not re.search(r'{}\n'.format(x), text):
                return False
        return True

    script_dir, log_dir = tmpdirs
    submit = module.Submitter(script=script_dir, log=log_dir)

    array = module.JobArray()
    array.add_job('./job1')
    fp = submit.submit_job(array, name='first').script
    assert search([expected[0], '== 0 ]\s+./job1'], fp.text())

    array.add_job('./job2')
    fp2 = submit.submit_job(array, name='second').script
    assert search([expected[1], '== 0 ]\s+./job1', '== 1 ]\s+./job2'],
                  fp2.text())

    assert submit.submit_job(module.JobArray(), name='third') is None


@pytest.mark.parametrize('module,flag', [
    (pbs, 'walltime'),
    (slurm, '-t'),
])
def test_empty_resources(tmpdirs, module, flag):
    resource = module.Resource()
    assert flag in str(resource)
    assert flag not in str(module.Resource(time=None))


@pytest.mark.parametrize('module', [
    slurm,
])
def test_wait_success(no_query, module):
    start = time.time()
    module.Waiter(['1'], interval=1, timeout=1).wait()
    assert time.time() - start > 1


@pytest.mark.parametrize('module', [
    slurm,
])
def test_wait_finish(no_query, module):
    start = time.time()
    module.Waiter(['2'], interval=1, timeout=1).wait()
    assert time.time() - start < 0.1


@pytest.mark.parametrize('module', [
    slurm,
])
def test_wait_no_jobs(no_query, module):
    start = time.time()
    module.Waiter([], interval=1, timeout=1).wait()
    assert time.time() - start < 0.1


def test_slurm_waiter_jobstatus(no_query):
    waiter = slurm.Waiter(list(map(str, list(range(1, 7)))))

    waiter.query()
    assert waiter.unsuccessful_jobs() == ['2', '5']
    assert waiter.successful_jobs() == ['3']

    waiter2 = slurm.Waiter('6')
    waiter2.query
    assert waiter2.unsuccessful_jobs() == []
    assert waiter2.successful_jobs() == []


def test_waiter_start_time_pickle():
    waiter = slurm.Waiter([])
    fh = BytesIO()
    pickle.dump(waiter, fh)
    fh.seek(0)
    loaded = pickle.load(fh)
    assert waiter._MODULE_START_TIME == loaded._MODULE_START_TIME
