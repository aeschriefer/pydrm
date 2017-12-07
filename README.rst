|Build Status|

PYDRM
=====

A pure python library to submit jobs to distributed resource management systems. Currently there is support for Sun Grid Engine, PBS/Torque, and slurm. 

* Free software: BSD license

Requirements
===========

- Python 2.7
- sh
- path.py


Installation
============

Using PIP via Github

.. code:: bash

    pip install git+git://github.com/aeschriefer/pydrm.git

Manually via GIT

.. code:: bash
	  git clone git://github.com/aeschriefer/pydrm.git pydrm
	  cd pydrm
	  python setup.py install

Usage
=====

.. code:: python

	  >>> from drm import get_drm_module
	  >>> drm_module = get_drm_module()
	  >>> submitter = drm_module.Submitter(log='logs/', script='jobs/')
	  >>> resource = drm_module.Resource(memInGB=5, workers=4)
	  >>> kwargs = {'name': 'job', 'hold': ['prev_job']}
	  >>> bash_string = 'sleep 1 && echo "done!"'
	  >>> submitter.submit_job(bash_string,
	                           resource=resource,
                                   **kwargs
                                  )
	  
pydrm only submits jobs, it does not do any monitoring of running or queued jobs.


Development
===========

To run the all tests run::

    py.test tests/
