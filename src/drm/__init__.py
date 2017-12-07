def get_drm_module():
    from distutils.spawn import find_executable
    if find_executable('pbsnodes'):
        from . import pbs
        return pbs

    elif find_executable('qacct'):
        from . import sge
        return sge

    elif find_executable('sbatch'):
        from . import slurm
        return slurm

    else:
        from . import bash
        return bash
    
