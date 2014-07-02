import sys

if sys.version_info < (2, 6):
    import warnings
    warnings.warn("This version is unsupported on Python versions older than 2.6", ImportWarning)

def _checkRequirePackage():
    import os, logging
    try:
        from Pyro4.constants import VERSION as __pyro4_version__
        import psutil as ps
    except :
        log = logging.getLogger("opencluster")
        log.error("opencluster require Pyro4 and psutil,please install Pyro4 and psutil firstly.")

_checkRequirePackage()
del _checkRequirePackage

__all__ = ['asyncexector', 'beancontext', 'configuration','contractor','contractorservice','errors','hbdaemon','item','meta','parallelservice','parkleader','parklocal','parkpatternexector','parkservice','servicecontext','worker','workman','workerparallel','workservice']
