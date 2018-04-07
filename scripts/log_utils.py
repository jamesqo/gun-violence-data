import logging as log

from inspect import stack

_funcs_logged = set()

def log_first_call(level=log.DEBUG):
    funcname = stack()[1].function
    if funcname not in _funcs_logged:
        _funcs_logged.add(funcname)
        log.log(level, "%s() called", funcname)
