"""
    File Manager
    
    Author: 
        Asyraf Nur
        
    Path: 
        pypetl/core/file.py

    Description:
        This python function developed in order to simplify all task related to the file management system
        
    Dependency:
        - json
"""

import json


try:
    from . import log
except ImportError:
    import log


def open_json(path, gap=""):
    fname = 'pypetl.core.file.open_json(%s)'%(path)
    gap_new = gap+"   "
    log.append('%s: Opening file...'%(fname), gap=gap_new)
    with open(path) as tmp:
        log.append('%s: Opened!'%(fname), gap=gap_new)
        return json.load(tmp)