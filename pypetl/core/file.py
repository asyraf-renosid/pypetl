"""
    File Manager
    
    author: Asyraf Nur
    path: pypetl/core/file.py

    Description:
        This python function developed in order to simplify 
        all task related to the file management system
"""

import json

def open_json(path):
    with open(path) as tmp:
        return json.load(tmp)