"""
    Log Manager
    
    Author: 
        Asyraf Nur
        
    Path: 
        pypetl.core.log
    
    Description:
        -
        
    Dependency:
        - datetime.datetime
        - datetime.timedelta
        - datetime.date
        - datetime.time
        
""" 
import os
from datetime import datetime, timedelta, date, time

# Global Variable
report = []
show = False
separatorTimeMessage = '>'

def configure(**kwargs):
    """
        Configure
        
        Author: 
            Asyraf Nur
            
        Path: 
            pypetl.core.log.configure()
        
        Description:
            -
            
        Dependency:
            -
            
    """ 
    global show
    kwargs_key = kwargs.keys()
    if 'show' in kwargs_key:
        show = kwargs['show']
    if 'sepTM' in kwargs_key:
        separatorTimeMessage = kwargs['sepTM']

def format(log, **kwargs):
    """
        Formatter
        
        Author: 
            Asyraf Nur
            
        Path: 
            pypetl.core.log.format()
        
        Description:
            -
            
        Dependency:
            - 
            
    """ 
    kwargs_key = kwargs.keys()
    if 'gap' in kwargs_key:
        return ('%s %s  %s%s'%(log[0], separatorTimeMessage, kwargs['gap'], log[1]))
    else:
        return ('%s %s  %s'%(log[0], separatorTimeMessage, log[1]))

def append(message, **kwargs):
    """
        Append Log Message
        
        Author: 
            Asyraf Nur
            
        Path: 
            pypetl.core.log.append()
        
        Description:
            -
            
        Dependency:
            - datetime.datetime
            - datetime.timedelta
            - datetime.date
            - datetime.time
            - pypetl.core.log.format()
            
    """ 
    kwargs_key = kwargs.keys()
    if 'show' in kwargs_key:
        tempshow = kwargs['show']
    else:
        tempshow = show
    
    log = [(datetime.utcnow() + timedelta(hours=7)).strftime('%Y-%m-%d %H:%M:%S'), message]
    report.append(log)
    
    if 'gap' in kwargs_key:
        if tempshow == True:
            print(format(log, gap=kwargs['gap']))
    else:
        if tempshow == True:
            print(format(log))
            

def show():
    """
        Show / Print All Recorded Log
        
        Author: 
            Asyraf Nur
            
        Path: 
            pypetl.core.log.show()
        
        Description:
            -
            
        Dependency:
            - pypetl.core.log.format()
            
    """ 
    for log in report:
        print(format(log))
