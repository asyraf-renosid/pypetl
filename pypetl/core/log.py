from inspect import Arguments
import os
from datetime import datetime, timedelta, date, time
from time import sleep

try:
    pass
except:
    pass

report = []
show = False

def configure(**kwargs):
    global show
    kwargs_key = kwargs.keys()
    if 'show' in kwargs_key:
        show = kwargs['show']

def format(log):
    return (' >  '.join(repr(l) for l in log)).replace("'","")

def append(message, **kwargs):
    tempshow = show
    kwargs_key = kwargs.keys()
    if 'show' in kwargs_key:
        tempshow = kwargs['show']
    log = [(datetime.utcnow() + timedelta(hours=7)).strftime('%Y-%m-%d %H:%M:%S'), message]
    report.append(log)
    if tempshow == True:
        print(format(log))

def show():
    for log in report:
        print(format(log))
