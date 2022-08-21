import os
try:
    from .core import log, aws, connection
except ImportError:
    from core import log, aws, connection

env = os.environ.get("AWS_EXECUTION_ENV")
aws.getSecretAll()

def start(gap=""):
    """
    
    """
    fname = "pypetl.engine.start()"
    gap_new = gap + "   "
    log.append('%s: Starting...'%(fname), gap=gap)
    if env == None:
        connection.startSSHAll(gap=gap_new)
        connection.startDBAll(gap=gap_new)
    else:
        connection.startDBAll(gap=gap_new)
    log.append('%s: Started!'%(fname), gap=gap)
    
def stop(gap=""):
    fname = "pypetl.engine.stop()"
    gap_new = gap + "   "
    log.append('%s: Stopping...'%(fname), gap=gap)
    if env == None:
        connection.stopDBAll(gap=gap_new)
        connection.stopSSHAll(gap=gap_new)
    else:
        connection.stopDBAll(gap=gap_new)
    log.append('%s: Stopped!'%(fname), gap=gap)

start()