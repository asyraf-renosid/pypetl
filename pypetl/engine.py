import os
try:
    from .core import log, aws, connection
except ImportError:
    from core import log, aws, connection

log.configure(show = True)
env = os.environ.get("AWS_EXECUTION_ENV")
aws.getSecretAll()

def start(gap=""):
    """
    
    """
    fname = "pypetl.engine.start()"
    gap_new = gap + "   "
    
    log.append('%s: Starting...')
    if env == None:
        connection.startSSHAll(gap=gap_new)
        connection.startDBAll(gap=gap_new)
    else:
        connection.startDBAll(gap=gap_new)
    
def stop(gap=""):
    if env == None:
        connection.stopDBAll()
        connection.stopSSHAll()
    else:
        connection.stopDBAll()

#start()
#stop()