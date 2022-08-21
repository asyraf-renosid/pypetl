import os
try:
    from .core import log, aws, connection
except ImportError:
    from core import log, aws, connection

log.configure(show = True)
env = os.environ.get("AWS_EXECUTION_ENV")
aws.getSecretAll()

def start():
    """
    
    """
    if env == None:
        connection.startSSHAll()
        connection.startDBAll()
    else:
        connection.startDBAll()
    
def stop():
    if env == None:
        connection.stopDBAll()
        connection.stopSSHAll()
    else:
        connection.stopDBAll()

#start()
#stop()