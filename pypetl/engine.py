import os
try:
    from .core import log, aws, connection
except ImportError:
    from core import log, aws, connection

log.configure(show = True)
aws.getSecretAll()
if os.environ.get("AWS_EXECUTION_ENV") == None:
    connection.startSSHAll()
    connection.startDBAll()
    connection.stopDBAll()
    connection.stopSSHAll
else:
    connection.startDBAll()
    connection.stopDBAll()
