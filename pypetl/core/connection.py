import paramiko
import redshift_connector as rc
import psycopg2 as pg
from io import StringIO
from sshtunnel import SSHTunnelForwarder

try:
    from . import aws, log, preference
except ImportError:
    import aws, log, preference

config = preference.config
credential = {}
session = {
    'database': {},
    'ssh': {}
}


def startDB(alias, engine, host, port, username, password, database):
    if engine=='postgres':
        session['database'][alias] = pg.connect(
            host = host,
            port = port,
            database = database,
            user = username,
            password = password
        )
    elif engine=='redshift':
        session['database'][alias] = rc.connect(
            host = host,
            port = port,
            database = database,
            user = username,
            password = password
        )

def startDBFromSecret(alias):
# ---------------------------------------------------------------------------------------------------- #
    global session
    credential = aws.secret['database'][alias]
    engine, host, port = credential['engine'], credential['host'], credential['port']
    username, password, database = credential['username'], credential['password'], credential['database']
    startDB(alias, engine, host, port, username, password, database)

def startDBAll():
    """
        Start All Database

        Description:
            This method can be used to start database session for all secret stored in AWS Secret Manager
    """
    for alias in aws.secret['database'].keys():
        startDBFromSecret(alias)

def stopDBFromSecret(alias):
    session['database'][alias].close()

def stopDBAll():
    for alias in session['database'].keys():
        stopDBFromSecret(alias)

def mergeRemoteAddress(aliases):
    result = []
    for alias in aliases:
        result.append((aws.secret['database'][alias]['host'], aws.secret['database'][alias]['port']))
    return result

def generateRSAKey(password):
    content = "-----BEGIN RSA PRIVATE KEY-----\n%s\n-----END RSA PRIVATE KEY-----"%(password.replace(" ", "\n"))
    result = paramiko.RSAKey.from_private_key(StringIO(content))
    return result

def startSSHFromSecret(alias):
    global session
    credential = aws.secret['ssh'][alias]
    host, port = credential['host'], int(credential['port'])
    username, password = credential['username'], credential['password']
    rsa = config['tunnel_connection'][alias]['rsa']
    remote_alias = config['tunnel_connection'][alias]['remote']
    remote_address = mergeRemoteAddress(remote_alias)
    pkey = generateRSAKey(password)
    if rsa == True:
        session['ssh'][alias] = SSHTunnelForwarder(
            (
                host, 
                port
            )
            , ssh_username = username
            , ssh_pkey = pkey
            , remote_bind_addresses = remote_address
        )
    else:
        session['ssh'][alias] = SSHTunnelForwarder(
            (
                host, 
                port
            )
            , ssh_username = username
            , ssh_password = password
            , remote_bind_addresses = remote_address
        )
    session['ssh'][alias].start()
    for db_alias in remote_alias:
        dbhost, dbport = session['ssh'][alias].local_bind_addresses[remote_alias.index(db_alias)]
        aws.secret['database'][db_alias]['host'] = dbhost
        aws.secret['database'][db_alias]['port'] = dbport

def startSSHAll():
    """
        Start All SSH Tunnel

        Description:
            This method can be used to start ssh tunnel session for all secret stored in AWS Secret Manager
    """
    for alias in aws.secret['ssh'].keys():
        startSSHFromSecret(alias)

def stopSSH(alias):
    session['ssh'][alias].stop()

def stopSSHAll():
    for alias in session['ssh'].keys():
        stopSSH(alias)