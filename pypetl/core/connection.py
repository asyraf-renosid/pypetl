"""
    Connection Manager
    
    Author: 
        Asyraf Nur
        
    Path: 
        pypetl.core.connection
    
    Description:
        -
        
    Dependency:
        - io
        - paramiko
        - sshtunnel
        - psycopg2 / psycopg2-binary
        - redshift_connector
        - pypetl.core.aws
        - pypetl.core.log
        - pypetl.core preference
        
""" 
import paramiko
import redshift_connector as rc
import psycopg2 as pg
from io import StringIO
from sshtunnel import SSHTunnelForwarder

try:
    from . import aws, log, preference
except ImportError:
    import aws, log, preference

# Global Variable
config = preference.config
credential = {}
session = {
    'database': {},
    'ssh': {}
}

def startDB(engine, host, port, username, password, database):
    """
        Start DB Session
        
        Author: 
            Asyraf Nur
            
        Path: 
            pypetl.core.log startDB()
        
        Description:
            -
            
        Dependency:
            -
            
    """ 
    if engine=='postgres':
        return pg.connect(
            host = host,
            port = port,
            database = database,
            user = username,
            password = password
        )
    elif engine=='redshift':
        return rc.connect(
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
    session['database'][alias] = startDB(alias, engine, host, port, username, password, database)

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
    return paramiko.RSAKey.from_private_key(StringIO(content))

def forwardTunnel(rsa, host, port, username, password, remote_address):
    if rsa == True:
        return SSHTunnelForwarder(
            (
                host, 
                port
            )
            , ssh_username = username
            , ssh_pkey = pkey
            , remote_bind_addresses = remote_address
        )
    else:
        return SSHTunnelForwarder(
            (
                host, 
                port
            )
            , ssh_username = username
            , ssh_password = password
            , remote_bind_addresses = remote_address
        )
    
def startSSHFromSecret(alias):
    global session
    credential = aws.secret['ssh'][alias]
    host, port = credential['host'], int(credential['port'])
    username, password = credential['username'], credential['password']
    rsa = config['tunnel_connection'][alias]['rsa']
    remote_alias = config['tunnel_connection'][alias]['remote']
    remote_address = mergeRemoteAddress(remote_alias)
    pkey = generateRSAKey(password)
    session['ssh']
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