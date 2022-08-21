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
        - psycopg2 / psycopg2-binary
        - redshift_connector / redshift-connector 
        - sshtunnel.SSHTunnelForwarder
        - operator.itemgetter
        - pypetl.core.aws
        - pypetl.core.log
        - pypetl.core preference
        
""" 
import paramiko
import redshift_connector as rc
import psycopg2 as pg
from io import StringIO
from sshtunnel import SSHTunnelForwarder
from operator import itemgetter

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

def startDB(engine, host, port, username, password, database, gap=""):
    """
        Start DB Session
        
        Author: 
            Asyraf Nur
            
        Path: 
            pypetl.core.connection.startDB()
        
        Description:
            Start database session using accessible credentials
            
        Dependency:
            - psycopg2 / psycopg2-binary
            - redshift_connector / redshift-connector
            
    """ 
    fname = 'pypetl.core.connection.startDB(%s, %s, %s, %s)'%(engine, host, port, database)
    gap_new = gap+"   "
    log.append('%s: Starting database session...'%(fname), gap=gap)
    if engine=='postgres':
        result = pg.connect(
            host = host,
            port = port,
            database = database,
            user = username,
            password = password
        )
    elif engine=='redshift':
        result = rc.connect(
            host = host,
            port = port,
            database = database,
            user = username,
            password = password
        )
    log.append('%s: Started!'%(fname), gap=gap)
    return result

def startDBFromSecret(alias, gap=""):
    """
        Start Database Session From Certain Secret
        
        Author: 
            Asyraf Nur
            
        Path: 
            pypetl.core.connection.startDBFromSecret()
        
        Description:
            Start database session using credential based on alias of the stored secrets 
            
        Dependency:
            - operator.itemgetter
            - pypetl.core.connection.startDB
            
    """ 
    fname = 'pypetl.core.connection.startDBFromSecret(%s)'%(alias)
    gap_new = gap+"   "
    log.append('%s: Starting...'%(fname), gap=gap)
    global session
    credential = aws.secret['database'][alias]
    engine, host, port, username, password, database = itemgetter('engine', 'host', 'port', 'username', 'password', 'database')(credential)
    session['database'][alias] = startDB(engine, host, port, username, password, database, gap=gap_new)
    log.append('%s: Started!'%(fname), gap=gap)

def startDBAll(gap=""):
    """
        Start All Database Session

        Author: 
            Asyraf Nur
            
        Path: 
            pypetl.core.connection.startDBAll()

        Description:
            This method can be used to start database session for all secret stored in AWS Secret Manager
                
        Dependency:
            - operator.itemgetter
            - pypetl.core.connection.startDBFromSecret

    """
    fname = 'pypetl.core.connection.startDBAll()'
    gap_new = gap+"   "
    log.append('%s: Starting...'%(fname), gap=gap)
    for alias in aws.secret['database'].keys():
        startDBFromSecret(alias, gap=gap_new)
    log.append('%s: Starting...'%(fname), gap=gap)

def stopDBFromSecret(alias, gap=""):
    """
        Stop Database Session From Certain Secret 

        Author: 
            Asyraf Nur
            
        Path: 
            pypetl.core.connection.stopDBFromSecret()

        Description:
            -

        Dependency:
            - 

    """
    fname = 'pypetl.core.connection.stopDBFromSecret(%s)'%(alias)
    gap_new = gap+"   "
    log.append('%s: Stopping...'%(fname), gap=gap)
    session['database'][alias].close()
    log.append('%s: Stopped!'%(fname), gap=gap)

def stopDBAll(gap=""):
    """
        Stop All Database Session 

        Author: 
            Asyraf Nur
            
        Path: 
            pypetl.core.connection.stopDBAll()

        Description:
            -

        Dependency:
            - 

    """
    fname = 'pypetl.core.connection.stopDBAll()'
    gap_new = gap+"   "
    log.append('%s: Stopping...'%(fname), gap=gap)
    for alias in session['database'].keys():
        stopDBFromSecret(alias, gap=gap_new)
    log.append('%s: Stopped!'%(fname), gap=gap)

def mergeRemoteAddress(aliases, credentials, gap=""):
    """
        Merge Remote Address  

        Author: 
            Asyraf Nur
            
        Path: 
            pypetl.core.connection.mergeRemoteAddress()

        Description:
            -

        Dependency:
            - 

    """
    fname = 'pypetl.core.connection.mergeRemoteAddress(%s)'%(aliases)
    gap_new = gap+"   "
    log.append('%s: Merging multiple remote addresses...'%(fname), gap=gap)
    result = []
    for alias in aliases:
        host, port = itemgetter('host', 'port')(credentials[alias])
        result.append((host, port))
    log.append('%s: Merged!'%(fname), gap=gap)
    return result

def generateRSAKey(password, gap=""):
    """
        Generate RSA Key  

        Author: 
            Asyraf Nur
            
        Path: 
            pypetl.core.connection.generateRSAKey()

        Description:
            -

        Dependency:
            - 

    """
    fname = 'pypetl.core.connection.generateRSAKey()'
    gap_new = gap+"   "
    log.append('%s: Generating RSA key...'%(fname), gap=gap)
    content = "-----BEGIN RSA PRIVATE KEY-----\n%s\n-----END RSA PRIVATE KEY-----"%(password.replace(" ", "\n"))
    result = paramiko.RSAKey.from_private_key(StringIO(content))
    log.append('%s: Generated!'%(fname), gap=gap)
    return result

def generateTunnelForwarder(rsa, host, port, username, password, remote_address, gap=""):
    """
        Generate SSH Tunnel Forwarder  

        Author: 
            Asyraf Nur
            
        Path: 
            pypetl.core.connection.generateTunnelForwarder()

        Description:
            -

        Dependency:
            - 

    """
    fname = 'pypetl.core.connection.generateTunnelForwarder(%s, %s, %s)'%(host, port, remote_address)
    gap_new = gap+"   "
    log.append('%s: Generating SSH tunnel forwarder...'%(fname), gap=gap)
    if rsa == True:
        pkey = generateRSAKey(password, gap=gap_new)
        result = SSHTunnelForwarder(
            (
                host, 
                port
            )
            , ssh_username = username
            , ssh_pkey = pkey
            , remote_bind_addresses = remote_address
        )
    else:
        result = SSHTunnelForwarder(
            (
                host, 
                port
            )
            , ssh_username = username
            , ssh_password = password
            , remote_bind_addresses = remote_address
        )
    log.append('%s: Generated!'%(fname), gap=gap)
    return result

def extractLocalAddress(session, aliases, gap=""):
    """
        Extract Session's Binded Local Address

        Author: 
            Asyraf Nur
            
        Path: 
            pypetl.core.connection.extractLocalAddress()

        Description:
            -

        Dependency:
            - 

    """
    fname = 'pypetl.core.connection.extractLocalAddress()'
    gap_new = gap+"   "
    log.append('%s: Extracting binded local addresses...'%(fname), gap=gap)
    result = {}
    for alias in aliases:
        host, port = session.local_bind_addresses[aliases.index(alias)]
        result[alias] = {
            "host": host,
            "port": port
        }
    log.append('%s: Extracted!'%(fname), gap=gap)
    return result

def startSSH(rsa, host, port, username, password, remote_address, gap=""):
    """
        Start SSH

        Author: 
            Asyraf Nur
            
        Path: 
            pypetl.core.connection.extractLocalAddress()

        Description:
            -

        Dependency:
            - 

    """
    fname = 'pypetl.core.connection.startSSH(%s, %s, %s)'%(host, port, remote_address)
    gap_new = gap+"   "
    log.append('%s: Starting...'%(fname), gap=gap)
    result = generateTunnelForwarder(rsa, host, port, username, password, remote_address, gap=gap_new)
    result.start()
    log.append('%s: Started!'%(fname), gap=gap)
    return result

def startSSHFromSecret(alias, gap=""):
    """
        Start SSH Session From Certain Secret  

        Author: 
            Asyraf Nur
            
        Path: 
            pypetl.core.connection.startSSHFromSecret()

        Description:
            -

        Dependency:
            - 

    """
    fname = 'pypetl.core.connection.startSSHFromSecret(%s)'%(alias)
    gap_new = gap+"   "
    log.append('%s: Starting...'%(fname), gap=gap)
    global session
    credential = aws.secret['ssh'][alias]
    credential_db = aws.secret['database']
    host, port, username, password = itemgetter('host', 'port', 'username', 'password')(credential)
    rsa = config['tunnel_connection'][alias]['rsa']
    remote_alias = config['tunnel_connection'][alias]['remote']
    remote_address = mergeRemoteAddress(remote_alias, credential_db, gap=gap_new)
    session['ssh'][alias] = startSSH(rsa, host, port, username, password, remote_address, gap=gap_new)
    local_address = extractLocalAddress(session['ssh'][alias], remote_alias, gap=gap_new)
    for alias in remote_alias:
        aws.secret['database'][alias].update(local_address[alias])
    log.append('%s: Started!'%(fname), gap=gap)
    
def startSSHAll(gap=""):
    """
        Start All SSH Session in the Secret  

        Author: 
            Asyraf Nur
            
        Path: 
            pypetl.core.connection.startSSHAll()

        Description:
            -

        Dependency:
            - 

    """
    fname = 'pypetl.core.connection.startSSHAll()'
    gap_new = gap+"   "
    log.append('%s: Starting...'%(fname), gap=gap)
    for alias in aws.secret['ssh'].keys():
        startSSHFromSecret(alias, gap=gap_new)
    log.append('%s: Started!'%(fname), gap=gap)

def stopSSHFromSecret(alias, gap=""):
    fname = 'pypetl.core.connection.stopSSHFromSecret(%s)'%(alias)
    gap_new = gap+"   "
    log.append('%s: Stopping...'%(fname), gap=gap)
    session['ssh'][alias].stop()
    log.append('%s: Stopped!'%(fname), gap=gap)

def stopSSHAll(gap=""):
    fname = 'pypetl.core.connection.stopSSHAll()'
    gap_new = gap+"   "
    log.append('%s: Stopping...'%(fname), gap=gap)
    for alias in session['ssh'].keys():
        stopSSHFromSecret(alias, gap_new)
    log.append('%s: Stopped!'%(fname), gap=gap)
    