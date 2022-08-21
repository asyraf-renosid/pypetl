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

def startDB(engine, host, port, username, password, database):
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
    global session
    credential = aws.secret['database'][alias]
    engine, host, port, username, password, database = itemgetter('engine', 'host', 'port', 'username', 'password', 'database')(credential)
    session['database'][alias] = startDB(engine, host, port, username, password, database)

def startDBAll():
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
    for alias in aws.secret['database'].keys():
        startDBFromSecret(alias)

def stopDBFromSecret(alias):
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
    session['database'][alias].close()

def stopDBAll():
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
    for alias in session['database'].keys():
        stopDBFromSecret(alias)

def mergeRemoteAddress(aliases, credentials):
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
    result = []
    for alias in aliases:
        host, port = itemgetter('host', 'port')(credentials[alias])
        result.append((host, port))
    return result

def generateRSAKey(password):
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
    content = "-----BEGIN RSA PRIVATE KEY-----\n%s\n-----END RSA PRIVATE KEY-----"%(password.replace(" ", "\n"))
    return paramiko.RSAKey.from_private_key(StringIO(content))

def generateTunnelForwarder(rsa, host, port, username, password, remote_address):
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
    if rsa == True:
        pkey = generateRSAKey(password)
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

def extractLocalAddress(session, aliases):
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
    result = {}
    for alias in aliases:
        host, port = session.local_bind_addresses[aliases.index(alias)]
        result[alias] = {
            "host": host,
            "port": port
        }
    return result

def startSSH(rsa, host, port, username, password, remote_address):
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
    result = generateTunnelForwarder(rsa, host, port, username, password, remote_address)
    result.start()
    return result

def startSSHFromSecret(alias):
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
    global session
    credential = aws.secret['ssh'][alias]
    credential_db = aws.secret['database']
    host, port, username, password = itemgetter('host', 'port', 'username', 'password')(credential)
    rsa = config['tunnel_connection'][alias]['rsa']
    remote_alias = config['tunnel_connection'][alias]['remote']
    remote_address = mergeRemoteAddress(remote_alias, credential_db)
    session['ssh'][alias] = startSSH(rsa, host, port, username, password, remote_address)
    print(aws.secret['database'])
    local_address = extractLocalAddress(session['ssh'][alias], remote_alias)
    for alias in remote_alias:
        aws.secret['database'][alias].update(local_address[alias])
    print(aws.secret['database'])
def startSSHAll():
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
    for alias in aws.secret['ssh'].keys():
        startSSHFromSecret(alias)

def stopSSHFromSecret(alias):
    session['ssh'][alias].stop()

def stopSSHAll():
    for alias in session['ssh'].keys():
        stopSSHFromSecret(alias)