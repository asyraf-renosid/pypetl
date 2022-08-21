"""
    AWS Manager (using boto3)
    
    Author: 
        Asyraf Nur
    
    Path: 
        pypetl.core.aws

    Description:
        ----
        
    Dependency:
        - boto3
        - json
        - pypetl.core.file
        - pypetl.core.log
        - pypetl.core.preference
"""
import os
import boto3
import json

try:
    from . import file, log, preference
except ImportError:
    import file, log, preference

# Global Variable
session = boto3.session.Session()
config = preference.config
secret = {
    'database': {},
    'ssh': {}
}

def validateSecretEngine(engine):
    """
        Validate Engine of the Secret
        
        Author: 
            Asyraf Nur
            
        Path: 
            pypetl.core.aws.validateSecretEngine()
        
        Description:
            -
            
        Dependency:
            -
            
    """ 
    if engine in ['redshift', 'postgres']:
        result = 'database'
    elif engine in ['ssh']:
        result = 'ssh'
    return result
        
def getSecret(id, alias, **kwargs):
    """
        Get Secret
        
        Author: 
            Asyraf Nur
            
        Path: 
            pypetl.core.aws.getSecret()
        
        Description:
            This method can be used to retrieve all secret stored in AWS Secret Manager based on Secret ID input and cache them in the memory
            
        Dependency:
            - json
            - pypetl.core.aws.validateSecretEngine
            
    """
    log.append('pypetl.core.aws.getSecret(%s, %s): Starting...'%(id, alias))
    global secret
    data = json.loads(session.client('secretsmanager').get_secret_value(SecretId=id)['SecretString'])
    engine = validateSecretEngine(data['engine'])
    secret[engine][alias] = {}
    for key in ["username", "password", "engine", "host", "port", "database"]:
        secret[engine][alias][key] = data[key]
    log.append('pypetl.core.aws.getSecret(%s, %s): Done!'%(id, alias))


def getSecretAll():
    """
        Get All Secret
 
        Author: 
            Asyraf Nur
            
        Path: 
            pypetl.core.aws.getSecret()
        
        Description:
            This method can be used to retrieve all secret stored in AWS Secret Manager based on stored Secret ID in the config.json file

    """
    log.append('pypetl.core.aws.getSecretAll(): Starting...')
    source = config['aws']['secret']
    for alias, id in source.items():
        getSecret(id, alias)
    log.append('pypetl.core.aws.getSecretAll(): Done!')
    
