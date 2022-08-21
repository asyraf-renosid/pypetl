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
import boto3
import json

try:
    from . import log, preference
except ImportError:
    import log, preference

# Global Variable
session = boto3.session.Session()
config = preference.config
secret = {
    'database': {},
    'ssh': {}
}

def validateSecretEngine(engine, gap=""):
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
    fname = 'pypetl.core.aws.validateSecretEngine(%s)'%(engine)
    gap_new = gap+"   "
    log.append('%s: Validating...'%(fname), gap=gap)
    if engine in ['redshift', 'postgres']:
        result = 'database'
    elif engine in ['ssh']:
        result = 'ssh'
    log.append('%s: Validated as %s!'%(fname, result), gap=gap)
    return result
        
def getSecret(id, alias, gap="", **kwargs):
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
    fname = 'pypetl.core.aws.getSecret(%s, %s)'%(id, alias)
    gap_new = gap+"   "
    log.append('%s: Starting...'%(fname), gap=gap)
    global secret
    data = json.loads(session.client('secretsmanager').get_secret_value(SecretId=id)['SecretString'])
    engine = validateSecretEngine(data['engine'], gap=gap_new)
    secret[engine][alias] = {}
    for key in ["username", "password", "engine", "host", "port", "database"]:
        secret[engine][alias][key] = data[key]
    log.append('%s: Done!'%(fname), gap=gap)


def getSecretAll(gap=""):
    """
        Get All Secret
 
        Author: 
            Asyraf Nur
            
        Path: 
            pypetl.core.aws.getSecret()
        
        Description:
            This method can be used to retrieve all secret stored in AWS Secret Manager based on stored Secret ID in the config.json file

    """
    fname = 'pypetl.core.aws.getSecretAll()'
    gap_new = gap+"   "
    log.append('%s: Starting...'%(fname), gap=gap)
    source = config['aws']['secret']
    for alias, id in source.items():
        getSecret(id, alias, gap=gap_new)
    log.append('%s: Done!'%(fname), gap=gap)
    
