import os
import json
import boto3

try:
    from . import file, log, preference
except ImportError:
    import file, log, preference

try:
    pass
except:
    pass

session = boto3.session.Session()
config = preference.config
secret = {
    'database': {},
    'ssh': {}
}

def getSecret(id, alias, **kwargs):
    """
        Get Secret

        Description:
            This method can be used to retrieve all secret stored in AWS Secret Manager
            based on Secret ID input

    """
    log.append('pypetl.core.aws.getSecret(%s, %s, %s): Starting...'%(id, alias, kwargs))
    global secret
    data = json.loads(session.client('secretsmanager').get_secret_value(SecretId=id)['SecretString'])
    if data['engine'] in ['redshift', 'postgres']:
        engine = 'database'
    elif data['engine'] in ['ssh']:
        engine = 'ssh'
    secret[engine][alias] = {}
    for key in ["username", "password", "engine", "host", "port", "database"]:
        secret[engine][alias][key] = data[key]
    log.append('pypetl.core.aws.getSecret(%s, %s, %s): Done!'%(id, alias, kwargs))


def getSecretAll():
    """
        Get All Secret

        Description:
            This method can be used to retrieve all secret stored in AWS Secret Manager
            based on stored Secret ID in the config.json file

    """
    log.append('pypetl.core.aws.getSecretAll(): Starting...')
    source = config['aws']['secret']
    for alias, id in source.items():
        getSecret(id, alias)
    log.append('pypetl.core.aws.getSecretAll(): Done!')
    