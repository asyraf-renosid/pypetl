import petl

from ..core import log, connection

def fromDBSecret(alias, query, gap="", cache=False):
    session = connection.session['database'][alias]
    session.commit()
    result = petl.fromdb(session, query)
    session.commit()
    if cache:
        result = result.cache()
    return result
