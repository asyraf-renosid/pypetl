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

def executeDBSecret(alias, query):
    session = connection.session['database'][alias]
    cursor = session.cursor()
    session.commit()
    cursor.execute(query)
    session.commit()

def toDBSecretDelete(alias, table, location_table, condition='id'):
    if table.nrows() != 0:
        delete_value = ', '.join( repr(v) for v in table.todataframe()[condition].values.tolist()).replace("'","")
        delete_query = 'DELETE FROM %s WHERE %s in ( %s );'%(
            location_table,
            condition,
            delete_value
        )
        executeDBSecret(alias, delete_query)

def toDBSecretUpdate(alias, table, location_table, condition='id'):
    if table.nrows() != 0:
        delete_value = ', '.join( repr(v) for v in table.todataframe()[condition].values.tolist()).replace("'","")
        delete_query = 'DELETE FROM %s WHERE %s in ( %s );'%(
            location_table,
            condition,
            delete_value
        )
        executeDBSecret(alias, delete_query)
        table_field = ', '.join( repr(v) for v in list(table.fieldnames())).replace("'","")
        table_value = ', '.join( repr(v) for v in list(table.data())).replace("[", "(").replace("]", ")").replace("None", "null").replace("'null'", "null")
        table_query = 'INSERT INTO %s ( %s ) VALUES %s ;'%(
            location_table,
            table_field,
            table_value
        )
        executeDBSecret(alias, table_query)

def toDBSecretInsert(alias, table, location_table):
    if table.nrows() != 0:
        table_field = ', '.join( repr(v) for v in list(table.fieldnames())).replace("'","")
        table_value = ', '.join( repr(v) for v in list(table.data())).replace("[", "(").replace("]", ")").replace("None", "null").replace("'null'", "null")
        table_query = 'INSERT INTO %s ( %s ) VALUES %s ;'%(
            location_table,
            table_field,
            table_value
        )
        
        executeDBSecret(alias, table_query)