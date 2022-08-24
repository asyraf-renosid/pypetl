import pypetl
from datetime import datetime

def run(tablename, select_data, source_date_up, source_date_in, constraint_pk, source_pk=""):
    if source_pk == "":
        source_pk = constraint_pk
    rds = 'public.%s'%(tablename)
    rds_metadata = ''
    redshift = 'renosdb_fact.%s'%(tablename)
    redshift_metadata = 'renosdb_metadata.%s'%(tablename)
    fname = "pipeline_fact.%s.run()"%(tablename)
    select_metadata = 'id, date_in, user_in, referenced_id, referenced_%s, referenced_date_etl'%(constraint_pk)

    pypetl.log.append("%s: Extracting data from database..."%(fname))

    source_redshift_metadata = pypetl.fromDBSecret(
        'redshift',
        'SELECT %s FROM %s'%(select_metadata, redshift_metadata),
        cache=True
    )     
    referenced_source_id = source_redshift_metadata.todataframe()['referenced_'+constraint_pk].values.tolist()
    referenced_id = source_redshift_metadata.todataframe()['referenced_id'].values.tolist()
    if referenced_id == []:
        source_rds = pypetl.fromDBSecret(
            'rds',
            "SELECT %s FROM %s;"%(select_data, rds),
            cache=True
        )
    else:
        source_rds = pypetl.fromDBSecret(
            'rds',
            "SELECT %s FROM %s WHERE TO_NUMBER( CONCAT(%s, TO_CHAR( CASE WHEN %s is null THEN %s ELSE %s END, 'YYYYMMDD')), '9999999999999999999999') not in (%s);"%(select_data, rds, source_pk, source_date_up, source_date_in, source_date_up, ', '.join(repr(d) for d in referenced_id)),
            cache=True
        )

    pypetl.log.append("%s: Extracted!"%(fname))
    pypetl.log.append("%s: Transforming data..."%(fname))

    transform_rds = source_rds\
        .addfield(
            'date_etl',
            lambda rec: rec['date_in'] if rec['date_up'] == None else rec['date_up']
        ).addfield(
            'referenced_id',
            lambda rec: int(str(rec[constraint_pk]) + rec['date_etl'].strftime('%Y%m%d')) 
        )
    transform_redshift_metadata = transform_rds\
            .cut(constraint_pk, 'date_etl', 'referenced_id')\
            .rename({constraint_pk: 'referenced_'+constraint_pk, 'date_etl': 'referenced_date_etl'})\
            .addfield('date_in', datetime.utcnow())\
            .addfield('user_in', 'lambda_renos_crawler')

    pypetl.log.append("%s: Transformed!"%(fname))

    pypetl.log.append("%s: Validating..."%(fname))
    data_count = transform_rds.nrows()
    pypetl.log.append("%s: Validated with %s data(s)!"%(fname, data_count))

    if data_count == 0:

        pypetl.log.append("%s: Finalization and data load process will be skipped!"%(fname))

    else:

        pypetl.log.append("%s: Finalizing..."%(fname))

        final_update = pypetl.data2str(
            transform_rds\
                .selectin(constraint_pk, referenced_source_id)
        ).cache()
        final_update_metadata = pypetl.data2str(
            transform_redshift_metadata\
                .selectin('referenced_'+constraint_pk, referenced_source_id)
        ).cache()
        final_insert = pypetl.data2str(
            transform_rds\
                .selectnotin(constraint_pk, referenced_source_id)
        ).cache()
        final_insert_metadata = pypetl.data2str(
            transform_redshift_metadata\
                .selectnotin('referenced_'+constraint_pk, referenced_source_id)
        ).cache()


        pypetl.log.append("%s: Finalized!"%(fname))
        pypetl.log.append("%s: Loading data to the database..."%(fname))

        pypetl.toDBSecretUpdate('redshift', final_update, redshift, constraint_pk)
        pypetl.toDBSecretUpdate('redshift', final_update_metadata, redshift_metadata, 'referenced_'+constraint_pk)        
        pypetl.toDBSecretInsert('redshift', final_insert, redshift)
        pypetl.toDBSecretInsert('redshift', final_insert_metadata, redshift_metadata)

        pypetl.log.append("%s: Loaded!"%(fname))
