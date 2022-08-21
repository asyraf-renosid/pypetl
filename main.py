import pypetl

try:
    data = pypetl.fromDBSecret(
        'rds',
        'SELECT * FROM public.rns_order'
    )
    print(data)
    
    pypetl.engine.stop()
except:
    pypetl.engine.stop()
    