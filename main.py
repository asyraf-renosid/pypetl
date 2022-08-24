import pypetl
import pipeline

try:
    pipeline.rns_order.run()    
    pypetl.engine.stop()
except Exception as e:
    print(str(e))
    pypetl.engine.stop()
    