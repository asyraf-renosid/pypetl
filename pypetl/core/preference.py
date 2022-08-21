try:
    from . import file, log
except ImportError:
    import file, log

fname = 'pypetl.core.preference'
gap = ""
log.append('%s: Loading configuration...'%(fname), gap=gap)
config = file.open_json('config.json')
if config['log']['show']:
    log.configure(show = True)
log.append('%s: Loaded!'%(fname), gap=gap)

