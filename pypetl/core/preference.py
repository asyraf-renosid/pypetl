try:
    from . import file, log
except ImportError:
    import file, log


config = file.open_json('config.json')

