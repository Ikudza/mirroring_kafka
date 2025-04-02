import logging

from pythonjsonlogger import jsonlogger

log = logging.getLogger('mirroring_kafka')


def init_logs(debug_include: bool = False):
    logging.basicConfig(level=logging.WARNING)
    root_log = logging.getLogger()
    root_log_handler = logging.StreamHandler()
    root_log_handler.setFormatter(
        jsonlogger.JsonFormatter(
            '%(asctime)s %(name)s %(levelname)s %(processName)s %(message)s')
    )
    for handler in root_log.handlers:
        root_log.removeHandler(handler)
    root_log.addHandler(root_log_handler)
    log.setLevel(logging.DEBUG if debug_include else logging.INFO)
