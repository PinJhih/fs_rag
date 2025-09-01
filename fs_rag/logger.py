import logging

from pythonjsonlogger import jsonlogger

def get_json_logger(name: str, log_level=logging.INFO) -> logging.Logger:
    logger = logging.getLogger(name)

    if not logger.handlers:
        logger.setLevel(log_level)
        logHandler = logging.StreamHandler()

        formatter = jsonlogger.JsonFormatter(
            fmt='%(asctime)s %(name)s %(levelname)s %(message)s'
        )
        logHandler.setFormatter(formatter)

        logger.addHandler(logHandler)
        logger.propagate = False
    return logger
