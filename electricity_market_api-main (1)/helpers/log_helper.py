import logging

def logException(e: Exception):
    logging.error(e, exc_info=True)

def logError(message):
    logging.error(message)