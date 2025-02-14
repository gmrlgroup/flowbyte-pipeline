
from dagster import get_dagster_logger
import os

def log_debug(message):
    logger = get_dagster_logger()
    logger.debug(message)



def log_info(message):
    logger = get_dagster_logger()
    logger.info(message)



def log_error(message):
    logger = get_dagster_logger()
    logger.error(message)
