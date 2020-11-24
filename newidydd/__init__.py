from .operations import BaseOperator

import logging

def set_up_logging():
    logger = logging.getLogger("newidydd")
    logger.setLevel(logging.DEBUG)
    fh = logging.StreamHandler()    
    formatter = logging.Formatter('[%(name)s] [%(levelname)-8s] %(asctime)s - %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)
