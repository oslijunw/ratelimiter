import logging


logger = logging.getLogger('RateLimiter')
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())
