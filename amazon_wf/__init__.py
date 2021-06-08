import logging
import sys
import pdb

logger = logging.getLogger()
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(name)s:%(message)s')

file_handler = logging.FileHandler('./amazon.log', mode="a")
file_handler.setFormatter(formatter)

#console_handler = logging.StreamHandler(sys.stdout)
#console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
#logger.addHandler(console_handler)
