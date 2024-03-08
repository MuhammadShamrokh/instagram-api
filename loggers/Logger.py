import logging


# initializing logger settings
formatter = logging.Formatter(fmt="%(asctime)s.%(msecs)03d %(levelname)s: %(message)s",
                              datefmt='%d-%m-%Y %H:%M:%S')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

screen_handler = logging.StreamHandler()
screen_handler.setFormatter(formatter)
file_handler = logging.FileHandler('C:/Users/muham/Desktop/VieRally/instagram-project/loggers/logs/snowball-logs.txt', 'a')
file_handler.setFormatter(formatter)

logger.addHandler(screen_handler)
logger.addHandler(file_handler)
