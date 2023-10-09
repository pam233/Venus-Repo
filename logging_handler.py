import logging

class Logger:
    def __init__(self, log_level=logging.DEBUG):
        logging.basicConfig(
            format="%(asctime)s;%(levelname)s;%(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            level=log_level
        )
        self.logger = logging.getLogger()

    def log_message(self, log_mess):
        if self.logger.level == logging.DEBUG:
            self.logger.debug(log_mess)
        elif self.logger.level == logging.INFO:
            self.logger.info(log_mess)
        elif self.logger.level == logging.WARNING:
            self.logger.warning(log_mess)
        elif self.logger.level == logging.ERROR:
            self.logger.error(log_mess)