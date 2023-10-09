import logging

class Logger:
  def __init__(self, log_file=None):
        self.log_file = log_file  # Store the log file name as an instance variable
        self._log_level = logging.DEBUG  # Default log level is DEBUG
        self._configure_logger()

  def _configure_logger(self):
        mode = 'w' if self.log_file else 'a'  # Use 'w' (write mode) if a log file is specified
        logging.basicConfig(
            format="%(asctime)s;%(levelname)s;%(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            level=self._log_level,
            handlers=[
                logging.FileHandler(self.log_file, mode=mode) if self.log_file else logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger()

  def set_log_level(self, log_level):
        self._log_level = log_level
        self.logger.setLevel(log_level)

  def log_message(self, log_mess):
        if self.logger.level == logging.DEBUG:
            self.logger.debug(log_mess)
        elif self.logger.level == logging.INFO:
            self.logger.info(log_mess)
        elif self.logger.level == logging.WARNING:
            self.logger.warning(log_mess)
        elif self.logger.level == logging.ERROR:
            self.logger.error(log_mess)
