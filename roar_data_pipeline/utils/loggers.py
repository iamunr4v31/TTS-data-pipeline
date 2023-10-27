import sqlite3
import logging
from abc import ABC, abstractmethod
from hydra.utils import instantiate
from omegaconf import DictConfig, OmegaConf


class Logger(ABC):
    @abstractmethod
    def __init__(self):
        ...

    @abstractmethod
    def log(self, message):
        ...

    @abstractmethod
    def close(self):
        ...


class Loggers:
    def __init__(self, cfg: DictConfig) -> None:
        self.loggers = []
        for logger in cfg:
            self.loggers.append(instantiate(logger))

    def log(self, status: int | bool, channel_id: str, video_id: str) -> None:
        for logger in self.loggers:
            logger.log(status, channel_id, video_id)

    def close(self) -> None:
        for logger in self.loggers:
            logger.close()


class DatabaseLogger(Logger):
    """A class to log and update download statuses in an SQLite database.

    Args:
        filepath (str): The file path for the SQLite database.

    Attributes:
        db_file (str): The file path of the SQLite database.
        connection (sqlite3.Connection): The connection to the SQLite database.
        cursor (sqlite3.Cursor): The cursor for executing SQL commands.
    """

    TABLE_EXISTS = []

    def __init__(self, filepath):
        self.db_file = filepath
        self.connection = sqlite3.connect(self.db_file)
        self.cursor = self.connection.cursor()

    def create_table(self, ctx: str):
        self.cursor.execute(
            f"""CREATE TABLE IF NOT EXISTS {ctx}_logs 
                            (Status Integer, Channel_ID text, Video_ID text PRIMARY KEY)"""
        )
        self.connection.commit()
        self.TABLE_EXISTS.append(ctx)

    def log(self, status: int | bool, channel_id: str, video_id: str, ctx: str):
        """Logs the download status, channel ID, and video ID in the SQLite database.

        Args:
            status (int): The download status (0 for incomplete, 1 for complete).
            channel_id (str): The ID of the YouTube channel.
            video_id (str): The ID of the YouTube video.

        Example:
            >>> status = 0  # 0 for incomplete
            >>> channel_id = "YOUR_CHANNEL_ID"  # Provide a valid channel ID
            >>> video_id = "YOUR_VIDEO_ID"  # Provide a valid video ID
            >>> logger.log(status, channel_id, video_id)
        """
        if ctx not in self.TABLE_EXISTS:
            self.create_table(ctx)
        self.cursor.execute(
            f"INSERT OR REPLACE INTO {ctx}_logs (Status, Channel_ID, Video_ID) VALUES (?, ?, ?)",
            (status, channel_id, video_id),
        )
        self.connection.commit()

    def close(self):
        """Closes the connection to the SQLite database.

        Example:
            >>> logger.close()
        """
        self.connection.close()


class ConsoleLogger(Logger):
    """A class to log and update download statuses in the console using the logging module.

    Args:
        None

    Attributes:
        logger (logging.Logger): The logger object for logging messages.
    """

    def __init__(self):
        """Initializes the ConsoleLogger.

        Args:
            None

        Example:
            >>> logger = ConsoleLogger()
        """
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

    def log(self, status: int | bool, channel_id: str, video_id: str):
        """Logs the download status, channel ID, and video ID in the console.

        Args:
            status (int | bool): The download status (0 for incomplete, 1 for complete).
            channel_id (str): The ID of the YouTube channel.
            video_id (str): The ID of the YouTube video.

        Example:
            >>> status = 0  # 0 for incomplete
            >>> channel_id = "YOUR_CHANNEL_ID"  # Provide a valid channel ID
            >>> video_id = "YOUR_VIDEO_ID"  # Provide a valid video ID
            >>> logger.log(status, channel_id, video_id)
        """
        self.logger.info(
            f"Status: {status}, Channel ID: {channel_id}, Video ID: {video_id}"
        )

    def close(self):
        """Closes the console logger.

        Example:
            >>> logger.close()
        """
        logging.shutdown()
