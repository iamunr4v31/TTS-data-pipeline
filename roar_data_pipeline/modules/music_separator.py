import os
import demucs.separate
from glob import glob
from pathlib import Path
from omegaconf import DictConfig
from multiprocessing import Pool
from roar_data_pipeline.utils.loggers import Logger, Loggers


class MusicSeparator:
    """A class for separating music from audio files using the Demucs model.

    Args:
        cfg (DictConfig): The configuration for the music separator.
        logger (Logger | Loggers): An instance of the logger or loggers used for logging.

    Attributes:
        in_path (Path): The input directory path to the audio files.
        out_path (Path): The output directory path for the separated music.
        audio_filepaths (list): A list of paths to the input audio files.
        num_workers (int): The number of worker processes for parallel execution.
        logger (Logger | Loggers): An instance of the logger or loggers used for logging.

    Examples:
        >>> from omegaconf import OmegaConf
        >>> from roar_data_pipeline.utils.logger import ConsoleLogger  # Replace with the actual logger module
        >>> config = OmegaConf.load("config.yaml")  # Replace with the actual configuration file
        >>> logger = ConsoleLogger()  # Replace with the actual logger instance
        >>> separator = MusicSeparator(config, logger)
        >>> separator()
    """

    def __init__(self, cfg: DictConfig, logger: Logger | Loggers) -> None:
        self.in_path = Path(cfg.in_path)
        assert (
            self.in_path.exists()
        ), f"Input Directory to track ({self.in_path}) does not exist"
        self.out_path = Path(cfg.get("out_path", self.in_path))
        self.out_path.mkdir(exist_ok=True)
        self.audio_filepaths = glob(f"{self.in_path}/*/*/.wav")
        self.num_workers = cfg.get("num_workers", 0)
        self.logger = logger

    def separate_audio(self, audio_filepath: str | Path) -> None:
        """Separates music from the provided audio file using the Demucs model.

        Args:
            audio_filepath (str | Path): The path to the input audio file.

        Example:
            >>> audio_filepath = "path/to/audio.wav"  # Replace with the actual audio file path
            >>> separator.separate_audio(audio_filepath)
        """
        assert os.path.isfile(audio_filepath), f"Audio not found: {audio_filepath}"
        demucs.separate.main(
            ["-d", "cpu", "-o", self.out_path, "-n", "hdemucs_mmi", audio_filepath]
        )
        channel_id = audio_filepath.parent.name
        video_id = audio_filepath.parents[1].name
        self.logger.log(True, channel_id, video_id, type(self).__name__)

    def separate_all(self) -> None:
        """Separates music from all audio files in the input directory.

        Example:
            >>> separator.separate_all()
        """
        processes = []
        with Pool(processes=self.num_workers) as pool:
            for path in self.audio_filepaths:
                processes.append(pool.apply_async(self.separate_audio, args=(path)))
            for process in processes:
                process.get()
        self.logger.close()

    def __call__(self) -> None:
        """Calls the 'separate_all' method when the instance is called.

        Example:
            >>> separator = MusicSeparator(config, logger)
            >>> separator()
        """
        self.separate_all()
