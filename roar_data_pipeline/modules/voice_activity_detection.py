import tempfile
import json
from glob import glob
from nemo.collections.asr.models import ClusteringDiarizer
from omegaconf import DictConfig, OmegaConf
from roar_data_pipeline.utils.loggers import Logger, Loggers


class VAD:
    """A class for performing Voice Activity Detection (VAD) using the ClusteringDiarizer model.

    Args:
        cfg (DictConfig): The configuration for the VAD.
        logger (Logger | Loggers): An instance of the logger or loggers used for logging.

    Attributes:
        vad_config (OmegaConf): The VAD configuration loaded from the specified file.
        in_path (str): The input directory path to the audio files.
        out_path (str): The output directory path for the processed audio files.
        audio_filepaths (list): A list of paths to the input audio files.
        num_workers (int): The number of worker processes for parallel execution.
        batch_size (int): The batch size for processing audio files.
        diarizer (ClusteringDiarizer): An instance of the ClusteringDiarizer model for VAD.
        logger (Logger | Loggers): An instance of the logger or loggers used for logging.

    Examples:
        >>> from omegaconf import OmegaConf
        >>> from roar_data_pipeline.utils.loggers import Logger  # Replace with the actual logger module
        >>> config = OmegaConf.load("config.yaml")  # Replace with the actual configuration file
        >>> logger = Logger()  # Replace with the actual logger instance
        >>> vad = VAD(config, logger)
        >>> vad()
    """

    def __init__(self, cfg: DictConfig, logger: Logger | Loggers) -> None:
        self.vad_config = OmegaConf.load(cfg.vad_config)
        self.update_vad_config()
        self.in_path = cfg.in_path
        self.out_path = cfg.get("out_path", self.in_path)
        self.audio_filepaths = glob(f"{self.in_path}/*/*/.wav")
        self.make_manifest()
        self.num_workers = cfg.get("num_workers", 0)
        self.batch_size = cfg.get("batch_size", 1)
        self.diarizer = ClusteringDiarizer(self.vad_config)
        self.logger = logger

    def update_vad_config(self) -> None:
        """Updates the VAD configuration parameters.

        Example:
            >>> vad.update_vad_config()
        """
        self.vad_config.diarizer.vad.parameters.onset = 0.8
        self.vad_config.diarizer.vad.parameters.offset = 0.6
        self.vad_config.diarizer.vad.parameters.pad_offset = -0.05
        self.diarizer.out_dir = self.out_path
        self.vad_config.num_workers = self.num_workers

    def make_manifest(self) -> None:
        """Creates a manifest file for the VAD process.

        Example:
            >>> vad.make_manifest()
        """
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as fp:
            for audio_file in self.audio_filepaths:
                entry = {
                    "audio_filepath": audio_file,
                    "offset": 0.0,
                    "duration": None,
                    "text": "-",
                    "label": "infer",
                }
                fp.write(json.dumps(entry) + "\n")
            self.diarizer.manifest_filepath = fp.name

    def diarize(self) -> None:
        """Performs voice activity detection (VAD) using the ClusteringDiarizer model.

        Example:
            >>> vad.diarize()
        """
        self.diarizer.diarize(batch_size=self.batch_size)

    def __call__(self) -> None:
        """Calls the 'diarize' method when the instance is called.

        Example:
            >>> vad = VAD(config, logger)
            >>> vad()
        """
        self.diarize()
