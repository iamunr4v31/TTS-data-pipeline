import os
from pathlib import Path
from omegaconf import DictConfig
from multiprocessing import Pool
from yt_dlp import YoutubeDL
from roar_data_pipeline.utils.loggers import Logger, Loggers


class Downloader:
    pass


class YoutubeDownloader(Downloader):
    """A class to manage the downloading of audio files from YouTube channels.

    Args:
        cfg (DictConfig): The configuration for the YouTube downloader.

    Attributes:
        channel_ids (List[str]): The list of channel IDs.
        channels_file (str): The file containing the list of channel IDs.
        save_dir (Path): The directory to save downloaded audio files.
        logs_dir (Path): The directory to store log files.
        num_workers (int): The number of workers for the downloading process.
        channel_to_video_map (Dict[str, List[str]]): A mapping of channel IDs to lists of video IDs.
        logger (Logger): An instance of the Logger class for logging download statuses.

    """

    def __init__(self, cfg: DictConfig, logger: Logger | Loggers) -> None:
        """Initializes the YoutubeDownloader with the provided configuration.

        Args:
            cfg (DictConfig): The configuration for the YouTube downloader.

        Example:
            >>> config = {...}  # Provide a valid configuration
            >>> downloader = YoutubeDownloader(config)
        """
        self.channel_ids = cfg.get("channel_ids", [])
        self.channels_file = cfg.get("channels_file", None)
        assert (
            self.channel_ids != [] or self.channels_file != ""
        ), "Channels list is Empty"
        if self.channels_file:
            with open(self.channels_file, "r") as f:
                self.channel_ids.extend(map(str.strip, f.readlines()))
        self.save_dir = Path(cfg.save_dir)
        self.logs_dir = Path(cfg.logs_dir)
        self.num_workers = cfg.get("num_workers", 0)
        self.channel_to_video_map = {}
        self.logger = logger
        self.downloader_kwargs = cfg.get("downloader_kwargs", {})

    def get_video_ids(self, channel_id: str) -> None:
        """Gets the video IDs from the provided channel ID and logs them as incomplete.

        Args:
            channel_id (str): The ID of the YouTube channel.

        Example:
            >>> channel_id = "YOUR_CHANNEL_ID"  # Provide a valid channel ID
            >>> downloader.get_video_ids(channel_id)
        """
        ytdl_opts = {
            "quiet": True,
            "force_generic_extractor": True,
            "extract_flat": True,
            "dump_single_json": True,
            "flat-playlist": True,
            "get-url": True,
        }
        channel_url = f"https://youtube.com/channel/{channel_id}"
        with YoutubeDL(ytdl_opts) as ytdl:
            result = ytdl.extract_info(channel_url, download=False)
        video_ids = [
            video.get("id") for video in result.get("entries", []) if video.get("id")
        ]
        for video_id in video_ids:
            self.logger.log(False, channel_id, video_id, type(self).__name__)
        self.channel_to_video_map[channel_id] = video_ids

    def download_audio(self, channel_id: str, video_id: str) -> None:
        """Downloads the audio corresponding to the provided channel and video IDs.

        Args:
            channel_id (str): The ID of the YouTube channel.
            video_id (str): The ID of the YouTube video.

        Example:
            >>> channel_id = "YOUR_CHANNEL_ID"  # Provide a valid channel ID
            >>> video_id = "YOUR_VIDEO_ID"  # Provide a valid video ID
            >>> downloader.download_audio(channel_id, video_id)
        """

        def logger_hook(d):
            if d["status"] == "finished":
                self.logger.log(True, channel_id, video_id, type(self).__name__)

        outtmpl = f"{self.save_dir}/{channel_id}/{video_id}/%(title)s_{video_id.replace('_', '-')}.%(ext)s"
        if not os.path.exists(os.path.dirname(outtmpl)):
            os.makedirs(os.path.dirname(outtmpl), exist_ok=True)
        ydl_opts = {
            "quiet": True,
            "format": "bestaudio/best",
            "postprocessors": [
                {
                    "key": "FFmpegExtractAudio",
                    "preferredcodec": "wav",
                    "preferredquality": "192",
                }
            ],
            "outtmpl": outtmpl,
            "progress_hooks": [logger_hook],
            **self.downloader_kwargs,
        }
        with YoutubeDL(ydl_opts) as ydl:
            ydl.download([f"https://www.youtube.com/watch?v={video_id}"])

    def start_download(self) -> None:
        """Starts the download process for all the videos in the channel_to_video_map.

        Example:
            >>> downloader.start_download()
        """
        processes = []
        with Pool(processes=self.num_workers) as pool:
            for channel_id, video_ids in self.channel_to_video_map.items():
                for video_id in video_ids:
                    processes.append(
                        pool.apply_async(
                            self.download_audio, args=(channel_id, video_id)
                        )
                    )
            for process in processes:
                process.get()
        self.logger.close()

    def __call__(self) -> None:
        self.start_download()
