from hydra.utils import instantiate
from omegaconf import DictConfig, OmegaConf
from roar_data_pipeline.utils import hydra_runner
from roar_data_pipeline.modules import YoutubeDownloader, VAD, MusicSeparator

## WiP


@hydra_runner(config_path="conf", config_name="tts_yt_data_pipeline")
def main(cfg):
    loggers = instantiate(cfg.logger)
    download_manager = YoutubeDownloader(cfg.download_manager, loggers)
    post_processors = []
    for processor in cfg.get("post_processors", []):
        post_processors.append(instantiate(processor))

    download_manager()
    for processor in post_processors:
        processor()


if __name__ == "__main__":
    main()
