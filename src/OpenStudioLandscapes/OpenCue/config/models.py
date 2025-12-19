import enum
import pathlib

from dagster import get_dagster_logger
from pydantic import (
    Field,
    HttpUrl,
    PositiveInt,
)

LOGGER = get_dagster_logger(__name__)

from OpenStudioLandscapes.engine.config.models import FeatureBaseModel

from OpenStudioLandscapes.OpenCue import dist

config_default = pathlib.Path(__file__).parent.joinpath("config_default.yml")
CONFIG_STR = config_default.read_text()


class Branches(enum.StrEnum):
    master = "master"


class Config(FeatureBaseModel):
    feature_name: str = dist.name

    definitions: str = "OpenStudioLandscapes.OpenCue.definitions"

    repository_url: HttpUrl = Field(
        default="https://github.com/AcademySoftwareFoundation/OpenCue.git",
    )
    repository_branch: Branches = Field(
        default=Branches.master,
        description="The branch of the OpenCue repository.",
        frozen=True,
        examples=[i.name for i in Branches],
    )
    repository_subdir: str = Field(
        default="OpenCue",
    )
    docker_compose_yml: str = Field(
        default="docker-compose.yml",
    )

    docker_compose_override: pathlib.Path = Field(
        default=pathlib.Path(
            "{DOT_LANDSCAPES}/{LANDSCAPE}/{FEATURE}/docker_compose/docker-compose.override.yml"
        ),
        description="The path to the `docker-compose.yml` file.",
        frozen=True,
    )

    OPENCUE_CUEBOT_USE_PREBUILT_DOCKER_IMAGE: bool = Field(
        default=True,
    )

    OPENCUE_CUEBOT_PREBUILT_DOCKER_IMAGE: str = Field(
        default="docker.io/opencue/cuebot",
    )

    # cuebot

    OPENCUE_WEB_PORT_CONTAINER: PositiveInt = Field(
        default=3000,
    )

    OPENCUE_WEB_PORT_HOST: PositiveInt = Field(
        default=1234,
        frozen=False,
    )

    OPENCUE_CUEBOT_GRPC_CUE_PORT_HOST: PositiveInt = Field(
        default=8443,
        frozen=False,
    )

    OPENCUE_CUEBOT_GRPC_CUE_PORT_CONTAINER: PositiveInt = Field(
        default=8443,
        frozen=False,
    )

    # rqd

    OPENCUE_CUEBOT_GRPC_RQD_PORT_HOST: PositiveInt = Field(
        default=8444,
        frozen=False,
    )

    OPENCUE_CUEBOT_GRPC_RQD_PORT_CONTAINER: PositiveInt = Field(
        default=8444,
        frozen=False,
    )

    # db

    OPENCUE_DB_INSTALL_DESTINATION: pathlib.Path = Field(
        # description="The host side LikeC4 datastore destination.",
        default=pathlib.Path(
            "{DOT_LANDSCAPES}/{LANDSCAPE}/{FEATURE}/data/opencue_db/postgresql"
        ),
    )

    OPENCUE_DB_PORT_HOST: PositiveInt = Field(
        default=5342,
        frozen=False,
    )

    OPENCUE_DB_PORT_CONTAINER: PositiveInt = Field(
        default=5432,
        frozen=False,
    )

    OPENCUE_DB_PGHOST: str = Field(
        default="opencue-db",
        frozen=False,
    )

    OPENCUE_DB_PGDATABASE: str = Field(
        default="cuebot",
        frozen=False,
    )

    OPENCUE_DB_PGPASSWORD: str = Field(
        default="cuebot_password",
        frozen=False,
    )

    OPENCUE_DB_PGUSER: str = Field(
        default="cuebot",
        frozen=False,
    )

    # EXPANDABLE PATHS
    @property
    def docker_compose_override_expanded(self) -> pathlib.Path:
        LOGGER.debug(f"{self.env = }")
        if self.env is None:
            raise KeyError("`env` is `None`.")
        LOGGER.debug(f"Expanding {self.docker_compose_override}...")
        ret = pathlib.Path(
            self.docker_compose_override.expanduser()
            .as_posix()
            .format(
                **{
                    "FEATURE": self.feature_name,
                    **self.env,
                }
            )
        )
        return ret

    @property
    def OPENCUE_DB_INSTALL_DESTINATION_expanded(self) -> pathlib.Path:
        LOGGER.debug(f"{self.env = }")
        if self.env is None:
            raise KeyError("`env` is `None`.")

        LOGGER.debug(f"Expanding {self.OPENCUE_DB_INSTALL_DESTINATION}...")
        ret = pathlib.Path(
            self.OPENCUE_DB_INSTALL_DESTINATION.expanduser()
            .as_posix()
            .format(
                **{
                    "FEATURE": self.feature_name,
                    **self.env,
                }
            )
        )
        return ret
