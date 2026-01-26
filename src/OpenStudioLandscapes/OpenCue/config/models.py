import enum
import pathlib
from typing import Dict, List

from dagster import get_dagster_logger
from pydantic import (
    Field,
    HttpUrl,
    PositiveInt,
)

LOGGER = get_dagster_logger(__name__)

from OpenStudioLandscapes.engine.config.models import FeatureBaseModel

from OpenStudioLandscapes.OpenCue import constants, dist


class Branches(enum.StrEnum):
    master = "master"


class Config(FeatureBaseModel):
    feature_name: str = dist.name

    group_name: str = constants.ASSET_HEADER["group_name"]

    key_prefixes: List[str] = constants.ASSET_HEADER["key_prefix"]

    opencue_str: str = "opencue"

    opencue_db: str = "opencue-db"
    opencue_flyway: str = "opencue-flyway"
    opencue_cuebot: str = "opencue-cuebot"
    opencue_cueweb: str = "opencue-cueweb"
    opencue_rest_gateway: str = "opencue-rest-gateway"
    opencue_rqd: str = "opencue-rqd"

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

    OPENCUE_DEPLOY_RQD: bool = Field(
        default=False,
    )

    OPENCUE_CUEBOT_USE_PREBUILT_DOCKER_IMAGE: bool = Field(
        default=True,
    )

    OPENCUE_CUEBOT_PREBUILT_DOCKER_IMAGE: str = Field(
        default="docker.io/opencue/cuebot",
    )

    # this doesn't seem to work with an override:
    # removing a service from an existing docker-compose
    # file. Maybe we read the original and generate
    # a new one based on those settings. This way,
    # we could remove a service arbitrarily.
    #
    # Looks like it IS possible:
    # - https://stackoverflow.com/a/78609241/2207196
    # - https://docs.docker.com/reference/compose-file/merge/#reset-value
    # deploy_rqd_on_cuebot_host: bool = Field(
    #     # Optional but set to True for
    #     # most basic AND functional use?
    #     default=False,
    # )

    # OPENCUE_SCHEDULER_DOCKER_IMAGE: str = Field(
    #     default="docker.io/opencue/scheduler",
    # )
    #
    # # scheduler
    # # https://docs.opencue.io/docs/getting-started/deploying-scheduler/
    #
    # OPENCUE_SCHEDULER_PORT_HOST: PositiveInt = Field(
    #     default=9090,
    # )
    #
    # OPENCUE_SCHEDULER_PORT_CONTAINER: PositiveInt = Field(
    #     default=9090,
    # )
    #
    # OPENCUE_SCHEDULER_GRPC_PORT_HOST: PositiveInt = Field(
    #     default=8444,
    # )
    #
    # OPENCUE_SCHEDULER_GRPC_PORT_CONTAINER: PositiveInt = Field(
    #     default=8444,
    # )

    # cueweb
    # https://docs.opencue.io/docs/getting-started/deploying-scheduler/

    OPENCUE_CUEWEB_PORT_HOST: PositiveInt = Field(
        default=3111,
    )

    OPENCUE_CUEWEB_PORT_CONTAINER: PositiveInt = Field(
        default=3000,
    )

    OPENCUE_CUEWEB_JWT_SECRET: str = Field(
        # Create secure JWT_SECRET
        # https://docs.opencue.io/docs/getting-started/deploying-rest-gateway/#step-2-deploy-rest-gateway-separately
        # export JWT_SECRET=$(openssl rand -base64 32)
        # Todo
        #  - [ ] set this dynamically after issue is fixed:
        #        - https://github.com/AcademySoftwareFoundation/OpenCue/issues/2127
        default="default-secret-key",
    )

    OPENCUE_CUEWEB_ADDITIONAL_ENV: Dict = Field(
        default={
            "NEXT_TELEMETRY_DISABLED": 1,
            "NEXT_PUBLIC_AUTH_PROVIDER": "",
        },
        description="Disabling third-party authentication is not possible at the moment. "
        "Bug report pending: https://github.com/AcademySoftwareFoundation/OpenCue/issues/2133",
    )

    # REST Gateway
    # https://docs.opencue.io/docs/getting-started/deploying-rest-gateway/

    OPENCUE_REST_GATEWAY_PORT_HOST: PositiveInt = Field(
        default=8448,
    )

    OPENCUE_REST_GATEWAY_PORT_CONTAINER: PositiveInt = Field(
        default=8448,
    )

    # OPENCUE_REST_GATEWAY_JWT_SECRET: str = Field(
    #     default="your-jwt-secret",
    # )

    # cuebot

    OPENCUE_WEB_PORT_HOST: PositiveInt = Field(
        default=1234,
        frozen=False,
    )

    OPENCUE_WEB_PORT_CONTAINER: PositiveInt = Field(
        default=3000,
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

    OPENCUE_DB_PGUSER: str = Field(
        default="cuebot",
        frozen=False,
    )

    OPENCUE_DB_PGPASSWORD: str = Field(
        default="cuebot_password",
        frozen=False,
    )

    # EXPANDABLE PATHS
    @property
    def OPENCUE_DB_INSTALL_DESTINATION_expanded(self) -> pathlib.Path:
        LOGGER.debug(f"{self.env = }")
        if self.env is None:
            raise KeyError("`env` is `None`.")

        LOGGER.debug(f"Expanding {self.OPENCUE_DB_INSTALL_DESTINATION}...")
        ret = pathlib.Path(
            self.OPENCUE_DB_INSTALL_DESTINATION.expanduser()  # pylint: disable=E1101
            .as_posix()
            .format(
                **{
                    "FEATURE": self.feature_name,
                    **self.env,
                }
            )
        )
        return ret


CONFIG_STR = Config.get_docs()
