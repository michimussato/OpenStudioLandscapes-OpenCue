__all__ = [
    "DOCKER_USE_CACHE",
    "ASSET_HEADER",
    "FEATURE_CONFIGS",
]

import pathlib
from typing import Any, Generator

from dagster import (
    AssetExecutionContext,
    AssetMaterialization,
    AssetOut,
    MetadataValue,
    Output,
    get_dagster_logger,
    multi_asset,
)

LOGGER = get_dagster_logger(__name__)

from OpenStudioLandscapes.engine.constants import DOCKER_USE_CACHE_GLOBAL
from OpenStudioLandscapes.engine.enums import (
    FeatureVolumeType,
    OpenStudioLandscapesConfig,
)

DOCKER_USE_CACHE = DOCKER_USE_CACHE_GLOBAL or False


GROUP = "OpenCue"
KEY = [GROUP]
FEATURE = f"OpenStudioLandscapes-{GROUP}".replace("_", "-")

ASSET_HEADER = {
    "group_name": GROUP,
    "key_prefix": KEY,
}

# Todo
#  - [ ] Add CueWeb
#        - https://docs.opencue.io/docs/quick-starts/quick-start-cueweb/
#        - https://docs.opencue.io/docs/user-guides/cueweb-user-guide/

# @formatter:off
FEATURE_CONFIGS = {
    OpenStudioLandscapesConfig.DEFAULT: {
        "DOCKER_USE_CACHE": DOCKER_USE_CACHE,
        # Todo:
        #  - [ ] Which one is the main HOSTNAME?
        "HOSTNAME": "opencue-web",  # Same as HOSTNAME_WEB. This is assumed for now (for teleport service). CueWeb is not implemented in this Feature yet.)
        "OPENCUE_WEB_PORT_CONTAINER": "3000",
        "OPENCUE_WEB_PORT_HOST": "1234",
        "HOSTNAME_WEB": "opencue-web",
        "HOSTNAME_DB": "opencue-db",
        "HOSTNAME_FLYWAY": "opencue-flyway",
        "HOSTNAME_CUEBOT": "opencue-cuebot",
        "HOSTNAME_RQD": "opencue-rqd",
        "TELEPORT_ENTRY_POINT_HOST": "{{HOSTNAME}}",  # Either a hardcoded str or a ref to a Variable (with double {{ }}!)
        "TELEPORT_ENTRY_POINT_PORT": "{{OPENCUE_WEB_PORT_HOST}}",  # Either a hardcoded str or a ref to a Variable (with double {{ }}!)
        # cuebot
        "OPENCUE_CUEBOT_USE_PREBUILT_DOCKER_IMAGE": True,  # See https://github.com/AcademySoftwareFoundation/OpenCue/blob/55a90b3ee3f5470866f45b504a5a552b9d93ef5a/docker-compose.yml#L34
        "OPENCUE_CUEBOT_PREBUILT_DOCKER_IMAGE": "docker.io/opencue/cuebot",
        "OPENCUE_CUEBOT_GRPC_CUE_PORT_HOST": "8443",
        "OPENCUE_CUEBOT_GRPC_CUE_PORT_CONTAINER": "8443",
        # rqd
        "OPENCUE_CUEBOT_GRPC_RQD_PORT_HOST": "8444",
        "OPENCUE_CUEBOT_GRPC_RQD_PORT_CONTAINER": "8444",
        # db
        # Inside Landscape:
        "OPENCUE_DB_INSTALL_DESTINATION": {
            FeatureVolumeType.CONTAINED: pathlib.Path(
                "{DOT_LANDSCAPES}",
                "{LANDSCAPE}",
                f"{GROUP}__{'__'.join(KEY)}",
                "data",
                "opencue-db",
                "postgresql",
            )
            .expanduser()
            .as_posix(),
            FeatureVolumeType.SHARED: pathlib.Path(
                "{DOT_LANDSCAPES}",
                "{DOT_SHARED_VOLUMES}",
                f"{GROUP}__{'__'.join(KEY)}",
                "data",
                "opencue-db",
                "postgresql",
            )
            .expanduser()
            .as_posix(),
        }[FeatureVolumeType.CONTAINED],
        "OPENCUE_DB_PORT_HOST": "5342",
        "OPENCUE_DB_PORT_CONTAINER": "5432",
        "OPENCUE_DB_PGHOST": "opencue-db",
        "OPENCUE_DB_PGDATABASE": "cuebot",
        "OPENCUE_DB_PGPASSWORD": "cuebot_password",
        "OPENCUE_DB_PGUSER": "cuebot",
    },
}
# @formatter:on


# Todo:
#  - [ ] move to common_assets
@multi_asset(
    name=f"constants_{GROUP}",
    outs={
        "NAME": AssetOut(
            **ASSET_HEADER,
            dagster_type=str,
            description="",
        ),
        "FEATURE_CONFIGS": AssetOut(
            **ASSET_HEADER,
            dagster_type=dict,
            description="",
        ),
        "DOCKER_COMPOSE": AssetOut(
            **ASSET_HEADER,
            dagster_type=pathlib.Path,
            description="",
        ),
        "DOCKER_COMPOSE_OVERRIDE": AssetOut(
            **ASSET_HEADER,
            dagster_type=pathlib.Path,
            description="",
        ),
    },
)
def constants_multi_asset(
    context: AssetExecutionContext,
) -> Generator[
    Output[dict[OpenStudioLandscapesConfig, dict[str, bool | str]]]
    | AssetMaterialization
    | Output[Any]
    | Output[pathlib.Path]
    | Any,
    None,
    None,
]:
    """ """

    yield Output(
        output_name="FEATURE_CONFIGS",
        value=FEATURE_CONFIGS,
    )

    yield AssetMaterialization(
        asset_key=context.asset_key_for_output("FEATURE_CONFIGS"),
        metadata={
            "__".join(
                context.asset_key_for_output("FEATURE_CONFIGS").path
            ): MetadataValue.json(FEATURE_CONFIGS),
        },
    )

    yield Output(
        output_name="NAME",
        value=__name__,
    )

    yield AssetMaterialization(
        asset_key=context.asset_key_for_output("NAME"),
        metadata={
            "__".join(context.asset_key_for_output("NAME").path): MetadataValue.path(
                __name__
            ),
        },
    )

    docker_compose = pathlib.Path(
        "{DOT_LANDSCAPES}",
        "{LANDSCAPE}",
        f"{ASSET_HEADER['group_name']}__{'_'.join(ASSET_HEADER['key_prefix'])}",
        "__".join(context.asset_key_for_output("DOCKER_COMPOSE").path),
        "docker_compose",
        "docker-compose.yml",
    )

    yield Output(
        output_name="DOCKER_COMPOSE",
        value=docker_compose,
    )

    yield AssetMaterialization(
        asset_key=context.asset_key_for_output("DOCKER_COMPOSE"),
        metadata={
            "__".join(
                context.asset_key_for_output("DOCKER_COMPOSE").path
            ): MetadataValue.path(docker_compose),
        },
    )

    docker_compose_override = pathlib.Path(
        "{DOT_LANDSCAPES}",
        "{LANDSCAPE}",
        f"{ASSET_HEADER['group_name']}__{'_'.join(ASSET_HEADER['key_prefix'])}",
        "__".join(context.asset_key_for_output("DOCKER_COMPOSE").path),
        "docker_compose_override",
        "docker-compose.override.yml",
    )

    yield Output(
        output_name="DOCKER_COMPOSE_OVERRIDE",
        value=docker_compose_override,
    )

    yield AssetMaterialization(
        asset_key=context.asset_key_for_output("DOCKER_COMPOSE_OVERRIDE"),
        metadata={
            "__".join(
                context.asset_key_for_output("DOCKER_COMPOSE_OVERRIDE").path
            ): MetadataValue.path(docker_compose_override),
        },
    )
