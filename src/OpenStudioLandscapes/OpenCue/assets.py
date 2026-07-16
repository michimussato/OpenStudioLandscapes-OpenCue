# pylint: disable=line-too-long,invalid-name
import copy
# import re
import enum
import time
import json
import pathlib
# import shutil
# import subprocess
import textwrap
# import urllib.parse
from typing import Dict, Generator, List, Union

import git
import yaml
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    AssetMaterialization,
    AssetsDefinition,
    MetadataValue,
    Output,
    asset,
)
from git.exc import GitCommandError
from OpenStudioLandscapes.engine.common_assets import (
    cmd,
    compose,
    docker_compose_graph,
    feature,
    feature_out,
    group_in,
    group_out,
)
from OpenStudioLandscapes.engine.config.models import ConfigEngine, DockerConfigModel
from OpenStudioLandscapes.engine.constants import (
    ASSET_HEADER_BASE,
    ConfigParent,
)
from OpenStudioLandscapes.engine.enums import (
    DockerComposePolicies,
)
# from OpenStudioLandscapes.engine.link.models import OpenStudioLandscapesFeatureIn
# from OpenStudioLandscapes.engine.policies.retry import build_docker_image_retry_policy
from OpenStudioLandscapes.engine.utils import (
    # create_image,
    get_docker_compose_names,
    parse_docker_image_path,
    # get_docker_run_cmd,
    # get_image_metadata,
    # get_pip_install_str,
    get_relative_path_via_common_root,
    get_image_name,
)
from OpenStudioLandscapes.engine.utils.docker.compose_dicts import (
    get_network_dicts,
)

from OpenStudioLandscapes.OpenCue import (
    ASSET_HEADER,
    config,
    dist,
)

# Current issue:
# dnf: command not found for rqd
# WTF?
# -> disabled for now

# https://github.com/yaml/pyyaml/issues/722#issuecomment-1969292770
yaml.SafeDumper.add_multi_representer(
    data_type=enum.Enum,
    representer=yaml.representer.SafeRepresenter.represent_str,
)


cmd: AssetsDefinition = cmd.get_feature__cmd(
    ASSET_HEADER=ASSET_HEADER,
)

CONFIG: AssetsDefinition = feature.get_feature__CONFIG(
    ASSET_HEADER=ASSET_HEADER,
    CONFIG_STR=config.models.CONFIG_STR,
    search_model_of_type=config.models.Config,
)

feature_in: AssetsDefinition = group_in.get_feature_in(
    ASSET_HEADER=ASSET_HEADER,
    ASSET_HEADER_BASE=ASSET_HEADER_BASE,
    ASSET_HEADER_FEATURE_IN={},
)

group_out: AssetsDefinition = group_out.get_group_out(
    ASSET_HEADER=ASSET_HEADER,
)


docker_compose_graph: AssetsDefinition = docker_compose_graph.get_docker_compose_graph(
    ASSET_HEADER=ASSET_HEADER,
)


compose: AssetsDefinition = compose.get_compose(
    ASSET_HEADER=ASSET_HEADER,
)


feature_out_v2: AssetsDefinition = feature_out.get_feature_out_v2(
    ASSET_HEADER=ASSET_HEADER,
)


# Produces
# - feature_in_parent
# - CONFIG_PARENT
# if ConfigParent is or type FeatureBaseModel
feature_in_parent: Union[AssetsDefinition, None] = group_in.get_feature_in_parent(
    ASSET_HEADER=ASSET_HEADER,
    config_parent=ConfigParent,
)


# IMPORTANT
# We need to rebuild OpenCue if something that gets burnt into the image has changed:
# - https://github.com/AcademySoftwareFoundation/OpenCue/issues/2133#issuecomment-4686953918
#
# for now, manually prepend this to /home/michael/.local/share/OpenStudioLandscapes/.landscapes/2026-07-16_09-14-15__sideways-flicker-truthful-motorcycle/OpenStudioLandscapes-OpenCue/docker_compose/docker_compose_up.sh
# $(which docker) --config ../../../2026-07-16_09-14-15__sideways-flicker-truthful-motorcycle/OpenStudioLandscapes/OpenStudioLandscapes_Base__docker_config_json compose --progress plain --file ../../../2026-07-16_09-14-15__sideways-flicker-truthful-motorcycle/OpenStudioLandscapes-OpenCue/docker_compose/docker-compose.yml --project-name 2026-07-16_09-14-15__sideways-flicker-truthful-motorcycle-default build --no-cache
#
# Or we could remove images on request
# docker image rm
# 2026-07-16_09-14-15__sideways-flicker-truthful-motorcycle-default-opencue-cuebot:latest   ca84b0321b18       1.07GB          427MB   U
# 2026-07-16_09-14-15__sideways-flicker-truthful-motorcycle-default-opencue-flyway:latest   62b254652cc1       2.07GB          644MB   U
# 2026-07-16_09-14-15__sideways-flicker-truthful-motorcycle-default-opencue-rqd:latest      3c99a29e57c7        148MB         37.2MB   U
# opencue/cueweb:latest                                                                     65bed261f11d       3.88GB          776MB   U
# opencue/rest-gateway:latest                                                               d5064b7d5f85        168MB         44.5MB   U


@asset(
    **ASSET_HEADER,
    ins={
        "CONFIG": AssetIn(
            AssetKey([*ASSET_HEADER["key_prefix"], "CONFIG"]),
        ),
    },
)
def clone_repository(
    context: AssetExecutionContext,
    CONFIG: config.models.Config,  # pylint: disable=redefined-outer-name
) -> Generator[Output[pathlib.Path] | AssetMaterialization, None, None]:

    env: dict = CONFIG.env

    repo_dir = pathlib.Path(
        env["DOT_LANDSCAPES"],
        env.get("LANDSCAPE", "default"),
        f"{dist.name}",
        "__".join(context.asset_key.path),
        "repos",
    )

    repository_dir_full = repo_dir / CONFIG.repository_subdir
    repository_dir_full.parent.mkdir(parents=True, exist_ok=True)

    try:
        git.Repo.clone_from(
            url=str(CONFIG.repository_url),
            to_path=repository_dir_full,
            branch=CONFIG.repository_branch,
        )
    except GitCommandError as e:
        context.log.warning("Pulling from Repo (%s)" % e)
        existing_repo = git.Repo(repository_dir_full)
        origin = existing_repo.remotes.origin
        origin.pull()

    yield Output(repository_dir_full)

    yield AssetMaterialization(
        asset_key=context.asset_key,
        metadata={
            "__".join(context.asset_key.path): MetadataValue.path(repository_dir_full),
        },
    )


@asset(
    **ASSET_HEADER,
    ins={
        "CONFIG": AssetIn(
            AssetKey([*ASSET_HEADER["key_prefix"], "CONFIG"]),
        ),
    },
)
def prepare_volumes(
    context: AssetExecutionContext,
    CONFIG: config.models.Config,  # pylint: disable=redefined-outer-name
) -> Generator[Output[Dict] | AssetMaterialization, None, None]:
    """https://www.opencue.io/docs/quick-starts/quick-start-linux/#deploying-the-opencue-sandbox-environment"""

    env: dict = CONFIG.env

    local_volumes_root = pathlib.Path(
        env["DOT_LANDSCAPES"],
        env.get("LANDSCAPE", "default"),
        f"{dist.name}",
        "__".join(context.asset_key.path),
    )

    volume_logs = local_volumes_root / "logs"
    volume_logs.mkdir(parents=True, exist_ok=True)
    volume_shots = local_volumes_root / "shots"
    volume_shots.mkdir(parents=True, exist_ok=True)

    ret = {
        "logs": volume_logs.as_posix(),
        "shots": volume_shots.as_posix(),
    }

    yield Output(ret)

    yield AssetMaterialization(
        asset_key=context.asset_key,
        metadata={
            "__".join(context.asset_key.path): MetadataValue.json(ret),
        },
    )


@asset(
    **ASSET_HEADER,
    ins={
        "CONFIG": AssetIn(
            AssetKey([*ASSET_HEADER["key_prefix"], "CONFIG"]),
        ),
    },
)
def compose_networks(
    context: AssetExecutionContext,
    CONFIG: config.models.Config,  # pylint: disable=redefined-outer-name
) -> Generator[
    Output[Dict[str, Dict[str, Dict[str, str]]]] | AssetMaterialization, None, None
]:

    env: Dict = CONFIG.env

    compose_network_mode = DockerComposePolicies.NETWORK_MODE.BRIDGE

    docker_dict = get_network_dicts(
        context=context,
        compose_network_mode=compose_network_mode,
        env=env,
    )

    docker_yaml = yaml.dump(docker_dict)

    yield Output(docker_dict)

    yield AssetMaterialization(
        asset_key=context.asset_key,
        metadata={
            "__".join(context.asset_key.path): MetadataValue.json(docker_dict),
            "compose_network_mode": MetadataValue.text(compose_network_mode.value),
            "docker_yaml": MetadataValue.md(f"```yaml\n{docker_yaml}\n```"),
        },
    )


@asset(
    **ASSET_HEADER,
    ins={
        "CONFIG": AssetIn(
            AssetKey([*ASSET_HEADER["key_prefix"], "CONFIG"]),
        ),
        "compose_networks": AssetIn(
            AssetKey([*ASSET_HEADER["key_prefix"], "compose_networks"]),
        ),
        "clone_repository": AssetIn(
            AssetKey([*ASSET_HEADER["key_prefix"], "clone_repository"]),
        ),
        "compose_opencue_base": AssetIn(
            AssetKey([*ASSET_HEADER["key_prefix"], "compose_opencue_base"]),
        ),
    },
    description=textwrap.dedent("""
        Based on
        - [docker-compose.yml](https://github.com/AcademySoftwareFoundation/OpenCue/blob/master/docker-compose.yml)
        
        Reference:
        ```
          cuebot:
            # Use build to compile from source with latest code (recommended for development)
            build:
              context: ./
              dockerfile: ./cuebot/Dockerfile
            # Use image for faster startup with pre-built image (uncomment to use)
            # image: opencue/cuebot
            links:
              - db
            ports:
              - "8443:8443"
            depends_on:
              db:
                condition: service_started
              flyway:
                condition: service_completed_successfully
            restart: always
            environment:
              - CUE_FRAME_LOG_DIR=/tmp/rqd/logs
            command: --datasource.cue-data-source.jdbc-url=jdbc:postgresql://db/cuebot --datasource.cue-data-source.username=cuebot --datasource.cue-data-source.password=cuebot_password
        ```
        """),
)
def compose_cuebot(
    context: AssetExecutionContext,
    CONFIG: config.models.Config,  # pylint: disable=redefined-outer-name
    compose_networks: Dict,  # pylint: disable=redefined-outer-name
    clone_repository: pathlib.Path,  # pylint: disable=redefined-outer-name
    compose_opencue_base: Dict,  # pylint: disable=redefined-outer-name
) -> Generator[Output[Dict] | AssetMaterialization, None, None]:
    """ """

    env: Dict = CONFIG.env

    config_engine: ConfigEngine = CONFIG.config_engine

    network_dict = {}
    ports_dict = {}

    if "networks" in compose_networks:
        network_dict = {"networks": list(compose_networks.get("networks", {}).keys())}
        ports_dict = {
            "ports": [
                f"{CONFIG.OPENCUE_CUEBOT_GRPC_CUE_PORT_HOST}:{CONFIG.OPENCUE_CUEBOT_GRPC_CUE_PORT_CONTAINER}",
            ]
        }
    elif "network_mode" in compose_networks:
        network_dict = {"network_mode": compose_networks["network_mode"]}

    volumes_dict = {"volumes": []}

    # For portability, convert absolute volume paths to relative paths

    _volume_relative = []

    for v in volumes_dict["volumes"]:

        host, container = v.split(":", maxsplit=1)

        volume_dir_host_rel_path = get_relative_path_via_common_root(
            context=context,
            path_src=CONFIG.docker_compose_expanded,
            path_dst=pathlib.Path(host),
            path_common_root=pathlib.Path(env["DOT_LANDSCAPES"]),
        )

        _volume_relative.append(
            f"{volume_dir_host_rel_path.as_posix()}:{container}",
        )

    volumes_dict = {
        "volumes": list(
            {
                *_volume_relative,
                *config_engine.global_bind_volumes,
                *CONFIG.local_bind_volumes,
            }
        )
    }

    service_name_cuebot = CONFIG.opencue_cuebot
    container_name_cuebot, host_name_cuebot = get_docker_compose_names(
        context=context,
        service_name=service_name_cuebot,
        landscape_id=env.get("LANDSCAPE", "default"),
        domain_lan=config_engine.openstudiolandscapes__domain_lan,
    )

    service_name_db = CONFIG.opencue_db
    container_name_db, host_name_db = get_docker_compose_names(
        context=context,
        service_name=service_name_db,
        landscape_id=env.get("LANDSCAPE", "default"),
        domain_lan=config_engine.openstudiolandscapes__domain_lan,
    )

    service_name_flyway = CONFIG.opencue_flyway
    # container_name_flyway, host_name_flyway = get_docker_compose_names(
    #     context=context,
    #     service_name=service_name_flyway,
    #     landscape_id=env.get("LANDSCAPE", "default"),
    #     domain_lan=config_engine.openstudiolandscapes__domain_lan,
    # )

    compose_cuebot_base = compose_opencue_base.get("services", {}).get("cuebot", {})
    compose_cuebot_base.pop("profiles", None)

    image_name = get_image_name(context=context)
    context.log.debug(f"{image_name = }")

    docker_config: DockerConfigModel = config_engine.openstudiolandscapes__docker_config

    image_prefixes = parse_docker_image_path(
        docker_config=docker_config,
        context=context,
    )
    context.log.debug(f"{image_prefixes = }")

    tags = [
        env.get("LANDSCAPE", str(time.time())),
    ]
    context.log.debug(f"{tags = }")

    image_data = {
        "image_name": image_name,
        "image_prefixes": image_prefixes,
        "image_tags": tags,
        "image_parent": {},
    }

    context_ = clone_repository.joinpath(compose_cuebot_base["build"]["context"])
    d = {
        "build": {
            # Just prepend the full path to the cloned repo
            "context": context_.as_posix(),
            "dockerfile": context_.joinpath(compose_cuebot_base["build"]["dockerfile"]).as_posix(),
        },
    }

    docker_dict = {
        "services": {
            service_name_cuebot: {
                # Todo
                #  - [ ] Using the base compose dict as the starting
                #        point and override with our values
                #        This might be error prone so maybe
                #        there is a better way.
                **compose_cuebot_base,
                # Todo:
                #  - [ ] prebuilt image?
                **d,
                # name and tag the resulting image:
                # - https://docs.docker.com/reference/compose-file/build/#tags
                "image": f"{image_data['image_prefixes']}{image_data['image_name']}:{image_data['image_tags'][0]}",
                "container_name": container_name_cuebot,
                "hostname": host_name_cuebot,
                "domainname": config_engine.openstudiolandscapes__domain_lan,
                # "restart": DockerComposePolicies.RESTART_POLICY.ALWAYS.value,
                "environment": {
                    "TZ": config_engine.tz,
                    "CUE_FRAME_LOG_DIR": "/tmp/rqd/logs",
                    **config_engine.global_environment_variables,
                    **CONFIG.local_environment_variables,
                },
                **copy.deepcopy(volumes_dict),
                **copy.deepcopy(network_dict),
                "links": [
                    service_name_db,
                ],
                "depends_on": {
                    service_name_db: {
                        "condition": "service_healthy",
                    },
                    service_name_flyway: {
                        "condition": "service_completed_successfully",
                    },
                },
                # "healthcheck": {
                #     # Todo:
                #     #  - [ ] fix: test succeeds even if Postgres is down
                #     #  "test": ["CMD-SHELL", "psql -U ${DB_USER} -d ${DB_MAIN} -c 'SELECT 1' || exit 1"],
                #     "test": ["CMD", "curl", "-f", f"http://localhost:{env.get('KITSU_PORT_CONTAINER')}"],
                #     "interval": "10s",
                #     "timeout": "2s",
                #     "retries": "3",
                # },
                "command": [
                    f"--datasource.cue-data-source.jdbc-url=jdbc:postgresql://{container_name_db}/{CONFIG.OPENCUE_DB_PGDATABASE}",
                    f"--datasource.cue-data-source.username={CONFIG.OPENCUE_DB_PGUSER}",
                    f"--datasource.cue-data-source.password={CONFIG.OPENCUE_DB_PGPASSWORD}",
                ],
                **copy.deepcopy(ports_dict),
            },
        },
    }

    docker_yaml = yaml.dump(docker_dict)

    yield Output(docker_dict)

    yield AssetMaterialization(
        asset_key=context.asset_key,
        metadata={
            "__".join(context.asset_key.path): MetadataValue.json(docker_dict),
            "docker_yaml": MetadataValue.md(f"```yaml\n{docker_yaml}\n```"),
        },
    )


@asset(
    **ASSET_HEADER,
    ins={
        # "CONFIG": AssetIn(
        #     AssetKey([*ASSET_HEADER["key_prefix"], "CONFIG"]),
        # ),
        # "compose_networks": AssetIn(
        #     AssetKey([*ASSET_HEADER["key_prefix"], "compose_networks"]),
        # ),
        "clone_repository": AssetIn(
            AssetKey([*ASSET_HEADER["key_prefix"], "clone_repository"]),
        ),
    },
    description=textwrap.dedent("""
        Get the official `docker-compose.yml` so that we can de-compose
        it to better configure individual services.
        
        A parsed copy of
        - [docker-compose.yml](https://github.com/AcademySoftwareFoundation/OpenCue/blob/master/docker-compose.yml)
        """),
)
def compose_opencue_base(
    context: AssetExecutionContext,
    # CONFIG: config.models.Config,  # pylint: disable=redefined-outer-name
    # compose_networks: Dict,  # pylint: disable=redefined-outer-name
    clone_repository: pathlib.Path,  # pylint: disable=redefined-outer-name
) -> Generator[Output[Dict] | AssetMaterialization, None, None]:
    """ """

    # env: Dict = CONFIG.env

    # config_engine: ConfigEngine = CONFIG.config_engine
    #
    # network_dict = {}
    # ports_dict = {}
    #
    # if "networks" in compose_networks:
    #     network_dict = {"networks": list(compose_networks.get("networks", {}).keys())}
    #     ports_dict = {
    #         "ports": [
    #             f"{CONFIG.OPENCUE_CUEBOT_GRPC_CUE_PORT_HOST}:{CONFIG.OPENCUE_CUEBOT_GRPC_CUE_PORT_CONTAINER}",
    #         ]
    #     }
    # elif "network_mode" in compose_networks:
    #     network_dict = {"network_mode": compose_networks["network_mode"]}
    #
    # volumes_dict = {"volumes": []}
    #
    # # For portability, convert absolute volume paths to relative paths
    #
    # _volume_relative = []

    # for v in volumes_dict["volumes"]:
    #
    #     host, container = v.split(":", maxsplit=1)
    #
    #     volume_dir_host_rel_path = get_relative_path_via_common_root(
    #         context=context,
    #         path_src=CONFIG.docker_compose_expanded,
    #         path_dst=pathlib.Path(host),
    #         path_common_root=pathlib.Path(env["DOT_LANDSCAPES"]),
    #     )
    #
    #     _volume_relative.append(
    #         f"{volume_dir_host_rel_path.as_posix()}:{container}",
    #     )

    # volumes_dict = {
    #     "volumes": list(
    #         {
    #             *_volume_relative,
    #             *config_engine.global_bind_volumes,
    #             *CONFIG.local_bind_volumes,
    #         }
    #     )
    # }

    # service_name_cuebot = CONFIG.opencue_cuebot
    # container_name_cuebot, host_name_cuebot = get_docker_compose_names(
    #     context=context,
    #     service_name=service_name_cuebot,
    #     landscape_id=env.get("LANDSCAPE", "default"),
    #     domain_lan=config_engine.openstudiolandscapes__domain_lan,
    # )

    # service_name_db = CONFIG.opencue_db
    # container_name_db, host_name_db = get_docker_compose_names(
    #     context=context,
    #     service_name=service_name_db,
    #     landscape_id=env.get("LANDSCAPE", "default"),
    #     domain_lan=config_engine.openstudiolandscapes__domain_lan,
    # )

    # service_name_flyway = CONFIG.opencue_flyway
    # container_name_flyway, host_name_flyway = get_docker_compose_names(
    #     context=context,
    #     service_name=service_name_flyway,
    #     landscape_id=env.get("LANDSCAPE", "default"),
    #     domain_lan=config_engine.openstudiolandscapes__domain_lan,
    # )

    # if CONFIG.OPENCUE_CUEBOT_USE_PREBUILT_DOCKER_IMAGE:
    #     d = {"image": CONFIG.OPENCUE_CUEBOT_PREBUILT_DOCKER_IMAGE}
    # else:
    #     d = {
    #         "build": {
    #             "context": clone_repository.as_posix(),
    #             "dockerfile": clone_repository.joinpath(
    #                 "cuebot",
    #                 "Dockerfile",
    #             ).as_posix(),
    #         },
    #     }

    # docker_dict = {
    #     "services": {
    #         service_name_cuebot: {
    #             # Todo:
    #             #  - [ ] prebuilt image?
    #             **d,
    #             "container_name": container_name_cuebot,
    #             "hostname": host_name_cuebot,
    #             "domainname": config_engine.openstudiolandscapes__domain_lan,
    #             "restart": DockerComposePolicies.RESTART_POLICY.ALWAYS.value,
    #             "environment": {
    #                 "TZ": config_engine.tz,
    #                 "CUE_FRAME_LOG_DIR": "/tmp/rqd/logs",
    #                 **config_engine.global_environment_variables,
    #                 **CONFIG.local_environment_variables,
    #             },
    #             **copy.deepcopy(volumes_dict),
    #             **copy.deepcopy(network_dict),
    #             "links": [
    #                 service_name_db,
    #             ],
    #             "depends_on": {
    #                 service_name_db: {
    #                     "condition": "service_started",
    #                 },
    #                 service_name_flyway: {
    #                     "condition": "service_completed_successfully",
    #                 },
    #             },
    #             # "healthcheck": {
    #             #     # Todo:
    #             #     #  - [ ] fix: test succeeds even if Postgres is down
    #             #     #  "test": ["CMD-SHELL", "psql -U ${DB_USER} -d ${DB_MAIN} -c 'SELECT 1' || exit 1"],
    #             #     "test": ["CMD", "curl", "-f", f"http://localhost:{env.get('KITSU_PORT_CONTAINER')}"],
    #             #     "interval": "10s",
    #             #     "timeout": "2s",
    #             #     "retries": "3",
    #             # },
    #             "command": [
    #                 f"--datasource.cue-data-source.jdbc-url=jdbc:postgresql://{container_name_db}/{CONFIG.OPENCUE_DB_PGDATABASE}",
    #                 f"--datasource.cue-data-source.username={CONFIG.OPENCUE_DB_PGUSER}",
    #                 f"--datasource.cue-data-source.password={CONFIG.OPENCUE_DB_PGPASSWORD}",
    #             ],
    #             **copy.deepcopy(ports_dict),
    #         },
    #     },
    # }

    docker_yaml = clone_repository.joinpath("docker-compose.yml").read_text()
    docker_dict = yaml.safe_load(docker_yaml)

    yield Output(docker_dict)

    yield AssetMaterialization(
        asset_key=context.asset_key,
        metadata={
            "__".join(context.asset_key.path): MetadataValue.json(docker_dict),
            "docker_yaml": MetadataValue.md(f"```yaml\n{docker_yaml}\n```"),
        },
    )


@asset(
    **ASSET_HEADER,
    ins={
        "CONFIG": AssetIn(
            AssetKey([*ASSET_HEADER["key_prefix"], "CONFIG"]),
        ),
        "compose_networks": AssetIn(
            AssetKey([*ASSET_HEADER["key_prefix"], "compose_networks"]),
        ),
        "clone_repository": AssetIn(
            AssetKey([*ASSET_HEADER["key_prefix"], "clone_repository"]),
        ),
        "compose_opencue_base": AssetIn(
            AssetKey([*ASSET_HEADER["key_prefix"], "compose_opencue_base"]),
        ),
    },
    description=textwrap.dedent("""
        Based on
        - [docker-compose.yml](https://github.com/AcademySoftwareFoundation/OpenCue/blob/master/docker-compose.yml)
        
        Reference:
        ```yaml
          flyway:
            build:
              context: ./
              dockerfile: ./sandbox/flyway.Dockerfile
            links:
              - db
            depends_on:
              - db
            environment:
              - PGUSER=cuebot
              - PGPASSWORD=cuebot_password
              - PGDATABASE=cuebot
              - PGHOST=db
              - PGPORT=5432
            command: /opt/scripts/migrate.sh
        ```
        """),
)
def compose_flyway(
    context: AssetExecutionContext,
    CONFIG: config.models.Config,  # pylint: disable=redefined-outer-name
    compose_networks: Dict,  # pylint: disable=redefined-outer-name
    clone_repository: pathlib.Path,  # pylint: disable=redefined-outer-name
    compose_opencue_base: Dict,  # pylint: disable=redefined-outer-name
) -> Generator[Output[Dict] | AssetMaterialization, None, None]:
    """ """

    env: Dict = CONFIG.env

    config_engine: ConfigEngine = CONFIG.config_engine

    network_dict = {}
    ports_dict = {}

    if "networks" in compose_networks:
        network_dict = {"networks": list(compose_networks.get("networks", {}).keys())}
        # ports_dict = {}
    elif "network_mode" in compose_networks:
        network_dict = {"network_mode": compose_networks["network_mode"]}

    volumes_dict = {"volumes": []}

    # For portability, convert absolute volume paths to relative paths

    _volume_relative = []

    for v in volumes_dict["volumes"]:

        host, container = v.split(":", maxsplit=1)

        volume_dir_host_rel_path = get_relative_path_via_common_root(
            context=context,
            path_src=CONFIG.docker_compose_expanded,
            path_dst=pathlib.Path(host),
            path_common_root=pathlib.Path(env["DOT_LANDSCAPES"]),
        )

        _volume_relative.append(
            f"{volume_dir_host_rel_path.as_posix()}:{container}",
        )

    volumes_dict = {
        "volumes": list(
            {
                *_volume_relative,
                *config_engine.global_bind_volumes,
                *CONFIG.local_bind_volumes,
            }
        )
    }

    service_name_db = CONFIG.opencue_db
    container_name_db, _ = get_docker_compose_names(
        context=context,
        service_name=service_name_db,
        landscape_id=env.get("LANDSCAPE", "default"),
        domain_lan=config_engine.openstudiolandscapes__domain_lan,
    )

    service_name_flyway = CONFIG.opencue_flyway
    container_name_flyway, host_name_flyway = get_docker_compose_names(
        context=context,
        service_name=service_name_flyway,
        landscape_id=env.get("LANDSCAPE", "default"),
        domain_lan=config_engine.openstudiolandscapes__domain_lan,
    )

    compose_flyway_base = compose_opencue_base.get("services", {}).get("flyway", {})
    compose_flyway_base.pop("profiles", None)

    context_ = clone_repository.joinpath(compose_flyway_base["build"]["context"])
    d = {
        "build": {
            # Just prepend the full path to the cloned repo
            "context": context_.as_posix(),
            "dockerfile": context_.joinpath(compose_flyway_base["build"]["dockerfile"]).as_posix(),
        },
    }

    #       context: /home/michael/.local/share/OpenStudioLandscapes/.landscapes/2026-07-16_09-14-15__sideways-flicker-truthful-motorcycle/OpenStudioLandscapes-OpenCue/OpenStudioLandscapes_OpenCue__clone_repository/repos/OpenCue
    #       context: /home/michael/.local/share/OpenStudioLandscapes/.landscapes/2026-07-16_09-14-15__sideways-flicker-truthful-motorcycle/OpenStudioLandscapes-OpenCue/OpenStudioLandscapes_OpenCue__clone_repository/repos/OpenCue

    docker_dict = {
        "services": {
            service_name_flyway: {
                # Todo
                #  - [ ] Using the base compose dict as the starting
                #        point and override with our values
                #        This might be error prone so maybe
                #        there is a better way.
                **compose_flyway_base,
                **d,
                "container_name": container_name_flyway,
                # Todo:
                #  - [x] prebuilt image?
                #        Not available
                # "build": {
                #     "context": clone_repository.as_posix(),
                #     "dockerfile": clone_repository.joinpath(
                #         "sandbox",
                #         "flyway.Dockerfile",
                #     ).as_posix(),
                # },
                "hostname": host_name_flyway,
                "domainname": config_engine.openstudiolandscapes__domain_lan,
                # "restart": DockerComposePolicies.RESTART_POLICY.NO.value,
                "environment": {
                    "TZ": config_engine.tz,
                    "PGHOST": container_name_db,
                    "PGDATABASE": CONFIG.OPENCUE_DB_PGDATABASE,
                    "PGPASSWORD": CONFIG.OPENCUE_DB_PGPASSWORD,
                    "PGUSER": CONFIG.OPENCUE_DB_PGUSER,
                    "PGPORT": str(CONFIG.OPENCUE_DB_PORT_CONTAINER),
                    **config_engine.global_environment_variables,
                    **CONFIG.local_environment_variables,
                },
                # "command": [
                #     "/opt/scripts/migrate.sh",
                #     # Todo:
                #     #   ?column?
                #     #  ----------
                #     #  1
                #     #  (1 row)
                #     #  Applying database migrations...
                #     #  A new version of Flyway is available
                #     #  Upgrade Flyway: https://rd.gt/3TItF25
                #     #  Flyway Community Edition 9.11.0 by Redgate
                #     #  See what's new here: https://flywaydb.org/documentation/learnmore/releaseNotes#9.11.0
                #     #  Database: jdbc:postgresql://opencue-db.2026-01-21_17-22-54__seasoned-jelly-wholesale-mixer:5432/cuebot (PostgreSQL 15.1)
                #     #  ERROR: Validate failed: Migrations have failed validation
                #     #  Detected resolved migration not applied to database: 35.
                #     #  To ignore this migration, set -ignoreMigrationPatterns='*:ignored'. To allow executing this migration, set -outOfOrder=true.
                #     #  Need more flexibility with validation rules? Learn more: https://rd.gt/3AbJUZE
                #     #  opencue-flyway.2026-01-21_17-22-54__seasoned-jelly-wholesale-mixer                         | A new version of Flyway is available
                #     #  opencue-flyway.2026-01-21_17-22-54__seasoned-jelly-wholesale-mixer                         | Upgrade Flyway: https://rd.gt/3TItF25
                #     #  opencue-flyway.2026-01-21_17-22-54__seasoned-jelly-wholesale-mixer                         | Flyway Community Edition 9.11.0 by Redgate
                #     #  opencue-flyway.2026-01-21_17-22-54__seasoned-jelly-wholesale-mixer                         | See what's new here: https://flywaydb.org/documentation/learnmore/releaseNotes#9.11.0
                #     #  opencue-flyway.2026-01-21_17-22-54__seasoned-jelly-wholesale-mixer                         |
                #     #  opencue-flyway.2026-01-21_17-22-54__seasoned-jelly-wholesale-mixer                         | Database: jdbc:postgresql://opencue-db.2026-01-21_17-22-54__seasoned-jelly-wholesale-mixer:5432/cuebot (PostgreSQL 15.1)
                #     #  opencue-flyway.2026-01-21_17-22-54__seasoned-jelly-wholesale-mixer                         | ERROR: Validate failed: Migrations have failed validation
                #     #  opencue-flyway.2026-01-21_17-22-54__seasoned-jelly-wholesale-mixer                         | Detected resolved migration not applied to database: 35.
                #     #  opencue-flyway.2026-01-21_17-22-54__seasoned-jelly-wholesale-mixer                         | To ignore this migration, set -ignoreMigrationPatterns='*:ignored'. To allow executing this migration, set -outOfOrder=true.
                #     #  opencue-flyway.2026-01-21_17-22-54__seasoned-jelly-wholesale-mixer                         | Need more flexibility with validation rules? Learn more: https://rd.gt/3AbJUZE
                #     #   Container opencue-flyway.2026-01-21_17-22-54__seasoned-jelly-wholesale-mixer Error service "opencue-flyway" didn't complete successfully: exit 1
                #     #  opencue-flyway.2026-01-21_17-22-54__seasoned-jelly-wholesale-mixer exited with code 1
                #     #  Didn't work: "-outOfOrder=true",
                #     #  Solution so far: re-create the database
                # ],
                **copy.deepcopy(volumes_dict),
                **copy.deepcopy(network_dict),
                "links": [
                    service_name_db,
                ],
                "depends_on": {
                    service_name_db: {
                        "condition": "service_healthy",
                    },
                },
                # "healthcheck": {
                #     # Todo:
                #     #  - [ ] fix: test succeeds even if Postgres is down
                #     #  "test": ["CMD-SHELL", "psql -U ${DB_USER} -d ${DB_MAIN} -c 'SELECT 1' || exit 1"],
                #     "test": ["CMD", "curl", "-f", f"http://localhost:{env.get('KITSU_PORT_CONTAINER')}"],
                #     "interval": "10s",
                #     "timeout": "2s",
                #     "retries": "3",
                # },
                **copy.deepcopy(ports_dict),
            },
        },
    }

    docker_yaml = yaml.dump(docker_dict)

    yield Output(docker_dict)

    yield AssetMaterialization(
        asset_key=context.asset_key,
        metadata={
            "__".join(context.asset_key.path): MetadataValue.json(docker_dict),
            "docker_yaml": MetadataValue.md(f"```yaml\n{docker_yaml}\n```"),
        },
    )


@asset(
    **ASSET_HEADER,
    ins={
        "CONFIG": AssetIn(
            AssetKey([*ASSET_HEADER["key_prefix"], "CONFIG"]),
        ),
        "compose_networks": AssetIn(
            AssetKey([*ASSET_HEADER["key_prefix"], "compose_networks"]),
        ),
        # "clone_repository": AssetIn(
        #     AssetKey([*ASSET_HEADER["key_prefix"], "clone_repository"]),
        # ),
        "compose_opencue_base": AssetIn(
            AssetKey([*ASSET_HEADER["key_prefix"], "compose_opencue_base"]),
        ),
    },
    description=textwrap.dedent("""
        Based on
        - [docker-compose.yml](https://github.com/AcademySoftwareFoundation/OpenCue/blob/master/docker-compose.yml)
        
        Reference:
        ```yaml
          db:
            image: postgres:15.1
            environment:
              - POSTGRES_USER=cuebot
              - POSTGRES_PASSWORD=cuebot_password
              - POSTGRES_DB=cuebot
            ports:
              - "5432:5432"
            volumes:
              - ./sandbox/db-data:/var/lib/postgresql/data
        ```
        """),
)
def compose_db(
    context: AssetExecutionContext,
    CONFIG: config.models.Config,  # pylint: disable=redefined-outer-name
    compose_networks: Dict,  # pylint: disable=redefined-outer-name
    # clone_repository: pathlib.Path,  # pylint: disable=redefined-outer-name
    compose_opencue_base: Dict,  # pylint: disable=redefined-outer-name
) -> Generator[Output[Dict] | AssetMaterialization, None, None]:

    env: Dict = CONFIG.env

    config_engine: ConfigEngine = CONFIG.config_engine

    network_dict = {}
    ports_dict = {}

    if "networks" in compose_networks:
        network_dict = {"networks": list(compose_networks.get("networks", {}).keys())}
        ports_dict = {
            "ports": [
                f"{CONFIG.OPENCUE_DB_PORT_HOST}:{CONFIG.OPENCUE_DB_PORT_CONTAINER}",
            ]
        }
    elif "network_mode" in compose_networks:
        network_dict = {"network_mode": compose_networks["network_mode"]}

    opencue_db_dir_host = CONFIG.OPENCUE_DB_INSTALL_DESTINATION_expanded

    opencue_db_dir_host.mkdir(parents=True, exist_ok=True)
    context.log.info(f"Directory {opencue_db_dir_host.as_posix()} created.")

    volumes_dict = {
        "volumes": [
            f"{opencue_db_dir_host.as_posix()}:/var/lib/postgresql/data:rw",
        ]
    }

    # For portability, convert absolute volume paths to relative paths

    _volume_relative = []

    for v in volumes_dict["volumes"]:

        host, container = v.split(":", maxsplit=1)

        volume_dir_host_rel_path = get_relative_path_via_common_root(
            context=context,
            path_src=CONFIG.docker_compose_expanded,
            path_dst=pathlib.Path(host),
            path_common_root=pathlib.Path(env["DOT_LANDSCAPES"]),
        )

        _volume_relative.append(
            f"{volume_dir_host_rel_path.as_posix()}:{container}",
        )

    volumes_dict = {
        "volumes": list(
            {
                *_volume_relative,
                *config_engine.global_bind_volumes,
                *CONFIG.local_bind_volumes,
            }
        )
    }

    service_name = CONFIG.opencue_db
    container_name, host_name = get_docker_compose_names(
        context=context,
        service_name=service_name,
        landscape_id=env.get("LANDSCAPE", "default"),
        domain_lan=config_engine.openstudiolandscapes__domain_lan,
    )

    compose_db_base = compose_opencue_base.get("services", {}).get("db", {})
    compose_db_base.pop("profiles", None)

    docker_dict = {
        "services": {
            service_name: {
                # Todo
                #  - [ ] Using the base compose dict as the starting
                #        point and override with our values
                #        This might be error prone so maybe
                #        there is a better way.
                **compose_db_base,
                "container_name": container_name,
                "hostname": host_name,
                "domainname": config_engine.openstudiolandscapes__domain_lan,
                # "restart": DockerComposePolicies.RESTART_POLICY.ALWAYS.value,
                "environment": {
                    "TZ": config_engine.tz,
                    "POSTGRES_USER": CONFIG.OPENCUE_DB_PGUSER,
                    "POSTGRES_PASSWORD": CONFIG.OPENCUE_DB_PGPASSWORD,
                    "POSTGRES_DB": CONFIG.OPENCUE_DB_PGDATABASE,
                    **config_engine.global_environment_variables,
                    **CONFIG.local_environment_variables,
                },
                **copy.deepcopy(volumes_dict),
                **copy.deepcopy(network_dict),
                **copy.deepcopy(ports_dict),
            },
        },
    }

    docker_yaml = yaml.dump(docker_dict)

    yield Output(docker_dict)

    yield AssetMaterialization(
        asset_key=context.asset_key,
        metadata={
            "__".join(context.asset_key.path): MetadataValue.json(docker_dict),
            "docker_yaml": MetadataValue.md(f"```yaml\n{docker_yaml}\n```"),
        },
    )


# @asset(
#     **ASSET_HEADER,
#     ins={
#         "feature_in": AssetIn(
#             AssetKey([*ASSET_HEADER["key_prefix"], "feature_in"]),
#         ),
#         "CONFIG": AssetIn(
#             AssetKey([*ASSET_HEADER["key_prefix"], "CONFIG"]),
#         ),
#     },
# )
# def write_dockerfile_cueweb(
#     context: AssetExecutionContext,
#     feature_in: OpenStudioLandscapesFeatureIn,  # pylint: disable=redefined-outer-name
#     CONFIG: config.models.Config,  # pylint: disable=redefined-outer-name
# ) -> Generator[Output[pathlib.Path] | AssetMaterialization, None, None]:
#     """ """
#
#     env: Dict = CONFIG.env
#
#     config_engine: ConfigEngine = CONFIG.config_engine
#
#     docker_config: DockerConfigModel = config_engine.openstudiolandscapes__docker_config
#
#     docker_image: Dict = feature_in.openstudiolandscapes_base.docker_image_base
#
#     docker_file = pathlib.Path(
#         env["DOT_LANDSCAPES"],
#         env.get("LANDSCAPE", "default"),
#         f"{dist.name}",
#         "__".join(context.asset_key.path),
#         "Dockerfiles",
#         "Dockerfile",
#     )
#
#     docker_file.parent.mkdir(parents=True, exist_ok=True)
#
#     #################################################
#
#     (
#         image_name,
#         image_prefixes,
#         tags,
#         build_base_parent_image_prefix,
#         build_base_parent_image_name,
#         build_base_parent_image_tags,
#     ) = get_image_metadata(
#         context=context,
#         docker_image=docker_image,
#         docker_config=docker_config,
#         env=env,
#     )
#
#     #################################################
#
#     # dnf_install_str_: str = get_dnf_install_str(
#     #     dnf_install_packages=[
#     #         *CONFIG.dnf_packages_base,
#     #         *CONFIG.openstudiolandscapes__rez_config.dnf_packages_rez,
#     #     ],
#     # )
#
#     pip_install_str: str = get_pip_install_str(pip_install_packages=CONFIG.pip_packages)
#
#     # Todo
#     #  - [x] [root@lenovo opencue]# rez env blender -- which blender
#     #        /data/share/tools/blender-5.0.1-linux-x64/blender
#     #        [root@lenovo opencue]# rez env blender -- blender -b -v
#     #        #
#     #        blender: error while loading shared libraries: libX11.so.6: cannot open shared object file: No such file or directory
#     #        [root@lenovo opencue]# rez env blender -- blender -b -v
#     #        Blender 5.0.1 (hash a3db93c5b259 built 2025-12-16 01:30:59)
#     #        Blender 5.0.1
#     #                build date: 2025-12-16
#     #                build time: 01:30:59
#     #                build commit date: 2025-12-15
#     #                build commit time: 16:36
#     #                build hash: a3db93c5b259
#     #                build branch: blender-v5.0-release
#     #                build platform: Linux
#     #                build type: Release
#     #                build c flags:  -Wall -Werror=implicit-function-declaration -Wstrict-prototypes -Werror=return-type -Werror=vla -Wmissing-prototypes -Wno-char-subscripts -Wno-unknown-pragmas -Wpointer-arith -Wunused-parameter -Wwrite-strings -Wlogical-op -Wundef -Winit-self -Wmissing-include-dirs -Wno-div-by-zero -Wtype-limits -Wformat-signedness -Wrestrict -Wno-stringop-overread -Wno-stringop-overflow -Wnonnull -Wabsolute-value -Wuninitialized -Wredundant-decls -Wshadow -Wimplicit-fallthrough=5 -Wno-error=unused-but-set-variable  -march=x86-64-v2 -std=gnu11 -pipe -fPIC -funsigned-char -fno-strict-aliasing -ffp-contract=off
#     #                build c++ flags:  -Wuninitialized -Wredundant-decls -Wall -Wno-invalid-offsetof -Wno-sign-compare -Wlogical-op -Winit-self -Wmissing-include-dirs -Wno-div-by-zero -Wtype-limits -Werror=return-type -Wno-char-subscripts -Wno-unknown-pragmas -Wpointer-arith -Wunused-parameter -Wwrite-strings -Wundef -Wcomma-subscript -Wformat-signedness -Wrestrict -Wno-suggest-override -Wuninitialized -Wno-stringop-overread -Wno-stringop-overflow -Wimplicit-fallthrough=5 -Wundef -Wmissing-declarations  -march=x86-64-v2 -pipe -fPIC -funsigned-char -fno-strict-aliasing -ffp-contract=off
#     #                build link flags:  -Wl,--version-script='/home/blender/git/blender-v500/blender.git/source/creator/symbols_unix.map'
#     #                build system: CMake
#
#     # @formatter:off
#     docker_file_str = textwrap.dedent("""\
#         # {auto_generated}
#         # {dagster_url}
#
#         ################################################################################
#         # Multi Stage: Stage 1
#         # FROM {parent_image} AS {image_name}
#         FROM {parent_image} AS base
#         LABEL authors="{AUTHOR}"
#
#         ENV CONTAINER_TIMEZONE={timezone}
#         ENV SET_CONTAINER_TIMEZONE=true
#
#         WORKDIR /usr/bin
#         RUN ln -s python3.9 python
#
#         WORKDIR /opt/opencue
#
#         # Prepend to PATH /opt/python{PYTHON_MAJ}.{PYTHON_MIN}/bin
#         ENV PATH="/opt/python{PYTHON_MAJ}.{PYTHON_MIN}/bin:$PATH"
#         # Prepend to PATH /opt/rez/bin/rez
#         ENV PATH="/opt/rez/bin/rez:$PATH"
#
#         ENV LC_ALL=C.UTF-8
#         ENV LANG=C.UTF-8
#
#         SHELL ["/bin/bash", "-c"]
#
#         # General packages
#         RUN dnf install -y {dnf_packages_general}
#
#         # Blender 5.0.1
#         # on docker.io/rockylinux:8.9
#         RUN dnf install -y {dnf_packages_blender_5}
#
#         ################################################################################
#         # Multi Stage: Stage Rez
#         # # Rez Installer
#         FROM base AS rez_installer
#
#         # COPY --from=build_python "/opt/python{PYTHON_MAJ}.{PYTHON_MIN}" "/opt/python{PYTHON_MAJ}.{PYTHON_MIN}"
#
#         WORKDIR /build/rez
#
#         RUN curl -L "https://github.com/AcademySoftwareFoundation/rez/archive/refs/tags/{rez_version}.tar.gz" -o rez-{rez_version}.tar.gz \\
#             && file rez-{rez_version}.tar.gz \\
#             && tar -xzvf rez-{rez_version}.tar.gz \\
#             && rm rez-{rez_version}.tar.gz
#
#         # https://github.com/AcademySoftwareFoundation/OpenCue/blob/master/rqd/Dockerfile
#         # comes with python39
#         # Todo:
#         #  - [ ] Install OpenStudioLandscapes Python (3.11)
#         RUN python3.9 ./rez-{rez_version}/install.py --verbose /opt/rez
#
#         RUN chmod +x /opt/rez/completion/complete.sh
#         RUN /opt/rez/completion/complete.sh
#
#         # # Rez Build Test
#         FROM rez_installer AS rez_build_test
#
#         WORKDIR /build/rez/rez-{rez_version}/example_packages/hello_world
#
#         RUN rez bind -vvvvv --quickstart
#         RUN rez build -vvvvv --install
#
#         RUN rez env -vvvvv hello_world -- hello
#
#         RUN echo "hello_world successfully tested" > /rez_hello_world_test.txt
#
#         ################################################################################
#         # Multi Stage: Stage FINAL
#         FROM base AS {image_name}
#
#         COPY --from=rez_installer  "/opt/rez" "/opt/rez"
#         COPY --from=rez_build_test "/rez_hello_world_test.txt" "/rez_hello_world_test.txt"
#
#         RUN python3.9 -m pip install --root-user-action=ignore --upgrade pip setuptools setuptools_scm wheel \\
#             && python3.9 -m pip cache purge
#
#         {pip_install_str}
#
#         WORKDIR /opt/opencue
#
#         # RQD gRPC server
#         EXPOSE 8444
#
#         # NOTE: This shell out is needed to avoid RQD getting PID 0 which leads to leaking child processes.
#         ENTRYPOINT ["/bin/bash", "-c", "set -e && rqd"]
#         """).format(
#         auto_generated=f"AUTO-GENERATED by Dagster Asset {'__'.join(context.asset_key.path)}",
#         dagster_url=urllib.parse.quote(
#             f"http://localhost:3000/asset-groups/{'%2F'.join(context.asset_key.path)}",
#             safe=":/%",
#         ),
#         pip_install_str=pip_install_str.format(
#             **env,
#         ),
#         dnf_packages_general=" ".join(CONFIG.dnf_packages_general),
#         dnf_packages_blender_5=" ".join(CONFIG.dnf_packages_blender_5),
#         rez_version=config_engine.openstudiolandscapes__rez_config.rez_version,
#         timezone=config_engine.tz,
#         image_name=image_name,
#         # Todo: this won't work as expected if len(tags) > 1
#         parent_image=CONFIG.OPENCUE_RQD_DOCKER_IMAGE,
#         **env,
#     )
#     # @formatter:on
#
#     with open(docker_file, "w") as fw:
#         fw.write(docker_file_str)
#
#     with open(docker_file, "r") as fr:
#         docker_file_content = fr.read()
#
#     yield Output(docker_file)
#
#     yield AssetMaterialization(
#         asset_key=context.asset_key,
#         metadata={
#             "__".join(context.asset_key.path): MetadataValue.path(docker_file),
#             docker_file.name: MetadataValue.md(f"```shell\n{docker_file_content}\n```"),
#             "env": MetadataValue.json(env),
#         },
#     )


# @asset(
#     **ASSET_HEADER,
#     ins={
#         # "feature_in": AssetIn(
#         #     AssetKey([*ASSET_HEADER["key_prefix"], "feature_in"]),
#         # ),
#         "compose_opencue_base": AssetIn(
#             AssetKey([*ASSET_HEADER["key_prefix"], "compose_opencue_base"]),
#         ),
#         "clone_repository": AssetIn(
#             AssetKey([*ASSET_HEADER["key_prefix"], "clone_repository"]),
#         ),
#         "CONFIG": AssetIn(
#             AssetKey([*ASSET_HEADER["key_prefix"], "CONFIG"]),
#         ),
#     },
# )
# def write_dockerfile_cueweb(
#     context: AssetExecutionContext,
#     # feature_in: OpenStudioLandscapesFeatureIn,  # pylint: disable=redefined-outer-name
#     compose_opencue_base: Dict,  # pylint: disable=redefined-outer-name
#     clone_repository: pathlib.Path,  # pylint: disable=redefined-outer-name
#     CONFIG: config.models.Config,  # pylint: disable=redefined-outer-name
# ) -> Generator[Output[pathlib.Path] | AssetMaterialization, None, None]:
#     """ """
#
#     env: Dict = CONFIG.env
#
#     # config_engine: ConfigEngine = CONFIG.config_engine
#
#     # docker_config: DockerConfigModel = config_engine.openstudiolandscapes__docker_config
#     #
#     # docker_image: Dict = feature_in.openstudiolandscapes_base.docker_image_base
#
#     # d = {
#     #     "build": {
#     #         "context": clone_repository.as_posix(),
#     #         "dockerfile": clone_repository.joinpath(
#     #             "cuebot",
#     #             "Dockerfile",
#     #         ).as_posix(),
#     #     },
#     # }
#
#     # docker_file_repo = clone_repository.joinpath(
#     #     "rqd",
#     #     "Dockerfile",
#     # )
#
#     compose_cueweb_base = compose_opencue_base.get("services", {}).get("cueweb", {})
#
#     context_ = clone_repository.joinpath(compose_cueweb_base["build"]["context"])
#     docker_file_repo = context_.joinpath(compose_cueweb_base["build"]["dockerfile"])
#
#     # docker_file = pathlib.Path(
#     #     env["DOT_LANDSCAPES"],
#     #     env.get("LANDSCAPE", "default"),
#     #     f"{dist.name}",
#     #     "__".join(context.asset_key.path),
#     #     "Dockerfiles",
#     #     "Dockerfile",
#     # )
#
#     docker_file = docker_file_repo.parent.joinpath("Dockerfile_Custom")
#
#     docker_file.parent.mkdir(parents=True, exist_ok=True)
#
#     image_name = get_image_name(context=context)
#     context.log.debug(f"{image_name = }")
#
#     with open(docker_file_repo, "r") as fr:
#         docker_file_repo_str = fr.read()
#         # docker_file_repo_str.replace(
#         #     "FROM openjdk:18-ea-18-slim-bullseye",
#         #     f"FROM openjdk:18-ea-18-slim-bullseye as {image_name}"
#         # )
#         # Todo
#         #  - [ ] This is a bit hacky
#         # docker_file_repo_str = re.sub(
#         #     "FROM openjdk:18-ea-18-slim-bullseye",
#         #     f"FROM openjdk:18-ea-18-slim-bullseye as {image_name}",
#         #     docker_file_repo_str,
#         # )
#         docker_file_repo_str = re.sub(
#             "FROM rockylinux:8.9",
#             f"FROM rockylinux:8.9 as {image_name}",
#             docker_file_repo_str,
#         )
#         context.log.debug(f"{docker_file_repo_str = }")
#
#     # #################################################
#     #
#     # (
#     #     image_name,
#     #     image_prefixes,
#     #     tags,
#     #     build_base_parent_image_prefix,
#     #     build_base_parent_image_name,
#     #     build_base_parent_image_tags,
#     # ) = get_image_metadata(
#     #     context=context,
#     #     docker_image=docker_image,
#     #     docker_config=docker_config,
#     #     env=env,
#     # )
#     #
#     # # #################################################
#     #
#     # # dnf_install_str_: str = get_dnf_install_str(
#     # #     dnf_install_packages=[
#     # #         *CONFIG.dnf_packages_base,
#     # #         *CONFIG.openstudiolandscapes__rez_config.dnf_packages_rez,
#     # #     ],
#     # # )
#     #
#     # pip_install_str: str = get_pip_install_str(pip_install_packages=CONFIG.pip_packages)
#     #
#     # # Todo
#     # #  - [x] [root@lenovo opencue]# rez env blender -- which blender
#     # #        /data/share/tools/blender-5.0.1-linux-x64/blender
#     # #        [root@lenovo opencue]# rez env blender -- blender -b -v
#     # #        #
#     # #        blender: error while loading shared libraries: libX11.so.6: cannot open shared object file: No such file or directory
#     # #        [root@lenovo opencue]# rez env blender -- blender -b -v
#     # #        Blender 5.0.1 (hash a3db93c5b259 built 2025-12-16 01:30:59)
#     # #        Blender 5.0.1
#     # #                build date: 2025-12-16
#     # #                build time: 01:30:59
#     # #                build commit date: 2025-12-15
#     # #                build commit time: 16:36
#     # #                build hash: a3db93c5b259
#     # #                build branch: blender-v5.0-release
#     # #                build platform: Linux
#     # #                build type: Release
#     # #                build c flags:  -Wall -Werror=implicit-function-declaration -Wstrict-prototypes -Werror=return-type -Werror=vla -Wmissing-prototypes -Wno-char-subscripts -Wno-unknown-pragmas -Wpointer-arith -Wunused-parameter -Wwrite-strings -Wlogical-op -Wundef -Winit-self -Wmissing-include-dirs -Wno-div-by-zero -Wtype-limits -Wformat-signedness -Wrestrict -Wno-stringop-overread -Wno-stringop-overflow -Wnonnull -Wabsolute-value -Wuninitialized -Wredundant-decls -Wshadow -Wimplicit-fallthrough=5 -Wno-error=unused-but-set-variable  -march=x86-64-v2 -std=gnu11 -pipe -fPIC -funsigned-char -fno-strict-aliasing -ffp-contract=off
#     # #                build c++ flags:  -Wuninitialized -Wredundant-decls -Wall -Wno-invalid-offsetof -Wno-sign-compare -Wlogical-op -Winit-self -Wmissing-include-dirs -Wno-div-by-zero -Wtype-limits -Werror=return-type -Wno-char-subscripts -Wno-unknown-pragmas -Wpointer-arith -Wunused-parameter -Wwrite-strings -Wundef -Wcomma-subscript -Wformat-signedness -Wrestrict -Wno-suggest-override -Wuninitialized -Wno-stringop-overread -Wno-stringop-overflow -Wimplicit-fallthrough=5 -Wundef -Wmissing-declarations  -march=x86-64-v2 -pipe -fPIC -funsigned-char -fno-strict-aliasing -ffp-contract=off
#     # #                build link flags:  -Wl,--version-script='/home/blender/git/blender-v500/blender.git/source/creator/symbols_unix.map'
#     # #                build system: CMake
#     #
#     # # @formatter:off
#     # docker_file_str = textwrap.dedent("""\
#     #     # {auto_generated}
#     #     # {dagster_url}
#     #
#     #     ################################################################################
#     #     # Multi Stage: Stage 1
#     #     # FROM {parent_image} AS {image_name}
#     #     FROM {parent_image} AS base
#     #     LABEL authors="{AUTHOR}"
#     #
#     #     ENV CONTAINER_TIMEZONE={timezone}
#     #     ENV SET_CONTAINER_TIMEZONE=true
#     #
#     #     WORKDIR /usr/bin
#     #     RUN ln -s python3.9 python
#     #
#     #     WORKDIR /opt/opencue
#     #
#     #     # Prepend to PATH /opt/python{PYTHON_MAJ}.{PYTHON_MIN}/bin
#     #     ENV PATH="/opt/python{PYTHON_MAJ}.{PYTHON_MIN}/bin:$PATH"
#     #     # Prepend to PATH /opt/rez/bin/rez
#     #     ENV PATH="/opt/rez/bin/rez:$PATH"
#     #
#     #     ENV LC_ALL=C.UTF-8
#     #     ENV LANG=C.UTF-8
#     #
#     #     SHELL ["/bin/bash", "-c"]
#     #
#     #     # General packages
#     #     RUN dnf install -y {dnf_packages_general}
#     #
#     #     # Blender 5.0.1
#     #     # on docker.io/rockylinux:8.9
#     #     RUN dnf install -y {dnf_packages_blender_5}
#     #
#     #     ################################################################################
#     #     # Multi Stage: Stage Rez
#     #     # # Rez Installer
#     #     FROM base AS rez_installer
#     #
#     #     # COPY --from=build_python "/opt/python{PYTHON_MAJ}.{PYTHON_MIN}" "/opt/python{PYTHON_MAJ}.{PYTHON_MIN}"
#     #
#     #     WORKDIR /build/rez
#     #
#     #     RUN curl -L "https://github.com/AcademySoftwareFoundation/rez/archive/refs/tags/{rez_version}.tar.gz" -o rez-{rez_version}.tar.gz \\
#     #         && file rez-{rez_version}.tar.gz \\
#     #         && tar -xzvf rez-{rez_version}.tar.gz \\
#     #         && rm rez-{rez_version}.tar.gz
#     #
#     #     # https://github.com/AcademySoftwareFoundation/OpenCue/blob/master/rqd/Dockerfile
#     #     # comes with python39
#     #     # Todo:
#     #     #  - [ ] Install OpenStudioLandscapes Python (3.11)
#     #     RUN python3.9 ./rez-{rez_version}/install.py --verbose /opt/rez
#     #
#     #     RUN chmod +x /opt/rez/completion/complete.sh
#     #     RUN /opt/rez/completion/complete.sh
#     #
#     #     # # Rez Build Test
#     #     FROM rez_installer AS rez_build_test
#     #
#     #     WORKDIR /build/rez/rez-{rez_version}/example_packages/hello_world
#     #
#     #     RUN rez bind -vvvvv --quickstart
#     #     RUN rez build -vvvvv --install
#     #
#     #     RUN rez env -vvvvv hello_world -- hello
#     #
#     #     RUN echo "hello_world successfully tested" > /rez_hello_world_test.txt
#     #
#     #     ################################################################################
#     #     # Multi Stage: Stage FINAL
#     #     FROM base AS {image_name}
#     #
#     #     COPY --from=rez_installer  "/opt/rez" "/opt/rez"
#     #     COPY --from=rez_build_test "/rez_hello_world_test.txt" "/rez_hello_world_test.txt"
#     #
#     #     RUN python3.9 -m pip install --root-user-action=ignore --upgrade pip setuptools setuptools_scm wheel \\
#     #         && python3.9 -m pip cache purge
#     #
#     #     {pip_install_str}
#     #
#     #     WORKDIR /opt/opencue
#     #
#     #     # RQD gRPC server
#     #     EXPOSE 8444
#     #
#     #     # NOTE: This shell out is needed to avoid RQD getting PID 0 which leads to leaking child processes.
#     #     ENTRYPOINT ["/bin/bash", "-c", "set -e && rqd"]
#     #     """).format(
#     #     auto_generated=f"AUTO-GENERATED by Dagster Asset {'__'.join(context.asset_key.path)}",
#     #     dagster_url=urllib.parse.quote(
#     #         f"http://localhost:3000/asset-groups/{'%2F'.join(context.asset_key.path)}",
#     #         safe=":/%",
#     #     ),
#     #     pip_install_str=pip_install_str.format(
#     #         **env,
#     #     ),
#     #     dnf_packages_general=" ".join(CONFIG.dnf_packages_general),
#     #     dnf_packages_blender_5=" ".join(CONFIG.dnf_packages_blender_5),
#     #     rez_version=config_engine.openstudiolandscapes__rez_config.rez_version,
#     #     timezone=config_engine.tz,
#     #     image_name=image_name,
#     #     # Todo: this won't work as expected if len(tags) > 1
#     #     parent_image=CONFIG.OPENCUE_RQD_DOCKER_IMAGE,
#     #     **env,
#     # )
#     # # @formatter:on
#     #
#     # with open(docker_file_repo, "r") as fr:
#     #     docker_file_str = fr.read()
#     #     context.log.debug(f"{docker_file_str = }")
#     #     docker_file_str.replace("FROM openjdk:18-ea-18-slim-bullseye", f"FROM openjdk:18-ea-18-slim-bullseye as {image_name}")
#     #     context.log.debug(f"{docker_file_str = }")
#     #     # fw.write(docker_file_str)
#
#     with open(docker_file, "w") as fw:
#         fw.write(docker_file_repo_str)
#
#     with open(docker_file, "r") as fr:
#         docker_file_content = fr.read()
#
#     yield Output(docker_file)
#
#     yield AssetMaterialization(
#         asset_key=context.asset_key,
#         metadata={
#             "__".join(context.asset_key.path): MetadataValue.path(docker_file),
#             docker_file.name: MetadataValue.md(f"```shell\n{docker_file_content}\n```"),
#             "env": MetadataValue.json(env),
#         },
#     )


# @asset(
#     **ASSET_HEADER,
#     ins={
#         "feature_in": AssetIn(
#             AssetKey([*ASSET_HEADER["key_prefix"], "feature_in"]),
#         ),
#         "CONFIG": AssetIn(
#             AssetKey([*ASSET_HEADER["key_prefix"], "CONFIG"]),
#         ),
#         "write_dockerfile_rqd": AssetIn(
#             AssetKey([*ASSET_HEADER["key_prefix"], "write_dockerfile_rqd"])
#         ),
#     },
#     retry_policy=build_docker_image_retry_policy,
# )
# def build_docker_image_rqd(
#     context: AssetExecutionContext,
#     feature_in: OpenStudioLandscapesFeatureIn,  # pylint: disable=redefined-outer-name
#     CONFIG: config.models.Config,  # pylint: disable=redefined-outer-name
#     write_dockerfile_rqd: pathlib.Path,  # pylint: disable=redefined-outer-name
# ) -> Generator[Output[Dict] | AssetMaterialization, None, None]:
#     """ """
#
#     env: Dict = CONFIG.env
#
#     docker_config_json: pathlib.Path = (
#         feature_in.openstudiolandscapes_base.docker_config_json
#     )
#
#     config_engine: ConfigEngine = CONFIG.config_engine
#
#     docker_config: DockerConfigModel = config_engine.openstudiolandscapes__docker_config
#
#     docker_image: Dict = feature_in.openstudiolandscapes_base.docker_image_base
#
#     #################################################
#
#     (
#         image_name,
#         image_prefixes,
#         tags,
#         build_base_parent_image_prefix,
#         build_base_parent_image_name,
#         build_base_parent_image_tags,
#     ) = get_image_metadata(
#         context=context,
#         docker_image=docker_image,
#         docker_config=docker_config,
#         env=env,
#     )
#
#     #################################################
#
#     image_data, logs = create_image(
#         context=context,
#         image_name=image_name,
#         image_prefixes=image_prefixes,
#         tags=tags,
#         docker_image=docker_image,
#         docker_config=docker_config,
#         docker_config_json=docker_config_json,
#         docker_file=write_dockerfile_rqd,
#     )
#
#     yield Output(image_data)
#
#     yield AssetMaterialization(
#         asset_key=context.asset_key,
#         metadata={
#             "__".join(context.asset_key.path): MetadataValue.json(image_data),
#             "env": MetadataValue.json(env),
#             "docker_image": MetadataValue.path(
#                 f"{image_data['image_prefixes']}{image_data['image_name']}:{image_data['image_tags'][0]}"
#             ),
#             "docker_cmd": MetadataValue.path(
#                 get_docker_run_cmd(
#                     context=context,
#                     image_data=image_data,
#                 )
#             ),
#             "logs": MetadataValue.json(logs),
#         },
#     )


@asset(
    **ASSET_HEADER,
    ins={
        "CONFIG": AssetIn(
            AssetKey([*ASSET_HEADER["key_prefix"], "CONFIG"]),
        ),
        "compose_networks": AssetIn(
            AssetKey([*ASSET_HEADER["key_prefix"], "compose_networks"]),
        ),
        # "build": AssetIn(
        #     AssetKey([*ASSET_HEADER["key_prefix"], "build_docker_image"]),
        # ),
        "clone_repository": AssetIn(
            AssetKey([*ASSET_HEADER["key_prefix"], "clone_repository"]),
        ),
        # "write_dockerfile_rqd": AssetIn(
        #     AssetKey([*ASSET_HEADER["key_prefix"], "write_dockerfile_rqd"]),
        # ),
        "prepare_volumes": AssetIn(
            AssetKey([*ASSET_HEADER["key_prefix"], "prepare_volumes"]),
        ),
        "compose_opencue_base": AssetIn(
            AssetKey([*ASSET_HEADER["key_prefix"], "compose_opencue_base"]),
        ),
        # "compose_cuebot": AssetIn(
        #     AssetKey([*ASSET_HEADER["key_prefix"], "compose_cuebot"]),
        # ),
    },
    description=textwrap.dedent("""
        Based on
        - [docker-compose.yml](https://github.com/AcademySoftwareFoundation/OpenCue/blob/master/docker-compose.yml)
        
        Reference:
        ```
          rqd:
            image: opencue/rqd
            environment:
              - PYTHONUNBUFFERED=1
              - CUEBOT_HOSTNAME=cuebot
            depends_on:
              cuebot:
                condition: service_healthy
            links:
              - cuebot
            ports:
              - "8444:8444"
            volumes:
              - /tmp/rqd/logs:/tmp/rqd/logs
              - /tmp/rqd/shots:/tmp/rqd/shots
        ```
        """),
)
def compose_rqd(
    context: AssetExecutionContext,
    CONFIG: config.models.Config,  # pylint: disable=redefined-outer-name
    compose_networks: Dict,  # pylint: disable=redefined-outer-name
    # build: Dict,  # pylint: disable=redefined-outer-name
    clone_repository: pathlib.Path,  # pylint: disable=redefined-outer-name
    # write_dockerfile_rqd: pathlib.Path,  # pylint: disable=redefined-outer-name
    prepare_volumes: Dict,  # pylint: disable=redefined-outer-name
    compose_opencue_base: Dict,  # pylint: disable=redefined-outer-name
    # compose_cuebot: Dict,  # pylint: disable=redefined-outer-name
) -> Generator[Output[Dict] | AssetMaterialization, None, None]:
    """ """

    env: Dict = CONFIG.env

    config_engine: ConfigEngine = CONFIG.config_engine

    network_dict = {}
    ports_dict = {}

    if "networks" in compose_networks:
        network_dict = {"networks": list(compose_networks.get("networks", {}).keys())}
        ports_dict = {
            "ports": [
                f"{CONFIG.OPENCUE_CUEBOT_GRPC_RQD_PORT_HOST}:{CONFIG.OPENCUE_CUEBOT_GRPC_RQD_PORT_CONTAINER}",
            ]
        }
    elif "network_mode" in compose_networks:
        network_dict = {"network_mode": compose_networks["network_mode"]}

    volumes_dict = {
        "volumes": [
            f"{prepare_volumes['logs']}:/tmp/rqd/logs:rw",
            f"{prepare_volumes['shots']}:/tmp/rqd/shots:rw",
        ]
    }

    # For portability, convert absolute volume paths to relative paths

    _volume_relative = []

    for v in volumes_dict["volumes"]:

        host, container = v.split(":", maxsplit=1)

        volume_dir_host_rel_path = get_relative_path_via_common_root(
            context=context,
            path_src=CONFIG.docker_compose_expanded,
            path_dst=pathlib.Path(host),
            path_common_root=pathlib.Path(env["DOT_LANDSCAPES"]),
        )

        _volume_relative.append(
            f"{volume_dir_host_rel_path.as_posix()}:{container}",
        )

    volumes_dict = {
        "volumes": list(
            {
                *_volume_relative,
                *config_engine.global_bind_volumes,
                *CONFIG.local_bind_volumes,
                "${HOME}/.opencue/sessions:${HOME}/.opencue/sessions",
                "/tmp/opencue:/tmp/opencue",
                "/var/folders:/var/folders:ro",
                #     volumes:
                #       - /tmp/rqd/logs:/tmp/rqd/logs
                #       - /tmp/rqd/shots:/tmp/rqd/shots
                #       # Mount session and temp dirs so rqd can access outline.yaml and user_dir
                #       # created by pycuerun on the host
                #       - ${HOME}/.opencue/sessions:${HOME}/.opencue/sessions
                #       - /tmp/opencue:/tmp/opencue
                #       - /var/folders:/var/folders:ro
            }
        )
    }

    service_name_cuebot = CONFIG.opencue_cuebot
    container_name_cuebot, host_name_cuebot = get_docker_compose_names(
        context=context,
        service_name=service_name_cuebot,
        landscape_id=env.get("LANDSCAPE", "default"),
        domain_lan=config_engine.openstudiolandscapes__domain_lan,
    )

    service_name = CONFIG.opencue_rqd
    container_name, host_name = get_docker_compose_names(
        context=context,
        service_name=service_name,
        landscape_id=env.get("LANDSCAPE", "default"),
        domain_lan=config_engine.openstudiolandscapes__domain_lan,
    )

    if CONFIG.OPENCUE_DEPLOY_RQD:

        compose_rqd_base = compose_opencue_base.get("services", {}).get("rqd", {})
        compose_rqd_base.pop("profiles", None)

        image_name = get_image_name(context=context)
        context.log.debug(f"{image_name = }")

        docker_config: DockerConfigModel = config_engine.openstudiolandscapes__docker_config

        image_prefixes = parse_docker_image_path(
            docker_config=docker_config,
            context=context,
        )
        context.log.debug(f"{image_prefixes = }")

        tags = [
            env.get("LANDSCAPE", str(time.time())),
        ]
        context.log.debug(f"{tags = }")

        image_data = {
            "image_name": image_name,
            "image_prefixes": image_prefixes,
            "image_tags": tags,
            "image_parent": {},
        }

        context_ = clone_repository.joinpath(compose_rqd_base["build"]["context"])
        d = {
            "build": {
                # Just prepend the full path to the cloned repo
                "context": context_.as_posix(),
                "dockerfile": context_.joinpath(compose_rqd_base["build"]["dockerfile"]).as_posix(),
            },
        }

        docker_dict = {
            "services": {
                service_name: {
                    # Todo
                    #  - [ ] Using the base compose dict as the starting
                    #        point and override with our values
                    #        This might be error prone so maybe
                    #        there is a better way.
                    **compose_rqd_base,
                    # "image": "%s%s:%s"
                    # % (
                    #     build["image_prefixes"],
                    #     build["image_name"],
                    #     build["image_tags"][0],
                    # ),
                    **d,
                    # name and tag the resulting image:
                    # - https://docs.docker.com/reference/compose-file/build/#tags
                    "image": f"{image_data['image_prefixes']}{image_data['image_name']}:{image_data['image_tags'][0]}",
                    "container_name": container_name,
                    "hostname": host_name,
                    "domainname": config_engine.openstudiolandscapes__domain_lan,
                    # "restart": DockerComposePolicies.RESTART_POLICY.ALWAYS.value,
                    "environment": {
                        "TZ": config_engine.tz,
                        "PYTHONUNBUFFERED": "1",
                        # Todo:
                        #  - [ ] use fqdn instead of just hostname?
                        #  - [ ] Better use container name so that we don't have to rely on external DNS
                        # "CUEBOT_HOSTNAME": host_name_cuebot,  # f"cuebot.{config_engine.openstudiolandscapes__domain_lan}",
                        "OPENRQD__GRPC__CUEBOT_ENDPOINTS": f"{container_name_cuebot}:{CONFIG.OPENCUE_CUEBOT_GRPC_CUE_PORT_CONTAINER}",
                        "OPENRQD__MACHINE__USE_IP_AS_HOSTNAME": False,
                        **config_engine.global_environment_variables,
                        **CONFIG.local_environment_variables,
                    },
                    "depends_on": {
                        service_name_cuebot: {
                            "condition": "service_healthy",
                        },
                    },
                    **copy.deepcopy(volumes_dict),
                    **copy.deepcopy(network_dict),
                    # "links": [
                    #     "cuebot",
                    # ],
                    **copy.deepcopy(ports_dict),
                },
            },
        }

    else:

        docker_dict = {}

    docker_yaml = yaml.dump(docker_dict)

    yield Output(docker_dict)

    yield AssetMaterialization(
        asset_key=context.asset_key,
        metadata={
            "__".join(context.asset_key.path): MetadataValue.json(docker_dict),
            "docker_yaml": MetadataValue.md(f"```yaml\n{docker_yaml}\n```"),
            "OPENCUE_DEPLOY_RQD": MetadataValue.bool(CONFIG.OPENCUE_DEPLOY_RQD),
        },
    )


@asset(
    **ASSET_HEADER,
    ins={
        "CONFIG": AssetIn(
            AssetKey([*ASSET_HEADER["key_prefix"], "CONFIG"]),
        ),
        "clone_repository": AssetIn(
            AssetKey([*ASSET_HEADER["key_prefix"], "clone_repository"]),
        ),
        "compose_networks": AssetIn(
            AssetKey([*ASSET_HEADER["key_prefix"], "compose_networks"]),
        ),
        "compose_opencue_base": AssetIn(
            AssetKey([*ASSET_HEADER["key_prefix"], "compose_opencue_base"]),
        ),
    },
    description=textwrap.dedent("""
        Official Resources:
        - [Deploying OpenCue REST Gateway](https://docs.opencue.io/docs/getting-started/deploying-rest-gateway/)
          - [Docker Compose Configuration (Separate File)](https://docs.opencue.io/docs/getting-started/deploying-rest-gateway/#docker-compose-configuration-separate-file)
        - [OpenCue REST API Reference](https://docs.opencue.io/docs/reference/rest-api-reference/)
        - [Using the REST API](https://docs.opencue.io/docs/user-guides/using-rest-api/)
        - [REST API Tutorial](https://docs.opencue.io/docs/tutorials/rest-api-tutorial/)
        """),
)
def compose_rest_gateway(
    context: AssetExecutionContext,
    CONFIG: config.models.Config,  # pylint: disable=redefined-outer-name
    clone_repository: pathlib.Path,  # pylint: disable=redefined-outer-name
    compose_networks: Dict,  # pylint: disable=redefined-outer-name
    compose_opencue_base: Dict,  # pylint: disable=redefined-outer-name
) -> Generator[Output[Dict] | AssetMaterialization, None, None]:
    """ """

    env: Dict = CONFIG.env

    config_engine: ConfigEngine = CONFIG.config_engine

    network_dict = {}
    ports_dict_rest_gateway = {}

    if "networks" in compose_networks:
        network_dict = {"networks": list(compose_networks.get("networks", {}).keys())}
        ports_dict_rest_gateway = {
            "ports": [
                f"{CONFIG.OPENCUE_REST_GATEWAY_PORT_HOST}:{CONFIG.OPENCUE_REST_GATEWAY_PORT_CONTAINER}",
            ]
        }
    elif "network_mode" in compose_networks:
        network_dict = {"network_mode": compose_networks["network_mode"]}

    volumes_dict = {"volumes": []}

    docker_compose_git_repository = pathlib.Path(
        clone_repository.joinpath("docker-compose.yml")
    )

    # For portability, convert absolute volume paths to relative paths

    _volume_relative_rest_gateway = []

    for v in volumes_dict["volumes"]:

        host, container = v.split(":", maxsplit=1)

        volume_dir_host_rel_path = get_relative_path_via_common_root(
            context=context,
            path_src=docker_compose_git_repository,
            path_dst=pathlib.Path(host),
            path_common_root=pathlib.Path(env["DOT_LANDSCAPES"]),
        )

        _volume_relative_rest_gateway.append(
            f"{volume_dir_host_rel_path.as_posix()}:{container}",
        )

    volumes_dict = {
        "volumes": list(
            {
                *_volume_relative_rest_gateway,
                *config_engine.global_bind_volumes,
                *CONFIG.local_bind_volumes,
            }
        )
    }

    # container_prefix = "opencue"

    # service_name_rqd = "rqd"
    # container_name_rqd, host_name_rqd = get_docker_compose_names(
    #     context=context,
    #     service_name=f"{container_prefix}-{service_name_rqd}",
    #     landscape_id=env.get("LANDSCAPE", "default"),
    #     domain_lan=config_engine.openstudiolandscapes__domain_lan,
    # )

    service_name_cuebot = CONFIG.opencue_cuebot
    container_name_cuebot, _ = get_docker_compose_names(
        context=context,
        service_name=service_name_cuebot,
        landscape_id=env.get("LANDSCAPE", "default"),
        domain_lan=config_engine.openstudiolandscapes__domain_lan,
    )

    service_name_db = CONFIG.opencue_db
    # container_name_db, _ = get_docker_compose_names(
    #     context=context,
    #     service_name=f"{container_prefix}-{service_name_db}",
    #     landscape_id=env.get("LANDSCAPE", "default"),
    #     domain_lan=config_engine.openstudiolandscapes__domain_lan,
    # )

    service_name_rest_gateway = CONFIG.opencue_rest_gateway
    container_name_rest_gateway, host_name_rest_gateway = get_docker_compose_names(
        context=context,
        service_name=service_name_rest_gateway,
        landscape_id=env.get("LANDSCAPE", "default"),
        domain_lan=config_engine.openstudiolandscapes__domain_lan,
    )

    compose_rest_gateway_base = compose_opencue_base.get("services", {}).get("rest-gateway")
    compose_rest_gateway_base.pop("profiles", None)

    image_name = get_image_name(context=context)
    context.log.debug(f"{image_name = }")

    docker_config: DockerConfigModel = config_engine.openstudiolandscapes__docker_config

    image_prefixes = parse_docker_image_path(
        docker_config=docker_config,
        context=context,
    )
    context.log.debug(f"{image_prefixes = }")

    tags = [
        env.get("LANDSCAPE", str(time.time())),
    ]
    context.log.debug(f"{tags = }")

    image_data = {
        "image_name": image_name,
        "image_prefixes": image_prefixes,
        "image_tags": tags,
        "image_parent": {},
    }

    context_ = clone_repository.joinpath(compose_rest_gateway_base["build"]["context"])
    d = {
        "build": {
            # Just prepend the full path to the cloned repo
            "context": context_.as_posix(),
            "dockerfile": context_.joinpath(compose_rest_gateway_base["build"]["dockerfile"]).as_posix(),
        },
    }

    docker_dict = {
        "services": {
            # https://docs.opencue.io/docs/getting-started/deploying-rest-gateway/#docker-compose-configuration-separate-file
            service_name_rest_gateway: {
                # Todo
                #  - [ ] Using the base compose dict as the starting
                #        point and override with our values
                #        This might be error prone so maybe
                #        there is a better way.
                **compose_rest_gateway_base,
                "container_name": container_name_rest_gateway,
                "hostname": host_name_rest_gateway,
                **d,
                # name and tag the resulting image:
                # - https://docs.docker.com/reference/compose-file/build/#tags
                "image": f"{image_data['image_prefixes']}{image_data['image_name']}:{image_data['image_tags'][0]}",
                # "build": {
                #     # https://docs.docker.com/reference/compose-file/build/#context
                #     "context": clone_repository.as_posix(),
                #     # https://docs.docker.com/reference/compose-file/build/#dockerfile
                #     "dockerfile": clone_repository.joinpath(
                #         "rest_gateway",
                #         "Dockerfile",
                #     ).as_posix(),
                # },
                "domainname": config_engine.openstudiolandscapes__domain_lan,
                "environment": {
                    "TZ": config_engine.tz,
                    # https://docs.opencue.io/docs/getting-started/deploying-rest-gateway/#configuration-options
                    "CUEBOT_ENDPOINT": f"{container_name_cuebot}:{CONFIG.OPENCUE_CUEBOT_GRPC_CUE_PORT_CONTAINER}",  # port might be implicit here
                    "REST_PORT": CONFIG.OPENCUE_REST_GATEWAY_PORT_CONTAINER,
                    "JWT_SECRET": CONFIG.OPENCUE_CUEWEB_JWT_SECRET,
                    "LOG_LEVEL": "debug",
                    "CORS_ALLOWED_ORIGINS": "*",
                    **config_engine.global_environment_variables,
                    **CONFIG.local_environment_variables,
                },
                "depends_on": {
                    # service_name_db: {
                    #     "condition": "service_started",
                    # },
                    service_name_cuebot: {
                        "condition": "service_healthy",
                    },
                },
                # "restart": DockerComposePolicies.RESTART_POLICY.UNLESS_STOPPED.value,
                # Todo
                #  - [ ] healthcheck
                #        maybe based on https://docs.opencue.io/docs/getting-started/deploying-rest-gateway/#step-4-verify-installation
                # "healthcheck": {
                #     "test": [],
                #     "interval": "30s",
                #     "timeout": "10s",
                #     "retries": "3",
                # },
                **copy.deepcopy(volumes_dict),
                **copy.deepcopy(network_dict),
                **copy.deepcopy(ports_dict_rest_gateway),
            },
        },
    }

    docker_yaml = yaml.dump(docker_dict)

    yield Output(docker_dict)

    yield AssetMaterialization(
        asset_key=context.asset_key,
        metadata={
            "docker_yaml": MetadataValue.md(f"```yaml\n{docker_yaml}\n```"),
        },
    )


@asset(
    **ASSET_HEADER,
    ins={
        "CONFIG": AssetIn(
            AssetKey([*ASSET_HEADER["key_prefix"], "CONFIG"]),
        ),
        "clone_repository": AssetIn(
            AssetKey([*ASSET_HEADER["key_prefix"], "clone_repository"]),
        ),
        "compose_networks": AssetIn(
            AssetKey([*ASSET_HEADER["key_prefix"], "compose_networks"]),
        ),
        "compose_opencue_base": AssetIn(
            AssetKey([*ASSET_HEADER["key_prefix"], "compose_opencue_base"]),
        ),
    },
    description=textwrap.dedent("""
        Official Resources:
        - [CueWeb User Guide](https://docs.opencue.io/docs/user-guides/cueweb-user-guide/)
        - [CueWeb Tutorial](https://docs.opencue.io/docs/tutorials/cueweb-tutorial/)
        - [CueWeb Development Guide](https://docs.opencue.io/docs/developer-guide/cueweb-development/)
        - [Deploying CueWeb](https://docs.opencue.io/docs/getting-started/deploying-cueweb/)
        - [CueWeb Reference](https://docs.opencue.io/docs/reference/cueweb/)
        """),
)
def compose_cueweb(
    context: AssetExecutionContext,
    CONFIG: config.models.Config,  # pylint: disable=redefined-outer-name
    clone_repository: pathlib.Path,  # pylint: disable=redefined-outer-name
    compose_networks: Dict,  # pylint: disable=redefined-outer-name
    compose_opencue_base: Dict,  # pylint: disable=redefined-outer-name
) -> Generator[Output[Dict] | AssetMaterialization, None, None]:
    """ """

    env: Dict = CONFIG.env

    config_engine: ConfigEngine = CONFIG.config_engine

    network_dict = {}
    ports_dict_cueweb = {}

    if "networks" in compose_networks:
        network_dict = {"networks": list(compose_networks.get("networks", {}).keys())}
        ports_dict_cueweb = {
            "ports": [
                f"{CONFIG.OPENCUE_CUEWEB_PORT_HOST}:{CONFIG.OPENCUE_CUEWEB_PORT_CONTAINER}",
            ]
        }
    elif "network_mode" in compose_networks:
        network_dict = {"network_mode": compose_networks["network_mode"]}

    docker_compose_git_repository = pathlib.Path(
        clone_repository.joinpath("docker-compose.yml")
    )

    volumes_cueweb = [
        # f"{scheduler_yaml.as_posix()}:/etc/cue-scheduler/scheduler.yaml:ro",
    ]

    # For portability, convert absolute volume paths to relative paths

    _volume_relative_cueweb = []

    for v in volumes_cueweb:

        host, container = v.split(":", maxsplit=1)

        volume_dir_host_rel_path = get_relative_path_via_common_root(
            context=context,
            path_src=docker_compose_git_repository,  # Probably because the root docker-compose is the one in the Git repo
            path_dst=pathlib.Path(host),
            path_common_root=pathlib.Path(env["DOT_LANDSCAPES"]),
        )

        _volume_relative_cueweb.append(
            f"{volume_dir_host_rel_path.as_posix()}:{container}",
        )

    # Todo
    #  - [ ] No volumes needed?
    # volumes_dict_cueweb = {
    #     "volumes": list(
    #         {
    #             *_volume_relative,
    #             *config_engine.global_bind_volumes,
    #             *CONFIG.local_bind_volumes,
    #         }
    #     ),
    # }

    # container_prefix = "opencue"

    # service_name_rqd = "rqd"
    # container_name_rqd, host_name_rqd = get_docker_compose_names(
    #     context=context,
    #     service_name=f"{container_prefix}-{service_name_rqd}",
    #     landscape_id=env.get("LANDSCAPE", "default"),
    #     domain_lan=config_engine.openstudiolandscapes__domain_lan,
    # )

    # service_name_cuebot = "cuebot"
    # container_name_cuebot, host_name_cuebot = get_docker_compose_names(
    #     context=context,
    #     service_name=f"{container_prefix}-{service_name_cuebot}",
    #     landscape_id=env.get("LANDSCAPE", "default"),
    #     domain_lan=config_engine.openstudiolandscapes__domain_lan,
    # )

    service_name_db = CONFIG.opencue_db
    container_name_db, _ = get_docker_compose_names(
        context=context,
        service_name=service_name_db,
        landscape_id=env.get("LANDSCAPE", "default"),
        domain_lan=config_engine.openstudiolandscapes__domain_lan,
    )

    service_name_rest_gateway = CONFIG.opencue_rest_gateway
    container_name_rest_gateway, host_name_rest_gateway = get_docker_compose_names(
        context=context,
        service_name=service_name_rest_gateway,
        landscape_id=env.get("LANDSCAPE", "default"),
        domain_lan=config_engine.openstudiolandscapes__domain_lan,
    )

    service_name_cueweb = CONFIG.opencue_cueweb
    container_name_cueweb, host_name_cueweb = get_docker_compose_names(
        context=context,
        service_name=service_name_cueweb,
        landscape_id=env.get("LANDSCAPE", "default"),
        domain_lan=config_engine.openstudiolandscapes__domain_lan,
    )

    compose_cueweb_base = compose_opencue_base.get("services", {}).get("cueweb", {})
    compose_cueweb_base.pop("profiles", None)

    context.log.info(clone_repository)
    context.log.info(compose_cueweb_base)
    context.log.info(clone_repository.joinpath(compose_cueweb_base["build"]["additional_contexts"]["project_root"]))

    """
    #96 103.5 Route (app)                                        Size  First Load JS
    #96 103.5   /                                           1.64 kB         269 kB
    #96 103.5   /_not-found                                 1.17 kB         224 kB
    #96 103.5   /admin/audit                                4.51 kB         234 kB
    #96 103.5   /allocations                                2.85 kB         327 kB
    #96 103.5   /api/admin/audit                              337 B         223 kB
    #96 103.5   /api/allocation/getall                        698 B         230 kB
    #96 103.5   /api/auth/[...nextauth]                       336 B         223 kB
    #96 103.5   /api/comment/action/delete                    696 B         230 kB
    #96 103.5   /api/comment/action/save                      697 B         230 kB
    #96 103.5   /api/countlines                               697 B         230 kB
    #96 103.5   /api/department/getdepartmentnames            695 B         230 kB
    #96 103.5   /api/department/gettasks                      697 B         230 kB
    #96 103.5   /api/facility/health                          696 B         230 kB
    #96 103.5   /api/filter/getactions                        697 B         230 kB
    #96 103.5   /api/filter/getmatchers                       696 B         230 kB
    #96 103.5   /api/filter/mutate                            697 B         230 kB
    #96 103.5   /api/frame/action/createdependonframe         696 B         230 kB
    #96 103.5   /api/frame/action/createdependonjob           698 B         230 kB
    #96 103.5   /api/frame/action/createdependonlayer         696 B         230 kB
    #96 103.5   /api/frame/action/dropdepends                 698 B         230 kB
    #96 103.5   /api/frame/action/eat                         697 B         230 kB
    #96 103.5   /api/frame/action/getdepends                  698 B         230 kB
    #96 103.5   /api/frame/action/kill                        696 B         230 kB
    #96 103.5   /api/frame/action/markaswaiting               697 B         230 kB
    #96 103.5   /api/frame/action/retry                       698 B         230 kB
    #96 103.5   /api/frame/getframe                           696 B         230 kB
    #96 103.5   /api/frame/preview                            337 B         223 kB
    #96 103.5   /api/getlines                                 698 B         230 kB
    #96 103.5   /api/getlog                                   336 B         223 kB
    #96 103.5   /api/getlogversions                           336 B         223 kB
    #96 103.5   /api/group/action/createsubgroup              696 B         230 kB
    #96 103.5   /api/group/action/delete                      696 B         230 kB
    #96 103.5   /api/group/action/reparentgroups              697 B         230 kB
    #96 103.5   /api/group/action/reparentjobs                696 B         230 kB
    #96 103.5   /api/group/action/update                      697 B         230 kB
    #96 103.5   /api/group/getjobs                            697 B         230 kB
    #96 103.5   /api/health                                   696 B         230 kB
    #96 103.5   /api/host/action/addcomment                   695 B         230 kB
    #96 103.5   /api/host/action/addtags                      697 B         230 kB
    #96 103.5   /api/host/action/delete                       697 B         230 kB
    #96 103.5   /api/host/action/lock                         695 B         230 kB
    #96 103.5   /api/host/action/reboot                       695 B         230 kB
    #96 103.5   /api/host/action/rebootwhenidle               698 B         230 kB
    #96 103.5   /api/host/action/redirecttojob                695 B         230 kB
    #96 103.5   /api/host/action/removetags                   699 B         230 kB
    #96 103.5   /api/host/action/renametag                    696 B         230 kB
    #96 103.5   /api/host/action/setallocation                697 B         230 kB
    #96 103.5   /api/host/action/sethardwarestate             698 B         230 kB
    #96 103.5   /api/host/action/takeownership                697 B         230 kB
    #96 103.5   /api/host/action/unlock                       694 B         230 kB
    #96 103.5   /api/host/findhost                            696 B         230 kB
    #96 103.5   /api/host/getcomments                         697 B         230 kB
    #96 103.5   /api/host/gethosts                            698 B         230 kB
    #96 103.5   /api/host/getprocs                            696 B         230 kB
    #96 103.5   /api/increment                                699 B         230 kB
    #96 103.5   /api/job/action/addcomment                    697 B         230 kB
    #96 103.5   /api/job/action/addrenderpart                 696 B         230 kB
    #96 103.5   /api/job/action/addsubscriber                 697 B         230 kB
    #96 103.5   /api/job/action/createdependonframe           695 B         230 kB
    #96 103.5   /api/job/action/createdependonjob             695 B         230 kB
    #96 103.5   /api/job/action/createdependonlayer           696 B         230 kB
    #96 103.5   /api/job/action/dropdepends                   696 B         230 kB
    #96 103.5   /api/job/action/eatframes                     696 B         230 kB
    #96 103.5   /api/job/action/getdepends                    697 B         230 kB
    #96 103.5   /api/job/action/getwhatdependsonthis          696 B         230 kB
    #96 103.5   /api/job/action/kill                          697 B         230 kB
    #96 103.5   /api/job/action/killframes                    696 B         230 kB
    #96 103.5   /api/job/action/markdoneframes                695 B         230 kB
    #96 103.5   /api/job/action/pause                         697 B         230 kB
    #96 103.5   /api/job/action/reorderframes                 697 B         230 kB
    #96 103.5   /api/job/action/retryframes                   697 B         230 kB
    #96 103.5   /api/job/action/setautoeat                    697 B         230 kB
    #96 103.5   /api/job/action/setmaxcores                   696 B         230 kB
    #96 103.5   /api/job/action/setmaxgpus                    697 B         230 kB
    #96 103.5   /api/job/action/setmaxretries                 697 B         230 kB
    #96 103.5   /api/job/action/setmincores                   697 B         230 kB
    #96 103.5   /api/job/action/setmingpus                    697 B         230 kB
    #96 103.5   /api/job/action/setpriority                   696 B         230 kB
    #96 103.5   /api/job/action/staggerframes                 696 B         230 kB
    #96 103.5   /api/job/action/unpause                       697 B         230 kB
    #96 103.5   /api/job/getcomments                          696 B         230 kB
    #96 103.5   /api/job/getframes                            698 B         230 kB
    #96 103.5   /api/job/getjob                               696 B         230 kB
    #96 103.5   /api/job/getjobs                              696 B         230 kB
    #96 103.5   /api/job/getlayers                            698 B         230 kB
    #96 103.5   /api/job/submit                               697 B         230 kB
    #96 103.5   /api/layer/action/createdependonframe         697 B         230 kB
    #96 103.5   /api/layer/action/createdependonjob           698 B         230 kB
    #96 103.5   /api/layer/action/createdependonlayer         697 B         230 kB
    #96 103.5   /api/layer/action/createframebyframedepend    696 B         230 kB
    #96 103.5   /api/layer/action/eatframes                   695 B         230 kB
    #96 103.5   /api/layer/action/getdepends                  694 B         230 kB
    #96 103.5   /api/layer/action/getoutputpaths              697 B         230 kB
    #96 103.5   /api/layer/action/kill                        696 B         230 kB
    #96 103.5   /api/layer/action/markdone                    697 B         230 kB
    #96 103.5   /api/layer/action/reorderframes               695 B         230 kB
    #96 103.5   /api/layer/action/retryframes                 697 B         230 kB
    #96 103.5   /api/layer/action/setmincores                 696 B         230 kB
    #96 103.5   /api/layer/action/setmingpumemory             698 B         230 kB
    #96 103.5   /api/layer/action/setminmemory                697 B         230 kB
    #96 103.5   /api/layer/action/settags                     697 B         230 kB
    #96 103.5   /api/layer/action/setthreadable               697 B         230 kB
    #96 103.5   /api/layer/action/staggerframes               696 B         230 kB
    #96 103.5   /api/limit/action/create                      697 B         230 kB
    #96 103.5   /api/limit/action/delete                      698 B         230 kB
    #96 103.5   /api/limit/action/rename                      696 B         230 kB
    #96 103.5   /api/limit/action/setmaxvalue                 696 B         230 kB
    #96 103.5   /api/limit/getall                             697 B         230 kB
    #96 103.5   /api/metrics                                  695 B         230 kB
    #96 103.5   /api/proc/action/kill                         698 B         230 kB
    #96 103.5   /api/proc/action/unbook                       697 B         230 kB
    #96 103.5   /api/proc/action/unbookone                    698 B         230 kB
    #96 103.5   /api/proc/getprocs                            696 B         230 kB
    #96 103.5   /api/redirect/search                          697 B         230 kB
    #96 103.5   /api/service/create                           697 B         230 kB
    #96 103.5   /api/service/delete                           698 B         230 kB
    #96 103.5   /api/service/getdefaultservices               697 B         230 kB
    #96 103.5   /api/service/update                           696 B         230 kB
    #96 103.5   /api/serviceoverride/mutate                   697 B         230 kB
    #96 103.5   /api/show/action/createsubscription           697 B         230 kB
    #96 103.5   /api/show/action/enablebooking                698 B         230 kB
    #96 103.5   /api/show/action/enabledispatching            696 B         230 kB
    #96 103.5   /api/show/action/setcommentemail              696 B         230 kB
    #96 103.5   /api/show/action/setdefaultmaxcores           697 B         230 kB
    #96 103.5   /api/show/action/setdefaultmincores           696 B         230 kB
    #96 103.5   /api/show/createshow                          697 B         230 kB
    #96 103.5   /api/show/findshow                            696 B         230 kB
    #96 103.5   /api/show/getactiveshows                      696 B         230 kB
    #96 103.5   /api/show/getdepartments                      696 B         230 kB
    #96 103.5   /api/show/getfilters                          698 B         230 kB
    #96 103.5   /api/show/getgroups                           697 B         230 kB
    #96 103.5   /api/show/getserviceoverrides                 696 B         230 kB
    #96 103.5   /api/show/getshows                            697 B         230 kB
    #96 103.5   /api/show/getsubscriptions                    696 B         230 kB
    #96 103.5   /api/stuck-frames                             699 B         230 kB
    #96 103.5   /api/stuck-frames/lastline                    338 B         223 kB
    #96 103.5   /api/subscription/delete                      696 B         230 kB
    #96 103.5   /api/subscription/setburst                    696 B         230 kB
    #96 103.5   /api/subscription/setsize                     696 B         230 kB
    #96 103.5   /api/task/mutate                              694 B         230 kB
    #96 103.5   /api/track                                    338 B         223 kB
    #96 103.5   /cuesubmit                                  39.9 kB         305 kB
    #96 103.5   /dashboard                                  73.8 kB         333 kB
    #96 103.5   /frames/[frame-name]                          67 kB         410 kB
    #96 103.5   /hosts                                      9.66 kB         393 kB
    #96 103.5   /hosts/[host-name]                          5.16 kB         359 kB
    #96 103.5   /icon.png                                       0 B            0 B
    #96 103.5   /jobs/[job-name]                            10.9 kB         366 kB
    #96 103.5   /jobs/[job-name]/comments                   7.75 kB         321 kB
    #96 103.5   /limits                                      5.3 kB         325 kB
    #96 103.5   /login                                      3.37 kB         248 kB
    #96 103.5   /login/ldap                                 2.15 kB         247 kB
    #96 103.5   /monitor-cue                                32.7 kB         391 kB
    #96 103.5   /plugins                                    11.1 kB         310 kB
    #96 103.5   /plugins/[plugin-name]                      10.4 kB         310 kB
    #96 103.5     /plugins/hello
    #96 103.5     /plugins/cue-progress-bar
    #96 103.5   /redirect                                   11.9 kB         286 kB
    #96 103.5   /services                                   10.5 kB         267 kB
    #96 103.5   /settings/facilities                        3.29 kB         233 kB
    #96 103.5   /shows                                      4.14 kB         337 kB
    #96 103.5   /shows/[showName]                           40.9 kB         311 kB
    #96 103.5   /split                                      5.23 kB         263 kB
    #96 103.5   /stuck-frames                               13.1 kB         279 kB
    #96 103.5   /subscription-graphs                        9.82 kB         286 kB
    #96 103.5   /subscriptions                              4.07 kB         336 kB
    #96 103.5   /unauthorized                               3.45 kB         248 kB
    #96 103.5 + First Load JS shared by all                    223 kB
    #96 103.5    chunks/4969-3bc2e81f6c9abd67.js              119 kB
    #96 103.5    chunks/4bd1b696-1c51cbc71cf5bae2.js         54.4 kB
    #96 103.5    chunks/52774a7f-5e6f1d4767aba7ea.js           39 kB
    #96 103.5    other shared chunks (total)                 10.3 kB
    """

    image_name = get_image_name(context=context)
    context.log.debug(f"{image_name = }")

    docker_config: DockerConfigModel = config_engine.openstudiolandscapes__docker_config

    image_prefixes = parse_docker_image_path(
        docker_config=docker_config,
        context=context,
    )
    context.log.debug(f"{image_prefixes = }")

    tags = [
        env.get("LANDSCAPE", str(time.time())),
    ]
    context.log.debug(f"{tags = }")

    image_data = {
        "image_name": image_name,
        "image_prefixes": image_prefixes,
        "image_tags": tags,
        "image_parent": {},
    }

    context_ = clone_repository.joinpath(compose_cueweb_base["build"]["context"])
    d = {
        "build": {
            "additional_contexts": {
                "project_root": clone_repository.joinpath(compose_cueweb_base["build"]["additional_contexts"]["project_root"]).as_posix(),
            },
            # Just prepend the full path to the cloned repo
            "context": context_.as_posix(),
            "dockerfile": context_.joinpath(compose_cueweb_base["build"]["dockerfile"]).as_posix(),
            "args": {
                # https://github.com/AcademySoftwareFoundation/OpenCue/issues/2133
                "NEXT_PUBLIC_AUTH_PROVIDER": "",
                # has to be specified at build time?
                # - yes
                "NEXT_PUBLIC_OPENCUE_ENDPOINT": f"http://{container_name_rest_gateway}:{CONFIG.OPENCUE_REST_GATEWAY_PORT_CONTAINER}",
                # Empty = client builds same-origin relative URLs, so the app
                # works from any host the user reaches it on (localhost on the
                # dev environment, the LAN IP from another device, etc.). Set to an
                # absolute URL only if the API is on a different origin than the UI.
                # "NEXT_PUBLIC_URL": f"http://{host_name_cueweb}:{CONFIG.OPENCUE_CUEWEB_PORT_HOST}",
                # NEXT_PUBLIC_PREVIEW_URL
                # NEXTAUTH_URL
                # "NEXT_JWT_SECRET": CONFIG.OPENCUE_CUEWEB_JWT_SECRET,
                # "NEXTAUTH_SECRET": CONFIG.OPENCUE_CUEWEB_JWT_SECRET,
                "NEXT_JWT_SECRET": "",
                "NEXTAUTH_SECRET": "",
            },
        },
    }

    # Convert list to dict
    args_ = {}
    # This comes a list:
    for i in compose_cueweb_base.get("build", {}).get("args", {}):
        k, v = i.split("=", maxsplit=1)
        args_[k] = v

    docker_dict = {
        "services": {
            # https://docs.opencue.io/docs/getting-started/deploying-rest-gateway/#docker-compose-configuration-separate-file
            service_name_cueweb: {
                # Todo
                #  - [ ] Using the base compose dict as the starting
                #        point and override with our values
                #        This might be error prone so maybe
                #        there is a better way.
                **compose_cueweb_base,
                "build": {
                    **compose_cueweb_base.get("build", {}),
                    "additional_contexts": d["build"]["additional_contexts"],
                    "context": d["build"]["context"],
                    "dockerfile": d["build"]["dockerfile"],
                    "args": {
                        ** args_,
                        ** d["build"]["args"],
                    },
                    # "additional_contexts": {
                    #     **compose_cueweb_base.get("additional_contexts", {}),
                    # },
                #     **compose_cueweb_base.get("build", {}),
                #     **d.get("build", {}).get("context", {}),
                #     **d.get("build", {}).get("dockerfile", {}),
                #     **d.get("build", {}).get("args", {}),
                },
                # name and tag the resulting image:
                # - https://docs.docker.com/reference/compose-file/build/#tags
                "image": f"{image_data['image_prefixes']}{image_data['image_name']}:{image_data['image_tags'][0]}",
                "container_name": container_name_cueweb,
                "hostname": host_name_cueweb,
                # "build": {
                #     # https://docs.docker.com/reference/compose-file/build/#context
                #     "context": clone_repository.joinpath(
                #         "cueweb",
                #     ).as_posix(),
                #     # https://docs.docker.com/reference/compose-file/build/#dockerfile
                #     "dockerfile": clone_repository.joinpath(
                #         "cueweb",
                #         "Dockerfile",
                #     ).as_posix(),
                #     "args": {
                #         # https://github.com/AcademySoftwareFoundation/OpenCue/issues/2133
                #         "NEXT_PUBLIC_AUTH_PROVIDER": "",
                #         "NEXT_PUBLIC_OPENCUE_ENDPOINT": f"http://{host_name_rest_gateway}:{CONFIG.OPENCUE_REST_GATEWAY_PORT_CONTAINER}",
                #         "NEXT_PUBLIC_URL": f"http://{host_name_cueweb}:{CONFIG.OPENCUE_CUEWEB_PORT_HOST}",
                #     },
                # },
                "domainname": config_engine.openstudiolandscapes__domain_lan,
                "environment": {
                    "TZ": config_engine.tz,
                    # https://docs.opencue.io/docs/getting-started/deploying-cueweb/#environment-configuration
                    # "NEXT_PUBLIC_OPENCUE_ENDPOINT": f"http://{host_name_rest_gateway}:{CONFIG.OPENCUE_REST_GATEWAY_PORT_CONTAINER}",
                    # "NEXT_PUBLIC_URL": f"http://{host_name_cueweb}:{CONFIG.OPENCUE_CUEWEB_PORT_HOST}",
                    # "NEXT_PUBLIC_OPENCUE_ENDPOINT": f"http://{container_name_rest_gateway}:{CONFIG.OPENCUE_REST_GATEWAY_PORT_CONTAINER}",
                    "NEXT_JWT_SECRET": CONFIG.OPENCUE_CUEWEB_JWT_SECRET,
                    "NEXTAUTH_SECRET": CONFIG.OPENCUE_CUEWEB_JWT_SECRET,
                    **CONFIG.OPENCUE_CUEWEB_ADDITIONAL_ENV,
                    **config_engine.global_environment_variables,
                    **CONFIG.local_environment_variables,
                },
                # "restart": DockerComposePolicies.RESTART_POLICY.ALWAYS.value,
                "volumes": [
                    *_volume_relative_cueweb,
                ],
                "depends_on": {
                    service_name_rest_gateway: {
                        "condition": "service_healthy",
                    },
                },
                # "healthcheck": {
                #     # Todo
                #     #  - [ ] Have bug fixed:
                #     #        https://github.com/AcademySoftwareFoundation/OpenCue/issues/2126
                #     # "test": [
                #     #     "CMD",
                #     #     "curl",
                #     #     "-f",
                #     #     f"http://localhost:{CONFIG.OPENCUE_CUEWEB_PORT_CONTAINER}/api/health"
                #     # ],
                #     "interval": "30s",
                #     "timeout": "10s",
                #     "retries": "3",
                # },
                "command": ["npm", "run", "start", "--verbose"],
                **copy.deepcopy(network_dict),
                **copy.deepcopy(ports_dict_cueweb),
            },
        },
    }

    docker_yaml = yaml.dump(docker_dict)

    yield Output(docker_dict)

    yield AssetMaterialization(
        asset_key=context.asset_key,
        metadata={
            "docker_yaml": MetadataValue.md(f"```yaml\n{docker_yaml}\n```"),
        },
    )


# @asset(
#     **ASSET_HEADER,
#     ins={
#         "CONFIG": AssetIn(
#             AssetKey([*ASSET_HEADER["key_prefix"], "CONFIG"]),
#         ),
#         "clone_repository": AssetIn(
#             AssetKey([*ASSET_HEADER["key_prefix"], "clone_repository"]),
#         ),
#     },
#     description=textwrap.dedent("""
#         Test requires `JWT_SECRET` set to default `default-secret-key`.
#
#         Bug report pending:
#         - [`test_rest_gateway_docker_compose.sh` with hard coded JWT secret: `Token validation error: token signature is invalid: signature is invalid`](https://github.com/AcademySoftwareFoundation/OpenCue/issues/2127)
#
#         If `JWT_SECRET` has a non default value, this manual test
#         can help:
#
#         ```shell
#         # Create a simple test token using openssl (less secure but works for testing)
#         # Note: This creates a basic token structure, may not work with all JWT implementations
#         HEADER=$(echo -n '{"alg":"HS256","typ":"JWT"}' | base64 | tr -d '=' | tr '/+' '_-' | tr -d '\\n')
#         PAYLOAD=$(echo -n '{"user":"test","exp":'$(date -d '+1 hour' +%s)'}' | base64 | tr -d '=' | tr '/+' '_-' | tr -d '\\n')
#         SIGNATURE=$(echo -n "${HEADER}.${PAYLOAD}" | openssl dgst -sha256 -hmac "$JWT_SECRET" -binary | base64 | tr -d '=' | tr '/+' '_-' | tr -d '\\n')
#         export JWT_TOKEN="${HEADER}.${PAYLOAD}.${SIGNATURE}"
#
#         # Test authenticated endpoint
#         curl -H "Authorization: Bearer $JWT_TOKEN" \\
#              -H "Content-Type: application/json" \\
#              -X POST "http://localhost:8448/show.ShowInterface/GetShows" \\
#              -d '{}'
#         ```
#         """),
# )
# def opencue_test_suite__rest_gateway_docker_compose(
#     context: AssetExecutionContext,
#     CONFIG: config.models.Config,  # pylint: disable=redefined-outer-name
#     clone_repository: pathlib.Path,  # pylint: disable=redefined-outer-name
# ) -> Generator[Output[subprocess.CompletedProcess] | AssetMaterialization, None, None]:
#
#     env: Dict = CONFIG.env
#
#     cmd = (
#         f"pwd "
#         "&& echo ${JWT_SECRET} "
#         "&& export JWT_SECRET "
#         f"&& . .venv/bin/activate "
#         f"&& cd {clone_repository.as_posix()}/rest_gateway "
#         f"&& {shutil.which('bash')} test_rest_gateway_docker_compose.sh"
#     )
#
#     proc: subprocess.CompletedProcess = subprocess.run(
#         # [
#         #     shutil.which("bash"),
#         #     "test_rest_gateway_docker_compose.sh",
#         # ],
#         cmd,
#         shell=True,
#         # cwd=clone_repository.joinpath(
#         #     "rest_gateway",
#         # ),
#         env={
#             **env,
#             "JWT_SECRET": CONFIG.OPENCUE_CUEWEB_JWT_SECRET,
#         },
#         stdout=subprocess.PIPE,
#         stderr=subprocess.PIPE,
#     )
#
#     yield Output(proc)
#
#     yield AssetMaterialization(
#         asset_key=context.asset_key,
#         metadata={
#             "stdout": MetadataValue.md(f"```shell\n{proc.stdout.decode('utf-8')}\n```"),
#             "stderr": MetadataValue.md(f"```shell\n{proc.stderr.decode('utf-8')}\n```"),
#         },
#     )


@asset(
    **ASSET_HEADER,
    ins={
        "compose_db": AssetIn(
            AssetKey([*ASSET_HEADER["key_prefix"], "compose_db"]),
        ),
        "compose_flyway": AssetIn(
            AssetKey([*ASSET_HEADER["key_prefix"], "compose_flyway"]),
        ),
        "compose_cuebot": AssetIn(
            AssetKey([*ASSET_HEADER["key_prefix"], "compose_cuebot"]),
        ),
        "compose_rqd": AssetIn(
            AssetKey([*ASSET_HEADER["key_prefix"], "compose_rqd"]),
        ),
        "compose_rest_gateway": AssetIn(
            AssetKey([*ASSET_HEADER["key_prefix"], "compose_rest_gateway"]),
        ),
        "compose_cueweb": AssetIn(
            AssetKey([*ASSET_HEADER["key_prefix"], "compose_cueweb"]),
        ),
    },
)
def compose_maps(
    context: AssetExecutionContext,
    **kwargs,  # pylint: disable=redefined-outer-name
) -> Generator[Output[List[Dict]] | AssetMaterialization, None, None]:

    ret = list(kwargs.values())

    context.log.info(ret)

    ret_json = json.dumps(ret, indent=2, default=str)

    yield Output(ret)

    yield AssetMaterialization(
        asset_key=context.asset_key,
        metadata={
            "__".join(context.asset_key.path): MetadataValue.md(
                f"```json\n{ret_json}\n```"
            ),
        },
    )
