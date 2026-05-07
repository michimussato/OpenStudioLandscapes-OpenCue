# pylint: disable=line-too-long,invalid-name
import copy
import enum
import json
import pathlib
import shutil
import subprocess
import textwrap
import urllib.parse
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
    ConfigParent,
    ASSET_HEADER_BASE,
)
from OpenStudioLandscapes.engine.enums import (
    DockerComposePolicies,
)
from OpenStudioLandscapes.engine.link.models import OpenStudioLandscapesFeatureIn
from OpenStudioLandscapes.engine.policies.retry import build_docker_image_retry_policy
from OpenStudioLandscapes.engine.utils import (
    get_pip_install_str,
    get_relative_path_via_common_root,
    get_image_metadata,
    create_image,
    get_docker_compose_names,
    get_docker_run_cmd,
)
from OpenStudioLandscapes.engine.utils.docker.compose_dicts import (
    get_network_dicts,
)

from OpenStudioLandscapes.OpenCue import (
    dist,
    constants,
    config,
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
    ASSET_HEADER=constants.ASSET_HEADER,
)

CONFIG: AssetsDefinition = feature.get_feature__CONFIG(
    ASSET_HEADER=constants.ASSET_HEADER,
    CONFIG_STR=config.models.CONFIG_STR,
    search_model_of_type=config.models.Config,
)

feature_in: AssetsDefinition = group_in.get_feature_in(
    ASSET_HEADER=constants.ASSET_HEADER,
    ASSET_HEADER_BASE=ASSET_HEADER_BASE,
    ASSET_HEADER_FEATURE_IN={},
)

group_out: AssetsDefinition = group_out.get_group_out(
    ASSET_HEADER=constants.ASSET_HEADER,
)


docker_compose_graph: AssetsDefinition = docker_compose_graph.get_docker_compose_graph(
    ASSET_HEADER=constants.ASSET_HEADER,
)


compose: AssetsDefinition = compose.get_compose(
    ASSET_HEADER=constants.ASSET_HEADER,
)


feature_out_v2: AssetsDefinition = feature_out.get_feature_out_v2(
    ASSET_HEADER=constants.ASSET_HEADER,
)


# Produces
# - feature_in_parent
# - CONFIG_PARENT
# if ConfigParent is or type FeatureBaseModel
feature_in_parent: Union[AssetsDefinition, None] = group_in.get_feature_in_parent(
    ASSET_HEADER=constants.ASSET_HEADER,
    config_parent=ConfigParent,
)


@asset(
    **constants.ASSET_HEADER,
    ins={
        "CONFIG": AssetIn(
            AssetKey([*constants.ASSET_HEADER["key_prefix"], "CONFIG"]),
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
    **constants.ASSET_HEADER,
    ins={
        "CONFIG": AssetIn(
            AssetKey([*constants.ASSET_HEADER["key_prefix"], "CONFIG"]),
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
    **constants.ASSET_HEADER,
    ins={
        "CONFIG": AssetIn(
            AssetKey([*constants.ASSET_HEADER["key_prefix"], "CONFIG"]),
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
    **constants.ASSET_HEADER,
    ins={
        "CONFIG": AssetIn(
            AssetKey([*constants.ASSET_HEADER["key_prefix"], "CONFIG"]),
        ),
        "compose_networks": AssetIn(
            AssetKey([*constants.ASSET_HEADER["key_prefix"], "compose_networks"]),
        ),
        "clone_repository": AssetIn(
            AssetKey([*constants.ASSET_HEADER["key_prefix"], "clone_repository"]),
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

    if CONFIG.OPENCUE_CUEBOT_USE_PREBUILT_DOCKER_IMAGE:
        d = {"image": CONFIG.OPENCUE_CUEBOT_PREBUILT_DOCKER_IMAGE}
    else:
        d = {
            "build": {
                "context": clone_repository.as_posix(),
                "dockerfile": clone_repository.joinpath(
                    "cuebot",
                    "Dockerfile",
                ).as_posix(),
            },
        }

    docker_dict = {
        "services": {
            service_name_cuebot: {
                # Todo:
                #  - [ ] prebuilt image?
                **d,
                "container_name": container_name_cuebot,
                "hostname": host_name_cuebot,
                "domainname": config_engine.openstudiolandscapes__domain_lan,
                "restart": DockerComposePolicies.RESTART_POLICY.ALWAYS.value,
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
                        "condition": "service_started",
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
    **constants.ASSET_HEADER,
    ins={
        "CONFIG": AssetIn(
            AssetKey([*constants.ASSET_HEADER["key_prefix"], "CONFIG"]),
        ),
        "compose_networks": AssetIn(
            AssetKey([*constants.ASSET_HEADER["key_prefix"], "compose_networks"]),
        ),
        "clone_repository": AssetIn(
            AssetKey([*constants.ASSET_HEADER["key_prefix"], "clone_repository"]),
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

    docker_dict = {
        "services": {
            service_name_flyway: {
                "container_name": container_name_flyway,
                # Todo:
                #  - [x] prebuilt image?
                #        Not available
                "build": {
                    "context": clone_repository.as_posix(),
                    "dockerfile": clone_repository.joinpath(
                        "sandbox",
                        "flyway.Dockerfile",
                    ).as_posix(),
                },
                "hostname": host_name_flyway,
                "domainname": config_engine.openstudiolandscapes__domain_lan,
                "restart": DockerComposePolicies.RESTART_POLICY.NO.value,
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
                "command": [
                    "/opt/scripts/migrate.sh",
                    # Todo:
                    #   ?column?
                    #  ----------
                    #  1
                    #  (1 row)
                    #  Applying database migrations...
                    #  A new version of Flyway is available
                    #  Upgrade Flyway: https://rd.gt/3TItF25
                    #  Flyway Community Edition 9.11.0 by Redgate
                    #  See what's new here: https://flywaydb.org/documentation/learnmore/releaseNotes#9.11.0
                    #  Database: jdbc:postgresql://opencue-db.2026-01-21_17-22-54__seasoned-jelly-wholesale-mixer:5432/cuebot (PostgreSQL 15.1)
                    #  ERROR: Validate failed: Migrations have failed validation
                    #  Detected resolved migration not applied to database: 35.
                    #  To ignore this migration, set -ignoreMigrationPatterns='*:ignored'. To allow executing this migration, set -outOfOrder=true.
                    #  Need more flexibility with validation rules? Learn more: https://rd.gt/3AbJUZE
                    #  opencue-flyway.2026-01-21_17-22-54__seasoned-jelly-wholesale-mixer                         | A new version of Flyway is available
                    #  opencue-flyway.2026-01-21_17-22-54__seasoned-jelly-wholesale-mixer                         | Upgrade Flyway: https://rd.gt/3TItF25
                    #  opencue-flyway.2026-01-21_17-22-54__seasoned-jelly-wholesale-mixer                         | Flyway Community Edition 9.11.0 by Redgate
                    #  opencue-flyway.2026-01-21_17-22-54__seasoned-jelly-wholesale-mixer                         | See what's new here: https://flywaydb.org/documentation/learnmore/releaseNotes#9.11.0
                    #  opencue-flyway.2026-01-21_17-22-54__seasoned-jelly-wholesale-mixer                         |
                    #  opencue-flyway.2026-01-21_17-22-54__seasoned-jelly-wholesale-mixer                         | Database: jdbc:postgresql://opencue-db.2026-01-21_17-22-54__seasoned-jelly-wholesale-mixer:5432/cuebot (PostgreSQL 15.1)
                    #  opencue-flyway.2026-01-21_17-22-54__seasoned-jelly-wholesale-mixer                         | ERROR: Validate failed: Migrations have failed validation
                    #  opencue-flyway.2026-01-21_17-22-54__seasoned-jelly-wholesale-mixer                         | Detected resolved migration not applied to database: 35.
                    #  opencue-flyway.2026-01-21_17-22-54__seasoned-jelly-wholesale-mixer                         | To ignore this migration, set -ignoreMigrationPatterns='*:ignored'. To allow executing this migration, set -outOfOrder=true.
                    #  opencue-flyway.2026-01-21_17-22-54__seasoned-jelly-wholesale-mixer                         | Need more flexibility with validation rules? Learn more: https://rd.gt/3AbJUZE
                    #   Container opencue-flyway.2026-01-21_17-22-54__seasoned-jelly-wholesale-mixer Error service "opencue-flyway" didn't complete successfully: exit 1
                    #  opencue-flyway.2026-01-21_17-22-54__seasoned-jelly-wholesale-mixer exited with code 1
                    #  Didn't work: "-outOfOrder=true",
                    #  Solution so far: re-create the database
                ],
                **copy.deepcopy(volumes_dict),
                **copy.deepcopy(network_dict),
                "links": [
                    service_name_db,
                ],
                "depends_on": {
                    service_name_db: {
                        "condition": "service_started",
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
    **constants.ASSET_HEADER,
    ins={
        "CONFIG": AssetIn(
            AssetKey([*constants.ASSET_HEADER["key_prefix"], "CONFIG"]),
        ),
        "compose_networks": AssetIn(
            AssetKey([*constants.ASSET_HEADER["key_prefix"], "compose_networks"]),
        ),
        # "clone_repository": AssetIn(
        #     AssetKey([*ASSET_HEADER["key_prefix"], "clone_repository"]),
        # ),
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

    docker_dict = {
        "services": {
            service_name: {
                "image": "docker.io/postgres:15.1",
                "container_name": container_name,
                "hostname": host_name,
                "domainname": config_engine.openstudiolandscapes__domain_lan,
                "restart": DockerComposePolicies.RESTART_POLICY.ALWAYS.value,
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


@asset(
    **constants.ASSET_HEADER,
    ins={
        "feature_in": AssetIn(
            AssetKey([*constants.ASSET_HEADER["key_prefix"], "feature_in"]),
        ),
        "CONFIG": AssetIn(
            AssetKey([*constants.ASSET_HEADER["key_prefix"], "CONFIG"]),
        ),
    },
)
def write_dockerfile(
    context: AssetExecutionContext,
    feature_in: OpenStudioLandscapesFeatureIn,  # pylint: disable=redefined-outer-name
    CONFIG: config.models.Config,  # pylint: disable=redefined-outer-name
) -> Generator[Output[pathlib.Path] | AssetMaterialization, None, None]:
    """ """

    env: Dict = CONFIG.env

    config_engine: ConfigEngine = CONFIG.config_engine

    docker_config: DockerConfigModel = config_engine.openstudiolandscapes__docker_config

    docker_image: Dict = feature_in.openstudiolandscapes_base.docker_image_base

    docker_file = pathlib.Path(
        env["DOT_LANDSCAPES"],
        env.get("LANDSCAPE", "default"),
        f"{dist.name}",
        "__".join(context.asset_key.path),
        "Dockerfiles",
        "Dockerfile",
    )

    docker_file.parent.mkdir(parents=True, exist_ok=True)

    #################################################

    (
        image_name,
        image_prefixes,
        tags,
        build_base_parent_image_prefix,
        build_base_parent_image_name,
        build_base_parent_image_tags,
    ) = get_image_metadata(
        context=context,
        docker_image=docker_image,
        docker_config=docker_config,
        env=env,
    )

    #################################################

    # dnf_install_str_: str = get_dnf_install_str(
    #     dnf_install_packages=[
    #         *CONFIG.dnf_packages_base,
    #         *CONFIG.openstudiolandscapes__rez_config.dnf_packages_rez,
    #     ],
    # )

    pip_install_str: str = get_pip_install_str(pip_install_packages=CONFIG.pip_packages)

    # Todo
    #  - [x] [root@lenovo opencue]# rez env blender -- which blender
    #        /data/share/tools/blender-5.0.1-linux-x64/blender
    #        [root@lenovo opencue]# rez env blender -- blender -b -v
    #        #
    #        blender: error while loading shared libraries: libX11.so.6: cannot open shared object file: No such file or directory
    #        [root@lenovo opencue]# rez env blender -- blender -b -v
    #        Blender 5.0.1 (hash a3db93c5b259 built 2025-12-16 01:30:59)
    #        Blender 5.0.1
    #                build date: 2025-12-16
    #                build time: 01:30:59
    #                build commit date: 2025-12-15
    #                build commit time: 16:36
    #                build hash: a3db93c5b259
    #                build branch: blender-v5.0-release
    #                build platform: Linux
    #                build type: Release
    #                build c flags:  -Wall -Werror=implicit-function-declaration -Wstrict-prototypes -Werror=return-type -Werror=vla -Wmissing-prototypes -Wno-char-subscripts -Wno-unknown-pragmas -Wpointer-arith -Wunused-parameter -Wwrite-strings -Wlogical-op -Wundef -Winit-self -Wmissing-include-dirs -Wno-div-by-zero -Wtype-limits -Wformat-signedness -Wrestrict -Wno-stringop-overread -Wno-stringop-overflow -Wnonnull -Wabsolute-value -Wuninitialized -Wredundant-decls -Wshadow -Wimplicit-fallthrough=5 -Wno-error=unused-but-set-variable  -march=x86-64-v2 -std=gnu11 -pipe -fPIC -funsigned-char -fno-strict-aliasing -ffp-contract=off
    #                build c++ flags:  -Wuninitialized -Wredundant-decls -Wall -Wno-invalid-offsetof -Wno-sign-compare -Wlogical-op -Winit-self -Wmissing-include-dirs -Wno-div-by-zero -Wtype-limits -Werror=return-type -Wno-char-subscripts -Wno-unknown-pragmas -Wpointer-arith -Wunused-parameter -Wwrite-strings -Wundef -Wcomma-subscript -Wformat-signedness -Wrestrict -Wno-suggest-override -Wuninitialized -Wno-stringop-overread -Wno-stringop-overflow -Wimplicit-fallthrough=5 -Wundef -Wmissing-declarations  -march=x86-64-v2 -pipe -fPIC -funsigned-char -fno-strict-aliasing -ffp-contract=off
    #                build link flags:  -Wl,--version-script='/home/blender/git/blender-v500/blender.git/source/creator/symbols_unix.map'
    #                build system: CMake

    # @formatter:off
    docker_file_str = textwrap.dedent("""\
        # {auto_generated}
        # {dagster_url}
        
        ################################################################################
        # Multi Stage: Stage 1
        # FROM {parent_image} AS {image_name}
        FROM {parent_image} AS base
        LABEL authors="{AUTHOR}"

        ENV CONTAINER_TIMEZONE={timezone}
        ENV SET_CONTAINER_TIMEZONE=true
        
        WORKDIR /usr/bin
        RUN ln -s python3.9 python
        
        WORKDIR /opt/opencue
            
        # Prepend to PATH /opt/python{PYTHON_MAJ}.{PYTHON_MIN}/bin
        ENV PATH="/opt/python{PYTHON_MAJ}.{PYTHON_MIN}/bin:$PATH"
        # Prepend to PATH /opt/rez/bin/rez
        ENV PATH="/opt/rez/bin/rez:$PATH"

        ENV LC_ALL=C.UTF-8
        ENV LANG=C.UTF-8

        SHELL ["/bin/bash", "-c"]
        
        # General packages
        RUN dnf install -y {dnf_packages_general}
            
        # Blender 5.0.1
        # on docker.io/rockylinux:8.9
        RUN dnf install -y {dnf_packages_blender_5}
        
        ################################################################################
        # Multi Stage: Stage Rez
        # # Rez Installer
        FROM base AS rez_installer
        
        # COPY --from=build_python "/opt/python{PYTHON_MAJ}.{PYTHON_MIN}" "/opt/python{PYTHON_MAJ}.{PYTHON_MIN}"

        WORKDIR /build/rez

        RUN curl -L "https://github.com/AcademySoftwareFoundation/rez/archive/refs/tags/{rez_version}.tar.gz" -o rez-{rez_version}.tar.gz \\
            && file rez-{rez_version}.tar.gz \\
            && tar -xzvf rez-{rez_version}.tar.gz \\
            && rm rez-{rez_version}.tar.gz

        # https://github.com/AcademySoftwareFoundation/OpenCue/blob/master/rqd/Dockerfile
        # comes with python39
        # Todo:
        #  - [ ] Install OpenStudioLandscapes Python (3.11)
        RUN python3.9 ./rez-{rez_version}/install.py --verbose /opt/rez

        RUN chmod +x /opt/rez/completion/complete.sh
        RUN /opt/rez/completion/complete.sh
        
        # # Rez Build Test
        FROM rez_installer AS rez_build_test
        
        WORKDIR /build/rez/rez-{rez_version}/example_packages/hello_world

        RUN rez bind -vvvvv --quickstart
        RUN rez build -vvvvv --install

        RUN rez env -vvvvv hello_world -- hello

        RUN echo "hello_world successfully tested" > /rez_hello_world_test.txt
        
        ################################################################################        
        # Multi Stage: Stage FINAL
        FROM base AS {image_name}
        
        COPY --from=rez_installer  "/opt/rez" "/opt/rez"
        COPY --from=rez_build_test "/rez_hello_world_test.txt" "/rez_hello_world_test.txt"

        RUN python3.9 -m pip install --root-user-action=ignore --upgrade pip setuptools setuptools_scm wheel \\
            && python3.9 -m pip cache purge

        {pip_install_str}
        
        WORKDIR /opt/opencue
        
        # RQD gRPC server
        EXPOSE 8444
        
        # NOTE: This shell out is needed to avoid RQD getting PID 0 which leads to leaking child processes.
        ENTRYPOINT ["/bin/bash", "-c", "set -e && rqd"]
        """).format(
        auto_generated=f"AUTO-GENERATED by Dagster Asset {'__'.join(context.asset_key.path)}",
        dagster_url=urllib.parse.quote(
            f"http://localhost:3000/asset-groups/{'%2F'.join(context.asset_key.path)}",
            safe=":/%",
        ),
        pip_install_str=pip_install_str.format(
            **env,
        ),
        dnf_packages_general=" ".join(CONFIG.dnf_packages_general),
        dnf_packages_blender_5=" ".join(CONFIG.dnf_packages_blender_5),
        rez_version=config_engine.openstudiolandscapes__rez_config.rez_version,
        timezone=config_engine.tz,
        image_name=image_name,
        # Todo: this won't work as expected if len(tags) > 1
        parent_image=CONFIG.OPENCUE_RQD_DOCKER_IMAGE,
        **env,
    )
    # @formatter:on

    with open(docker_file, "w") as fw:
        fw.write(docker_file_str)

    with open(docker_file, "r") as fr:
        docker_file_content = fr.read()

    yield Output(docker_file)

    yield AssetMaterialization(
        asset_key=context.asset_key,
        metadata={
            "__".join(context.asset_key.path): MetadataValue.path(docker_file),
            docker_file.name: MetadataValue.md(f"```shell\n{docker_file_content}\n```"),
            "env": MetadataValue.json(env),
        },
    )


@asset(
    **constants.ASSET_HEADER,
    ins={
        "feature_in": AssetIn(
            AssetKey([*constants.ASSET_HEADER["key_prefix"], "feature_in"]),
        ),
        "CONFIG": AssetIn(
            AssetKey([*constants.ASSET_HEADER["key_prefix"], "CONFIG"]),
        ),
        "write_dockerfile": AssetIn(
            AssetKey([*constants.ASSET_HEADER["key_prefix"], "write_dockerfile"])
        ),
    },
    retry_policy=build_docker_image_retry_policy,
)
def build_docker_image(
    context: AssetExecutionContext,
    feature_in: OpenStudioLandscapesFeatureIn,  # pylint: disable=redefined-outer-name
    CONFIG: config.models.Config,  # pylint: disable=redefined-outer-name
    write_dockerfile: pathlib.Path,  # pylint: disable=redefined-outer-name
) -> Generator[Output[Dict] | AssetMaterialization, None, None]:
    """ """

    env: Dict = CONFIG.env

    docker_config_json: pathlib.Path = (
        feature_in.openstudiolandscapes_base.docker_config_json
    )

    config_engine: ConfigEngine = CONFIG.config_engine

    docker_config: DockerConfigModel = config_engine.openstudiolandscapes__docker_config

    docker_image: Dict = feature_in.openstudiolandscapes_base.docker_image_base

    #################################################

    (
        image_name,
        image_prefixes,
        tags,
        build_base_parent_image_prefix,
        build_base_parent_image_name,
        build_base_parent_image_tags,
    ) = get_image_metadata(
        context=context,
        docker_image=docker_image,
        docker_config=docker_config,
        env=env,
    )

    #################################################

    image_data, logs = create_image(
        context=context,
        image_name=image_name,
        image_prefixes=image_prefixes,
        tags=tags,
        docker_image=docker_image,
        docker_config=docker_config,
        docker_config_json=docker_config_json,
        docker_file=write_dockerfile,
    )

    yield Output(image_data)

    yield AssetMaterialization(
        asset_key=context.asset_key,
        metadata={
            "__".join(context.asset_key.path): MetadataValue.json(image_data),
            "env": MetadataValue.json(env),
            "docker_image": MetadataValue.path(
                f"{image_data['image_prefixes']}{image_data['image_name']}:{image_data['image_tags'][0]}"
            ),
            "docker_cmd": MetadataValue.path(
                get_docker_run_cmd(
                    context=context,
                    image_data=image_data,
                )
            ),
            "logs": MetadataValue.json(logs),
        },
    )


@asset(
    **constants.ASSET_HEADER,
    ins={
        "CONFIG": AssetIn(
            AssetKey([*constants.ASSET_HEADER["key_prefix"], "CONFIG"]),
        ),
        "compose_networks": AssetIn(
            AssetKey([*constants.ASSET_HEADER["key_prefix"], "compose_networks"]),
        ),
        "build": AssetIn(
            AssetKey([*constants.ASSET_HEADER["key_prefix"], "build_docker_image"]),
        ),
        # "clone_repository": AssetIn(
        #     AssetKey([*constants.ASSET_HEADER["key_prefix"], "clone_repository"]),
        # ),
        "prepare_volumes": AssetIn(
            AssetKey([*constants.ASSET_HEADER["key_prefix"], "prepare_volumes"]),
        ),
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
    build: Dict,  # pylint: disable=redefined-outer-name
    # clone_repository: pathlib.Path,  # pylint: disable=redefined-outer-name
    prepare_volumes: Dict,  # pylint: disable=redefined-outer-name
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

        docker_dict = {
            "services": {
                service_name: {
                    "image": "%s%s:%s"
                    % (
                        build["image_prefixes"],
                        build["image_name"],
                        build["image_tags"][0],
                    ),
                    "container_name": container_name,
                    "hostname": host_name,
                    "domainname": config_engine.openstudiolandscapes__domain_lan,
                    "restart": DockerComposePolicies.RESTART_POLICY.ALWAYS.value,
                    "environment": {
                        "TZ": config_engine.tz,
                        "PYTHONUNBUFFERED": "1",
                        # Todo:
                        #  - [ ] use fqdn instead of just hostname?
                        "CUEBOT_HOSTNAME": container_name_cuebot,  # f"cuebot.{config_engine.openstudiolandscapes__domain_lan}",
                        **config_engine.global_environment_variables,
                        **CONFIG.local_environment_variables,
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
    **constants.ASSET_HEADER,
    ins={
        "CONFIG": AssetIn(
            AssetKey([*constants.ASSET_HEADER["key_prefix"], "CONFIG"]),
        ),
        "clone_repository": AssetIn(
            AssetKey([*constants.ASSET_HEADER["key_prefix"], "clone_repository"]),
        ),
        "compose_networks": AssetIn(
            AssetKey([*constants.ASSET_HEADER["key_prefix"], "compose_networks"]),
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

    docker_dict = {
        "services": {
            # https://docs.opencue.io/docs/getting-started/deploying-rest-gateway/#docker-compose-configuration-separate-file
            service_name_rest_gateway: {
                "container_name": container_name_rest_gateway,
                "hostname": host_name_rest_gateway,
                "build": {
                    # https://docs.docker.com/reference/compose-file/build/#context
                    "context": clone_repository.as_posix(),
                    # https://docs.docker.com/reference/compose-file/build/#dockerfile
                    "dockerfile": clone_repository.joinpath(
                        "rest_gateway",
                        "Dockerfile",
                    ).as_posix(),
                },
                "domainname": config_engine.openstudiolandscapes__domain_lan,
                "environment": {
                    "TZ": config_engine.tz,
                    # https://docs.opencue.io/docs/getting-started/deploying-rest-gateway/#configuration-options
                    "CUEBOT_ENDPOINT": f"{container_name_cuebot}:{CONFIG.OPENCUE_CUEBOT_GRPC_CUE_PORT_CONTAINER}",
                    "REST_PORT": CONFIG.OPENCUE_REST_GATEWAY_PORT_CONTAINER,
                    "JWT_SECRET": CONFIG.OPENCUE_CUEWEB_JWT_SECRET,
                    "LOG_LEVEL": "debug",
                    "CORS_ALLOWED_ORIGINS": "*",
                    **config_engine.global_environment_variables,
                    **CONFIG.local_environment_variables,
                },
                "depends_on": {
                    service_name_db: {
                        "condition": "service_started",
                    },
                    service_name_cuebot: {
                        "condition": "service_started",
                    },
                },
                "restart": DockerComposePolicies.RESTART_POLICY.ALWAYS.value,
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
    **constants.ASSET_HEADER,
    ins={
        "CONFIG": AssetIn(
            AssetKey([*constants.ASSET_HEADER["key_prefix"], "CONFIG"]),
        ),
        "clone_repository": AssetIn(
            AssetKey([*constants.ASSET_HEADER["key_prefix"], "clone_repository"]),
        ),
        "compose_networks": AssetIn(
            AssetKey([*constants.ASSET_HEADER["key_prefix"], "compose_networks"]),
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

    docker_dict = {
        "services": {
            # https://docs.opencue.io/docs/getting-started/deploying-rest-gateway/#docker-compose-configuration-separate-file
            service_name_cueweb: {
                "container_name": container_name_cueweb,
                "hostname": host_name_cueweb,
                "build": {
                    # https://docs.docker.com/reference/compose-file/build/#context
                    "context": clone_repository.joinpath(
                        "cueweb",
                    ).as_posix(),
                    # https://docs.docker.com/reference/compose-file/build/#dockerfile
                    "dockerfile": clone_repository.joinpath(
                        "cueweb",
                        "Dockerfile",
                    ).as_posix(),
                },
                "domainname": config_engine.openstudiolandscapes__domain_lan,
                "environment": {
                    "TZ": config_engine.tz,
                    # https://docs.opencue.io/docs/getting-started/deploying-cueweb/#environment-configuration
                    "NEXT_PUBLIC_OPENCUE_ENDPOINT": f"http://{host_name_rest_gateway}:{CONFIG.OPENCUE_REST_GATEWAY_PORT_CONTAINER}",
                    "NEXT_PUBLIC_URL": f"http://{host_name_cueweb}:{CONFIG.OPENCUE_CUEWEB_PORT_HOST}",
                    "NEXT_JWT_SECRET": CONFIG.OPENCUE_CUEWEB_JWT_SECRET,
                    **CONFIG.OPENCUE_CUEWEB_ADDITIONAL_ENV,
                    **config_engine.global_environment_variables,
                    **CONFIG.local_environment_variables,
                },
                "restart": DockerComposePolicies.RESTART_POLICY.ALWAYS.value,
                "volumes": [
                    *_volume_relative_cueweb,
                ],
                "depends_on": {
                    service_name_rest_gateway: {
                        "condition": "service_started",
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


@asset(
    **constants.ASSET_HEADER,
    ins={
        "CONFIG": AssetIn(
            AssetKey([*constants.ASSET_HEADER["key_prefix"], "CONFIG"]),
        ),
        "clone_repository": AssetIn(
            AssetKey([*constants.ASSET_HEADER["key_prefix"], "clone_repository"]),
        ),
    },
    description=textwrap.dedent("""
        Test requires `JWT_SECRET` set to default `default-secret-key`.
        
        Bug report pending: 
        - [`test_rest_gateway_docker_compose.sh` with hard coded JWT secret: `Token validation error: token signature is invalid: signature is invalid`](https://github.com/AcademySoftwareFoundation/OpenCue/issues/2127)
        
        If `JWT_SECRET` has a non default value, this manual test 
        can help:
        
        ```shell
        # Create a simple test token using openssl (less secure but works for testing)
        # Note: This creates a basic token structure, may not work with all JWT implementations
        HEADER=$(echo -n '{"alg":"HS256","typ":"JWT"}' | base64 | tr -d '=' | tr '/+' '_-' | tr -d '\\n')
        PAYLOAD=$(echo -n '{"user":"test","exp":'$(date -d '+1 hour' +%s)'}' | base64 | tr -d '=' | tr '/+' '_-' | tr -d '\\n')
        SIGNATURE=$(echo -n "${HEADER}.${PAYLOAD}" | openssl dgst -sha256 -hmac "$JWT_SECRET" -binary | base64 | tr -d '=' | tr '/+' '_-' | tr -d '\\n')
        export JWT_TOKEN="${HEADER}.${PAYLOAD}.${SIGNATURE}"
        
        # Test authenticated endpoint
        curl -H "Authorization: Bearer $JWT_TOKEN" \\
             -H "Content-Type: application/json" \\
             -X POST "http://localhost:8448/show.ShowInterface/GetShows" \\
             -d '{}'
        ```
        """),
)
def opencue_test_suite__rest_gateway_docker_compose(
    context: AssetExecutionContext,
    CONFIG: config.models.Config,  # pylint: disable=redefined-outer-name
    clone_repository: pathlib.Path,  # pylint: disable=redefined-outer-name
) -> Generator[Output[subprocess.CompletedProcess] | AssetMaterialization, None, None]:

    env: Dict = CONFIG.env

    cmd = (
        f"pwd "
        "&& echo ${JWT_SECRET} "
        "&& export JWT_SECRET "
        f"&& . .venv/bin/activate "
        f"&& cd {clone_repository.as_posix()}/rest_gateway "
        f"&& {shutil.which('bash')} test_rest_gateway_docker_compose.sh"
    )

    proc: subprocess.CompletedProcess = subprocess.run(
        # [
        #     shutil.which("bash"),
        #     "test_rest_gateway_docker_compose.sh",
        # ],
        cmd,
        shell=True,
        # cwd=clone_repository.joinpath(
        #     "rest_gateway",
        # ),
        env={
            **env,
            "JWT_SECRET": CONFIG.OPENCUE_CUEWEB_JWT_SECRET,
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    yield Output(proc)

    yield AssetMaterialization(
        asset_key=context.asset_key,
        metadata={
            "stdout": MetadataValue.md(f"```shell\n{proc.stdout.decode('utf-8')}\n```"),
            "stderr": MetadataValue.md(f"```shell\n{proc.stderr.decode('utf-8')}\n```"),
        },
    )


@asset(
    **constants.ASSET_HEADER,
    ins={
        "compose_db": AssetIn(
            AssetKey([*constants.ASSET_HEADER["key_prefix"], "compose_db"]),
        ),
        "compose_flyway": AssetIn(
            AssetKey([*constants.ASSET_HEADER["key_prefix"], "compose_flyway"]),
        ),
        "compose_cuebot": AssetIn(
            AssetKey([*constants.ASSET_HEADER["key_prefix"], "compose_cuebot"]),
        ),
        "compose_rqd": AssetIn(
            AssetKey([*constants.ASSET_HEADER["key_prefix"], "compose_rqd"]),
        ),
        "compose_rest_gateway": AssetIn(
            AssetKey([*constants.ASSET_HEADER["key_prefix"], "compose_rest_gateway"]),
        ),
        "compose_cueweb": AssetIn(
            AssetKey([*constants.ASSET_HEADER["key_prefix"], "compose_cueweb"]),
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
