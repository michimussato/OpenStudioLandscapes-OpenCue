import copy
import json
import pathlib
from collections import ChainMap
from functools import reduce
from typing import Generator

import git
import yaml
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    AssetMaterialization,
    MetadataValue,
    Output,
    asset,
)

from OpenStudioLandscapes.engine.constants import *
from OpenStudioLandscapes.engine.enums import *
from docker_compose_graph.utils import *
from git.exc import GitCommandError

from OpenStudioLandscapes.OpenCue.constants import *

from docker_compose_graph.yaml_tags.overrides import *

from OpenStudioLandscapes.engine.utils import *

from OpenStudioLandscapes.engine.common_assets.constants import get_constants
from OpenStudioLandscapes.engine.common_assets.docker_config import get_docker_config
from OpenStudioLandscapes.engine.common_assets.env import get_env
from OpenStudioLandscapes.engine.common_assets.group_in import get_group_in
from OpenStudioLandscapes.engine.common_assets.group_out import get_group_out
from OpenStudioLandscapes.engine.common_assets.docker_compose_graph import get_docker_compose_graph
from OpenStudioLandscapes.engine.common_assets.feature_out import get_feature_out
from OpenStudioLandscapes.engine.common_assets.compose import get_compose
from OpenStudioLandscapes.engine.common_assets.docker_config_json import get_docker_config_json

# Todo
#  opencue-flyway exited with code 0
#  dependency failed to start: container opencue-cuebot has no healthcheck configured


constants = get_constants(
    ASSET_HEADER=ASSET_HEADER,
)


docker_config = get_docker_config(
    ASSET_HEADER=ASSET_HEADER,
)


group_in = get_group_in(
    ASSET_HEADER=ASSET_HEADER,
    ASSET_HEADER_PARENT=ASSET_HEADER_BASE,
    input_name=str(GroupIn.BASE_IN),
)


env = get_env(
    ASSET_HEADER=ASSET_HEADER,
)


group_out = get_group_out(
    ASSET_HEADER=ASSET_HEADER,
)


docker_compose_graph = get_docker_compose_graph(
    ASSET_HEADER=ASSET_HEADER,
)


# compose = get_compose(
#     ASSET_HEADER=ASSET_HEADER,
# )


feature_out = get_feature_out(
    ASSET_HEADER=ASSET_HEADER,
    feature_out_ins={
        "env": dict,
        "compose": dict,
        "group_in": dict,
    },
)


docker_config_json = get_docker_config_json(
    ASSET_HEADER=ASSET_HEADER,
)


@asset(
    **ASSET_HEADER,
)
def repository_opencue(
    context: AssetExecutionContext,
) -> Generator[Output[dict[str, str | None]] | AssetMaterialization, None, None]:
    repository_dict = {
        "branch": "master",
        "repository_dir": "OpenCue",
        "repository_url": "https://github.com/AcademySoftwareFoundation/OpenCue.git",
        "repository_dir_full": None,
    }

    yield Output(repository_dict)

    yield AssetMaterialization(
        asset_key=context.asset_key,
        metadata={
            "__".join(context.asset_key.path): MetadataValue.json(repository_dict),
        },
    )


@asset(
    **ASSET_HEADER,
    ins={
        "env": AssetIn(
            AssetKey([*ASSET_HEADER["key_prefix"], "env"]),
        ),
        "DOCKER_COMPOSE_OVERRIDE": AssetIn(
            AssetKey([*ASSET_HEADER["key_prefix"], "DOCKER_COMPOSE_OVERRIDE"]),
        ),
    },
)
def env_override(
    context: AssetExecutionContext,
    env: dict,  # pylint: disable=redefined-outer-name
    DOCKER_COMPOSE_OVERRIDE: pathlib.Path,  # pylint: disable=redefined-outer-name
) -> Generator[Output[dict[str, str | None]] | AssetMaterialization, None, None]:
    """Instead of changing the OpenStudioLandscapes.engine.base.ops.op_env operator,
    I thought it would be easier to just feed in the additional DOCKER_COMPOSE_OVERRIDE
    path into the env and go from there."""

    env_in = copy.deepcopy(env)

    env_in.update(
        expand_dict_vars(
            dict_to_expand={
                "DOCKER_COMPOSE_OVERRIDE": DOCKER_COMPOSE_OVERRIDE.as_posix(),
            },
            kv=env_in,
        )
    )

    yield Output(env_in)

    yield AssetMaterialization(
        asset_key=context.asset_key,
        metadata={
            "__".join(context.asset_key.path): MetadataValue.json(env_in),
        },
    )


@asset(
    **ASSET_HEADER,
    ins={
        "env": AssetIn(
            AssetKey([*ASSET_HEADER["key_prefix"], "env_override"]),
        ),
        "repository_opencue": AssetIn(
            AssetKey([*ASSET_HEADER["key_prefix"], "repository_opencue"]),
        ),
    },
)
def clone_repository(
    context: AssetExecutionContext,
    env: dict,
    repository_opencue: dict[str, str | None],
) -> Generator[Output[dict[str, str]] | AssetMaterialization, None, None]:

    repo_dir = pathlib.Path(
        env["DOT_LANDSCAPES"],
        env.get("LANDSCAPE", "default"),
        f"{ASSET_HEADER['group_name']}__{'__'.join(ASSET_HEADER['key_prefix'])}",
        "__".join(context.asset_key.path),
        "repos",
    )

    repository_dir_full = repo_dir / repository_opencue["repository_dir"]
    repository_dir_full.parent.mkdir(parents=True, exist_ok=True)

    repository_opencue["repository_dir_full"] = repository_dir_full.as_posix()
    context.log.info(repository_opencue["repository_dir_full"])

    try:
        git.Repo.clone_from(
            url=repository_opencue["repository_url"],
            to_path=repository_opencue["repository_dir_full"],
            branch=repository_opencue["branch"],
        )
    except GitCommandError as e:
        context.log.warning("Pulling from Repo (%s)" % e)
        existing_repo = git.Repo(repository_opencue["repository_dir_full"])
        origin = existing_repo.remotes.origin
        origin.pull()

    yield Output(repository_opencue)

    yield AssetMaterialization(
        asset_key=context.asset_key,
        metadata={
            "__".join(context.asset_key.path): MetadataValue.json(repository_opencue),
        },
    )


@asset(
    **ASSET_HEADER,
    ins={
        "env": AssetIn(
            AssetKey([*ASSET_HEADER["key_prefix"], "env_override"]),
        ),
    },
)
def prepare_volumes(
    context: AssetExecutionContext,
    env: dict,  # pylint: disable=redefined-outer-name
    # script_prepare_db: dict[str, str],  # pylint: disable=redefined-outer-name
) -> Generator[Output[dict] | AssetMaterialization, None, None]:
    """https://www.opencue.io/docs/quick-starts/quick-start-linux/#deploying-the-opencue-sandbox-environment"""

    local_volumes_root = pathlib.Path(
        env["DOT_LANDSCAPES"],
        env.get("LANDSCAPE", "default"),
        f"{ASSET_HEADER['group_name']}__{'__'.join(ASSET_HEADER['key_prefix'])}",
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
)
def compose_networks(
    context: AssetExecutionContext,
) -> Generator[
    Output[dict[str, dict[str, dict[str, str]]]] | AssetMaterialization, None, None
]:

    compose_network_mode = ComposeNetworkMode.DEFAULT

    if compose_network_mode == ComposeNetworkMode.DEFAULT:
        docker_dict = {
            "networks": {
                "opencue": {
                    "name": "network_opencue",
                },
            },
        }

    else:
        docker_dict = {
            "network_mode": compose_network_mode.value,
        }

    docker_yaml = yaml.dump(docker_dict)

    yield Output(docker_dict)

    yield AssetMaterialization(
        asset_key=context.asset_key,
        metadata={
            "__".join(context.asset_key.path): MetadataValue.json(docker_dict),
            "compose_network_mode": MetadataValue.text(compose_network_mode.value),
            "docker_dict": MetadataValue.md(
                f"```json\n{json.dumps(docker_dict, indent=2)}\n```"
            ),
            "docker_yaml": MetadataValue.md(f"```shell\n{docker_yaml}\n```"),
        },
    )


# Todo:
#  - [ ] Maybe fix this Non-Standard `compose` implementation
@asset(
    **ASSET_HEADER,
    ins={
        "env": AssetIn(
            AssetKey([*ASSET_HEADER["key_prefix"], "env_override"]),
        ),
        "compose_networks": AssetIn(
            AssetKey([*ASSET_HEADER["key_prefix"], "compose_networks"]),
        ),
        "clone_repository": AssetIn(
            AssetKey([*ASSET_HEADER["key_prefix"], "clone_repository"]),
        ),
        "prepare_volumes": AssetIn(
            AssetKey([*ASSET_HEADER["key_prefix"], "prepare_volumes"]),
        ),
    },
)
def compose(
    context: AssetExecutionContext,
    env: dict,  # pylint: disable=redefined-outer-name
    compose_networks: dict,  # pylint: disable=redefined-outer-name
    clone_repository: dict,  # pylint: disable=redefined-outer-name
    # Todo:
    #  - [ ] remove unused?
    prepare_volumes: dict,  # pylint: disable=redefined-outer-name
) -> Generator[
    Output[dict[str, list[dict[str, list[str]]]]] | AssetMaterialization, None, None
]:
    """
    Source: https://github.com/AcademySoftwareFoundation/OpenCue/blob/master/docker-compose.yml

    Args:
        context:
        env:
        compose_networks:
        clone_repository:
        prepare_volumes:

    Returns:

    """

    network_dict = {}
    ports_dict = {}
    ports_dict_rqd = {}
    ports_dict_cuebot = {}
    ports_dict_db = {}

    if "networks" in compose_networks:
        network_dict = {"networks": list(compose_networks.get("networks", {}).keys())}
        ports_dict = {"ports": []}
        ports_dict_rqd = {
            "ports": OverrideArray(
                [
                    f"{env.get('OPENCUE_CUEBOT_GRPC_RQD_PORT_HOST')}:{env.get('OPENCUE_CUEBOT_GRPC_RQD_PORT_CONTAINER')}",
                ]
            ),
        }
        ports_dict_cuebot = {
            "ports": OverrideArray(
                [
                    f"{env.get('OPENCUE_CUEBOT_GRPC_CUE_PORT_HOST')}:{env.get('OPENCUE_CUEBOT_GRPC_CUE_PORT_CONTAINER')}",
                ]
            ),
        }
        ports_dict_db = {
            "ports": OverrideArray(
                [
                    f"{env.get('OPENCUE_DB_PORT_HOST')}:{env.get('OPENCUE_DB_PORT_CONTAINER')}",
                ]
            ),
        }
    elif "network_mode" in compose_networks:
        network_dict = {"network_mode": compose_networks.get("network_mode")}

    docker_compose_git_repository = (
        pathlib.Path(clone_repository["repository_dir_full"]) / "docker-compose.yml"
    )

    opencue_db_dir_host = pathlib.Path(env["OPENCUE_DB_INSTALL_DESTINATION"])

    opencue_db_dir_host.mkdir(parents=True, exist_ok=True)
    context.log.info(f"Directory {opencue_db_dir_host.as_posix()} created.")

    service_name_db = "db"
    # Todo
    container_name_db = "--".join(
        [f"ayon-{service_name_db}", env.get("LANDSCAPE", "default")]
    )
    # host_name_db = ".".join([service_name_db, env["ROOT_DOMAIN"]])
    volumes_db = [
        f"{opencue_db_dir_host.as_posix()}:/var/lib/postgresql/data:rw",
    ]

    _volume_relative_db = []

    for v in volumes_db:

        host, container = v.split(":", maxsplit=1)

        volume_dir_host_rel_path = get_relative_path_via_common_root(
            context=context,
            path_src=docker_compose_git_repository,  # Probably because the root docker-compose is the one in the Git repo
            path_dst=pathlib.Path(host),
            path_common_root=pathlib.Path(env["DOT_LANDSCAPES"]),
        )

        _volume_relative_db.append(
            f"{volume_dir_host_rel_path.as_posix()}:{container}",
        )

    # /home/michael/git/repos/OpenStudioLandscapes/.landscapes/2025-07-13-18-26-40-db99ad3811304b75836a8c97a2987c12/OpenCue__OpenCue/OpenCue__DOCKER_COMPOSE/docker_compose/docker-compose.yml
    # /home/michael/git/repos/OpenStudioLandscapes/.landscapes/2025-07-13-18-26-40-db99ad3811304b75836a8c97a2987c12/OpenCue__OpenCue/OpenCue__compose/docker-compose.override.yml

    service_name_flyway = "flyway"
    # Todo
    container_name_flyway = "--".join(
        [f"ayon-{service_name_flyway}", env.get("LANDSCAPE", "default")]
    )
    # host_name_flyway = ".".join([service_name_flyway, env["ROOT_DOMAIN"]])

    service_name_cuebot = "cuebot"
    # Todo
    container_name_cuebot = "--".join(
        [f"ayon-{service_name_cuebot}", env.get("LANDSCAPE", "default")]
    )
    # host_name_cubot = ".".join([service_name_cuebot, env["ROOT_DOMAIN"]])

    service_name_rqd = "rqd"
    # Todo
    container_name_rqd = "--".join(
        [f"ayon-{service_name_rqd}", env.get("LANDSCAPE", "default")]
    )
    # host_name_rqd = ".".join([service_name_rqd, env["ROOT_DOMAIN"]])
    volumes_rqd = [
        f"{prepare_volumes['logs']}:/tmp/rqd/logs:rw",
        f"{prepare_volumes['shots']}:/tmp/rqd/shots:rw",
    ]

    _volume_relative_rqd = []

    for v in volumes_rqd:

        host, container = v.split(":", maxsplit=1)

        volume_dir_host_rel_path = get_relative_path_via_common_root(
            context=context,
            path_src=docker_compose_git_repository,  # Probably because the root docker-compose is the one in the Git repo
            path_dst=pathlib.Path(host),
            path_common_root=pathlib.Path(env["DOT_LANDSCAPES"]),
        )

        _volume_relative_rqd.append(
            f"{volume_dir_host_rel_path.as_posix()}:{container}",
        )

    docker_dict_override = {
        "networks": compose_networks.get("networks", []),
        "services": {
            service_name_db: {
                "container_name": container_name_db,
                "hostname": "opencue-db",
                "domainname": env.get("ROOT_DOMAIN"),
                "environment": {
                    "POSTGRES_DB": env.get('OPENCUE_DB_PGDATABASE'),
                    "POSTGRES_PASSWORD": env.get('OPENCUE_DB_PGPASSWORD'),
                    "POSTGRES_USER": env.get('OPENCUE_DB_PGUSER'),
                },
                "volumes": [
                    *_volume_relative_db,
                ],
                **copy.deepcopy(network_dict),
                **copy.deepcopy(ports_dict_db),
            },
            service_name_flyway: {
                "container_name": container_name_flyway,
                "hostname": "opencue-flyway",
                "domainname": env.get("ROOT_DOMAIN"),
                "environment": {
                    "PGHOST": env.get('OPENCUE_DB_PGHOST'),
                    "PGDATABASE": env.get('OPENCUE_DB_PGDATABASE'),
                    "PGPASSWORD": env.get('OPENCUE_DB_PGPASSWORD'),
                    "PGUSER": env.get('OPENCUE_DB_PGUSER'),
                    "PGPORT": env.get('OPENCUE_DB_PORT_CONTAINER'),
                },
                **copy.deepcopy(network_dict),
                # "networks": [
                #     "mongodb",
                #     "repository",
                # ],
            },
            service_name_cuebot: {
                "container_name": container_name_cuebot,
                "hostname": "opencue-cuebot",
                "domainname": env.get("ROOT_DOMAIN"),
                # This might not be very helpful as a health check
                # but a health check seems mandatory for rqd to be
                # dependent on this service
                "healthcheck": {
                    "test": [
                        "CMD",
                        "pidof",
                        "java",
                    ],
                    "interval": "10s",
                    "timeout": "2s",
                    "retries": "3",
                },
                "environment": {
                    "CUE_FRAME_LOG_DIR": "/tmp/rqd/logs",
                },
                # Todo:
                #  - [ ] Need to find out whether `ports` Override
                #  also overrides the exports in the source ayon-docker-compose.yml
                #  "exports": OverrideArray([]),
                # "ports": OverrideArray(
                #     [
                #         f"{env.get('AYON_PORT_HOST')}:{env.get('AYON_PORT_CONTAINER')}",
                #     ]
                # ),
                **copy.deepcopy(network_dict),
                **copy.deepcopy(ports_dict_cuebot),
            },
            service_name_rqd: {
                "container_name": container_name_rqd,
                "hostname": "opencue-rqd",
                "domainname": env.get("ROOT_DOMAIN"),
                "environment": {
                    "PYTHONUNBUFFERED": "1",
                    "CUEBOT_HOSTNAME": "opencue-cuebot",
                },
                "restart": "always",
                "volumes": [
                    *_volume_relative_rqd,
                ],
                **copy.deepcopy(network_dict),
                **copy.deepcopy(ports_dict_rqd),
            },
        },
    }

    if "networks" in compose_networks:
        network_dict = copy.deepcopy(compose_networks)
    else:
        network_dict = {}

    docker_chainmap = ChainMap(
        network_dict,
        docker_dict_override,
    )

    docker_dict = reduce(deep_merge, docker_chainmap.maps)

    docker_compose_override = pathlib.Path(env["DOCKER_COMPOSE_OVERRIDE"])

    docker_compose_override.parent.mkdir(parents=True, exist_ok=True)

    docker_yaml_override: str = yaml.dump(docker_dict)

    with open(docker_compose_override, "w") as fw:
        fw.write(docker_yaml_override)

    # Write compose override to disk here to be able to reference
    # it in the following step.
    # It seems that it's necessary to apply overrides in
    # include: path

    # Convert absolute paths in `include` to
    # relative ones
    DOCKER_COMPOSE = pathlib.Path(env["DOCKER_COMPOSE"])
    DOCKER_COMPOSE.parent.mkdir(parents=True, exist_ok=True)

    rel_paths = []
    dot_landscapes = pathlib.Path(env["DOT_LANDSCAPES"])

    # Todo:
    #  - [x] find a better way to implement relpath with `from` and `via`
    #  - [x] externalize
    for path in [
        docker_compose_git_repository.as_posix(),
        docker_compose_override.as_posix(),
    ]:
        rel_path = get_relative_path_via_common_root(
            context=context,
            path_src=DOCKER_COMPOSE,
            path_dst=pathlib.Path(path),
            path_common_root=dot_landscapes,
        )

        rel_paths.append(rel_path.as_posix())

    docker_dict_include = {
        "include": [
            {
                "path": rel_paths,
            },
        ],
    }

    docker_yaml_include = yaml.dump(docker_dict_include)

    # Write docker-compose.yaml
    with open(DOCKER_COMPOSE, mode="w", encoding="utf-8") as fw:
        fw.write(docker_yaml_include)

    yield Output(docker_dict_include)

    yield AssetMaterialization(
        asset_key=context.asset_key,
        metadata={
            "__".join(context.asset_key.path): MetadataValue.json(docker_dict_include),
            "docker_yaml_override": MetadataValue.md(
                f"```yaml\n{docker_yaml_override}\n```"
            ),
            "path_docker_yaml_override": MetadataValue.path(docker_compose_override),
            # Todo: "cmd_docker_run": MetadataValue.path(cmd_list_to_str(cmd_docker_run)),
        },
    )
