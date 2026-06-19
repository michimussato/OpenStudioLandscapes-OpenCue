[![ Logo OpenStudioLandscapes ](https://github.com/michimussato/OpenStudioLandscapes/raw/main/media/images/logo128.png)](https://github.com/michimussato/OpenStudioLandscapes)

***

1. [Feature: OpenStudioLandscapes-OpenCue](#feature-openstudiolandscapes-opencue)
   1. [Brief](#brief)
   2. [Clone](#clone)
      1. [Clone and Install](#clone-and-install)
   3. [Configure](#configure)
      1. [Default Configuration](#default-configuration)
   4. [Local Development/Unit Testing/Debugging](#local-developmentunit-testingdebugging)
2. [External Resources](#external-resources)
   1. [Official Documentation](#official-documentation)
   2. [Components](#components)
3. [Community](#community)

***

This `README.md` was dynamically created with [OpenStudioLandscapesUtil-ReadmeGenerator](https://github.com/michimussato/OpenStudioLandscapesUtil-ReadmeGenerator).

***

# Feature: OpenStudioLandscapes-OpenCue

## Brief

This is an extension to the OpenStudioLandscapes ecosystem. The full documentation of OpenStudioLandscapes is available [here](https://github.com/michimussato/OpenStudioLandscapes).

> [!NOTE]
> 
> You feel like writing your own Feature? Go and check out the 
> [OpenStudioLandscapes-Template](https://github.com/michimussato/OpenStudioLandscapes-Template).

## Clone

Clone this repository into `OpenStudioLandscapes/.features` (assuming the current working directory to be the Git repository root `./OpenStudioLandscapes`):

```shell
# cd OpenStudioLandscapes
source .venv/bin/activate
openstudiolandscapes clone-feature --repo=https://github.com/michimussato/OpenStudioLandscapes-OpenCue.git
deactivate
# Check the resulting console output for installation instructions
```

### Clone and Install

```shell
# cd OpenStudioLandscapes
source .venv/bin/activate
openstudiolandscapes clone-feature --repo=https://github.com/michimussato/OpenStudioLandscapes-OpenCue.git \
    && pip install --editable ./.features/OpenStudioLandscapes-OpenCue
deactivate
```

For more info on `pip` see [VCS Support of `pip`](https://pip.pypa.io/en/stable/topics/vcs-support/).

## Configure

OpenStudioLandscapes will search for a local config store. The default location is `~/.config/OpenStudioLandscapes/config-store/` but you can specify a different location if you need to.

> [!TIP]
> 
> To specify a config store location different from
> the default location, check out the OpenStudioLandscapes 
> [CLI Section](https://github.com/michimussato/OpenStudioLandscapes#cli)
> to find out how to do that.

A local config store location will be created if it doesn't exist, together with the `config.yml` files for each individual Feature.

> [!TIP]
> 
> The config store root will be initialized as a local Git
> controlled repository. This makes it easy to track changes
> you made to the `config.yml`.

The following settings are available in `OpenStudioLandscapes-OpenCue` and are based on [`OpenStudioLandscapes-OpenCue/tree/main/src/OpenStudioLandscapes/OpenCue/config/models.py`](https://github.com/michimussato/OpenStudioLandscapes-OpenCue/tree/main/src/OpenStudioLandscapes/OpenCue/config/models.py).

### Default Configuration

<details open>
<summary><code>config.yml</code></summary>


```yaml
OPENCUE_CUEBOT_GRPC_CUE_PORT_CONTAINER:
  default: 8443
  exclusiveMinimum: 0
  title: Opencue Cuebot Grpc Cue Port Container
  type: integer
OPENCUE_CUEBOT_GRPC_CUE_PORT_HOST:
  default: 8443
  exclusiveMinimum: 0
  title: Opencue Cuebot Grpc Cue Port Host
  type: integer
OPENCUE_CUEBOT_GRPC_RQD_PORT_CONTAINER:
  default: 8444
  exclusiveMinimum: 0
  title: Opencue Cuebot Grpc Rqd Port Container
  type: integer
OPENCUE_CUEBOT_GRPC_RQD_PORT_HOST:
  default: 8444
  exclusiveMinimum: 0
  title: Opencue Cuebot Grpc Rqd Port Host
  type: integer
OPENCUE_CUEBOT_PREBUILT_DOCKER_IMAGE:
  default: docker.io/opencue/cuebot
  title: Opencue Cuebot Prebuilt Docker Image
  type: string
OPENCUE_CUEBOT_USE_PREBUILT_DOCKER_IMAGE:
  default: true
  title: Opencue Cuebot Use Prebuilt Docker Image
  type: boolean
OPENCUE_CUEWEB_ADDITIONAL_ENV:
  additionalProperties: true
  default:
    NEXT_PUBLIC_AUTH_PROVIDER: ''
    NEXT_TELEMETRY_DISABLED: 1
  description: 'Disabling third-party authentication is not possible at the moment.
    Bug report pending: https://github.com/AcademySoftwareFoundation/OpenCue/issues/2133'
  title: Opencue Cueweb Additional Env
  type: object
OPENCUE_CUEWEB_JWT_SECRET:
  default: default-secret-key
  title: Opencue Cueweb Jwt Secret
  type: string
OPENCUE_CUEWEB_PORT_CONTAINER:
  default: 3000
  exclusiveMinimum: 0
  title: Opencue Cueweb Port Container
  type: integer
OPENCUE_CUEWEB_PORT_HOST:
  default: 3111
  exclusiveMinimum: 0
  title: Opencue Cueweb Port Host
  type: integer
OPENCUE_DB_INSTALL_DESTINATION:
  default: '{DOT_LANDSCAPES}/{LANDSCAPE}/{FEATURE}/data/opencue_db/postgresql'
  format: path
  title: Opencue Db Install Destination
  type: string
OPENCUE_DB_PGDATABASE:
  default: cuebot
  title: Opencue Db Pgdatabase
  type: string
OPENCUE_DB_PGHOST:
  default: opencue-db
  title: Opencue Db Pghost
  type: string
OPENCUE_DB_PGPASSWORD:
  default: cuebot_password
  title: Opencue Db Pgpassword
  type: string
OPENCUE_DB_PGUSER:
  default: cuebot
  title: Opencue Db Pguser
  type: string
OPENCUE_DB_PORT_CONTAINER:
  default: 5432
  exclusiveMinimum: 0
  title: Opencue Db Port Container
  type: integer
OPENCUE_DB_PORT_HOST:
  default: 5342
  exclusiveMinimum: 0
  title: Opencue Db Port Host
  type: integer
OPENCUE_DEPLOY_RQD:
  default: false
  title: Opencue Deploy Rqd
  type: boolean
OPENCUE_REST_GATEWAY_PORT_CONTAINER:
  default: 8448
  exclusiveMinimum: 0
  title: Opencue Rest Gateway Port Container
  type: integer
OPENCUE_REST_GATEWAY_PORT_HOST:
  default: 8448
  exclusiveMinimum: 0
  title: Opencue Rest Gateway Port Host
  type: integer
OPENCUE_RQD_DOCKER_IMAGE:
  default: docker.io/opencue/rqd
  title: Opencue Rqd Docker Image
  type: string
OPENCUE_WEB_PORT_CONTAINER:
  default: 3000
  exclusiveMinimum: 0
  title: Opencue Web Port Container
  type: integer
OPENCUE_WEB_PORT_HOST:
  default: 1234
  exclusiveMinimum: 0
  title: Opencue Web Port Host
  type: integer
compose_scope:
  default: default
  examples:
  - default
  - license_server
  - worker
  title: Compose Scope
  type: string
dnf_packages_blender_5:
  default:
  - libX11
  - libXrender
  - libXfixes
  - libXi
  - libxkbcommon
  - libSM
  - libGL
  items: {}
  title: Dnf Packages Blender 5
  type: array
dnf_packages_general:
  default:
  - which
  - file
  items: {}
  title: Dnf Packages General
  type: array
docker_compose:
  default: '{DOT_LANDSCAPES}/{LANDSCAPE}/{FEATURE}/docker_compose/docker-compose.yml'
  description: The path to the `docker-compose.yml` file.
  format: path
  title: Docker Compose
  type: string
docker_compose_yml:
  default: docker-compose.yml
  title: Docker Compose Yml
  type: string
enabled:
  default: true
  description: Whether the Feature is enabled or not.
  title: Enabled
  type: boolean
env:
  additionalProperties: true
  title: Env
  type: object
feature_name:
  default: OpenStudioLandscapes-OpenCue
  title: Feature Name
  type: string
group_name:
  default: OpenStudioLandscapes_OpenCue
  title: Group Name
  type: string
key_prefixes:
  default:
  - OpenStudioLandscapes_OpenCue
  items:
    type: string
  title: Key Prefixes
  type: array
local_bind_volumes:
  description: Here you can define Feature specific, arbitrary, absolute bind volume
    mappings.
  items:
    type: string
  title: Local Bind Volumes
  type: array
local_environment_variables:
  additionalProperties:
    type: string
  description: Here you can define Feature specific, arbitrary environment variables.
  title: Local Environment Variables
  type: object
opencue_cuebot:
  default: opencue-cuebot
  title: Opencue Cuebot
  type: string
opencue_cueweb:
  default: opencue-cueweb
  title: Opencue Cueweb
  type: string
opencue_db:
  default: opencue-db
  title: Opencue Db
  type: string
opencue_flyway:
  default: opencue-flyway
  title: Opencue Flyway
  type: string
opencue_rest_gateway:
  default: opencue-rest-gateway
  title: Opencue Rest Gateway
  type: string
opencue_rqd:
  default: opencue-rqd
  title: Opencue Rqd
  type: string
opencue_str:
  default: opencue
  title: Opencue Str
  type: string
pip_packages:
  default: []
  items: {}
  title: Pip Packages
  type: array
repository_branch:
  $ref: '#/$defs/Branches'
  default: master
  description: The branch of the OpenCue repository.
  examples:
  - master
repository_subdir:
  default: OpenCue
  title: Repository Subdir
  type: string
repository_url:
  default: https://github.com/AcademySoftwareFoundation/OpenCue.git
  format: uri
  maxLength: 2083
  minLength: 1
  title: Repository Url
  type: string

```

</details>


## Local Development/Unit Testing/Debugging

This is for isolated development, unit testing and debugging. Instead of the [`OpenStudioLandscapes-OpenCue/tree/main/src/OpenStudioLandscapes/OpenCue/definitions.py`](https://github.com/michimussato/OpenStudioLandscapes-OpenCue/tree/main/src/OpenStudioLandscapes/OpenCue/definitions.py), the accompanying [`OpenStudioLandscapes-OpenCue/tree/main/workspace.yaml`](https://github.com/michimussato/OpenStudioLandscapes-OpenCue/tree/main/workspace.yaml) loads the [`OpenStudioLandscapes-OpenCue/tree/main/src/OpenStudioLandscapes/OpenCue/_definitions_with_upstream_specs.py`](https://github.com/michimussato/OpenStudioLandscapes-OpenCue/tree/main/src/OpenStudioLandscapes/OpenCue/_definitions_with_upstream_specs.py) which also contains [`AssetSpec`](https://release-1-9-13.archive.dagster-docs.io/api/dagster/assets#dagster.AssetSpec) definitions for upstream dependencies as [external assets](https://release-1-9-13.archive.dagster-docs.io/guides/build/assets/external-assets).

```shell
# cd ./.features/OpenStudioLandscapes-OpenCue
python3.11 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip setuptools setuptools_scm wheel
pip install --editable .[dev]
dagster dev --workspace workspace.yaml
```

***

# External Resources

[![Logo OpenCue ](https://docs.opencue.io/assets/images/opencue_logo_with_text.png)](https://www.opencue.io/)

OpenCue is an official ASWF project and provides an open source render management system.

## Official Documentation

- [Homepage](https://www.opencue.io/)
- [Documentation](https://docs.opencue.io/docs/)]
- [Tutorials](https://docs.opencue.io/docs/tutorials)
- [Reference](https://docs.opencue.io/docs/reference)
- [User Guides](https://docs.opencue.io/docs/user-guides)
- [GitHub](https://github.com/AcademySoftwareFoundation/OpenCue)

## Components

- [OpenCue Overview](https://docs.opencue.io/docs/concepts/opencue-overview/)

***

# Community

| Feature                                   | GitHub                                                                                                                                                 | Discord                                                                      |
| ----------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------- |
| OpenStudioLandscapes                      | [https://github.com/michimussato/OpenStudioLandscapes](https://github.com/michimussato/OpenStudioLandscapes)                                           | [# openstudiolandscapes-general](https://discord.gg/F6bDRWsHac)              |
| OpenStudioLandscapes-Ayon                 | [https://github.com/michimussato/OpenStudioLandscapes-Ayon](https://github.com/michimussato/OpenStudioLandscapes-Ayon)                                 | [# openstudiolandscapes-ayon](https://discord.gg/gd6etWAF3v)                 |
| OpenStudioLandscapes-Dagster              | [https://github.com/michimussato/OpenStudioLandscapes-Dagster](https://github.com/michimussato/OpenStudioLandscapes-Dagster)                           | [# openstudiolandscapes-dagster](https://discord.gg/jwB3DwmKvs)              |
| OpenStudioLandscapes-Deadline-10-2        | [https://github.com/michimussato/OpenStudioLandscapes-Deadline-10-2](https://github.com/michimussato/OpenStudioLandscapes-Deadline-10-2)               | [# openstudiolandscapes-deadline-10-2](https://discord.gg/p2UjxHk4Y3)        |
| OpenStudioLandscapes-Deadline-10-2-Worker | [https://github.com/michimussato/OpenStudioLandscapes-Deadline-10-2-Worker](https://github.com/michimussato/OpenStudioLandscapes-Deadline-10-2-Worker) | [# openstudiolandscapes-deadline-10-2-worker](https://discord.gg/ttkbfkzUmf) |
| OpenStudioLandscapes-Flamenco             | [https://github.com/michimussato/OpenStudioLandscapes-Flamenco](https://github.com/michimussato/OpenStudioLandscapes-Flamenco)                         | [# openstudiolandscapes-flamenco](https://discord.gg/EPrX5fzBCf)             |
| OpenStudioLandscapes-Flamenco-Worker      | [https://github.com/michimussato/OpenStudioLandscapes-Flamenco-Worker](https://github.com/michimussato/OpenStudioLandscapes-Flamenco-Worker)           | [# openstudiolandscapes-flamenco-worker](https://discord.gg/Sa2zFqSc4p)      |
| OpenStudioLandscapes-Grafana              | [https://github.com/michimussato/OpenStudioLandscapes-Grafana](https://github.com/michimussato/OpenStudioLandscapes-Grafana)                           | [# openstudiolandscapes-grafana](https://discord.gg/gEDQ8vJWDb)              |
| OpenStudioLandscapes-Kitsu                | [https://github.com/michimussato/OpenStudioLandscapes-Kitsu](https://github.com/michimussato/OpenStudioLandscapes-Kitsu)                               | [# openstudiolandscapes-kitsu](https://discord.gg/6cc6mkReJ7)                |
| OpenStudioLandscapes-LikeC4               | [https://github.com/michimussato/OpenStudioLandscapes-LikeC4](https://github.com/michimussato/OpenStudioLandscapes-LikeC4)                             | [# openstudiolandscapes-likec4](https://discord.gg/qAYYsKYF6V)               |
| OpenStudioLandscapes-OpenCue              | [https://github.com/michimussato/OpenStudioLandscapes-OpenCue](https://github.com/michimussato/OpenStudioLandscapes-OpenCue)                           | [# openstudiolandscapes-opencue](https://discord.gg/3DdCZKkVyZ)              |
| OpenStudioLandscapes-OpenCue-Worker       | [https://github.com/michimussato/OpenStudioLandscapes-OpenCue-Worker](https://github.com/michimussato/OpenStudioLandscapes-OpenCue-Worker)             | [# openstudiolandscapes-opencue-worker](https://discord.gg/n9fxxhHa3V)       |
| OpenStudioLandscapes-RustDeskServer       | [https://github.com/michimussato/OpenStudioLandscapes-RustDeskServer](https://github.com/michimussato/OpenStudioLandscapes-RustDeskServer)             | [# openstudiolandscapes-rustdeskserver](https://discord.gg/nJ8Ffd2xY3)       |
| OpenStudioLandscapes-Syncthing            | [https://github.com/michimussato/OpenStudioLandscapes-Syncthing](https://github.com/michimussato/OpenStudioLandscapes-Syncthing)                       | [# openstudiolandscapes-syncthing](https://discord.gg/upb9MCqb3X)            |
| OpenStudioLandscapes-Template             | [https://github.com/michimussato/OpenStudioLandscapes-Template](https://github.com/michimussato/OpenStudioLandscapes-Template)                         | [# openstudiolandscapes-template](https://discord.gg/J59GYp3Wpy)             |
| OpenStudioLandscapes-VERT                 | [https://github.com/michimussato/OpenStudioLandscapes-VERT](https://github.com/michimussato/OpenStudioLandscapes-VERT)                                 | [# openstudiolandscapes-vert](https://discord.gg/EPrX5fzBCf)                 |
| OpenStudioLandscapes-filebrowser          | [https://github.com/michimussato/OpenStudioLandscapes-filebrowser](https://github.com/michimussato/OpenStudioLandscapes-filebrowser)                   | [# openstudiolandscapes-filebrowser](https://discord.gg/stzNsZBmwk)          |
| OpenStudioLandscapes-n8n                  | [https://github.com/michimussato/OpenStudioLandscapes-n8n](https://github.com/michimussato/OpenStudioLandscapes-n8n)                                   | [# openstudiolandscapes-n8n](https://discord.gg/yFYrG999wE)                  |

To follow up on the previous LinkedIn publications, visit:

- [OpenStudioLandscapes on LinkedIn](https://www.linkedin.com/company/106731439/).
- [Search for tag #OpenStudioLandscapes on LinkedIn](https://www.linkedin.com/search/results/all/?keywords=%23openstudiolandscapes).

***

Last changed: **2026-06-18 22:03:05 UTC**