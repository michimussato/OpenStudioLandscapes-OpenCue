[![ Logo OpenStudioLandscapes ](https://github.com/michimussato/OpenStudioLandscapes/raw/main/media/images/logo128.png)](https://github.com/michimussato/OpenStudioLandscapes)

***

1. [Feature: OpenStudioLandscapes-OpenCue](#feature-openstudiolandscapes-opencue)
   1. [Brief](#brief)
   2. [Configuration](#configuration)
2. [Community](#community)
3. [Technical Reference](#technical-reference)
   1. [Requirements](#requirements)
   2. [Install](#install)
      1. [This Feature](#this-feature)
   3. [Testing](#testing)
      1. [pre-commit](#pre-commit)
      2. [nox](#nox)

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

## Configuration

OpenStudioLandscapes will search for a local config store. The default location is `~/.config/OpenStudioLandscapes/config-store/` but you can specify a different location if you need to.

A local config store location will be created if it doesn't exist, together with the `config.yml` files for each individual Feature.

> [!TIP]
> 
> The config store root will be initialized as a local Git
> controlled repository. This makes it easy to track changes
> you made to the `config.yml`.

> [!TIP]
> 
> To specify a config store location different than
> the default, you can do so be setting the environment variable
> `OPENSTUDIOLANDSCAPES__CONFIGSTORE_ROOT`:
> 
> ```shell
> OPENSTUDIOLANDSCAPES__CONFIGSTORE_ROOT="~/.config/OpenStudioLandscapes/my-custom-config-store"
> ```

The following settings are available in `OpenStudioLandscapes-OpenCue` and are based on [`OpenStudioLandscapes-OpenCue/tree/main/OpenStudioLandscapes/OpenCue/config/models.py`](https://github.com/michimussato/OpenStudioLandscapes-OpenCue/tree/main/OpenStudioLandscapes/OpenCue/config/models.py).

```yaml
# ===
# env
# ---
#
# Type: typing.Dict
# Base Class Info:
#     Required:
#         False
#     Description:
#         None
#     Default value:
#         None
# Description:
#     None
# Required:
#     False
# Examples:
#     None


# =============
# config_engine
# -------------
#
# Type: <class 'OpenStudioLandscapes.engine.config.models.ConfigEngine'>
# Base Class Info:
#     Required:
#         False
#     Description:
#         None
#     Default value:
#         None
# Description:
#     None
# Required:
#     False
# Examples:
#     None


# =============
# config_parent
# -------------
#
# Type: <class 'OpenStudioLandscapes.engine.config.models.FeatureBaseModel'>
# Base Class Info:
#     Required:
#         False
#     Description:
#         None
#     Default value:
#         None
# Description:
#     None
# Required:
#     False
# Examples:
#     None


# ============
# distribution
# ------------
#
# Type: <class 'importlib.metadata.Distribution'>
# Base Class Info:
#     Required:
#         False
#     Description:
#         None
#     Default value:
#         None
# Description:
#     None
# Required:
#     False
# Examples:
#     None


# ==========
# group_name
# ----------
#
# Type: <class 'str'>
# Base Class Info:
#     Required:
#         False
#     Description:
#         None
#     Default value:
#         None
# Description:
#     None
# Required:
#     False
# Examples:
#     None


# ============
# key_prefixes
# ------------
#
# Type: typing.List[str]
# Base Class Info:
#     Required:
#         False
#     Description:
#         None
#     Default value:
#         None
# Description:
#     None
# Required:
#     False
# Examples:
#     None


# =======
# enabled
# -------
#
# Type: <class 'bool'>
# Base Class Info:
#     Required:
#         False
#     Description:
#         Whether the Feature is enabled or not.
#     Default value:
#         True
# Description:
#     Whether the Feature is enabled or not.
# Required:
#     False
# Examples:
#     None


# =============
# compose_scope
# -------------
#
# Type: <class 'str'>
# Base Class Info:
#     Required:
#         False
#     Description:
#         None
#     Default value:
#         default
# Description:
#     None
# Required:
#     False
# Examples:
#     ['default', 'license_server', 'worker']


# ============
# feature_name
# ------------
#
# Type: <class 'str'>
# Base Class Info:
#     Required:
#         True
#     Description:
#         The name of the feature. It is derived from the `OpenStudioLandscapes.<Feature>.dist` attribute.
#     Default value:
#         PydanticUndefined
# Description:
#     None
# Required:
#     False
# Examples:
#     None
feature_name: OpenStudioLandscapes-OpenCue


# ==============
# docker_compose
# --------------
#
# Type: <class 'pathlib.Path'>
# Base Class Info:
#     Required:
#         False
#     Description:
#         The path to the `docker-compose.yml` file.
#     Default value:
#         {DOT_LANDSCAPES}/{LANDSCAPE}/{FEATURE}/docker_compose/docker-compose.yml
# Description:
#     The path to the `docker-compose.yml` file.
# Required:
#     False
# Examples:
#     None


# ===========
# opencue_str
# -----------
#
# Type: <class 'str'>
# Description:
#     None
# Required:
#     False
# Examples:
#     None
opencue_str: opencue


# ==========
# opencue_db
# ----------
#
# Type: <class 'str'>
# Description:
#     None
# Required:
#     False
# Examples:
#     None
opencue_db: opencue-db


# ==============
# opencue_flyway
# --------------
#
# Type: <class 'str'>
# Description:
#     None
# Required:
#     False
# Examples:
#     None
opencue_flyway: opencue-flyway


# ==============
# opencue_cuebot
# --------------
#
# Type: <class 'str'>
# Description:
#     None
# Required:
#     False
# Examples:
#     None
opencue_cuebot: opencue-cuebot


# ==============
# opencue_cueweb
# --------------
#
# Type: <class 'str'>
# Description:
#     None
# Required:
#     False
# Examples:
#     None
opencue_cueweb: opencue-cueweb


# ====================
# opencue_rest_gateway
# --------------------
#
# Type: <class 'str'>
# Description:
#     None
# Required:
#     False
# Examples:
#     None
opencue_rest_gateway: opencue-rest-gateway


# ===========
# opencue_rqd
# -----------
#
# Type: <class 'str'>
# Description:
#     None
# Required:
#     False
# Examples:
#     None
opencue_rqd: opencue-rqd


# ==============
# repository_url
# --------------
#
# Type: <class 'pydantic.networks.HttpUrl'>
# Description:
#     None
# Required:
#     False
# Examples:
#     None
repository_url: https://github.com/AcademySoftwareFoundation/OpenCue.git


# =================
# repository_branch
# -----------------
#
# Type: <enum 'Branches'>
# Description:
#     The branch of the OpenCue repository.
# Required:
#     False
# Examples:
#     ['master']
repository_branch: master


# =================
# repository_subdir
# -----------------
#
# Type: <class 'str'>
# Description:
#     None
# Required:
#     False
# Examples:
#     None
repository_subdir: OpenCue


# ==================
# docker_compose_yml
# ------------------
#
# Type: <class 'str'>
# Description:
#     None
# Required:
#     False
# Examples:
#     None
docker_compose_yml: docker-compose.yml


# ==================
# OPENCUE_DEPLOY_RQD
# ------------------
#
# Type: <class 'bool'>
# Description:
#     None
# Required:
#     False
# Examples:
#     None
OPENCUE_DEPLOY_RQD: false


# ========================================
# OPENCUE_CUEBOT_USE_PREBUILT_DOCKER_IMAGE
# ----------------------------------------
#
# Type: <class 'bool'>
# Description:
#     None
# Required:
#     False
# Examples:
#     None
OPENCUE_CUEBOT_USE_PREBUILT_DOCKER_IMAGE: true


# ====================================
# OPENCUE_CUEBOT_PREBUILT_DOCKER_IMAGE
# ------------------------------------
#
# Type: <class 'str'>
# Description:
#     None
# Required:
#     False
# Examples:
#     None
OPENCUE_CUEBOT_PREBUILT_DOCKER_IMAGE: docker.io/opencue/cuebot


# ========================
# OPENCUE_CUEWEB_PORT_HOST
# ------------------------
#
# Type: <class 'int'>
# Description:
#     None
# Required:
#     False
# Examples:
#     None
OPENCUE_CUEWEB_PORT_HOST: 3100


# =============================
# OPENCUE_CUEWEB_PORT_CONTAINER
# -----------------------------
#
# Type: <class 'int'>
# Description:
#     None
# Required:
#     False
# Examples:
#     None
OPENCUE_CUEWEB_PORT_CONTAINER: 3000


# =========================
# OPENCUE_CUEWEB_JWT_SECRET
# -------------------------
#
# Type: <class 'str'>
# Description:
#     None
# Required:
#     False
# Examples:
#     None
OPENCUE_CUEWEB_JWT_SECRET: default-secret-key


# ==============================
# OPENCUE_REST_GATEWAY_PORT_HOST
# ------------------------------
#
# Type: <class 'int'>
# Description:
#     None
# Required:
#     False
# Examples:
#     None
OPENCUE_REST_GATEWAY_PORT_HOST: 8448


# ===================================
# OPENCUE_REST_GATEWAY_PORT_CONTAINER
# -----------------------------------
#
# Type: <class 'int'>
# Description:
#     None
# Required:
#     False
# Examples:
#     None
OPENCUE_REST_GATEWAY_PORT_CONTAINER: 8448


# =====================
# OPENCUE_WEB_PORT_HOST
# ---------------------
#
# Type: <class 'int'>
# Description:
#     None
# Required:
#     False
# Examples:
#     None
OPENCUE_WEB_PORT_HOST: 1234


# ==========================
# OPENCUE_WEB_PORT_CONTAINER
# --------------------------
#
# Type: <class 'int'>
# Description:
#     None
# Required:
#     False
# Examples:
#     None
OPENCUE_WEB_PORT_CONTAINER: 3000


# =================================
# OPENCUE_CUEBOT_GRPC_CUE_PORT_HOST
# ---------------------------------
#
# Type: <class 'int'>
# Description:
#     None
# Required:
#     False
# Examples:
#     None
OPENCUE_CUEBOT_GRPC_CUE_PORT_HOST: 8443


# ======================================
# OPENCUE_CUEBOT_GRPC_CUE_PORT_CONTAINER
# --------------------------------------
#
# Type: <class 'int'>
# Description:
#     None
# Required:
#     False
# Examples:
#     None
OPENCUE_CUEBOT_GRPC_CUE_PORT_CONTAINER: 8443


# =================================
# OPENCUE_CUEBOT_GRPC_RQD_PORT_HOST
# ---------------------------------
#
# Type: <class 'int'>
# Description:
#     None
# Required:
#     False
# Examples:
#     None
OPENCUE_CUEBOT_GRPC_RQD_PORT_HOST: 8444


# ======================================
# OPENCUE_CUEBOT_GRPC_RQD_PORT_CONTAINER
# --------------------------------------
#
# Type: <class 'int'>
# Description:
#     None
# Required:
#     False
# Examples:
#     None
OPENCUE_CUEBOT_GRPC_RQD_PORT_CONTAINER: 8444


# ==============================
# OPENCUE_DB_INSTALL_DESTINATION
# ------------------------------
#
# Type: <class 'pathlib.Path'>
# Description:
#     None
# Required:
#     False
# Examples:
#     None
OPENCUE_DB_INSTALL_DESTINATION: '{DOT_LANDSCAPES}/{LANDSCAPE}/{FEATURE}/data/opencue_db/postgresql'


# ====================
# OPENCUE_DB_PORT_HOST
# --------------------
#
# Type: <class 'int'>
# Description:
#     None
# Required:
#     False
# Examples:
#     None
OPENCUE_DB_PORT_HOST: 5342


# =========================
# OPENCUE_DB_PORT_CONTAINER
# -------------------------
#
# Type: <class 'int'>
# Description:
#     None
# Required:
#     False
# Examples:
#     None
OPENCUE_DB_PORT_CONTAINER: 5432


# =================
# OPENCUE_DB_PGHOST
# -----------------
#
# Type: <class 'str'>
# Description:
#     None
# Required:
#     False
# Examples:
#     None
OPENCUE_DB_PGHOST: opencue-db


# =====================
# OPENCUE_DB_PGDATABASE
# ---------------------
#
# Type: <class 'str'>
# Description:
#     None
# Required:
#     False
# Examples:
#     None
OPENCUE_DB_PGDATABASE: cuebot


# =================
# OPENCUE_DB_PGUSER
# -----------------
#
# Type: <class 'str'>
# Description:
#     None
# Required:
#     False
# Examples:
#     None
OPENCUE_DB_PGUSER: cuebot


# =====================
# OPENCUE_DB_PGPASSWORD
# ---------------------
#
# Type: <class 'str'>
# Description:
#     None
# Required:
#     False
# Examples:
#     None
OPENCUE_DB_PGPASSWORD: cuebot_password
```

***

***

# Community

| Feature                              | GitHub                                                                                                                                       | Discord                                                                 |
| ------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------- |
| OpenStudioLandscapes                 | [https://github.com/michimussato/OpenStudioLandscapes](https://github.com/michimussato/OpenStudioLandscapes)                                 | [# openstudiolandscapes-general](https://discord.gg/F6bDRWsHac)         |
| OpenStudioLandscapes-Ayon            | [https://github.com/michimussato/OpenStudioLandscapes-Ayon](https://github.com/michimussato/OpenStudioLandscapes-Ayon)                       | [# openstudiolandscapes-ayon](https://discord.gg/gd6etWAF3v)            |
| OpenStudioLandscapes-Dagster         | [https://github.com/michimussato/OpenStudioLandscapes-Dagster](https://github.com/michimussato/OpenStudioLandscapes-Dagster)                 | [# openstudiolandscapes-dagster](https://discord.gg/jwB3DwmKvs)         |
| OpenStudioLandscapes-Flamenco        | [https://github.com/michimussato/OpenStudioLandscapes-Flamenco](https://github.com/michimussato/OpenStudioLandscapes-Flamenco)               | [# openstudiolandscapes-flamenco](https://discord.gg/EPrX5fzBCf)        |
| OpenStudioLandscapes-Flamenco-Worker | [https://github.com/michimussato/OpenStudioLandscapes-Flamenco-Worker](https://github.com/michimussato/OpenStudioLandscapes-Flamenco-Worker) | [# openstudiolandscapes-flamenco-worker](https://discord.gg/Sa2zFqSc4p) |
| OpenStudioLandscapes-Kitsu           | [https://github.com/michimussato/OpenStudioLandscapes-Kitsu](https://github.com/michimussato/OpenStudioLandscapes-Kitsu)                     | [# openstudiolandscapes-kitsu](https://discord.gg/6cc6mkReJ7)           |
| OpenStudioLandscapes-RustDeskServer  | [https://github.com/michimussato/OpenStudioLandscapes-RustDeskServer](https://github.com/michimussato/OpenStudioLandscapes-RustDeskServer)   | [# openstudiolandscapes-rustdeskserver](https://discord.gg/nJ8Ffd2xY3)  |
| OpenStudioLandscapes-Template        | [https://github.com/michimussato/OpenStudioLandscapes-Template](https://github.com/michimussato/OpenStudioLandscapes-Template)               | [# openstudiolandscapes-template](https://discord.gg/J59GYp3Wpy)        |
| OpenStudioLandscapes-VERT            | [https://github.com/michimussato/OpenStudioLandscapes-VERT](https://github.com/michimussato/OpenStudioLandscapes-VERT)                       | [# openstudiolandscapes-twingate](https://discord.gg/FYaFRUwbYr)        |

To follow up on the previous LinkedIn publications, visit:

- [OpenStudioLandscapes on LinkedIn](https://www.linkedin.com/company/106731439/).
- [Search for tag #OpenStudioLandscapes on LinkedIn](https://www.linkedin.com/search/results/all/?keywords=%23openstudiolandscapes).

***

# Technical Reference

## Requirements

- `python-3.11`
- `OpenStudioLandscapes`

## Install

### This Feature

Clone this repository into `OpenStudioLandscapes/.features` (assuming the current working directory to be the Git repository root `./OpenStudioLandscapes`):

```shell
git -C ./.features clone https://github.com/michimussato/OpenStudioLandscapes-OpenCue.git
```

Install into OpenStudioLandscapes `venv` (`./OpenStudioLandscapes/.venv`):

```shell
source .venv/bin/activate
python -m pip install --upgrade pip setuptools
pip install -e "./.features/OpenStudioLandscapes-OpenCue[dev]"
pip install -e ".[dev]"
```

For more info see [VCS Support of pip](https://pip.pypa.io/en/stable/topics/vcs-support/).

## Testing

### pre-commit

- https://pre-commit.com
- https://pre-commit.com/hooks.html

```shell
pre-commit install
```

### nox

#### Generate Report

```shell
nox --no-error-on-missing-interpreters --report .nox/nox-report.json
```

#### Re-Generate this README

```shell
nox -v --add-timestamp --session readme
```

#### pylint

```shell
nox -v --add-timestamp --session lint
```

##### pylint: disable=redefined-outer-name

- [`W0621`](https://pylint.pycqa.org/en/latest/user_guide/messages/warning/redefined-outer-name.html): Due to Dagsters way of piping arguments into assets.

#### SBOM

Acronym for Software Bill of Materials

```shell
nox -v --add-timestamp --session sbom
```

We create the following SBOMs:

- [`cyclonedx-bom`](https://pypi.org/project/cyclonedx-bom/)
- [`pipdeptree`](https://pypi.org/project/pipdeptree/) (Dot)
- [`pipdeptree`](https://pypi.org/project/pipdeptree/) (Mermaid)

SBOMs for the different Python interpreters defined in [`.noxfile.VERSIONS`](https://github.com/michimussato/OpenStudioLandscapes-OpenCue/tree/main/noxfile.py) will be created in the [`.sbom`](https://github.com/michimussato/OpenStudioLandscapes-OpenCue/tree/main/.sbom) directory of this repository.

- `cyclone-dx`
- `pipdeptree` (Dot)
- `pipdeptree` (Mermaid)

Currently, the following Python interpreters are enabled for testing:

- `python3.11`

***

Last changed: **2025-12-23 22:40:08 UTC**