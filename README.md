[![ Logo OpenStudioLandscapes ](https://github.com/michimussato/OpenStudioLandscapes/raw/main/media/images/logo128.png)](https://github.com/michimussato/OpenStudioLandscapes)

***

1. [Feature: OpenStudioLandscapes-OpenCue](#feature-openstudiolandscapes-opencue)
   1. [Brief](#brief)
   2. [Requirements](#requirements)
   3. [Install](#install)
      1. [This Feature](#this-feature)
   4. [Add to OpenStudioLandscapes](#add-to-openstudiolandscapes)
   5. [Testing](#testing)
      1. [pre-commit](#pre-commit)
      2. [nox](#nox)
   6. [Variables](#variables)
      1. [Feature Configs](#feature-configs)
2. [Community](#community)

***

This `README.md` was dynamically created with [OpenStudioLandscapesUtil-ReadmeGenerator](https://github.com/michimussato/OpenStudioLandscapesUtil-ReadmeGenerator).

***

# Feature: OpenStudioLandscapes-OpenCue

## Brief

This is an extension to the OpenStudioLandscapes ecosystem. The full documentation of OpenStudioLandscapes is available [here](https://github.com/michimussato/OpenStudioLandscapes).

You feel like writing your own Feature? Go and check out the [OpenStudioLandscapes-Template](https://github.com/michimussato/OpenStudioLandscapes-Template).

## Requirements

- `python-3.11`
- `OpenStudioLandscapes`

## Install

### This Feature

Clone this repository into `OpenStudioLandscapes/.features`:

```shell
# cd .features
git clone https://github.com/michimussato/OpenStudioLandscapes-OpenCue.git
```

Create `venv`:

```shell
# cd .features/OpenStudioLandscapes-OpenCue
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip setuptools
```

Configure `venv`:

```shell
# cd .features/OpenStudioLandscapes-OpenCue
pip install -e "../../[dev]"
pip install -e ".[dev]"
```

For more info see [VCS Support of pip](https://pip.pypa.io/en/stable/topics/vcs-support/).

## Add to OpenStudioLandscapes

Add the following code to `OpenStudioLandscapes.engine.features.FEATURES`:

```python
FEATURES.update(
    "OpenStudioLandscapes-OpenCue": {
        "enabled": True|False,
        # - from ENVIRONMENT VARIABLE (.env):
        #   "enabled": get_bool_env("ENV_VAR")
        # - combined:
        #   "enabled": True|False or get_bool_env(
        #       "OPENSTUDIOLANDSCAPES__ENABLE_FEATURE_OPENSTUDIOLANDSCAPES_OPENCUE"
        #   )
        "module": "OpenStudioLandscapes.OpenCue.definitions",
        "compose_scope": ComposeScope.DEFAULT,
        "feature_config": OpenStudioLandscapesConfig.DEFAULT,
    }
)
```

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

#### Generate Sphinx Documentation

```shell
nox -v --add-timestamp --session docs
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

## Variables

The following variables are being declared in `OpenStudioLandscapes.OpenCue.constants` and are accessible throughout the [`OpenStudioLandscapes-OpenCue`](https://github.com/michimussato/OpenStudioLandscapes-OpenCue/tree/main/src/OpenStudioLandscapes/OpenCue/constants.py) package.

| Variable           | Type   |
| :----------------- | :----- |
| `DOCKER_USE_CACHE` | `bool` |
| `ASSET_HEADER`     | `dict` |
| `FEATURE_CONFIGS`  | `dict` |

### Feature Configs

#### Feature Config: default

| Variable                                 | Type   | Value                                                                      |
| :--------------------------------------- | :----- | :------------------------------------------------------------------------- |
| `DOCKER_USE_CACHE`                       | `bool` | `False`                                                                    |
| `HOSTNAME_DB`                            | `str`  | `opencue-db`                                                               |
| `HOSTNAME_FLYWAY`                        | `str`  | `opencue-flyway`                                                           |
| `HOSTNAME_CUEBOT`                        | `str`  | `opencue-cuebot`                                                           |
| `HOSTNAME_RQD`                           | `str`  | `opencue-rqd`                                                              |
| `TELEPORT_ENTRY_POINT_HOST`              | `str`  | `{{HOSTNAME}}`                                                             |
| `TELEPORT_ENTRY_POINT_PORT`              | `str`  | `{{DAGSTER_DEV_PORT_HOST}}`                                                |
| `OPENCUE_CUEBOT_GRPC_CUE_PORT_HOST`      | `str`  | `8443`                                                                     |
| `OPENCUE_CUEBOT_GRPC_CUE_PORT_CONTAINER` | `str`  | `8443`                                                                     |
| `OPENCUE_CUEBOT_GRPC_RQD_PORT_HOST`      | `str`  | `8444`                                                                     |
| `OPENCUE_CUEBOT_GRPC_RQD_PORT_CONTAINER` | `str`  | `8444`                                                                     |
| `OPENCUE_DB_INSTALL_DESTINATION`         | `str`  | `{DOT_LANDSCAPES}/{LANDSCAPE}/OpenCue__OpenCue/data/opencue-db/postgresql` |
| `OPENCUE_DB_PORT_HOST`                   | `str`  | `5342`                                                                     |
| `OPENCUE_DB_PORT_CONTAINER`              | `str`  | `5432`                                                                     |
| `OPENCUE_DB_PGHOST`                      | `str`  | `opencue-db`                                                               |
| `OPENCUE_DB_PGDATABASE`                  | `str`  | `cuebot`                                                                   |
| `OPENCUE_DB_PGPASSWORD`                  | `str`  | `cuebot_password`                                                          |
| `OPENCUE_DB_PGUSER`                      | `str`  | `cuebot`                                                                   |

# Community

| Feature                             | GitHub                                                                                                                                     | Discord                                                                |
| ----------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------- |
| OpenStudioLandscapes                | [https://github.com/michimussato/OpenStudioLandscapes](https://github.com/michimussato/OpenStudioLandscapes)                               | [# openstudiolandscapes-general](https://discord.gg/F6bDRWsHac)        |
| OpenStudioLandscapes-Ayon           | [https://github.com/michimussato/OpenStudioLandscapes-Ayon](https://github.com/michimussato/OpenStudioLandscapes-Ayon)                     | [# openstudiolandscapes-ayon](https://discord.gg/gd6etWAF3v)           |
| OpenStudioLandscapes-Dagster        | [https://github.com/michimussato/OpenStudioLandscapes-Dagster](https://github.com/michimussato/OpenStudioLandscapes-Dagster)               | [# openstudiolandscapes-dagster](https://discord.gg/jwB3DwmKvs)        |
| OpenStudioLandscapes-Kitsu          | [https://github.com/michimussato/OpenStudioLandscapes-Kitsu](https://github.com/michimussato/OpenStudioLandscapes-Kitsu)                   | [# openstudiolandscapes-kitsu](https://discord.gg/6cc6mkReJ7)          |
| OpenStudioLandscapes-RustDeskServer | [https://github.com/michimussato/OpenStudioLandscapes-RustDeskServer](https://github.com/michimussato/OpenStudioLandscapes-RustDeskServer) | [# openstudiolandscapes-rustdeskserver](https://discord.gg/nJ8Ffd2xY3) |
| OpenStudioLandscapes-Template       | [https://github.com/michimussato/OpenStudioLandscapes-Template](https://github.com/michimussato/OpenStudioLandscapes-Template)             | [# openstudiolandscapes-template](https://discord.gg/J59GYp3Wpy)       |
| OpenStudioLandscapes-Twingate       | [https://github.com/michimussato/OpenStudioLandscapes-Twingate](https://github.com/michimussato/OpenStudioLandscapes-Twingate)             | [# openstudiolandscapes-twingate](https://discord.gg/tREYa6UNJf)       |

To follow up on the previous LinkedIn publications, visit:

- [OpenStudioLandscapes on LinkedIn](https://www.linkedin.com/company/106731439/).
- [Search for tag #OpenStudioLandscapes on LinkedIn](https://www.linkedin.com/search/results/all/?keywords=%23openstudiolandscapes).