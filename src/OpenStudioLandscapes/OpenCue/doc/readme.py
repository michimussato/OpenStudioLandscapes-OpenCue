import textwrap

import snakemd


"""
[CueGUI](https://docs.opencue.io/docs/getting-started/installing-cuegui/)

```
# https://docs.opencue.io/docs/getting-started/installing-cuegui/#option-1-installing-from-pypi
python3.11 -m .venv
source .venv/bin/activate
pip install --upgrade pip

pip install opencue-cuegui
# cuebot:CONFIG.OPENCUE_CUEBOT_GRPC_CUE_PORT_HOST
# localhost:8443
# CUEBOT_HOSTS=localhost:8443 cuegui
CUEBOT_HOSTS=$CUEBOT_HOSTNAME_OR_IP cuegui
```

[CueSubmit](https://docs.opencue.io/docs/getting-started/installing-cuesubmit/)

```
# https://docs.opencue.io/docs/getting-started/installing-cuegui/#option-1-installing-from-pypi
python3.11 -m .venv
source .venv/bin/activate
pip install --upgrade pip

pip install opencue-cuesubmit
# cuebot:CONFIG.OPENCUE_CUEBOT_GRPC_CUE_PORT_HOST
# localhost:8443
# CUEBOT_HOSTS=localhost:8443 cuesubmit
CUEBOT_HOSTS=$CUEBOT_HOSTNAME_OR_IP cuesubmit
```

[RQD](https://docs.opencue.io/docs/getting-started/deploying-rqd/)
- [Docker](https://docs.opencue.io/docs/getting-started/deploying-rqd/#option-1-downloading-and-running-rqd-from-dockerhub)
- [Pypi](https://docs.opencue.io/docs/getting-started/deploying-rqd/#option-3-installing-from-pypi)

```
# https://docs.opencue.io/docs/getting-started/installing-cuegui/#option-1-installing-from-pypi
python3.11 -m .venv
source .venv/bin/activate
pip install --upgrade pip

pip install opencue-rqd
# cuebot:CONFIG.OPENCUE_CUEBOT_GRPC_CUE_PORT_HOST
# localhost:8443
# CUEBOT_HOSTNAME=localhost rqd
CUEBOT_HOSTNAME=CUEBOT_HOSTNAME rqd
```
"""


# Todo:
#  - [ ] RQD is actually the worker. Break it out to separate compose scope.


def readme_feature(
    doc: snakemd.Document,
    main_header: str,
) -> snakemd.Document:

    # # Some Specific information
    #
    # doc.add_heading(
    #     text=main_header,
    #     level=1,
    # )
    #
    # # Logo
    #
    # doc.add_paragraph(
    #     snakemd.Inline(
    #         text=textwrap.dedent(
    #             """\
    #             Logo Ayon\
    #             """
    #         ),
    #         image={
    #             "Ayon": "https://ynput.io/wp-content/uploads/2023/04/ayon-whiteg-dot.svg",
    #         }["Ayon"],
    #         link="https://ynput.io/ayon/",
    #     ).__str__()
    # )
    #
    # doc.add_paragraph(
    #     text=textwrap.dedent(
    #         """\
    #         Ayon is written and maintained by Ynput, a company based
    #         in Czech Republic:\
    #         """
    #     )
    # )
    #
    # # Logo
    #
    # doc.add_paragraph(
    #     snakemd.Inline(
    #         text=textwrap.dedent(
    #             """\
    #             Logo Ynput\
    #             """
    #         ),
    #         image={
    #             "Ynput": "https://ynput.io/wp-content/uploads/2022/09/ynput-logo-small-bg.svg",
    #         }["Ynput"],
    #         link="https://ynput.io",
    #     ).__str__()
    # )
    #
    # doc.add_paragraph(
    #     text=textwrap.dedent(
    #         """\
    #         Ynput offers different versions of Ayon\
    #         """
    #     )
    # )
    #
    # doc.add_unordered_list(
    #     [
    #         "Community",
    #         "Pro Cloud",
    #         "Studio Cloud",
    #     ]
    # )
    #
    # doc.add_paragraph(
    #     text=textwrap.dedent(
    #         """\
    #         `OpenStudioLandscapes-Ayon` is based on the [Community](https://ynput.io/ayon/pricing/)
    #         version provided by their own Docker image:\
    #         """
    #     )
    # )
    #
    # doc.add_unordered_list(
    #     [
    #         "[https://github.com/ynput/ayon-docker](https://github.com/ynput/ayon-docker)",
    #     ]
    # )
    #
    # doc.add_heading(
    #     text="Official Documentation",
    #     level=2,
    # )
    #
    # doc.add_unordered_list(
    #     [
    #         "[Features](https://docs.ayon.dev/features)",
    #         "[User Docs](https://docs.ayon.dev/docs/artist_getting_started)",
    #         "[Admin Docs](https://docs.ayon.dev/docs/system_introduction)",
    #         "[Dev Docs](https://docs.ayon.dev/docs/dev_introduction)",
    #     ]
    # )
    #
    # doc.add_heading(
    #     text="Dev Resources",
    #     level=3,
    # )
    #
    # doc.add_unordered_list(
    #     [
    #         "[REST API Docs](https://docs.ayon.dev/api)",
    #         "[GraphQL API Explorer](https://playground.ayon.app/explorer)",
    #         "[Python API Docs](https://docs.ayon.dev/ayon-python-api)",
    #         "[C++ API Docs](https://docs.ayon.dev/ayon-cpp-api)",
    #         "[USD Resolver Docs](https://docs.ayon.dev/ayon-usd-resolver)",
    #         "[Frontend React Components](https://components.ayon.dev)",
    #     ]
    # )

    return doc


if __name__ == "__main__":
    pass
