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

"""
Docs:
- https://docs.opencue.io/docs

Tutorials:
- https://docs.opencue.io/docs/tutorials

Reference:
- https://docs.opencue.io/docs/reference

User Guides:
- https://docs.opencue.io/docs/user-guides
"""


# Todo:
#  - [ ] RQD is actually the worker. Break it out to separate compose scope.


def readme_feature(
    doc: snakemd.Document,
    main_header: str,
) -> snakemd.Document:

    # Some Specific information

    doc.add_heading(
        text=main_header,
        level=1,
    )

    # Logo

    doc.add_paragraph(
        snakemd.Inline(
            text=textwrap.dedent("""\
                Logo OpenCue\
                """),
            image="https://docs.opencue.io/assets/images/opencue_logo_with_text.png",
            link="https://www.opencue.io/",
        ).__str__()
    )

    doc.add_paragraph(text=textwrap.dedent("""\
            OpenCue is an official ASWF project and provides 
            an open source render management system.\
            """))

    doc.add_heading(
        text="Official Documentation",
        level=2,
    )

    doc.add_unordered_list(
        [
            "[Homepage](https://www.opencue.io/)",
            "[Documentation](https://docs.opencue.io/docs/)]",
            "[Tutorials](https://docs.opencue.io/docs/tutorials)",
            "[Reference](https://docs.opencue.io/docs/reference)",
            "[User Guides](https://docs.opencue.io/docs/user-guides)",
            "[GitHub](https://github.com/AcademySoftwareFoundation/OpenCue)",
        ]
    )

    doc.add_heading(
        text="Components",
        level=2,
    )

    doc.add_unordered_list(
        [
            "[OpenCue Overview](https://docs.opencue.io/docs/concepts/opencue-overview/)",
        ]
    )

    doc.add_horizontal_rule()

    return doc


if __name__ == "__main__":
    pass
