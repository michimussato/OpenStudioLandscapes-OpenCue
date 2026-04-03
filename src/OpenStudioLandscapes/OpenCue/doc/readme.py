import textwrap

import snakemd

"""
[CueGUI](https://docs.opencue.io/docs/getting-started/installing-cuegui/)

Requirements:
sh: line 1: /usr/bin/xterm: No such file or directory
- xterm

xterm: cannot load font "-misc-fixed-medium-r-semicondensed--13-120-75-75-c-60-iso10646-1"
https://forum.manjaro.org/t/xterm-missing-default-fonts/35113/2
- 

```
# https://docs.opencue.io/docs/getting-started/installing-cuegui/#option-1-installing-from-pypi
python3.11 -m venv py311_cuegui
source py311_cuegui/bin/activate
pip install --upgrade pip setuptools wheel opencue-cuegui

# pip install --upgrade opencue-cuegui
# cuebot:CONFIG.OPENCUE_CUEBOT_GRPC_CUE_PORT_HOST
# localhost:8443
# CUEBOT_HOSTS=localhost:8443 cuegui
CUEBOT_HOSTS=$CUEBOT_HOSTNAME_OR_IP cuegui
```

[CueSubmit](https://docs.opencue.io/docs/getting-started/installing-cuesubmit/)

```
# https://docs.opencue.io/docs/getting-started/installing-cuegui/#option-1-installing-from-pypi
python3.11 -m venv py311_cuesubmit
source py311_cuesubmit/bin/activate
pip install --upgrade pip setuptools wheel opencue-cuegui opencue-cuesubmit

# pip install opencue-cuesubmit
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
python3.11 -m venv py311_rqd
source py311_rqd/bin/activate
pip install --upgrade pip setuptools wheel opencue-cuegui opencue-rqd

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
