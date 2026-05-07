from dagster import (
    Definitions,
    load_assets_from_modules,
)

import OpenStudioLandscapes.OpenCue.assets
from OpenStudioLandscapes.OpenCue import *

LOGGER.info(f"Loading {dist.name} assets...")

assets_base = load_assets_from_modules(
    modules=[OpenStudioLandscapes.OpenCue.assets],
)


defs = Definitions(
    assets=[
        *assets_base,
    ],
)
