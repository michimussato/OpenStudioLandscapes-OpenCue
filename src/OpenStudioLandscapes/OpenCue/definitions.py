from dagster import (
    Definitions,
    load_assets_from_modules,
)

import OpenStudioLandscapes.OpenCue.assets
import OpenStudioLandscapes.OpenCue.constants

assets = load_assets_from_modules(
    modules=[OpenStudioLandscapes.OpenCue.assets],
)

constants = load_assets_from_modules(
    modules=[OpenStudioLandscapes.OpenCue.constants],
)


defs = Definitions(
    assets=[
        *assets,
        *constants,
    ],
)
