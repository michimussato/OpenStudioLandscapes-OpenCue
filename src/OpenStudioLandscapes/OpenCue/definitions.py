from dagster import (
    Definitions,
    load_assets_from_modules,
)

import OpenStudioLandscapes.OpenCue.assets

assets = load_assets_from_modules(
    modules=[OpenStudioLandscapes.OpenCue.assets],
)


defs = Definitions(
    assets=[
        *assets,
    ],
)
