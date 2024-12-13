import warnings
from dagster import ExperimentalWarning

warnings.filterwarnings("ignore", category=ExperimentalWarning)

from dagster import (
    Definitions,
    PipesSubprocessClient,
    PipesEnvContextInjector,
)

from .assets import materialize_subprocess, check_subprocess

defs = Definitions(
    assets=[materialize_subprocess],
    asset_checks=[check_subprocess],
    resources={"pipes_subprocess_client": PipesSubprocessClient(
        context_injector=PipesEnvContextInjector(),
    )},
)