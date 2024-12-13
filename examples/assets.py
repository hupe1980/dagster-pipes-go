import shutil

from dagster import (
    asset,
    asset_check,
    AssetExecutionContext,
    AssetCheckExecutionContext,
    AssetCheckResult,
    MaterializeResult,
    PipesSubprocessClient,
    file_relative_path,
)

@asset
def materialize_subprocess(
    context: AssetExecutionContext, pipes_subprocess_client: PipesSubprocessClient
) -> MaterializeResult:
    cmd = [shutil.which("go"), "run", file_relative_path(__file__, "materialize/main.go")]
    result = pipes_subprocess_client.run(
        command=cmd, 
        context=context,
    )

    return result.get_materialize_result()

@asset_check(asset=materialize_subprocess)
def check_subprocess(
    context: AssetCheckExecutionContext, pipes_subprocess_client: PipesSubprocessClient
) -> AssetCheckResult:
    cmd = [shutil.which("go"), "run", file_relative_path(__file__, "check/main.go")]
    result = pipes_subprocess_client.run(
        command=cmd, 
        context=context.op_execution_context,
    )

    return result.get_asset_check_result()