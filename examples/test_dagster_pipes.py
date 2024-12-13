from dagster import (
    AssetKey,
    AssetCheckSeverity,
    DataVersion,
    PipesSubprocessClient,
    build_asset_context,
    TextMetadataValue,
)

from .assets import materialize_subprocess, check_subprocess

def test_materialize_result():
    with build_asset_context(resources={"pipes_subprocess_client": PipesSubprocessClient()}) as context:
        result = materialize_subprocess(context)

        assert result.asset_key == AssetKey(['materialize_subprocess'])
        assert result.data_version == DataVersion(value='1.0')
        assert result.metadata["foo"] == TextMetadataValue(text='bar')

def test_check_result():
    with build_asset_context(resources={"pipes_subprocess_client": PipesSubprocessClient()}) as context:
        result = check_subprocess(context)

        assert result.asset_key == AssetKey(['materialize_subprocess'])
        assert result.check_name == "check_subprocess"
        assert result.severity == AssetCheckSeverity.ERROR
        assert result.passed == False
        assert result.metadata["foo"] == TextMetadataValue(text='bar')
