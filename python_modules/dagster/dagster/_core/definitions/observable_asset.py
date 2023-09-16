from typing import Sequence

from dagster._core.definitions.asset_spec import (
    SYSTEM_METADATA_KEY_ASSET_VARIETAL,
    AssetSpec,
    AssetVarietal,
    ObservableAssetSpec,
)
from dagster._core.definitions.decorators.asset_decorator import multi_asset


def create_unexecutable_observable_assets_def(specs: Sequence[ObservableAssetSpec]):
    @multi_asset(
        specs=[
            AssetSpec(
                key=spec.key,
                description=spec.description,
                group_name=spec.group_name,
                metadata={
                    **(spec.metadata or {}),
                    **{SYSTEM_METADATA_KEY_ASSET_VARIETAL: AssetVarietal.UNEXECUTABLE.value},
                },
                deps=[dep.asset_key for dep in spec.deps],
            )
            for spec in specs
        ]
    )
    def an_asset() -> None:
        raise NotImplementedError()

    return an_asset