import re
from typing import Any, Callable, Iterable, Mapping, Optional, Tuple

from dagster import AssetsDefinition, BackfillPolicy, PartitionsDefinition, multi_asset
from dagster._core.definitions.asset_dep import CoercibleToAssetDep
from dagster._core.definitions.asset_out import AssetOut
from dagster._core.types.dagster_type import Nothing


def sling_asset(
    *,
    stream_mappings: Iterable[Tuple[str, str, str]],
    deps: Optional[Iterable[CoercibleToAssetDep]] = None,
    name: Optional[str] = None,
    partitions_def: Optional[PartitionsDefinition] = None,
    backfill_policy: Optional[BackfillPolicy] = None,
    op_tags: Optional[Mapping[str, Any]] = None,
) -> Callable[..., AssetsDefinition]:
    outs = {}
    for _, target_object, _ in stream_mappings:
        output_name = re.sub(r"[^a-zA-Z0-9_]", "_", target_object)
        outs[output_name] = AssetOut(
            key=output_name,
            dagster_type=Nothing,
            is_required=False,
        )

    def inner(fn) -> AssetsDefinition:
        asset_definition = multi_asset(
            deps=deps,
            outs=outs,
            name=name,
            compute_kind="sling",
            partitions_def=partitions_def,
            can_subset=False,
            op_tags=op_tags,
            backfill_policy=backfill_policy,
        )(fn)

        return asset_definition

    return inner
