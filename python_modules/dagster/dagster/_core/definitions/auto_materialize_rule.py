import datetime
from abc import ABC, abstractmethod, abstractproperty
from collections import defaultdict
from typing import (
    TYPE_CHECKING,
    AbstractSet,
    Dict,
    Iterable,
    Mapping,
    NamedTuple,
    Optional,
    Sequence,
    Set,
)

import pytz

import dagster._check as check
from dagster._annotations import experimental, public
from dagster._core.definitions.asset_subset import AssetSubset
from dagster._core.definitions.auto_materialize_rule_evaluation import (
    AutoMaterializeDecisionType,
    AutoMaterializeRuleSnapshot,
    ParentUpdatedRuleEvaluationData,
    WaitingOnAssetsRuleEvaluationData,
)
from dagster._core.definitions.events import AssetKey, AssetKeyPartitionKey
from dagster._core.definitions.freshness_based_auto_materialize import (
    freshness_evaluation_results_for_asset_key,
)
from dagster._core.definitions.multi_dimensional_partitions import MultiPartitionsDefinition
from dagster._core.definitions.time_window_partitions import (
    get_time_partitions_def,
)
from dagster._core.storage.dagster_run import RunsFilter
from dagster._core.storage.tags import AUTO_MATERIALIZE_TAG
from dagster._serdes.serdes import (
    whitelist_for_serdes,
)
from dagster._utils.schedules import (
    cron_string_iterator,
    is_valid_cron_string,
    reverse_cron_string_iterator,
)

from .asset_condition_evaluation_context import AssetConditionEvaluationContext
from .asset_graph import sort_key_for_asset_partition

if TYPE_CHECKING:
    from dagster._core.definitions.asset_condition import AssetConditionResult


class AutoMaterializeRule(ABC):
    """An AutoMaterializeRule defines a bit of logic which helps determine if a materialization
    should be kicked off for a given asset partition.

    Each rule can have one of two decision types, `MATERIALIZE` (indicating that an asset partition
    should be materialized) or `SKIP` (indicating that the asset partition should not be
    materialized).

    Materialize rules are evaluated first, and skip rules operate over the set of candidates that
    are produced by the materialize rules. Other than that, there is no ordering between rules.
    """

    @abstractproperty
    def decision_type(self) -> AutoMaterializeDecisionType:
        """The decision type of the rule (either `MATERIALIZE` or `SKIP`)."""
        ...

    @abstractproperty
    def description(self) -> str:
        """A human-readable description of this rule. As a basic guideline, this string should
        complete the sentence: 'Indicates an asset should be (materialize/skipped) when ____'.
        """
        ...

    @abstractmethod
    def evaluate_for_asset(
        self, context: AssetConditionEvaluationContext
    ) -> "AssetConditionResult":
        """The core evaluation function for the rule. This function takes in a context object and
        returns a mapping from evaluated rules to the set of asset partitions that the rule applies
        to.
        """
        ...

    @public
    @staticmethod
    def materialize_on_required_for_freshness() -> "MaterializeOnRequiredForFreshnessRule":
        """Materialize an asset partition if it is required to satisfy a freshness policy of this
        asset or one of its downstream assets.

        Note: This rule has no effect on partitioned assets.
        """
        return MaterializeOnRequiredForFreshnessRule()

    @public
    @staticmethod
    def materialize_on_cron(
        cron_schedule: str, timezone: str = "UTC", all_partitions: bool = False
    ) -> "MaterializeOnCronRule":
        """Materialize an asset partition if it has not been materialized since the latest cron
        schedule tick. For assets with a time component to their partitions_def, this rule will
        request all partitions that have been missed since the previous tick.

        Args:
            cron_schedule (str): A cron schedule string (e.g. "`0 * * * *`") indicating the ticks for
                which this rule should fire.
            timezone (str): The timezone in which this cron schedule should be evaluated. Defaults
                to "UTC".
            all_partitions (bool): If True, this rule fires for all partitions of this asset on each
                cron tick. If False, this rule fires only for the last partition of this asset.
                Defaults to False.
        """
        check.param_invariant(
            is_valid_cron_string(cron_schedule), "cron_schedule", "must be a valid cron string"
        )
        check.param_invariant(
            timezone in pytz.all_timezones_set, "timezone", "must be a valid timezone"
        )
        return MaterializeOnCronRule(
            cron_schedule=cron_schedule, timezone=timezone, all_partitions=all_partitions
        )

    @public
    @staticmethod
    def materialize_on_parent_updated(
        updated_parent_filter: Optional["AutoMaterializeAssetPartitionsFilter"] = None,
    ) -> "MaterializeOnParentUpdatedRule":
        """Materialize an asset partition if one of its parents has been updated more recently
        than it has.

        Note: For time-partitioned or dynamic-partitioned assets downstream of an unpartitioned
        asset, this rule will only fire for the most recent partition of the downstream.

        Args:
            updated_parent_filter (Optional[AutoMaterializeAssetPartitionsFilter]): Filter to apply
                to updated parents. If a parent was updated but does not pass the filter criteria,
                then it won't count as updated for the sake of this rule.
        """
        return MaterializeOnParentUpdatedRule(updated_parent_filter=updated_parent_filter)

    @public
    @staticmethod
    def materialize_on_missing() -> "MaterializeOnMissingRule":
        """Materialize an asset partition if it has never been materialized before. This rule will
        not fire for non-root assets unless that asset's parents have been updated.
        """
        return MaterializeOnMissingRule()

    @public
    @staticmethod
    def skip_on_parent_missing() -> "SkipOnParentMissingRule":
        """Skip materializing an asset partition if one of its parent asset partitions has never
        been materialized (for regular assets) or observed (for observable source assets).
        """
        return SkipOnParentMissingRule()

    @public
    @staticmethod
    def skip_on_parent_outdated() -> "SkipOnParentOutdatedRule":
        """Skip materializing an asset partition if any of its parents has not incorporated the
        latest data from its ancestors.
        """
        return SkipOnParentOutdatedRule()

    @public
    @staticmethod
    def skip_on_not_all_parents_updated(
        require_update_for_all_parent_partitions: bool = False,
    ) -> "SkipOnNotAllParentsUpdatedRule":
        """Skip materializing an asset partition if any of its parents have not been updated since
        the asset's last materialization.

        Args:
            require_update_for_all_parent_partitions (Optional[bool]): Applies only to an unpartitioned
                asset or an asset partition that depends on more than one partition in any upstream asset.
                If true, requires all upstream partitions in each upstream asset to be materialized since
                the downstream asset's last materialization in order to update it. If false, requires at
                least one upstream partition in each upstream asset to be materialized since the downstream
                asset's last materialization in order to update it. Defaults to false.
        """
        return SkipOnNotAllParentsUpdatedRule(require_update_for_all_parent_partitions)

    @public
    @staticmethod
    def skip_on_required_but_nonexistent_parents() -> "SkipOnRequiredButNonexistentParentsRule":
        """Skip an asset partition if it depends on parent partitions that do not exist.

        For example, imagine a downstream asset is time-partitioned, starting in 2022, but has a
        time-partitioned parent which starts in 2023. This rule will skip attempting to materialize
        downstream partitions from before 2023, since the parent partitions do not exist.
        """
        return SkipOnRequiredButNonexistentParentsRule()

    @public
    @staticmethod
    def skip_on_backfill_in_progress(
        all_partitions: bool = False,
    ) -> "SkipOnBackfillInProgressRule":
        """Skip an asset's partitions if targeted by an in-progress backfill.

        Args:
            all_partitions (bool): If True, skips all partitions of the asset being backfilled,
                regardless of whether the specific partition is targeted by a backfill.
                If False, skips only partitions targeted by a backfill. Defaults to False.
        """
        return SkipOnBackfillInProgressRule(all_partitions)

    def to_snapshot(self) -> AutoMaterializeRuleSnapshot:
        """Returns a serializable snapshot of this rule for historical evaluations."""
        return AutoMaterializeRuleSnapshot(
            class_name=self.__class__.__name__,
            description=self.description,
            decision_type=self.decision_type,
        )

    def __eq__(self, other) -> bool:
        # override the default NamedTuple __eq__ method to factor in types
        return type(self) == type(other) and super().__eq__(other)

    def __hash__(self) -> int:
        # override the default NamedTuple __hash__ method to factor in types
        return hash(hash(type(self)) + super().__hash__())


@whitelist_for_serdes
class MaterializeOnRequiredForFreshnessRule(
    AutoMaterializeRule, NamedTuple("_MaterializeOnRequiredForFreshnessRule", [])
):
    @property
    def decision_type(self) -> AutoMaterializeDecisionType:
        return AutoMaterializeDecisionType.MATERIALIZE

    @property
    def description(self) -> str:
        return "required to meet this or downstream asset's freshness policy"

    def evaluate_for_asset(
        self, context: AssetConditionEvaluationContext
    ) -> "AssetConditionResult":
        from .asset_condition import AssetConditionResult

        true_subset, subsets_with_metadata = freshness_evaluation_results_for_asset_key(
            context.root_context
        )
        return AssetConditionResult.create(context, true_subset, subsets_with_metadata)


@whitelist_for_serdes
class MaterializeOnCronRule(
    AutoMaterializeRule,
    NamedTuple(
        "_MaterializeOnCronRule",
        [("cron_schedule", str), ("timezone", str), ("all_partitions", bool)],
    ),
):
    @property
    def decision_type(self) -> AutoMaterializeDecisionType:
        return AutoMaterializeDecisionType.MATERIALIZE

    @property
    def description(self) -> str:
        return f"not materialized since last cron schedule tick of '{self.cron_schedule}' (timezone: {self.timezone})"

    def missed_cron_ticks(
        self, context: AssetConditionEvaluationContext
    ) -> Sequence[datetime.datetime]:
        """Returns the cron ticks which have been missed since the previous cursor was generated."""
        if not context.previous_evaluation_timestamp:
            previous_dt = next(
                reverse_cron_string_iterator(
                    end_timestamp=context.evaluation_time.timestamp(),
                    cron_string=self.cron_schedule,
                    execution_timezone=self.timezone,
                )
            )
            return [previous_dt]
        missed_ticks = []
        for dt in cron_string_iterator(
            start_timestamp=context.previous_evaluation_timestamp,
            cron_string=self.cron_schedule,
            execution_timezone=self.timezone,
        ):
            if dt > context.evaluation_time:
                break
            missed_ticks.append(dt)
        return missed_ticks

    def get_new_asset_partitions_to_request(
        self, context: AssetConditionEvaluationContext
    ) -> AbstractSet[AssetKeyPartitionKey]:
        missed_ticks = self.missed_cron_ticks(context)

        if not missed_ticks:
            return set()

        partitions_def = context.partitions_def
        if partitions_def is None:
            return {AssetKeyPartitionKey(context.asset_key)}

        # if all_partitions is set, then just return all partitions if any ticks have been missed
        if self.all_partitions:
            return {
                AssetKeyPartitionKey(context.asset_key, partition_key)
                for partition_key in partitions_def.get_partition_keys(
                    current_time=context.evaluation_time,
                    dynamic_partitions_store=context.instance_queryer,
                )
            }

        # for partitions_defs without a time component, just return the last partition if any ticks
        # have been missed
        time_partitions_def = get_time_partitions_def(partitions_def)
        if time_partitions_def is None:
            return {
                AssetKeyPartitionKey(
                    context.asset_key,
                    partitions_def.get_last_partition_key(
                        dynamic_partitions_store=context.instance_queryer
                    ),
                )
            }

        missed_time_partition_keys = filter(
            None,
            [
                time_partitions_def.get_last_partition_key(
                    current_time=missed_tick,
                    dynamic_partitions_store=context.instance_queryer,
                )
                for missed_tick in missed_ticks
            ],
        )
        # for multi partitions definitions, request to materialize all partitions for each missed
        # cron schedule tick
        if isinstance(partitions_def, MultiPartitionsDefinition):
            return {
                AssetKeyPartitionKey(context.asset_key, partition_key)
                for time_partition_key in missed_time_partition_keys
                for partition_key in partitions_def.get_multipartition_keys_with_dimension_value(
                    partitions_def.time_window_dimension.name,
                    time_partition_key,
                    dynamic_partitions_store=context.instance_queryer,
                )
            }
        else:
            return {
                AssetKeyPartitionKey(context.asset_key, time_partition_key)
                for time_partition_key in missed_time_partition_keys
            }

    def evaluate_for_asset(
        self, context: AssetConditionEvaluationContext
    ) -> "AssetConditionResult":
        from .asset_condition import AssetConditionResult

        new_asset_partitions_to_request = self.get_new_asset_partitions_to_request(context)
        asset_subset_to_request = AssetSubset.from_asset_partitions_set(
            context.asset_key, context.partitions_def, new_asset_partitions_to_request
        ) | (
            context.previous_true_subset.as_valid(context.partitions_def)
            - context.materialized_requested_or_discarded_since_previous_tick_subset
        )

        return AssetConditionResult.create(context, true_subset=asset_subset_to_request)


@whitelist_for_serdes
@experimental
class AutoMaterializeAssetPartitionsFilter(
    NamedTuple(
        "_AutoMaterializeAssetPartitionsFilter",
        [("latest_run_required_tags", Optional[Mapping[str, str]])],
    )
):
    """A filter that can be applied to an asset partition, during auto-materialize evaluation, and
    returns a boolean for whether it passes.

    Attributes:
        latest_run_required_tags (Optional[Sequence[str]]): `passes` returns
            True if the run responsible for the latest materialization of the asset partition
            has all of these tags.
    """

    @property
    def description(self) -> str:
        return f"latest run includes required tags: {self.latest_run_required_tags}"

    def passes(
        self,
        context: AssetConditionEvaluationContext,
        asset_partitions: Iterable[AssetKeyPartitionKey],
    ) -> Iterable[AssetKeyPartitionKey]:
        if self.latest_run_required_tags is None:
            return asset_partitions

        will_update_asset_partitions: Set[AssetKeyPartitionKey] = set()

        asset_partitions_by_latest_run_id: Dict[str, Set[AssetKeyPartitionKey]] = defaultdict(set)
        for asset_partition in asset_partitions:
            if context.will_update_asset_partition(asset_partition):
                will_update_asset_partitions.add(asset_partition)
            else:
                record = context.instance_queryer.get_latest_materialization_or_observation_record(
                    asset_partition
                )

                if record is None:
                    raise RuntimeError(
                        f"No materialization record found for asset partition {asset_partition}"
                    )

                asset_partitions_by_latest_run_id[record.run_id].add(asset_partition)

        if len(asset_partitions_by_latest_run_id) > 0:
            run_ids_with_required_tags = context.instance_queryer.instance.get_run_ids(
                filters=RunsFilter(
                    run_ids=list(asset_partitions_by_latest_run_id.keys()),
                    tags=self.latest_run_required_tags,
                )
            )
        else:
            run_ids_with_required_tags = set()

        updated_partitions_with_required_tags = {
            asset_partition
            for run_id, run_id_asset_partitions in asset_partitions_by_latest_run_id.items()
            if run_id in run_ids_with_required_tags
            for asset_partition in run_id_asset_partitions
        }

        if (
            self.latest_run_required_tags.items()
            <= {
                AUTO_MATERIALIZE_TAG: "true",
                **context.daemon_context.auto_materialize_run_tags,
            }.items()
        ):
            return will_update_asset_partitions | updated_partitions_with_required_tags
        else:
            return updated_partitions_with_required_tags

    def __hash__(self):
        return hash(frozenset((self.latest_run_required_tags or {}).items()))


@whitelist_for_serdes
class MaterializeOnParentUpdatedRule(
    AutoMaterializeRule,
    NamedTuple(
        "_MaterializeOnParentUpdatedRule",
        [("updated_parent_filter", Optional[AutoMaterializeAssetPartitionsFilter])],
    ),
):
    def __new__(cls, updated_parent_filter: Optional[AutoMaterializeAssetPartitionsFilter] = None):
        return super().__new__(cls, updated_parent_filter=updated_parent_filter)

    @property
    def decision_type(self) -> AutoMaterializeDecisionType:
        return AutoMaterializeDecisionType.MATERIALIZE

    @property
    def description(self) -> str:
        base = "upstream data has changed since latest materialization"
        if self.updated_parent_filter is not None:
            return f"{base} and matches filter '{self.updated_parent_filter.description}'"
        else:
            return base

    def evaluate_for_asset(
        self, context: AssetConditionEvaluationContext
    ) -> "AssetConditionResult":
        """Evaluates the set of asset partitions of this asset whose parents have been updated,
        or will update on this tick.
        """
        from .asset_condition import AssetConditionResult

        asset_partitions_by_updated_parents: Mapping[
            AssetKeyPartitionKey, Set[AssetKeyPartitionKey]
        ] = defaultdict(set)
        asset_partitions_by_will_update_parents: Mapping[
            AssetKeyPartitionKey, Set[AssetKeyPartitionKey]
        ] = defaultdict(set)

        subset_to_evaluate = context.candidate_parent_has_or_will_update_subset
        for asset_partition in subset_to_evaluate.asset_partitions:
            parent_asset_partitions = context.asset_graph.get_parents_partitions(
                dynamic_partitions_store=context.instance_queryer,
                current_time=context.instance_queryer.evaluation_time,
                asset_key=asset_partition.asset_key,
                partition_key=asset_partition.partition_key,
            ).parent_partitions

            updated_parent_asset_partitions = context.instance_queryer.get_parent_asset_partitions_updated_after_child(
                asset_partition,
                parent_asset_partitions,
                # do a precise check for updated parents, factoring in data versions, as long as
                # we're within reasonable limits on the number of partitions to check
                respect_materialization_data_versions=context.daemon_context.respect_materialization_data_versions
                and len(parent_asset_partitions) + subset_to_evaluate.size < 100,
                # ignore self-dependencies when checking for updated parents, to avoid historical
                # rematerializations from causing a chain of materializations to be kicked off
                ignored_parent_keys={context.asset_key},
            )
            for parent in updated_parent_asset_partitions:
                asset_partitions_by_updated_parents[parent].add(asset_partition)

            for parent in parent_asset_partitions:
                if context.will_update_asset_partition(parent):
                    asset_partitions_by_will_update_parents[parent].add(asset_partition)

        updated_and_will_update_parents = (
            asset_partitions_by_updated_parents.keys()
            | asset_partitions_by_will_update_parents.keys()
        )
        filtered_updated_and_will_update_parents = (
            self.updated_parent_filter.passes(context, updated_and_will_update_parents)
            if self.updated_parent_filter
            else updated_and_will_update_parents
        )

        updated_parent_assets_by_asset_partition: Dict[
            AssetKeyPartitionKey, Set[AssetKey]
        ] = defaultdict(set)
        will_update_parent_assets_by_asset_partition: Dict[
            AssetKeyPartitionKey, Set[AssetKey]
        ] = defaultdict(set)

        for updated_or_will_update_parent in filtered_updated_and_will_update_parents:
            for child in asset_partitions_by_updated_parents.get(updated_or_will_update_parent, []):
                updated_parent_assets_by_asset_partition[child].add(
                    updated_or_will_update_parent.asset_key
                )

            for child in asset_partitions_by_will_update_parents.get(
                updated_or_will_update_parent, []
            ):
                will_update_parent_assets_by_asset_partition[child].add(
                    updated_or_will_update_parent.asset_key
                )

        asset_partitions_by_evaluation_data = defaultdict(set)
        for asset_partition in (
            updated_parent_assets_by_asset_partition.keys()
            | will_update_parent_assets_by_asset_partition.keys()
        ):
            asset_partitions_by_evaluation_data[
                ParentUpdatedRuleEvaluationData(
                    updated_asset_keys=frozenset(
                        updated_parent_assets_by_asset_partition.get(asset_partition, [])
                    ),
                    will_update_asset_keys=frozenset(
                        will_update_parent_assets_by_asset_partition.get(asset_partition, [])
                    ),
                ).frozen_metadata
            ].add(asset_partition)

        true_subset, subsets_with_metadata = context.add_evaluation_data_from_previous_tick(
            asset_partitions_by_evaluation_data,
            ignore_subset=context.materialized_requested_or_discarded_since_previous_tick_subset,
        )
        return AssetConditionResult.create(context, true_subset, subsets_with_metadata)


@whitelist_for_serdes
class MaterializeOnMissingRule(AutoMaterializeRule, NamedTuple("_MaterializeOnMissingRule", [])):
    @property
    def decision_type(self) -> AutoMaterializeDecisionType:
        return AutoMaterializeDecisionType.MATERIALIZE

    @property
    def description(self) -> str:
        return "materialization is missing"

    def get_handled_subset(self, context: AssetConditionEvaluationContext) -> AssetSubset:
        """Returns the AssetSubset which has been handled (materialized, requested, or discarded).
        Accounts for cases in which the partitions definition may have changed between ticks.
        """
        previous_handled_subset = (
            context.previous_evaluation_state.get_extra_state(context.condition, AssetSubset)
            if context.previous_evaluation_state
            else None
        ) or context.instance_queryer.get_materialized_asset_subset(asset_key=context.asset_key)

        return (
            context.materialized_since_previous_tick_subset
            | context.previous_tick_requested_subset
            | previous_handled_subset
        )

    def evaluate_for_asset(
        self, context: AssetConditionEvaluationContext
    ) -> "AssetConditionResult":
        """Evaluates the set of asset partitions for this asset which are missing and were not
        previously discarded.
        """
        from .asset_condition import AssetConditionResult

        if context.asset_key in context.asset_graph.root_materializable_or_observable_asset_keys:
            handled_subset = self.get_handled_subset(context)
            unhandled_candidates = (
                context.candidate_subset
                & handled_subset.as_valid(context.partitions_def).inverse(
                    context.partitions_def, context.evaluation_time, context.instance_queryer
                )
                if handled_subset.size > 0
                else context.candidate_subset
            )
        else:
            # to retain compatibility with the previous behavior of this rule, we only count a non-root
            # asset as missing if at least one of its parents has updated since the previous tick
            handled_subset = None
            unhandled_candidates = (
                AssetSubset.from_asset_partitions_set(
                    context.asset_key,
                    context.partitions_def,
                    {
                        ap
                        for ap in context.candidate_parent_has_or_will_update_subset.asset_partitions
                        if not context.instance_queryer.asset_partition_has_materialization_or_observation(
                            ap
                        )
                    },
                )
                | context.previous_true_subset
            ) - context.previous_tick_requested_subset

        return AssetConditionResult.create(
            context,
            true_subset=unhandled_candidates,
            # we keep track of the handled subset instead of the unhandled subset because new
            # partitions may spontaneously jump into existence at any time
            extra_state=handled_subset,
        )


@whitelist_for_serdes
class SkipOnParentOutdatedRule(AutoMaterializeRule, NamedTuple("_SkipOnParentOutdatedRule", [])):
    @property
    def decision_type(self) -> AutoMaterializeDecisionType:
        return AutoMaterializeDecisionType.SKIP

    @property
    def description(self) -> str:
        return "waiting on upstream data to be up to date"

    def evaluate_for_asset(
        self, context: AssetConditionEvaluationContext
    ) -> "AssetConditionResult":
        from .asset_condition import AssetConditionResult

        asset_partitions_by_evaluation_data = defaultdict(set)

        # only need to evaluate net-new candidates and candidates whose parents have changed
        subset_to_evaluate = (
            context.candidates_not_evaluated_on_previous_tick_subset
            | context.candidate_parent_has_or_will_update_subset
        )
        for candidate in subset_to_evaluate.asset_partitions:
            outdated_ancestors = set()
            # find the root cause of why this asset partition's parents are outdated (if any)
            for parent in context.get_parents_that_will_not_be_materialized_on_current_tick(
                asset_partition=candidate
            ):
                if context.instance_queryer.have_ignorable_partition_mapping_for_outdated(
                    candidate.asset_key, parent.asset_key
                ):
                    continue
                outdated_ancestors.update(
                    context.instance_queryer.get_outdated_ancestors(asset_partition=parent)
                )
            if outdated_ancestors:
                asset_partitions_by_evaluation_data[
                    WaitingOnAssetsRuleEvaluationData(frozenset(outdated_ancestors)).frozen_metadata
                ].add(candidate)

        true_subset, subsets_with_metadata = context.add_evaluation_data_from_previous_tick(
            asset_partitions_by_evaluation_data, ignore_subset=subset_to_evaluate
        )
        return AssetConditionResult.create(context, true_subset, subsets_with_metadata)


@whitelist_for_serdes
class SkipOnParentMissingRule(AutoMaterializeRule, NamedTuple("_SkipOnParentMissingRule", [])):
    @property
    def decision_type(self) -> AutoMaterializeDecisionType:
        return AutoMaterializeDecisionType.SKIP

    @property
    def description(self) -> str:
        return "waiting on upstream data to be present"

    def evaluate_for_asset(
        self,
        context: AssetConditionEvaluationContext,
    ) -> "AssetConditionResult":
        from .asset_condition import AssetConditionResult

        asset_partitions_by_evaluation_data = defaultdict(set)

        # only need to evaluate net-new candidates and candidates whose parents have changed
        subset_to_evaluate = (
            context.candidates_not_evaluated_on_previous_tick_subset
            | context.candidate_parent_has_or_will_update_subset
        )
        for candidate in subset_to_evaluate.asset_partitions:
            missing_parent_asset_keys = set()
            for parent in context.get_parents_that_will_not_be_materialized_on_current_tick(
                asset_partition=candidate
            ):
                # ignore non-observable sources, which will never have a materialization or observation
                if context.asset_graph.is_source(
                    parent.asset_key
                ) and not context.asset_graph.is_observable(parent.asset_key):
                    continue
                if not context.instance_queryer.asset_partition_has_materialization_or_observation(
                    parent
                ):
                    missing_parent_asset_keys.add(parent.asset_key)
            if missing_parent_asset_keys:
                asset_partitions_by_evaluation_data[
                    WaitingOnAssetsRuleEvaluationData(
                        frozenset(missing_parent_asset_keys)
                    ).frozen_metadata
                ].add(candidate)

        true_subset, subsets_with_metadata = context.add_evaluation_data_from_previous_tick(
            asset_partitions_by_evaluation_data, ignore_subset=subset_to_evaluate
        )
        return AssetConditionResult.create(context, true_subset, subsets_with_metadata)


@whitelist_for_serdes
class SkipOnNotAllParentsUpdatedRule(
    AutoMaterializeRule,
    NamedTuple(
        "_SkipOnNotAllParentsUpdatedRule", [("require_update_for_all_parent_partitions", bool)]
    ),
):
    """An auto-materialize rule that enforces that an asset can only be materialized if all parents
    have been materialized since the asset's last materialization.

    Attributes:
        require_update_for_all_parent_partitions (Optional[bool]): Applies only to an unpartitioned
            asset or an asset partition that depends on more than one partition in any upstream asset.
            If true, requires all upstream partitions in each upstream asset to be materialized since
            the downstream asset's last materialization in order to update it. If false, requires at
            least one upstream partition in each upstream asset to be materialized since the downstream
            asset's last materialization in order to update it. Defaults to false.
    """

    @property
    def decision_type(self) -> AutoMaterializeDecisionType:
        return AutoMaterializeDecisionType.SKIP

    @property
    def description(self) -> str:
        if self.require_update_for_all_parent_partitions is False:
            return "waiting on upstream data to be updated"
        else:
            return "waiting until all upstream partitions are updated"

    def evaluate_for_asset(
        self,
        context: AssetConditionEvaluationContext,
    ) -> "AssetConditionResult":
        from .asset_condition import AssetConditionResult

        asset_partitions_by_evaluation_data = defaultdict(set)

        # only need to evaluate net-new candidates and candidates whose parents have changed
        subset_to_evaluate = (
            context.candidates_not_evaluated_on_previous_tick_subset
            | context.candidate_parent_has_or_will_update_subset
        )
        for candidate in subset_to_evaluate.asset_partitions:
            parent_partitions = context.asset_graph.get_parents_partitions(
                context.instance_queryer,
                context.instance_queryer.evaluation_time,
                context.asset_key,
                candidate.partition_key,
            ).parent_partitions

            updated_parent_partitions = (
                context.instance_queryer.get_parent_asset_partitions_updated_after_child(
                    candidate,
                    parent_partitions,
                    context.daemon_context.respect_materialization_data_versions,
                    ignored_parent_keys=set(),
                )
                | context.parent_will_update_subset.asset_partitions
            )

            if self.require_update_for_all_parent_partitions:
                # All upstream partitions must be updated in order for the candidate to be updated
                non_updated_parent_keys = {
                    parent.asset_key for parent in parent_partitions - updated_parent_partitions
                }
            else:
                # At least one upstream partition in each upstream asset must be updated in order
                # for the candidate to be updated
                parent_asset_keys = context.asset_graph.get_parents(context.asset_key)
                updated_parent_keys = {ap.asset_key for ap in updated_parent_partitions}
                non_updated_parent_keys = parent_asset_keys - updated_parent_keys

            # do not require past partitions of this asset to be updated
            non_updated_parent_keys -= {context.asset_key}

            if non_updated_parent_keys:
                asset_partitions_by_evaluation_data[
                    WaitingOnAssetsRuleEvaluationData(
                        frozenset(non_updated_parent_keys)
                    ).frozen_metadata
                ].add(candidate)

        true_subset, subsets_with_metadata = context.add_evaluation_data_from_previous_tick(
            asset_partitions_by_evaluation_data, ignore_subset=subset_to_evaluate
        )
        return AssetConditionResult.create(context, true_subset, subsets_with_metadata)


@whitelist_for_serdes
class SkipOnRequiredButNonexistentParentsRule(
    AutoMaterializeRule, NamedTuple("_SkipOnRequiredButNonexistentParentsRule", [])
):
    @property
    def decision_type(self) -> AutoMaterializeDecisionType:
        return AutoMaterializeDecisionType.SKIP

    @property
    def description(self) -> str:
        return "required parent partitions do not exist"

    def evaluate_for_asset(
        self, context: AssetConditionEvaluationContext
    ) -> "AssetConditionResult":
        from .asset_condition import AssetConditionResult

        asset_partitions_by_evaluation_data = defaultdict(set)

        subset_to_evaluate = (
            context.candidates_not_evaluated_on_previous_tick_subset
            | context.candidate_parent_has_or_will_update_subset
        )
        for candidate in subset_to_evaluate.asset_partitions:
            nonexistent_parent_partitions = context.asset_graph.get_parents_partitions(
                context.instance_queryer,
                context.instance_queryer.evaluation_time,
                candidate.asset_key,
                candidate.partition_key,
            ).required_but_nonexistent_parents_partitions

            nonexistent_parent_keys = {parent.asset_key for parent in nonexistent_parent_partitions}
            if nonexistent_parent_keys:
                asset_partitions_by_evaluation_data[
                    WaitingOnAssetsRuleEvaluationData(
                        frozenset(nonexistent_parent_keys)
                    ).frozen_metadata
                ].add(candidate)

        true_subset, subsets_with_metadata = context.add_evaluation_data_from_previous_tick(
            asset_partitions_by_evaluation_data, ignore_subset=subset_to_evaluate
        )
        return AssetConditionResult.create(context, true_subset, subsets_with_metadata)


@whitelist_for_serdes
class SkipOnBackfillInProgressRule(
    AutoMaterializeRule,
    NamedTuple("_SkipOnBackfillInProgressRule", [("all_partitions", bool)]),
):
    @property
    def decision_type(self) -> AutoMaterializeDecisionType:
        return AutoMaterializeDecisionType.SKIP

    @property
    def description(self) -> str:
        if self.all_partitions:
            return "part of an asset targeted by an in-progress backfill"
        else:
            return "targeted by an in-progress backfill"

    def evaluate_for_asset(
        self, context: AssetConditionEvaluationContext
    ) -> "AssetConditionResult":
        from .asset_condition import AssetConditionResult

        backfilling_subset = (
            # this backfilling subset is aware of the current partitions definitions, and so will
            # be valid
            (context.instance_queryer.get_active_backfill_target_asset_graph_subset())
            .get_asset_subset(context.asset_key, context.asset_graph)
            .as_valid(context.partitions_def)
        )

        if backfilling_subset.size == 0:
            true_subset = context.empty_subset()
        elif self.all_partitions:
            true_subset = context.candidate_subset
        else:
            true_subset = context.candidate_subset & backfilling_subset

        return AssetConditionResult.create(context, true_subset)


@whitelist_for_serdes
class DiscardOnMaxMaterializationsExceededRule(
    AutoMaterializeRule, NamedTuple("_DiscardOnMaxMaterializationsExceededRule", [("limit", int)])
):
    @property
    def decision_type(self) -> AutoMaterializeDecisionType:
        return AutoMaterializeDecisionType.DISCARD

    @property
    def description(self) -> str:
        return f"exceeds {self.limit} materialization(s) per minute"

    def evaluate_for_asset(
        self, context: AssetConditionEvaluationContext
    ) -> "AssetConditionResult":
        from .asset_condition import AssetConditionResult

        # the set of asset partitions which exceed the limit
        rate_limited_asset_partitions = set(
            sorted(
                context.candidate_subset.asset_partitions,
                key=lambda x: sort_key_for_asset_partition(context.asset_graph, x),
            )[self.limit :]
        )

        return AssetConditionResult.create(
            context,
            AssetSubset.from_asset_partitions_set(
                context.asset_key, context.partitions_def, rate_limited_asset_partitions
            ),
        )
