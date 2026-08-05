import json

import dagster as dg

from gaia.defs.jobs import local_workflow_job
from gaia.defs.partitions import ALL_COUNTRIES

MAX_CONCURRENT = 3
MINIMUM_INTERVAL_SECONDS = 60
PARTITION_TAG = "dagster/partition"

_IN_PROGRESS_STATUSES = [
    dg.DagsterRunStatus.STARTING,
    dg.DagsterRunStatus.STARTED,
    dg.DagsterRunStatus.QUEUED,
    dg.DagsterRunStatus.CANCELING,
]

_ALL_COUNTRY_KEYS = set(ALL_COUNTRIES)


def _in_flight_countries(context: dg.SensorEvaluationContext) -> list[str]:
    records = context.instance.get_run_records(
        filters=dg.RunsFilter(
            job_name=local_workflow_job.name,
            statuses=_IN_PROGRESS_STATUSES,
        ),
        limit=1000,
    )
    return sorted(
        {
            run.dagster_run.tags.get(PARTITION_TAG)
            for run in records
            if run.dagster_run.tags.get(PARTITION_TAG) in _ALL_COUNTRY_KEYS
        }
    )


@dg.sensor(
    job=local_workflow_job,
    minimum_interval_seconds=MINIMUM_INTERVAL_SECONDS,
    description=(
        f"Launch {local_workflow_job.name} country runs one at a time, keeping "
        f"at most {MAX_CONCURRENT} in flight until every country has run once."
    ),
)
def country_workflow_sensor(context: dg.SensorEvaluationContext):
    launched = set(json.loads(context.cursor)) if context.cursor else set()

    in_flight = _in_flight_countries(context)
    slots = MAX_CONCURRENT - len(in_flight)
    if slots <= 0:
        return dg.SkipReason(
            f"{len(in_flight)} countries already running (max {MAX_CONCURRENT})"
        )

    pending = [c for c in ALL_COUNTRIES if c not in launched]
    to_launch = pending[:slots]
    if not to_launch:
        return dg.SkipReason("Every country has already been launched")

    run_requests = [
        dg.RunRequest(
            partition_key=country,
            run_key=f"country-{country}",
        )
        for country in to_launch
    ]
    launched.update(to_launch)

    return dg.SensorResult(
        run_requests=run_requests,
        cursor=json.dumps(sorted(launched)),
    )
