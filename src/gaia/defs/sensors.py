import dagster as dg

from gaia.defs.jobs import local_workflow_job
from gaia.defs.partitions import ALL_COUNTRIES

MAX_IN_FLIGHT = 1
MINIMUM_INTERVAL_SECONDS = 60
PARTITION_TAG = "dagster/partition"
SKIP_AFTER_CONSECUTIVE_FAILURES = 3

_IN_PROGRESS_STATUSES = [
    dg.DagsterRunStatus.STARTING,
    dg.DagsterRunStatus.STARTED,
    dg.DagsterRunStatus.QUEUED,
    dg.DagsterRunStatus.CANCELING,
]
_TERMINAL_CANCELED = dg.DagsterRunStatus.CANCELED
_TERMINAL_SUCCESS = dg.DagsterRunStatus.SUCCESS
_TERMINAL_FAILURE = dg.DagsterRunStatus.FAILURE

_ALL_COUNTRY_KEYS = set(ALL_COUNTRIES)


def _latest_run_statuses_by_country(
    context: dg.SensorEvaluationContext,
) -> dict[str, list[dg.DagsterRunStatus]]:
    """All runs of the job per country, most recent first."""
    records = context.instance.get_run_records(
        filters=dg.RunsFilter(job_name=local_workflow_job.name),
        limit=None,
    )
    records = sorted(records, key=lambda r: r.create_timestamp, reverse=True)
    by_country: dict[str, list[dg.DagsterRunStatus]] = {}
    for record in records:
        country = record.dagster_run.tags.get(PARTITION_TAG)
        if country in _ALL_COUNTRY_KEYS:
            by_country.setdefault(country, []).append(record.dagster_run.status)
    return by_country


def _in_flight_countries(context: dg.SensorEvaluationContext) -> set[str]:
    records = context.instance.get_run_records(
        filters=dg.RunsFilter(
            job_name=local_workflow_job.name,
            statuses=_IN_PROGRESS_STATUSES,
        ),
        limit=1000,
    )
    return {
        record.dagster_run.tags.get(PARTITION_TAG)
        for record in records
        if record.dagster_run.tags.get(PARTITION_TAG) in _ALL_COUNTRY_KEYS
    }


def _consecutive_failures(
    statuses: list[dg.DagsterRunStatus],
) -> int:
    """Number of trailing FAILURE statuses, stopping at the first non-failure."""
    count = 0
    for status in statuses:
        if status == _TERMINAL_FAILURE:
            count += 1
        else:
            break
    return count


def _progress_summary(
    succeeded: set[str],
    skipped: set[str],
    in_flight: set[str],
    next_country: str | None = None,
) -> str:
    """Human-readable progress shown as the sensor cursor / skip message in the UI."""
    parts = [f"{len(succeeded)}/{len(ALL_COUNTRIES)} countries succeeded"]
    if in_flight:
        parts.append(f"{len(in_flight)} running")
    if skipped:
        parts.append(f"{len(skipped)} skipped")
    if next_country:
        parts.append(f"next: {next_country}")
    return " | ".join(parts)


@dg.sensor(
    job=local_workflow_job,
    minimum_interval_seconds=MINIMUM_INTERVAL_SECONDS,
    description=(
        "Run countries one at a time until every country has succeeded. A failed "
        "country is retried; after "
        f"{SKIP_AFTER_CONSECUTIVE_FAILURES} consecutive failures it is skipped and "
        "revisited once the remaining countries are done. Progress is derived from "
        "the run history, so the sensor picks up where it left off even after restarts."
    ),
)
def country_workflow_sensor(context: dg.SensorEvaluationContext):
    statuses = _latest_run_statuses_by_country(context)

    succeeded = {
        country
        for country, history in statuses.items()
        if history and history[0] == _TERMINAL_SUCCESS
    }

    consecutive_failures = {}
    for country, history in statuses.items():
        if not history or history[0] != _TERMINAL_FAILURE:
            continue
        consecutive_failures[country] = _consecutive_failures(history)

    skipped = {
        country
        for country, count in consecutive_failures.items()
        if count >= SKIP_AFTER_CONSECUTIVE_FAILURES
    }

    active_pending = [
        country
        for country in ALL_COUNTRIES
        if country not in succeeded and country not in skipped
    ]

    in_flight = _in_flight_countries(context)

    if active_pending:
        next_country = active_pending[0]
    elif skipped:
        next_country = next(country for country in ALL_COUNTRIES if country in skipped)
    else:
        next_country = None

    progress = _progress_summary(succeeded, skipped, in_flight, next_country)
    context.update_cursor(progress)

    if len(in_flight) >= MAX_IN_FLIGHT:
        return dg.SkipReason(
            f"{len(in_flight)} countries already running (max {MAX_IN_FLIGHT}). "
            f"{progress}"
        )

    if active_pending:
        if consecutive_failures.get(next_country, 0) >= 1:
            context.log.warning(
                f"Retrying {next_country} after "
                f"{consecutive_failures[next_country]} consecutive failures"
            )
    elif skipped:
        context.log.warning(
            f"All active countries done; revisiting skipped country {next_country}"
        )
    else:
        return dg.SkipReason(f"Every country has succeeded. {progress}")

    attempt = len(statuses.get(next_country, [])) + 1
    return dg.SensorResult(
        run_requests=[
            dg.RunRequest(
                partition_key=next_country,
                run_key=f"country-{next_country}-{attempt}",
            )
        ]
    )
