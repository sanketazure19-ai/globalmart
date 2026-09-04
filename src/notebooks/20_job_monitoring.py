# Databricks notebook source

dbutils.widgets.dropdown("env", "dev", ["dev", "staging", "prod"])

env = dbutils.widgets.get("env")

print(f"Monitoring environment: {env}")


# COMMAND ----------

from databricks.sdk import WorkspaceClient
from datetime import datetime, timezone

PIPELINE_JOB_IDS = {
    "dev": 807751325954226,
    "staging": 627194409174026,
    "prod": 583220992582703
}

job_id = PIPELINE_JOB_IDS[env]

w = WorkspaceClient()

LONG_RUNNING_MINUTES = 30
LOOKBACK_RUNS = 10

print(f"Pipeline Job ID: {job_id}")
print(f"Checking last {LOOKBACK_RUNS} runs")
print(f"Long-running threshold: {LONG_RUNNING_MINUTES} minutes")


# COMMAND ----------

runs = list(
    w.jobs.list_runs(
        job_id=job_id,
        limit=LOOKBACK_RUNS
    )
)

print(f"Runs found: {len(runs)}")

for run in runs:
    print(
        f"Run ID: {run.run_id} | "
        f"State: {run.state.life_cycle_state} | "
        f"Result: {run.state.result_state}"
    )


# COMMAND ----------

monitoring_data = []

for run in runs:

    start_time = run.start_time / 1000 if run.start_time else None
    end_time = run.end_time / 1000 if run.end_time else None

    duration_minutes = None

    if start_time:
        end_timestamp = (
            end_time
            if end_time
            else datetime.now(timezone.utc).timestamp()
        )

        duration_minutes = round(
            (end_timestamp - start_time) / 60,
            2
        )

    monitoring_data.append({
        "run_id": run.run_id,
        "job_id": job_id,

        "life_cycle_state": (
            run.state.life_cycle_state.value
            if run.state and run.state.life_cycle_state
            else None
        ),

        "result_state": (
            run.state.result_state.value
            if run.state and run.state.result_state
            else None
        ),

        "start_time": (
            datetime.fromtimestamp(
                start_time,
                timezone.utc
            )
            if start_time
            else None
        ),

        "end_time": (
            datetime.fromtimestamp(
                end_time,
                timezone.utc
            )
            if end_time
            else None
        ),

        "duration_minutes": duration_minutes
    })


monitoring_df = spark.createDataFrame(monitoring_data)

display(
    monitoring_df.orderBy(
        "start_time",
        ascending=False
    )
)


# COMMAND ----------

failed_runs = monitoring_df.filter(
    monitoring_df.result_state == "FAILED"
)

long_running_runs = monitoring_df.filter(
    monitoring_df.duration_minutes > LONG_RUNNING_MINUTES
)

print(f"Failed runs: {failed_runs.count()}")
print(f"Long-running runs: {long_running_runs.count()}")


# COMMAND ----------

total_runs = monitoring_df.count()
failed_count = failed_runs.count()
long_running_count = long_running_runs.count()

status = (
    "ALERT"
    if failed_count > 0 or long_running_count > 0
    else "HEALTHY"
)

print("=== GlobalMart Job Monitoring ===")
print(f"Environment: {env}")
print(f"Pipeline Job ID: {job_id}")
print(f"Runs checked: {total_runs}")
print(f"Failed runs: {failed_count}")
print(f"Long-running runs: {long_running_count}")
print(f"Status: {status}")


# COMMAND ----------

if failed_count > 0 or long_running_count > 0:

    print("ALERT: GlobalMart pipeline requires attention.")

    if failed_count > 0:
        print(
            f"Detected {failed_count} failed run(s)."
        )

    if long_running_count > 0:
        print(
            f"Detected {long_running_count} run(s) "
            f"exceeding {LONG_RUNNING_MINUTES} minutes."
        )

    raise RuntimeError(
        "GlobalMart job monitoring detected an issue."
    )

else:

    print(
        "HEALTHY: No failed or long-running runs detected."
    )