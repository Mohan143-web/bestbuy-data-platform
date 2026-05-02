"""Spark dataframe quality helpers used by the streaming jobs."""

from __future__ import annotations

from schema_contracts import EVENT_CONTRACTS


def apply_contract_rules(df, topic: str):
    """Add quality metadata columns to a parsed Spark dataframe."""
    from pyspark.sql import functions as F

    required_fields = EVENT_CONTRACTS[topic]["required"].keys()
    error_columns = [
        F.when(F.col(field).isNull(), F.lit(f"{field}:null")) for field in required_fields
    ]

    df_with_errors = df.withColumn("_dq_errors_raw", F.array(*error_columns))
    df_with_errors = df_with_errors.withColumn(
        "_dq_errors", F.expr("filter(_dq_errors_raw, x -> x is not null)")
    )
    return (
        df_with_errors.drop("_dq_errors_raw")
        .withColumn("_dq_is_valid", F.size(F.col("_dq_errors")) == 0)
        .withColumn("_dq_checked_at", F.current_timestamp())
    )


def add_late_event_flag(df, topic: str):
    """Flag records older than the topic contract allows."""
    from pyspark.sql import functions as F

    contract = EVENT_CONTRACTS[topic]
    max_lateness = int(contract["max_lateness_minutes"])
    event_time_col = contract["event_time"]
    return df.withColumn(
        "_dq_is_late",
        F.col(event_time_col) < (F.current_timestamp() - F.expr(f"INTERVAL {max_lateness} MINUTES")),
    )

