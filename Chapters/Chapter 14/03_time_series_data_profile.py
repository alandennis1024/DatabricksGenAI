# Chapter 14 reference: Data-quality monitoring profile.
# Unity Catalog table and supported SDK/permissions required.
# Live Databricks execution has NOT been verified.

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dataquality import (
    Monitor, DataProfilingConfig, TimeSeriesConfig,
    AggregationGranularity,
)

w = WorkspaceClient()
schema = w.schemas.get(full_name="cobalt_prod.system_logs")
table = w.tables.get(
    full_name="cobalt_prod.system_logs.qa_assistant_inference_payload"
)
config = DataProfilingConfig(
    output_schema_id=schema.schema_id,
    assets_dir="/Workspace/Shared/ch14_profile",
    time_series=TimeSeriesConfig(
        timestamp_column="event_time",
        granularities=[AggregationGranularity.AGGREGATION_GRANULARITY_1_DAY],
    ),
)
w.data_quality.create_monitor(
    monitor=Monitor(object_type="table", object_id=table.table_id,
                    data_profiling_config=config)
)
