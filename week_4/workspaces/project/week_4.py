from datetime import datetime
from typing import List

from dagster import (
    AssetSelection,
    Nothing,
    OpExecutionContext,
    ScheduleDefinition,
    String,
    asset,
    AssetIn,
    define_asset_job,
    load_assets_from_current_module,
)
from workspaces.types import Aggregation, Stock


@asset(config_schema={"s3_key": String},
    required_resource_keys={"s3"})
def get_s3_data(context: OpExecutionContext):
    return [Stock.from_list(record) for record in context.resources.s3.get_data(key_name=context.op_config['s3_key'])]



@asset(ins= {"stocks": AssetIn("get_s3_data")},)
def process_data(stocks):
    stocks.sort(key=lambda x:x.high, reverse = True)
    st = stocks[0]
    return Aggregation(date = st.date , high = st.high)



@asset(ins= {"aggregations": AssetIn(dagster_type = Aggregation)},
    required_resource_keys={"redis"})
def put_redis_data(context: OpExecutionContext, aggregations):
    context.resources.redis.put_data(str(aggregations.date) , str(aggregations.high))
    return



@asset(
    ins={"aggregations": AssetIn(dagster_type= Aggregation)},
    required_resource_keys={"s3"})
def put_s3_data(context: OpExecutionContext, aggregations):
    file_name = "/data/result.csv"
    context.resources.s3.put_data(Key=file_name, data=aggregations)
    return


project_assets = load_assets_from_current_module()

docker = {
    "resources": {
        "s3": {"config": S3},
        "redis": {"config": REDIS},
    },
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}

machine_learning_asset_job = define_asset_job(
    name="machine_learning_asset_job",
)

machine_learning_schedule = ScheduleDefinition(job=machine_learning_asset_job, cron_schedule="*/15 * * * *")
