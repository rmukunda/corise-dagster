from datetime import datetime
from typing import List

from dagster import (
    In,
    Nothing,
    OpExecutionContext,
    Out,
    String,
    ResourceDefinition,
    RetryPolicy,
    RunRequest,
    ScheduleDefinition,
    SensorEvaluationContext,
    ScheduleEvaluationContext,
    SkipReason,
    graph,
    op,
    schedule,
    sensor,
    static_partitioned_config,
    
)
from workspaces.config import REDIS, S3
from workspaces.project.sensors import get_s3_keys
from workspaces.resources import mock_s3_resource, redis_resource, s3_resource
from workspaces.types import Aggregation, Stock


@op
def get_s3_data_():
    pass

@op(config_schema={"s3_key": String},
    out= {"stocks": Out(dagster_type = List[Stock])},
    required_resource_keys={"s3"})
def get_s3_data(context: OpExecutionContext):
    return [Stock.from_list(record) for record in context.resources.s3.get_data(key_name=context.op_config['s3_key'])]

@op
def process_data_():
    pass


@op
def put_redis_data_():
    pass


@op
def put_s3_data_():
    pass

@op(ins= {"stocks": In(dagster_type = List[Stock])},
    out = {"highs": Out(dagster_type = Aggregation)},)
def process_data(context, stocks):
    stocks.sort(key=lambda x:x.high, reverse = True)
    st = stocks[0]
    return Aggregation(date = st.date , high = st.high)

@op(ins= {"aggregations": In(dagster_type = Aggregation)},
    required_resource_keys={"redis"})
def put_redis_data(context: OpExecutionContext, aggregations):
    context.resources.redis.put_data(str(aggregations.date) , str(aggregations.high))
    return

@op(
    ins={"aggregations": In(dagster_type= Aggregation)},
    required_resource_keys={"s3"})
def put_s3_data(context: OpExecutionContext, aggregations: Aggregation):
    file_name = "/data/result.csv"
    context.resources.s3.put_data(Key=file_name, data=aggregations)
    return

@graph
def machine_learning_graph():
    agg = process_data(get_s3_data())
    put_redis_data(agg)
    put_s3_data(agg)



local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}


docker = {
    "resources": {
        "s3": {"config": S3},
        "redis": {"config": REDIS},
    },
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}

parts = [str(i) for i in range(1,11)]

@static_partitioned_config(partition_keys=parts)
def docker_config(partition_key: str):
    return {
            "resources": {
                "s3": {"config": S3},
                "redis": {"config": REDIS},
            },
            "ops": {"get_s3_data": {"config": {"s3_key": f'prefix/stock_{partition_key}.csv'}}},
        }



machine_learning_retry_docker = RetryPolicy(max_retries=10, delay=1)

machine_learning_job_local = machine_learning_graph.to_job(
    name="machine_learning_job_local",
    config=local,
    resource_defs={
        "s3": mock_s3_resource, 
        "redis": ResourceDefinition.mock_resource()
        }
)
machine_learning_schedule_local = ScheduleDefinition(job=machine_learning_job_local, cron_schedule="*/15 * * * *")

machine_learning_job_docker = machine_learning_graph.to_job(
    name="machine_learning_job_docker",
    config=docker_config,
    resource_defs={
        "s3": s3_resource,
        "redis": redis_resource
    },
    op_retry_policy=machine_learning_retry_docker,
)


@schedule(job=machine_learning_job_docker, cron_schedule="0 * * * *")
def machine_learning_schedule_docker(context):
    for part in parts:
        yield RunRequest(run_key=part, partition_key=part)


@sensor(job_name='machine_learning_job_docker', minimum_interval_seconds=60)
def machine_learning_sensor_docker(context: SensorEvaluationContext):
    new_keys = get_s3_keys(bucket='dagster', prefix='prefix', endpoint_url='http://localstack:4566')
    if not new_keys:
        yield SkipReason(f"No new s3 files found in bucket.")
        return
    for new_key in new_keys:
        yield RunRequest(run_key=new_key
                         , run_config={
                            "resources": {
                                "s3": {"config": S3},
                                "redis": {"config": REDIS},
                            },
                            "ops": {
                                        "get_s3_data": {"config": {"s3_key": new_key}},},},)
    return
