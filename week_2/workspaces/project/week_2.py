from datetime import datetime
from typing import List
#from resources import s3_resource, postgres_resource, mock_s3_resource, redis_resource

from dagster import (
    In,
    Nothing,
    OpExecutionContext,
    Out,
    ResourceDefinition,
    String,
    graph,
    op,
)
from workspaces.config import REDIS, S3, S3_FILE
from workspaces.resources import mock_s3_resource, redis_resource, s3_resource
from workspaces.types import Aggregation, Stock


@op(config_schema={"s3_key": String},
    out= {"stocks": Out(dagster_type = List[Stock])},
    required_resource_keys={"s3"})
def get_s3_data(context: OpExecutionContext):
    return [Stock.from_list(record) for record in context.resources.s3.get_data(key_name=context.op_config['s3_key'])]



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
    "ops": {"get_s3_data": {"config": {"s3_key": S3_FILE}}},
}

docker = {
    "resources": {
        "s3": {"config": S3},
        "redis": {"config": REDIS},
    },
    "ops": {"get_s3_data": {"config": {"s3_key": S3_FILE}}},
}

machine_learning_job_local = machine_learning_graph.to_job(
    name="machine_learning_job_local",
    config=local,
    resource_defs={
        "s3": mock_s3_resource, 
        "redis": ResourceDefinition.mock_resource()
        }
)

machine_learning_job_docker = machine_learning_graph.to_job(
    name="machine_learning_job_docker",
    config=docker,
    resource_defs={
        "s3": s3_resource,
        "redis": redis_resource
    }
)
