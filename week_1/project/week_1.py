import csv
from datetime import datetime
from typing import Iterator, List

from dagster import (
    In,
    Nothing,
    OpExecutionContext,
    Out,
    String,
    job,
    op,
    usable_as_dagster_type,
)
from pydantic import BaseModel


@usable_as_dagster_type(description="Stock data")
class Stock(BaseModel):
    date: datetime
    close: float
    volume: int
    open: float
    high: float
    low: float

    @classmethod
    def from_list(cls, input_list: List[str]):
        """Do not worry about this class method for now"""
        return cls(
            date=datetime.strptime(input_list[0], "%Y/%m/%d"),
            close=float(input_list[1]),
            volume=int(float(input_list[2])),
            open=float(input_list[3]),
            high=float(input_list[4]),
            low=float(input_list[5]),
        )


@usable_as_dagster_type(description="Aggregation of stock data")
class Aggregation(BaseModel):
    date: datetime
    high: float


def csv_helper(file_name: str) -> Iterator[Stock]:
    with open(file_name) as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            yield Stock.from_list(row)


@op(config_schema={"s3_key": String},
    out= {"stocks": Out(dagster_type = List[Stock])}, )
def get_s3_data_op(context):
    return [record for record in csv_helper(context.op_config['s3_key'])]


@op(ins= {"stocks": In(dagster_type = List[Stock])},
    out = {"highs": Out(dagster_type = Aggregation)},)
def process_data_op(context, stocks):
    stocks.sort(key=lambda x:x.high, reverse = True)
    st = stocks[0]
    return Aggregation(date = st.date , high = st.high)


@op(ins= {"aggregations": In(dagster_type = Aggregation)},)
def put_redis_data_op(context, aggregations):
    pass


@op(ins= {"aggregations": In(dagster_type = Aggregation)},)
def put_s3_data_op(context, aggregations):
    pass


@job
def machine_learning_job():
    agg = process_data_op(get_s3_data_op())
    put_redis_data_op(agg)
    put_s3_data_op(agg)
    
#job = machine_learning_job.to_job()
