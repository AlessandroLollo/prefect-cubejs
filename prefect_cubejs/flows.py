"""This is an example flows module"""
from prefect import flow

from prefect_cubejs.tasks import (
    goodbye_prefect_cubejs,
    hello_prefect_cubejs,
)


@flow
def hello_and_goodbye():
    """
    Sample flow that says hello and goodbye!
    """
    print(hello_prefect_cubejs)
    print(goodbye_prefect_cubejs)
