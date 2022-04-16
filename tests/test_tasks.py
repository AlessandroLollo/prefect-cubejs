from prefect import flow

from prefect_cubejs.tasks import (
    goodbye_prefect_cubejs,
    hello_prefect_cubejs,
)


def test_hello_prefect_cubejs():
    @flow
    def test_flow():
        return hello_prefect_cubejs()

    flow_state = test_flow()
    task_state = flow_state.result()
    assert task_state.result() == "Hello, prefect-cubejs!"


def goodbye_hello_prefect_cubejs():
    @flow
    def test_flow():
        return goodbye_prefect_cubejs()

    flow_state = test_flow()
    task_state = flow_state.result()
    assert task_state.result() == "Goodbye, prefect-cubejs!"
