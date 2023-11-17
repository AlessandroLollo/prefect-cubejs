import pytest
import responses
from prefect import flow

from prefect_cubejs.exceptions import (
    CubeJSAPIFailureException,
    CubeJSConfigurationException,
)

from prefect_cubejs.blocks import AuthHeader, SecurityContext
from prefect_cubejs.tasks import build_pre_aggregations

from pydantic.v1 import SecretStr

# run: pytest -s tests/test_jobs.py

security_context = {
    "expiresIn": 1,
    "foo": "bar",
}

selector = {
    "action": "post",
    "selector": {
        "contexts": [
            {"securityContext": {"tenant": "t1"}},
            {"securityContext": {"tenant": "t2"}},
        ],
        "timezones": ["UTC", "America/Los_Angeles"],
    },
}

response_tokens = [
    "be598e318484848cbb06291baa59ca3a",
    "d4bb22530aa9905219b2f0e6a214c39f",
    "e1578a60514a7c55689016adf0863965",
]

response_status_missing_partition = {
    "e1578a60514a7c55689016adf0863965": {
        "table": "preaggs.e_commerce__manual_updates20201201_kuggpskn_alfb3s4u_1hmrdkc",
        "status": "missing_partition",
        "selector": {
            "cubes": ["ECommerce"],
            "preAggregations": ["ECommerce.ManualUpdates"],
            "contexts": [{"securityContext": {"tenant": "t2"}}],
            "timezones": ["America/Los_Angeles"],
            "dataSources": ["default"],
        },
    },
    "d4bb22530aa9905219b2f0e6a214c39f": {
        "table": "preaggs.e_commerce__manual_updates20201101_rvfrwirb_ucnfhp2g_1hmrdkc",
        "status": "missing_partition",
        "selector": {
            "cubes": ["ECommerce"],
            "preAggregations": ["ECommerce.ManualUpdates"],
            "contexts": [{"securityContext": {"tenant": "t2"}}],
            "timezones": ["America/Los_Angeles"],
            "dataSources": ["default"],
        },
    },
    "be598e318484848cbb06291baa59ca3a": {
        "table": "preaggs.e_commerce__manual_updates20201201_kbn0y0iy_fvfip33o_1hmrdkc",
        "status": "missing_partition",
        "selector": {
            "cubes": ["ECommerce"],
            "preAggregations": ["ECommerce.ManualUpdates"],
            "contexts": [{"securityContext": {"tenant": "t2"}}],
            "timezones": ["UTC"],
            "dataSources": ["default"],
        },
    },
}

response_status_failure = {
    "e1578a60514a7c55689016adf0863965": {
        "table": "preaggs.e_commerce__manual_updates20201201_kuggpskn_alfb3s4u_1hmrdkc",
        "status": "done",
        "selector": {
            "cubes": ["ECommerce"],
            "preAggregations": ["ECommerce.ManualUpdates"],
            "contexts": [{"securityContext": {"tenant": "t2"}}],
            "timezones": ["America/Los_Angeles"],
            "dataSources": ["default"],
        },
    },
    "d4bb22530aa9905219b2f0e6a214c39f": {
        "table": "preaggs.e_commerce__manual_updates20201101_rvfrwirb_ucnfhp2g_1hmrdkc",
        "status": "done",
        "selector": {
            "cubes": ["ECommerce"],
            "preAggregations": ["ECommerce.ManualUpdates"],
            "contexts": [{"securityContext": {"tenant": "t2"}}],
            "timezones": ["America/Los_Angeles"],
            "dataSources": ["default"],
        },
    },
    "be598e318484848cbb06291baa59ca3a": {
        "table": "preaggs.e_commerce__manual_updates20201201_kbn0y0iy_fvfip33o_1hmrdkc",
        "status": "failure: returned error",
        "selector": {
            "cubes": ["ECommerce"],
            "preAggregations": ["ECommerce.ManualUpdates"],
            "contexts": [{"securityContext": {"tenant": "t2"}}],
            "timezones": ["UTC"],
            "dataSources": ["default"],
        },
    },
}

response_status_processing = {
    "e1578a60514a7c55689016adf0863965": {
        "table": "preaggs.e_commerce__manual_updates20201201_kuggpskn_alfb3s4u_1hmrdkc",
        "status": "done",
        "selector": {
            "cubes": ["ECommerce"],
            "preAggregations": ["ECommerce.ManualUpdates"],
            "contexts": [{"securityContext": {"tenant": "t2"}}],
            "timezones": ["America/Los_Angeles"],
            "dataSources": ["default"],
        },
    },
    "d4bb22530aa9905219b2f0e6a214c39f": {
        "table": "preaggs.e_commerce__manual_updates20201101_rvfrwirb_ucnfhp2g_1hmrdkc",
        "status": "processing",
        "selector": {
            "cubes": ["ECommerce"],
            "preAggregations": ["ECommerce.ManualUpdates"],
            "contexts": [{"securityContext": {"tenant": "t2"}}],
            "timezones": ["America/Los_Angeles"],
            "dataSources": ["default"],
        },
    },
    "be598e318484848cbb06291baa59ca3a": {
        "table": "preaggs.e_commerce__manual_updates20201201_kbn0y0iy_fvfip33o_1hmrdkc",
        "status": "scheduled",
        "selector": {
            "cubes": ["ECommerce"],
            "preAggregations": ["ECommerce.ManualUpdates"],
            "contexts": [{"securityContext": {"tenant": "t2"}}],
            "timezones": ["UTC"],
            "dataSources": ["default"],
        },
    },
}

response_status_done = {
    "e1578a60514a7c55689016adf0863965": {
        "table": "preaggs.e_commerce__manual_updates20201201_kuggpskn_alfb3s4u_1hmrdkc",
        "status": "done",
        "selector": {
            "cubes": ["ECommerce"],
            "preAggregations": ["ECommerce.ManualUpdates"],
            "contexts": [{"securityContext": {"tenant": "t2"}}],
            "timezones": ["America/Los_Angeles"],
            "dataSources": ["default"],
        },
    },
    "d4bb22530aa9905219b2f0e6a214c39f": {
        "table": "preaggs.e_commerce__manual_updates20201101_rvfrwirb_ucnfhp2g_1hmrdkc",
        "status": "done",
        "selector": {
            "cubes": ["ECommerce"],
            "preAggregations": ["ECommerce.ManualUpdates"],
            "contexts": [{"securityContext": {"tenant": "t2"}}],
            "timezones": ["America/Los_Angeles"],
            "dataSources": ["default"],
        },
    },
    "be598e318484848cbb06291baa59ca3a": {
        "table": "preaggs.e_commerce__manual_updates20201201_kbn0y0iy_fvfip33o_1hmrdkc",
        "status": "done",
        "selector": {
            "cubes": ["ECommerce"],
            "preAggregations": ["ECommerce.ManualUpdates"],
            "contexts": [{"securityContext": {"tenant": "t2"}}],
            "timezones": ["UTC"],
            "dataSources": ["default"],
        },
    },
}


def test_no_params():
    @flow(name="test_no_params")
    def test_flow():
        result = build_pre_aggregations(
            auth_header=AuthHeader(
                security_context=None,
                api_secret=SecretStr("secret")
            )
        )
        return result

    msg_match = "Missing both `subdomain` and `url`."
    with pytest.raises(CubeJSConfigurationException, match=msg_match):
        test_flow()


def test_no_selector():
    @flow(name="test_no_selector")
    def test_flow():
        result = build_pre_aggregations(
            auth_header=AuthHeader(
                security_context=None,
                api_secret=SecretStr("23dff8b29cf20df38a4c78dfaf689fa55916add4d27ee3dd9ba75d1")
            ),
            url="http://localhost:4000/cubejs-system"
        )
        return result

    msg_match = "Missing `selector`."
    with pytest.raises(CubeJSConfigurationException, match=msg_match):
        test_flow()


@responses.activate
def test_internal_error():
    @flow(name="test_internal_error")
    def test_flow():
        result = build_pre_aggregations(
            auth_header=AuthHeader(
                security_context=None,
                api_secret=SecretStr("23dff8b29cf20df38a4c78dfaf689fa55916add4d27ee3dd9ba75d1")
            ),
            url="http://localhost:4000/cubejs-system",
            selector=selector,
        )
        return result

    responses.add(
        responses.POST,
        "http://localhost:4000/cubejs-system/v1/pre-aggregations/jobs",
        json={"error": "500"},
        status=500,
    )

    msg_match = "Cube `pre-aggregations/jobs` API failed: Internal Server Error"
    with pytest.raises(CubeJSAPIFailureException, match=msg_match):
        test_flow()


@responses.activate
def test_no_wait_completion():
    @flow(name="test_no_wait_completion")
    def test_flow():
        result = build_pre_aggregations(
            auth_header=AuthHeader(
                security_context=None,
                api_secret=SecretStr("23dff8b29cf20df38a4c78dfaf689fa55916add4d27ee3dd9ba75d1")
            ),
            url="http://localhost:4000/cubejs-system",
            selector=selector,
        )
        return result

    responses.add(
        responses.POST,
        "http://localhost:4000/cubejs-system/v1/pre-aggregations/jobs",
        json=response_tokens,
        status=200,
    )
    test_flow()
    assert len(responses.calls) == 1


@responses.activate
def test_wait_completion_one_step():
    @flow(name="test_wait_completion_one_step")
    def test_flow():
        result = build_pre_aggregations(
            auth_header=AuthHeader(
                security_context=None,
                api_secret=SecretStr("23dff8b29cf20df38a4c78dfaf689fa55916add4d27ee3dd9ba75d1")
            ),
            url="http://localhost:4000/cubejs-system",
            selector=selector,
            wait_for_job_run_completion=True,
            wait_time_between_api_calls=0,
        )
        return result

    responses.add(
        responses.POST,
        "http://localhost:4000/cubejs-system/v1/pre-aggregations/jobs",
        json=response_tokens,
        status=200,
    )
    responses.add(
        responses.POST,
        "http://localhost:4000/cubejs-system/v1/pre-aggregations/jobs",
        json=response_status_done,
        status=200,
    )
    test_flow()
    assert len(responses.calls) == 2


@responses.activate
def test_wait_completion_two_step():
    @flow(name="test_wait_completion_two_step")
    def test_flow():
        result = build_pre_aggregations(
            auth_header=AuthHeader(
                security_context=None,
                api_secret=SecretStr("23dff8b29cf20df38a4c78dfaf689fa55916add4d27ee3dd9ba75d1")
            ),
            url="http://localhost:4000/cubejs-system",
            selector=selector,
            wait_for_job_run_completion=True,
            wait_time_between_api_calls=0,
        )
        return result

    responses.add(
        responses.POST,
        "http://localhost:4000/cubejs-system/v1/pre-aggregations/jobs",
        json=response_tokens,
        status=200,
    )
    responses.add(
        responses.POST,
        "http://localhost:4000/cubejs-system/v1/pre-aggregations/jobs",
        json=response_status_processing,
        status=200,
    )
    responses.add(
        responses.POST,
        "http://localhost:4000/cubejs-system/v1/pre-aggregations/jobs",
        json=response_status_done,
        status=200,
    )
    test_flow()
    assert len(responses.calls) == 3


@responses.activate
def test_wait_completion_three_step():
    @flow(name="test_wait_completion_three_step")
    def test_flow():
        result = build_pre_aggregations(
            auth_header=AuthHeader(
                security_context=None,
                api_secret=SecretStr("23dff8b29cf20df38a4c78dfaf689fa55916add4d27ee3dd9ba75d1")
            ),
            url="http://localhost:4000/cubejs-system",
            selector=selector,
            wait_for_job_run_completion=True,
            wait_time_between_api_calls=0,
        )
        return result

    responses.add(
        responses.POST,
        "http://localhost:4000/cubejs-system/v1/pre-aggregations/jobs",
        json=response_tokens,
        status=200,
    )
    responses.add(
        responses.POST,
        "http://localhost:4000/cubejs-system/v1/pre-aggregations/jobs",
        json=response_status_processing,
        status=200,
    )
    responses.add(
        responses.POST,
        "http://localhost:4000/cubejs-system/v1/pre-aggregations/jobs",
        json=response_status_processing,
        status=200,
    )
    responses.add(
        responses.POST,
        "http://localhost:4000/cubejs-system/v1/pre-aggregations/jobs",
        json=response_status_done,
        status=200,
    )

    test_flow()
    assert len(responses.calls) == 4


@responses.activate
def test_missing_partitions():
    @flow(name="test_missing_partitions")
    def test_flow():
        result = build_pre_aggregations(
            auth_header=AuthHeader(
                security_context=None,
                api_secret=SecretStr("23dff8b29cf20df38a4c78dfaf689fa55916add4d27ee3dd9ba75d1")
            ),
            url="http://localhost:4000/cubejs-system",
            selector=selector,
            wait_for_job_run_completion=True,
            wait_time_between_api_calls=0,
        )
        return result

    responses.add(
        responses.POST,
        "http://localhost:4000/cubejs-system/v1/pre-aggregations/jobs",
        json=response_tokens,
        status=200,
    )
    responses.add(
        responses.POST,
        "http://localhost:4000/cubejs-system/v1/pre-aggregations/jobs",
        json=response_status_missing_partition,
        status=200,
    )
    msg_match = "Cube pre-aggregations build failed: missing partitions."
    with pytest.raises(CubeJSAPIFailureException, match=msg_match):
        test_flow()
        assert len(responses.calls) == 2


@responses.activate
def test_failure():
    @flow(name="test_failure")
    def test_flow():
        result = build_pre_aggregations(
            auth_header=AuthHeader(
                security_context=None,
                api_secret=SecretStr("23dff8b29cf20df38a4c78dfaf689fa55916add4d27ee3dd9ba75d1")
            ),
            url="http://localhost:4000/cubejs-system",
            selector=selector,
            wait_for_job_run_completion=True,
            wait_time_between_api_calls=0,
        )
        return result

    responses.add(
        responses.POST,
        "http://localhost:4000/cubejs-system/v1/pre-aggregations/jobs",
        json=response_tokens,
        status=200,
    )
    responses.add(
        responses.POST,
        "http://localhost:4000/cubejs-system/v1/pre-aggregations/jobs",
        json=response_status_failure,
        status=200,
    )
    msg_match = "Cube pre-aggregations build failed: failure: returned error."
    with pytest.raises(CubeJSAPIFailureException, match=msg_match):
        test_flow()
        assert len(responses.calls) == 2
