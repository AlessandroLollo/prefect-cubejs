from urllib.parse import quote_plus

import jwt
import pytest
import responses
from prefect import flow

from prefect_cubejs.exceptions import (
    CubeJSAPIFailureException,
    CubeJSConfigurationException,
)
from prefect_cubejs.blocks import AuthHeader, SecurityContext
from prefect_cubejs.tasks import run_query

from pydantic.v1 import SecretStr
from prefect.blocks.fields import SecretDict

import logging


def test_run_with_no_values_raises():
    @flow(name="test_flow_1")
    def test_flow():
        return run_query(
            auth_header=AuthHeader(
                security_context=None,
                api_secret=SecretStr("secret")
            ),
            query={"measure": "count"}
        )

    msg_match = "Missing both `subdomain` and `url`."
    with pytest.raises(CubeJSConfigurationException, match=msg_match):
        test_flow()


def test_run_without_query_raises():
    @flow(name="test_flow_3")
    def test_flow():
        return run_query(
            auth_header=AuthHeader(
                security_context=None,
                api_secret=SecretStr("secret")
            ),
            subdomain="xyz",
            query=None
        )

    msg_match = "Missing `query`."
    with pytest.raises(CubeJSConfigurationException, match=msg_match):
        test_flow()


@responses.activate
def test_run_with_failing_api_raises():
    @flow(name="test_flow_4")
    def test_flow():
        return run_query(
            auth_header=AuthHeader(
                security_context=None,
                api_secret=SecretStr("secret")
            ),
            subdomain="test",
            query={"measures": "count"}
        )

    responses.add(
        responses.GET, "https://test.cubecloud.dev/cubejs-api/v1/load", status=123
    )

    msg_match = "Cube.js load API failed!"
    with pytest.raises(CubeJSAPIFailureException, match=msg_match):
        test_flow()


@responses.activate
def test_run_with_continue_waiting():
    api_url = "https://test.cubecloud.dev/cubejs-api/v1/load"

    responses.add(
        responses.GET,
        api_url,
        status=200,
        json={"error": "Continue wait"},
    )

    responses.add(
        responses.GET,
        api_url,
        status=200,
        json={"data": "result"},
    )

    @flow(name="test_flow_5")
    def test_flow():
        return run_query(
            auth_header=AuthHeader(
                security_context=None,
                api_secret=SecretStr("secret")
            ),
            subdomain="test",
            query={"measures": "count"}
        )

    expected_url = api_url + "?query=" + quote_plus('{"measures": "count"}')

    data = test_flow()

    assert responses.assert_call_count(expected_url, 2) is True
    assert isinstance(data, dict)


@responses.activate
def test_run_with_security_context():
    responses.add(
        responses.GET,
        "https://test.cubecloud.dev/cubejs-api/v1/load",
        status=200,
        json={"data": "result"},
    )

    @flow(name="test_flow_6")
    def test_flow():
        return run_query(
            auth_header=AuthHeader(
                security_context=SecurityContext(security_context=SecretDict({"foo": "bar"})),
                api_secret=SecretStr("secret")
            ),
            subdomain="test",
            query={"measures": "count"}
        )

    expected_jwt = jwt.encode(
        payload={"foo": "bar", "expiresIn": "7d"}, key="secret", algorithm="HS256"
    )

    test_flow()

    assert responses.calls[0].request.headers["Authorization"] == expected_jwt


@responses.activate
def test_run_with_max_wait_time_raises():
    responses.add(
        responses.GET,
        "https://test.cubecloud.dev/cubejs-api/v1/load",
        status=200,
        json={"error": "Continue wait"},
    )

    @flow(name="test_flow_7")
    def test_flow():
        return run_query(
            auth_header=AuthHeader(
                security_context=SecurityContext(security_context=SecretDict({"foo": "bar"})),
                api_secret=SecretStr("secret")
            ),
            subdomain="test",
            query={"measures": "count"},
            wait_time_between_api_calls=1,
            max_wait_time=3
        )

    msg_match = "Cube.js API took longer than 3 seconds to provide a response."

    with pytest.raises(CubeJSAPIFailureException, match=msg_match):
        test_flow()


@responses.activate
def test_run_with_include_generated_sql():
    responses.add(
        responses.GET,
        "https://test.cubecloud.dev/cubejs-api/v1/load",
        status=200,
        json={"data": "result"},
    )

    responses.add(
        responses.GET,
        "https://test.cubecloud.dev/cubejs-api/v1/sql",
        status=200,
        json={"sql": "sql"},
    )

    @flow(name="test_flow_8")
    def test_flow():
        return run_query(
            auth_header=AuthHeader(
                security_context=SecurityContext(security_context=SecretDict({"foo": "bar"})),
                api_secret=SecretStr("secret")
            ),
            subdomain="test",
            query={"measures": "count"},
            include_generated_sql=True,
        )

    data = test_flow()

    assert isinstance(data, dict)
    assert "data" in data.keys()
    assert "sql" in data.keys()


@responses.activate
def test_run_with_api_secret_warns(caplog):
    responses.add(
        responses.GET,
        "https://test.cubecloud.dev/cubejs-api/v1/load",
        status=200,
        json={"data": "result"},
    )

    @flow(name="test_flow_9")
    def test_flow():
        return run_query(
            api_secret="secret",
            security_context={"foo": "bar"},
            subdomain="test",
            query={"measures": "count"}
        )

    expected_jwt = jwt.encode(
        payload={"foo": "bar", "expiresIn": "7d"}, key="secret", algorithm="HS256"
    )
    with caplog.at_level(logging.WARNING):
        test_flow()

        assert responses.calls[0].request.headers["Authorization"] == expected_jwt
        warn_msg = "You're still using `security_context` and `api_secret`. Please consider switching to `auth_header`"
        assert warn_msg in caplog.text
