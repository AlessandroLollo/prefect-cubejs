from urllib.parse import quote_plus

import jwt
import pytest
import responses
from prefect import flow

from prefect_cubejs.exceptions import (
    CubeJSAPIFailureException,
    CubeJSConfigurationException,
)
from prefect_cubejs.tasks import run_query


def test_run_with_no_values_raises():
    @flow
    def test_flow():
        return run_query()

    msg_match = "Missing both `subdomain` and `url`."
    with pytest.raises(CubeJSConfigurationException, match=msg_match):
        test_flow().result().result()


def test_run_without_api_secret_api_secret_env_var():
    @flow
    def test_flow():
        return run_query(subdomain="xyz")

    msg_match = "Missing `api_secret` and `api_secret_env_var` not found."
    with pytest.raises(CubeJSConfigurationException, match=msg_match):
        test_flow().result().result()


def test_run_without_query_raises():
    @flow
    def test_flow():
        return run_query(subdomain="xyz", api_secret="secret")

    msg_match = "Missing `query`."
    with pytest.raises(CubeJSConfigurationException, match=msg_match):
        test_flow().result().result()


@responses.activate
def test_run_with_failing_api_raises():
    @flow
    def test_flow():
        return run_query(
            subdomain="test", api_secret="foo", query={"measures": "count"}
        )

    responses.add(
        responses.GET, "https://test.cubecloud.dev/cubejs-api/v1/load", status=123
    )

    msg_match = "Cube.js load API failed!"
    with pytest.raises(CubeJSAPIFailureException, match=msg_match):
        test_flow().result().result()


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

    @flow
    def test_flow():
        return run_query(
            subdomain="test", api_secret="foo", query={"measures": "count"}
        )

    expected_url = api_url + "?query=" + quote_plus('{"measures": "count"}')

    data = test_flow().result().result()

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

    @flow
    def test_flow():
        return run_query(
            subdomain="test",
            api_secret="foo",
            query={"measures": "count"},
            security_context={"foo": "bar"},
        )

    expected_jwt = jwt.encode(
        payload={"foo": "bar", "expiresIn": "7d"}, key="foo", algorithm="HS256"
    )

    test_flow().result().result()

    assert responses.calls[0].request.headers["Authorization"] == expected_jwt


@responses.activate
def test_run_with_max_wait_time_raises():
    responses.add(
        responses.GET,
        "https://test.cubecloud.dev/cubejs-api/v1/load",
        status=200,
        json={"error": "Continue wait"},
    )

    @flow
    def test_flow():
        return run_query(
            subdomain="test",
            api_secret="foo",
            query={"measures": "count"},
            security_context={"foo": "bar"},
            wait_time_between_api_calls=1,
            max_wait_time=3,
        )

    msg_match = "Cube.js API took longer than 3 seconds to provide a response."

    with pytest.raises(CubeJSAPIFailureException, match=msg_match):
        test_flow().result().result()


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

    @flow
    def test_flow():
        return run_query(
            subdomain="test",
            api_secret="foo",
            query={"measures": "count"},
            include_generated_sql=True,
        )

    data = test_flow().result().result()

    assert isinstance(data, dict)
    assert "data" in data.keys()
    assert "sql" in data.keys()
