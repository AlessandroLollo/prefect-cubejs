"""
Collection of tasks to interact with Cube.js
"""
import json
import os
from typing import Dict, List, Union

from prefect import task

from prefect_cubejs.exceptions import CubeJSConfigurationException
from prefect_cubejs.utils import CubeJSClient


@task
def run_query(
    subdomain: str = None,
    url: str = None,
    api_secret: str = None,
    api_secret_env_var: str = "CUBEJS_API_SECRET",
    query: Union[Dict, List[Dict]] = None,
    include_generated_sql: bool = False,
    security_context: Union[str, Dict] = None,
    wait_time_between_api_calls: int = 10,
    max_wait_time: int = None,
):
    """
    TODO
    """
    if not subdomain and not url:
        msg = "Missing both `subdomain` and `url`."
        raise CubeJSConfigurationException(msg)

    if not api_secret and api_secret_env_var not in os.environ:
        msg = "Missing `api_secret` and `api_secret_env_var` not found."
        raise CubeJSConfigurationException(msg)

    if not query:
        msg = "Missing `query`."
        raise CubeJSConfigurationException(msg)

    secret = api_secret if api_secret else os.environ[api_secret_env_var]

    wait_api_call_secs = (
        wait_time_between_api_calls if wait_time_between_api_calls > 0 else 10
    )

    cubejs_client = CubeJSClient(
        subdomain=subdomain,
        url=url,
        security_context=security_context,
        secret=secret,
        wait_api_call_secs=wait_api_call_secs,
        max_wait_time=max_wait_time,
    )

    params = {"query": json.dumps(query)}

    # Retrieve data from Cube.js
    data = cubejs_client.get_data(
        params=params, include_generated_sql=include_generated_sql
    )

    return data
