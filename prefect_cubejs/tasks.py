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
    This task calls Cueb.js load API and returns the result
    as a JSON object.
    More info about Cube.js load API at
    https://cube.dev/docs/rest-api#api-reference-v-1-load.

    Args:
        - subdomain (str, optional): The subdomain to use to get the data.
            If provided, `subdomain` takes precedence over `url`.
            This is likely to be useful to Cube Cloud users.
        - url (str, optional): The URL to use to get the data.
            This is likely to be useful to users of self-hosted Cube.js.
        - api_secret (str, optional): The API secret used to generate an
            API token for authentication.
            If provided, it takes precedence over `api_secret_env_var`.
        - api_secret_env_var (str, optional): The name of the env var that contains
            the API secret to use to generate an API token for authentication.
            Defaults to `CUBEJS_API_SECRET`.
        - query (dict, list, optional): `dict` or `list` representing
            valid Cube.js queries.
            If you pass multiple queries, then be aware of Cube.js Data Blending.
            More info at https://cube.dev/docs/rest-api#api-reference-v-1-load
            and at https://cube.dev/docs/schema/advanced/data-blending.
            Query format can be found at: https://cube.dev/docs/query-format.
        - include_generated_sql: (bool, optional): Whether the return object should
            include SQL info or not.
            Default to `False`.
        - security_context (str, dict, optional): The security context to use
            during authentication.
            If the security context does not contain an expiration period,
            then a 7-day expiration period is added automatically.
            More info at: https://cube.dev/docs/security/context.
        - wait_time_between_api_calls (int, optional): The number of seconds to
            wait between API calls.
            Default to 10.
        - max_wait_time (int, optional): The number of seconds to wait for the
            Cube.js load API to return a response.
        - **kwargs (optional): Additional keyword arguments to pass to the
            standard Task initalization.

    Raises:
        - `CubeJSConfigurationException` if both `subdomain` and `url` are missing.
        - `CubeJSConfigurationException` if `api_token` is missing
            and `api_token_env_var` cannot be found.
        - `CubeJSConfigurationException` if `query` is missing.
        - `prefect.engine.signals.FAIL` if the Cube.js load API fails.
        - `prefect.engine.signals.FAIL` if the Cube.js load API takes more than
            `max_wait_time` seconds to respond.

    Returns:
        - The Cube.js JSON response, augmented with SQL
            information if `include_generated_sql` is `True`.
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
