"""
Collection of tasks to interact with Cube.js
"""
import json
import os
import time
from typing import Dict, List, Optional, Union

from prefect import get_run_logger, task

from prefect_cubejs.exceptions import (
    CubeJSAPIFailureException,
    CubeJSConfigurationException,
)
from prefect_cubejs.blocks import AuthHeader, SecurityContext
from prefect_cubejs.utils import CubeJSClient

from prefect.blocks.fields import SecretDict


def _get_auth_header(api_secret: Optional[str], api_secret_env_var: Optional[str], security_context: Optional[Union[str, Dict]]) -> AuthHeader:
    """
    Helper method the build the `AuthHeader` object required by `CubeJSClient`.

    Args:
        api_secret (str, optional): The API secret used to generate an
            API token for authentication.
            If provided, it takes precedence over `api_secret_env_var`.
        api_secret_env_var (str, optional): The name of the env var that contains
            the API secret to use to generate an API token for authentication.
            Defaults to `CUBEJS_API_SECRET`.
        security_context (str, dict, optional): The security context to use
            during authentication.
            If the security context does not contain an expiration period,
            then a 7-day expiration period is added automatically.
            More info at https://cube.dev/docs/security/context.
    
    Returns:
        An `AuthHeader` object generated using the provided
            API secret and security context.
    """

    logger = get_run_logger()
    
    if not api_secret and api_secret_env_var not in os.environ:
        msg = "Missing `api_secret` and `api_secret_env_var` not found."
        raise CubeJSConfigurationException(msg)
    
    logger.warning("You're still using `security_context` and `api_secret`. Please consider switching to `auth_header`")
    context = None
    if security_context:
        sec_context = SecretDict(json.loads(security_context) if isinstance(security_context, str) else security_context)
        context = SecurityContext(security_context=sec_context)
    return AuthHeader(
        security_context=context,
        api_secret=api_secret if api_secret else os.environ[api_secret_env_var]
    )



@task
def run_query(
    query: Union[Dict, List[Dict]],
    auth_header: Optional[AuthHeader] = None,
    subdomain: Optional[str] = None,
    url: Optional[str] = None,
    api_secret: Optional[str] = None,
    api_secret_env_var: Optional[str] = "CUBEJS_API_SECRET",
    security_context: Optional[Union[str, Dict]] = None,
    include_generated_sql: Optional[bool] = False,
    wait_time_between_api_calls: Optional[int] = 10,
    max_wait_time: Optional[int] = None,
) -> Dict:
    """
    This task calls Cube.js load API and returns the result
    as a JSON object.
    More info about Cube.js load API at
    https://cube.dev/docs/rest-api#api-reference-v-1-load.

    Args:
        query: `dict` or `list` representing
            valid Cube.js queries.
            If you pass multiple queries, then be aware of Cube.js Data Blending.
            More info at https://cube.dev/docs/rest-api#api-reference-v-1-load
            and at https://cube.dev/docs/schema/advanced/data-blending.
            Query format can be found at: https://cube.dev/docs/query-format.
        auth_header: The `AuthHeader` object to be used
            when interacting with Cube.js APIs.
        subdomain: The subdomain to use to get the data.
            If provided, `subdomain` takes precedence over `url`.
            This is likely to be useful to Cube Cloud users.
        url: The URL to use to get the data.
            This is likely to be useful to users of self-hosted Cube.js.
        api_secret: The API secret used to generate an
            API token for authentication.
            If provided, it takes precedence over `api_secret_env_var`.
        api_secret_env_var: The name of the env var that contains
            the API secret to use to generate an API token for authentication.
            Defaults to `CUBEJS_API_SECRET`.
        include_generated_sql: Whether the return object should
            include SQL info or not.
            Default to `False`.
        security_context: The security context to use
            during authentication.
            If the security context does not contain an expiration period,
            then a 7-day expiration period is added automatically.
            More info at: https://cube.dev/docs/security/context.
        wait_time_between_api_calls: The number of seconds to
            wait between API calls.
            Default to 10.
        max_wait_time: The number of seconds to wait for the
            Cube.js load API to return a response.

    Raises:
        - `CubeJSConfigurationException` if both `subdomain` and `url` are missing.
        - `CubeJSConfigurationException` if `query` is missing.
        - `CubeJSAPIFailureException` if the Cube.js load API fails.
        - `CubeJSAPIFailureException` if the Cube.js load API takes more than
            `max_wait_time` seconds to respond.

    Returns:
        The Cube.js JSON response, augmented with SQL
            information if `include_generated_sql` is `True`.
    """

    if not subdomain and not url:
        msg = "Missing both `subdomain` and `url`."
        raise CubeJSConfigurationException(msg)

    if not query:
        msg = "Missing `query`."
        raise CubeJSConfigurationException(msg)

    if not auth_header:
        auth_header = _get_auth_header(
            api_secret=api_secret,
            api_secret_env_var=api_secret_env_var,
            security_context=security_context
        )

    wait_api_call_secs = (
        wait_time_between_api_calls if wait_time_between_api_calls > 0 else 10
    )

    cubejs_client = CubeJSClient(
        subdomain=subdomain,
        url=url,
        auth_header=auth_header,
        wait_api_call_secs=wait_api_call_secs,
        max_wait_time=max_wait_time,
    )

    params = {"query": json.dumps(query)}

    # Retrieve data from Cube.js
    data = cubejs_client.get_data(
        params=params, include_generated_sql=include_generated_sql
    )

    return data


@task
def build_pre_aggregations(
    auth_header: AuthHeader,
    subdomain: Optional[str] = None,
    url: Optional[str] = None,
    api_secret: Optional[str] = None,
    api_secret_env_var: Optional[str] = "CUBEJS_API_SECRET",
    security_context: Optional[Union[str, Dict]] = None,
    selector: Dict = None,
    wait_for_job_run_completion: bool = False,
    wait_time_between_api_calls: Optional[int] = 10,
) -> Union[bool, Dict]:
    """
    Task run method to perform pre-aggregations build.

    Args:
        auth_header (AuthHeader): The `AuthHeader` object to be used
            when interacting with Cube.js APIs.
        subdomain (str, optional): The subdomain to use to get the data.
            If provided, `subdomain` takes precedence over `url`.
            This is likely to be useful to Cube Cloud users.
        url (str, optional): The URL to use to get the data.
            This is likely the preferred method for self-hosted Cube
            deployments.
            For Cube Cloud deployments, the URL should be in the form
            `https://<cubejs-generated-host>/cubejs-api`.
        api_secret: The API secret used to generate an
            API token for authentication.
            If provided, it takes precedence over `api_secret_env_var`.
        api_secret_env_var: The name of the env var that contains
            the API secret to use to generate an API token for authentication.
            Defaults to `CUBEJS_API_SECRET`.
        security_context (str, dict, optional): The security context to use
            during authentication.
            If the security context does not contain an expiration period,
            then a 7-day expiration period is added automatically.
            More info at https://cube.dev/docs/security/context.
        selector (dict): `dict` representing valid Cube `pre-aggregations/jobs`
            API `selector` object.
        wait_for_job_run_completion (boolean, optional):
            Whether the task should wait for the job run completion or not.
            Default to False.
        wait_time_between_api_calls (int, optional): The number of seconds to
            wait between API calls.
            Default to 10.

    Raises:
        - `CubeJSConfigurationException` if both `subdomain` and `url` are missing.
        - `CubeJSConfigurationException` if `api_token` is missing and
            `api_token_env_var` cannot be found.
        - `CubeJSConfigurationException` if `selector` is missing.
        - `CubeJSAPIFailureException` if the Cube `pre-aggregations/jobs` API fails.
        - `CubeJSAPIFailureException` if any pre-aggregations were not built.

    Returns:
        If `wait_for_job_run_completion = False`, then returns the Cube
            `pre-aggregations/jobs` API trigger run result.  
            If `wait_for_job_run_completion = True`, then returns `True` if
            pre-aggregations were successfully built. Raise otherwise.
    """

    logger = get_run_logger()

    # assert
    if not subdomain and not url:
        raise CubeJSConfigurationException("Missing both `subdomain` and `url`.")

    if not selector:
        raise CubeJSConfigurationException("Missing `selector`.")
    
    if not auth_header:
        auth_header = _get_auth_header(
            api_secret=api_secret,
            api_secret_env_var=api_secret_env_var,
            security_context=security_context
        )

    cubejs_client = CubeJSClient(
        subdomain=subdomain,
        url=url,
        auth_header=auth_header,
        wait_api_call_secs=None,
        max_wait_time=None,
    )

    # post
    query = json.dumps(
        {
            "action": "post",
            "selector": selector,
        }
    )
    tokens = cubejs_client.pre_aggregations_jobs(query=query)
    if not wait_for_job_run_completion:
        return tokens

    # wait for the job completion
    iterate = len(tokens) > 0
    while iterate:

        # fetch
        logger.info(
            f"waiting {wait_time_between_api_calls}sec for the job completion..."
        )
        time.sleep(wait_time_between_api_calls)
        query = json.dumps(
            {
                "action": "get",
                "resType": "object",
                "tokens": tokens,
            }
        )
        statuses = cubejs_client.pre_aggregations_jobs(query=query)

        # check
        missing_only = True
        all_tokens = statuses.keys()
        in_process = []
        for token in all_tokens:
            status = statuses[token]["status"]
            if status.find("failure") >= 0:
                msg = f"""
                Cube pre-aggregations build failed: {status}.
                """
                raise CubeJSAPIFailureException(msg)
            if status != "missing_partition":
                missing_only = False
            if status != "done":
                in_process.append(token)

        if missing_only:
            msg = """
            Cube pre-aggregations build failed: missing partitions.
            """
            raise CubeJSAPIFailureException(msg)

        iterate = len(in_process) > 0

    # result
    return True
