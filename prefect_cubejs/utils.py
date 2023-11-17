"""
Cube.js utils classes
"""
import time
from typing import Dict, Union

import jwt
from requests import Session

from prefect_cubejs.exceptions import CubeJSAPIFailureException

from prefect_cubejs.blocks import AuthHeader


class CubeJSClient:
    """
    Class that represents a Cube.js client that can be used
    to interact with Cube.js APIs.
    """

    # Cube Cloud base URL
    __CUBEJS_CLOUD_BASE_URL = "https://{subdomain}.cubecloud.dev"

    def __init__(
        self,
        subdomain: str,
        url: str,
        auth_header: AuthHeader,
        wait_api_call_secs: int,
        max_wait_time: int,
    ):
        """
        Initialize a `CubeJSClient`.
        The client can be used to interact with Cube.js APIs.

        Args:
            - subdomain (str): Cube Cloud subdomain.
            - url (str): Cube.js URL (likely to be used in self-hosted Cube.js
                deployments).
            - auth_header (AuthHeader): The `AuthHeader` object to be used
                when interacting with Cube.js APIs.
            - wait_api_call_secs (int): Number of seconds to wait
                between API calls.
            - max_wait_time (int): The maximum amount of seconds to wait for
                an API call to respond.
        """
        self.subdomain = subdomain
        self.url = url
        self.auth_header = auth_header
        self.cube_base_url = self._get_cube_base_url()
        self.query_api_url = self._get_query_api_url()
        self.generated_sql_api_url = self._get_generated_sql_api_url()
        self.pre_aggregations_jobs_api_url = self._get_pre_aggregations_jobs_api_url()
        self.wait_api_call_secs = wait_api_call_secs
        self.max_wait_time = max_wait_time

    def _get_cube_base_url(self) -> str:
        """
        Get Cube.js base URL.

        Returns:
            - Cube.js API base url.
        """
        cube_base_url = self.__CUBEJS_CLOUD_BASE_URL
        if self.subdomain:
            cube_base_url = (
                f"{cube_base_url.format(subdomain=self.subdomain)}/cubejs-api"
            )
        else:
            cube_base_url = self.url
        return cube_base_url

    def _get_query_api_url(self) -> str:
        """
        Get Cube.js Query API URL.

        Returns:
            - Cube.js Query API URL.
        """
        return f"{self.cube_base_url}/v1/load"

    def _get_generated_sql_api_url(self) -> str:
        """
        Get Cube.js Query SQL API URL.

        Returns:
            - Cube.js Query SQL API URL.
        """

        return f"{self.cube_base_url}/v1/sql"

    def _get_pre_aggregations_jobs_api_url(self) -> str:
        """
        Get Cube Pre-Aggregations Jobs API URL.

        Returns:
            - Cube Pre-Aggregations Jobs API URL.
        """
        return f"{self.cube_base_url}/v1/pre-aggregations/jobs"

    def _get_data_from_url(self, api_url: str, params: Dict) -> Dict:
        """
        Retrieve data from a Cube.js API.

        Args:
            - api_url (str): The URL of the Cube API to call.
            - params (dict): Parameters to be passed to the API call.

        Raises:
            - `CubeJSAPIFailureException` if the response has `status_code != 200`.
            - `CubeJSAPIFailureException` if the REST APIs takes too long to respond,
                with regards to `max_wait_time`.

        Returns:
            - Cube.js REST API JSON response
        """
        session = Session()
        session.headers = {
            "Content-type": "application/json",
            "Authorization": self.auth_header.api_token.get_secret_value(),
        }
        elapsed_wait_time = 0
        while not self.max_wait_time or elapsed_wait_time <= self.max_wait_time:

            with session.get(url=api_url, params=params) as response:
                if response.status_code == 200:
                    data = response.json()

                    if "error" in data.keys() and "Continue wait" in data["error"]:
                        time.sleep(self.wait_api_call_secs)
                        elapsed_wait_time += self.wait_api_call_secs
                        continue

                    else:
                        return data

                else:
                    msg = f"Cube.js load API failed! Error is: {response.reason}"
                    raise CubeJSAPIFailureException(msg)
        msg = f"""
        Cube.js API took longer than {self.max_wait_time} seconds to provide a response.
        """
        raise CubeJSAPIFailureException(msg)

    def get_data(
        self,
        params: Dict,
        include_generated_sql: bool,
    ) -> Dict:
        """
        Retrieve data from Cube.js `/load` REST API.

        Args:
            - params (dict): Parameters to pass to the `/load` REST API.
            - include_generated_sql (bool): Whether to include the
                corresponding generated SQL or not.

        Returns:
            - Cube.js `/load` API JSON response, augmented with SQL
                information if `include_generated_sql` is `True`.
        """
        data = self._get_data_from_url(api_url=self.query_api_url, params=params)

        if include_generated_sql:
            data["sql"] = self._get_data_from_url(
                api_url=self.generated_sql_api_url, params=params
            )["sql"]

        return data

    def pre_aggregations_jobs(
        self,
        query: Dict,
    ) -> Dict:
        """
        Call Cube `pre-aggregations/jobs` REST API enpoint and return list of
        added jobs ids as a JSON object.

        Args:
            - query (dict): Parameters to pass to the `pre-aggregations/jobs` REST API.

        Returns:
            - Cube `pre-aggregations/jobs` API JSON response.
        """

        session = Session()
        session.headers = {
            "Content-type": "application/json",
            "Authorization": self.auth_header.api_token.get_secret_value(),
        }

        with session.post(
            url=self.pre_aggregations_jobs_api_url, data=query
        ) as response:
            if response.status_code == 200:
                res = response.json()
                return res
            else:
                msg = f"""
                Cube `pre-aggregations/jobs` API failed: {response.reason}
                """
                raise CubeJSAPIFailureException(msg)
