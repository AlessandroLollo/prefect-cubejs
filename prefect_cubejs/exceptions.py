"""
Exceptions to be used when interacting with Cube.js
"""


class CubeJSConfigurationException(Exception):
    """
    Exception to raise when a Cube.js task is misconfigured.
    """

    pass


class CubeJSAPIFailureException(Exception):
    """
    Exception to raise when a Cube.js task fails to execute.
    """

    pass
