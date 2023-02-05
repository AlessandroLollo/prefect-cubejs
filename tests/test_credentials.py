from pydantic import SecretStr

from prefect_cubejs.credentials import CubeJSSecret


def test_secret_creation():
    expected_secret = "foo"
    scrt = CubeJSSecret(secret=SecretStr(expected_secret))

    assert scrt.secret.get_secret_value() == expected_secret
