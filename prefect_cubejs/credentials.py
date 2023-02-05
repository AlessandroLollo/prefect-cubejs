"""Block that contains the secret to authenticate with CubeJS APIs."""
from prefect.blocks.core import Block
from pydantic import Field, SecretStr


class CubeJSSecret(Block):
    """Block that contains the secret to authenticate with CubeJS APIs."""

    _block_type_name = "CubeJS secret"
    _logo_url = "https://avatars.githubusercontent.com/u/52467369?s=200&v=4"  # noqa

    secret: SecretStr = Field(
        ...,
        title="CubeJS secret.",
        description="Secret to be used to interact with CubeJS.",
    )
