"CubeJS blocks"
from prefect.blocks.core import Block
from prefect.blocks.fields import SecretDict

import jwt
from pydantic.v1 import SecretStr
from typing import Dict, Optional, Union


class SecurityContext(Block):
    """
    This block can be used to safely store a CubeJS security context.
    If the `exp` or `expiresIn` keys are not provided, they will be
    added automatically.

    Args:
        security_context (SecretDict): JSON object to use as the security context
            while interacting with CubeJS APIs.
    
    Example:
        Load stored CubeJS Security Context
        ```python
        from prefect_cubejs.blocks import SecurityContext
        sec_context_block = SecurityContext.load("BLOCK_NAME")
        ```
    """  # noqa E501

    security_context: SecretDict

    _block_type_name = "CubeJS Security Context"
    _block_type_slug = "cubejs-security-context"
    _logo_url = "http://www.todo.com"
    _description = "Block that contains a valid Cube JS security context to be used when interacting with CubeJS APIs."


class AuthHeader(Block):
    """
    This block can be used to safely store the informations required
    to build the authorization header that will be used to interact
    with CubeJS API.

    Args:
        security_context (Optional[SecurityContext]): An optional
            `SecurityContext` block.
    
    Example:
        Load stored CubeJS Authorization Header
        ```python
        from prefect_cubejs.blocks import AuthHeader
        sec_context_block = AuthHeader.load("BLOCK_NAME")
        ```
    """

    security_context: Optional[SecurityContext]
    api_secret: SecretStr

    _block_type_name = "CubeJS Authorization Context"
    _block_type_slug = "cubejs-auth-context"
    _logo_url = "http://www.todo.com"
    _description = "Block that contains a valid Cube JS authorization context (optional security context + API secret) to be used when interacting with CubeJS APIs."

    @property
    def api_token(self) -> SecretStr:
        api_token = jwt.encode(payload={}, key=self.api_secret.get_secret_value())
        if isinstance(self.security_context, SecurityContext):
            extended_context = self.security_context.security_context.get_secret_value()
            if (
                "exp" not in extended_context
                and "expiresIn" not in extended_context
            ):
                extended_context["expiresIn"] = "7d"
            api_token = jwt.encode(
                payload=extended_context, key=self.api_secret.get_secret_value(), algorithm="HS256"
            )

        return SecretStr(api_token)