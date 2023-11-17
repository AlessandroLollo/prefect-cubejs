from prefect_cubejs.tasks import _get_auth_header

if __name__ == "__main__":
    _get_auth_header(
        api_secret="secret",
        api_secret_env_var=None,
        security_context={"foo": "bar"}
    )