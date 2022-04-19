# prefect-cubejs

## Welcome!

Prefect collection of tasks to interact with Cube.js

## Getting Started

### Python setup

Requires an installation of Python 3.7+.

We recommend using a Python virtual environment manager such as pipenv, conda or virtualenv.

These tasks are designed to work with Prefect 2.0. For more information about how to use Prefect, please refer to the [Prefect documentation](https://orion-docs.prefect.io/).

### Installation

Install `prefect-cubejs` with `pip`:

```bash
pip install prefect-cubejs
```

### Write and run a flow

```python
from prefect import flow
from prefect_cubejs.tasks import (
    run_query
)


@flow
def example_flow():
    run_query()

example_flow()
```

## Resources

If you encounter any bugs while using `prefect-cubejs`, feel free to open an issue in the [prefect-cubejs](https://github.com/AlessandroLollo/prefect-cubejs) repository.

If you have any questions or issues while using `prefect-cubejs`, you can find help in either the [Prefect Discourse forum](https://discourse.prefect.io/) or the [Prefect Slack community](https://prefect.io/slack).

## Development

If you'd like to install a version of `prefect-cubejs` for development, clone the repository and perform an editable install with `pip`:

```bash
git clone https://github.com/AlessandroLollo/prefect-cubejs.git

cd prefect-cubejs/

pip install -e ".[dev]"

# Install linting pre-commit hooks
pre-commit install
```
