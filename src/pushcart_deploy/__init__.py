"""Deployment helper package for Pushcart configurations.

To be used in conjunction with a checked-out Git repo holding Pushcart data pipeline
configurations, and a target Databricks environment.

Notes
-----
Can be run in either CI/CD, or locally.
Requires Databricks CLI to already be configured for your target Databricks environment

"""


from .metadata import Metadata
from .setup import Setup

__all__ = ["configuration", "databricks-api", "validation", "Metadata", "Setup"]
