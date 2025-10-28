from dagster import Definitions
from jobs import banking_pipeline

defs = Definitions(
    jobs=[banking_pipeline],
)
