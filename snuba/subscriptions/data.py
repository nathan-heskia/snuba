from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Tuple

from snuba.datasets.dataset import Dataset
from snuba.datasets.factory import get_dataset
from snuba.views import validate_request_content
from snuba.query.types import Condition
from snuba.request import Request
from snuba.request.schema import RequestSchema


@dataclass(frozen=True)
class Subscription:
    """
    Represents the state of a subscription.
    """
    id: str
    project_id: int
    dataset: str
    conditions: List[Condition]
    aggregations: List[Tuple[str, str, str]]
    time_window: int
    resolution: int

    def build_request(self, timestamp: datetime, offset: int, timer) -> (Dataset, Request):
        """
        Returns a (Dataset, Request) tuple that can be used to run a query via
        `parse_and_run_query`.
        :param timestamp: Date that the query should run up until
        :param offset: Maximum offset we should query for
        """
        dataset = get_dataset(self.dataset)
        schema = RequestSchema.build_with_extensions(dataset.get_extensions())
        return dataset, validate_request_content(
            {
                'project': self.project_id,
                'conditions': self.conditions + [[["ifnull", ["offset", 0]], "<=", offset]],
                'aggregations': self.aggregations,
                'from_date': (timestamp - timedelta(minutes=self.time_window)).isoformat(),
                'to_date': timestamp.isoformat(),
            },
            schema,
            timer,
            dataset,
        )
