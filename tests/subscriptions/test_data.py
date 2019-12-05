import calendar
from datetime import datetime, timedelta
from unittest.mock import Mock
import uuid

from snuba import settings
from snuba.datasets.events import EventsDataset
from snuba.datasets.factory import enforce_table_writer, get_dataset
from snuba.subscriptions.data import Subscription
from snuba.views import parse_and_run_query
from tests.base import BaseEventsTest


class TestApi(BaseEventsTest):
    def setup_method(self, test_method, dataset_name="events"):
        super().setup_method(test_method, dataset_name)
        # values for test data
        self.project_ids = [1, 2]
        self.environments = [u"prod"]
        self.platforms = ["a", "b", "c", "d", "e", "f"]  # 6 platforms
        self.hashes = [x * 32 for x in "0123456789ab"]  # 12 hashes
        self.group_ids = [int(hsh[:16], 16) for hsh in self.hashes]
        self.minutes = 180

        self.base_time = datetime.utcnow().replace(
            minute=0, second=0, microsecond=0
        ) - timedelta(minutes=self.minutes)
        self.generate_fizzbuzz_events()

    def generate_fizzbuzz_events(self):
        """
        Generate a deterministic set of events across a time range.
        """

        events = []
        for tick in range(self.minutes):
            tock = tick + 1
            for p in self.project_ids:
                # project N sends an event every Nth minute
                if tock % p == 0:
                    events.append(
                        enforce_table_writer(self.dataset)
                        .get_stream_loader()
                        .get_processor()
                        .process_insert(
                            {
                                "project_id": p,
                                "event_id": uuid.uuid4().hex,
                                "deleted": 0,
                                "datetime": (
                                    self.base_time + timedelta(minutes=tick)
                                ).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                                "message": "a message",
                                "search_message": "a long search message"
                                if p == 3
                                else None,
                                "platform": self.platforms[
                                    (tock * p) % len(self.platforms)
                                ],
                                "primary_hash": self.hashes[
                                    (tock * p) % len(self.hashes)
                                ],
                                "group_id": self.group_ids[
                                    (tock * p) % len(self.hashes)
                                ],
                                "retention_days": settings.DEFAULT_RETENTION_DAYS,
                                "data": {
                                    # Project N sends every Nth (mod len(hashes)) hash (and platform)
                                    "received": calendar.timegm(
                                        (
                                            self.base_time + timedelta(minutes=tick)
                                        ).timetuple()
                                    ),
                                    "tags": {
                                        # Sentry
                                        "environment": self.environments[
                                            (tock * p) % len(self.environments)
                                        ],
                                        "sentry:release": str(tick),
                                        "sentry:dist": "dist1",
                                        "os.name": "windows",
                                        "os.rooted": 1,
                                        # User
                                        "foo": "baz",
                                        "foo.bar": "qux",
                                        "os_name": "linux",
                                    },
                                    "exception": {
                                        "values": [
                                            {
                                                "stacktrace": {
                                                    "frames": [
                                                        {
                                                            "filename": "foo.py",
                                                            "lineno": tock,
                                                        },
                                                        {
                                                            "filename": "bar.py",
                                                            "lineno": tock * 2,
                                                        },
                                                    ]
                                                }
                                            }
                                        ]
                                    },
                                },
                            }
                        )
                    )
        self.write_processed_records(events)

    def test_conditions(self):
        subscription = Subscription(
            id="hello",
            project_id=2,
            dataset="events",
            conditions=[["platform", "NOT IN", ["b", "c", "d", "e", "f"]]],
            # conditions=[],
            aggregations=[["count()", "", "count"]],
            time_window=500,
            resolution=1,
        )
        dataset, request = subscription.build_request(datetime.utcnow(), 100, Mock())
        assert isinstance(dataset, EventsDataset)
        query_result = parse_and_run_query(dataset, request, Mock(), referrer='subscription')
        assert query_result.result['data'][0]['count'] == 30
