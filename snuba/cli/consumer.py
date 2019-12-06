import logging
import signal
from typing import Optional

import click

from snuba import settings
from snuba.datasets.factory import get_dataset, DATASET_NAMES
from snuba.datasets.cdc import CdcDataset
from snuba.consumers.consumer_builder import ConsumerBuilder
from snuba.stateful_consumer.consumer_state_machine import ConsumerStateMachine
from snuba.utils.streams.consumers.backends.kafka import KafkaTopic


@click.command()
@click.option(
    "--raw-events-topic",
    "raw_events_topic_name",
    default=None,
    help="Topic to consume raw events from.",
)
@click.option(
    "--replacements-topic",
    "replacements_topic_name",
    default=None,
    help="Topic to produce replacement messages info.",
)
@click.option(
    "--commit-log-topic",
    "commit_log_topic_name",
    default=None,
    help="Topic for committed offsets to be written to, triggering post-processing task(s)",
)
@click.option(
    "--control-topic", default=None, help="Topic used to control the snapshot"
)
@click.option(
    "--consumer-group",
    default="snuba-consumers",
    help="Consumer group use for consuming the raw events topic.",
)
@click.option(
    "--bootstrap-server",
    default=None,
    multiple=True,
    help="Kafka bootstrap server to use.",
)
@click.option(
    "--dataset",
    default="events",
    type=click.Choice(DATASET_NAMES),
    help="The dataset to target",
)
@click.option(
    "--max-batch-size",
    default=settings.DEFAULT_MAX_BATCH_SIZE,
    help="Max number of messages to batch in memory before writing to Kafka.",
)
@click.option(
    "--max-batch-time-ms",
    default=settings.DEFAULT_MAX_BATCH_TIME_MS,
    help="Max length of time to buffer messages in memory before writing to Kafka.",
)
@click.option(
    "--auto-offset-reset",
    default="error",
    type=click.Choice(["error", "earliest", "latest"]),
    help="Kafka consumer auto offset reset.",
)
@click.option(
    "--queued-max-messages-kbytes",
    default=settings.DEFAULT_QUEUED_MAX_MESSAGE_KBYTES,
    type=int,
    help="Maximum number of kilobytes per topic+partition in the local consumer queue.",
)
@click.option(
    "--queued-min-messages",
    default=settings.DEFAULT_QUEUED_MIN_MESSAGES,
    type=int,
    help="Minimum number of messages per topic+partition librdkafka tries to maintain in the local consumer queue.",
)
@click.option("--log-level", default=settings.LOG_LEVEL, help="Logging level to use.")
@click.option(
    "--dogstatsd-host",
    default=settings.DOGSTATSD_HOST,
    help="Host to send DogStatsD metrics to.",
)
@click.option(
    "--dogstatsd-port",
    default=settings.DOGSTATSD_PORT,
    type=int,
    help="Port to send DogStatsD metrics to.",
)
@click.option(
    "--stateful-consumer",
    default=False,
    type=bool,
    help="Runs a stateful consumer (that manages snapshots) instead of a basic one.",
)
def consumer(
    *,
    raw_events_topic_name: Optional[str],
    replacements_topic_name: Optional[str],
    commit_log_topic_name: Optional[str],
    control_topic,
    consumer_group,
    bootstrap_server,
    dataset,
    max_batch_size,
    max_batch_time_ms,
    auto_offset_reset,
    queued_max_messages_kbytes,
    queued_min_messages,
    log_level,
    dogstatsd_host,
    dogstatsd_port,
    stateful_consumer,
) -> None:

    import sentry_sdk

    sentry_sdk.init(dsn=settings.SENTRY_DSN)

    logging.basicConfig(
        level=getattr(logging, log_level.upper()), format="%(asctime)s %(message)s"
    )
    dataset_name = dataset
    dataset = get_dataset(dataset_name)

    consumer_builder = ConsumerBuilder(
        dataset_name=dataset_name,
        raw_topic=(
            KafkaTopic(raw_events_topic_name)
            if raw_events_topic_name is not None
            else None
        ),
        replacements_topic=(
            KafkaTopic(replacements_topic_name)
            if replacements_topic_name is not None
            else None
        ),
        commit_log_topic=(
            KafkaTopic(commit_log_topic_name)
            if commit_log_topic_name is not None
            else None
        ),
        max_batch_size=max_batch_size,
        max_batch_time_ms=max_batch_time_ms,
        bootstrap_servers=bootstrap_server,
        group_id=consumer_group,
        auto_offset_reset=auto_offset_reset,
        queued_max_messages_kbytes=queued_max_messages_kbytes,
        queued_min_messages=queued_min_messages,
        dogstatsd_host=dogstatsd_host,
        dogstatsd_port=dogstatsd_port,
    )

    if stateful_consumer:
        assert isinstance(
            dataset, CdcDataset
        ), "Only CDC dataset have a control topic thus are supported."
        context = ConsumerStateMachine(
            consumer_builder=consumer_builder,
            topic=control_topic or dataset.get_default_control_topic(),
            group_id=consumer_group,
            dataset=dataset,
        )

        def handler(signum, frame):
            context.signal_shutdown()

        signal.signal(signal.SIGINT, handler)
        signal.signal(signal.SIGTERM, handler)

        context.run()
    else:
        consumer = consumer_builder.build_base_consumer()

        def handler(signum, frame):
            consumer.signal_shutdown()

        signal.signal(signal.SIGINT, handler)
        signal.signal(signal.SIGTERM, handler)

        consumer.run()
