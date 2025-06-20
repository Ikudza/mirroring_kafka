from contextlib import asynccontextmanager
from typing import NamedTuple, AsyncIterator

import aiokafka
import aiokafka.helpers

from mirroring_kafka.logger import log


Past = list[tuple[int, bytes]]


class KafkaSettings(NamedTuple):
    servers: str
    topic: str
    sasl_mechanism: str
    security_protocol: str
    ca_file: str | None
    username: str | None
    password: str | None
    cert_file: str | None
    key_file: str | None


@asynccontextmanager
async def get_consumer(
    settings: KafkaSettings,
    topics: list[str],
) -> AsyncIterator[aiokafka.AIOKafkaConsumer]:

    if settings.ca_file:
        ssl_context = aiokafka.helpers.create_ssl_context(
            cafile=settings.ca_file,
        )
    else:
        ssl_context = None

    if settings.cert_file and settings.key_file:
        if ssl_context:
            ssl_context.load_cert_chain(
                certfile=settings.cert_file,
                keyfile=settings.key_file,
            )
        else:
            ssl_context = aiokafka.helpers.create_ssl_context(
                certfile=settings.cert_file,
                keyfile=settings.key_file,
            )

    consumer = aiokafka.AIOKafkaConsumer(
        *topics,
        bootstrap_servers=settings.servers,
        group_id=f'mirroring_kafka_{settings.topic}_vq1',
        fetch_max_wait_ms=1000,
        sasl_mechanism=settings.sasl_mechanism,
        security_protocol=settings.security_protocol,
        ssl_context=ssl_context,
        sasl_plain_username=settings.username,
        sasl_plain_password=settings.password,
        enable_auto_commit=False,
        auto_offset_reset='latest',
        max_poll_records=100,
    )
    log.debug(f'Init consumer: {topics}')

    await consumer.start()

    try:
        yield consumer
    finally:
        await consumer.stop()


@asynccontextmanager
async def get_producer(
    settings: KafkaSettings,
) -> AsyncIterator[aiokafka.AIOKafkaConsumer]:

    if settings.ca_file:
        ssl_context = aiokafka.helpers.create_ssl_context(cafile=settings.ca_file)
    else:
        ssl_context = None

    if settings.cert_file and settings.key_file:
        if ssl_context:
            ssl_context.load_cert_chain(
                certfile=settings.cert_file,
                keyfile=settings.key_file,
            )
        else:
            ssl_context = aiokafka.helpers.create_ssl_context(
                certfile=settings.cert_file,
                keyfile=settings.key_file,
            )

    producer = aiokafka.AIOKafkaProducer(
        bootstrap_servers=settings.servers,
        acks='all',
        compression_type='lz4',
        sasl_mechanism=settings.sasl_mechanism,
        security_protocol=settings.security_protocol,
        ssl_context=ssl_context,
        sasl_plain_username=settings.username,
        sasl_plain_password=settings.password,
        max_request_size=10485880,  # have problem with event, size = 2481486
    )
    log.debug(f'Init producer: {settings.topic}')

    await producer.start()

    try:
        yield producer
    finally:
        await producer.stop()
