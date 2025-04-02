import asyncio

from mirroring_kafka.logger import log
from mirroring_kafka.utils import KafkaSettings, get_consumer, get_producer


async def run_mirroring(
    src_kafka_settings: KafkaSettings,
    dest_kafka_settings: KafkaSettings,
    stopping: asyncio.Event,
):
    log.info('Mirroring started')
    while not stopping.is_set():

        async with get_consumer(
            src_kafka_settings,
            [src_kafka_settings.topic],
        ) as src_consumer, get_producer(dest_kafka_settings) as dest_producer:
            data = await src_consumer.getmany(timeout_ms=10000, max_records=1000)
            log.debug(f'Read {len(data)} messages')
            for pt, messages in data.items():
                for msg in messages:
                    await dest_producer.send_and_wait(
                        topic=dest_kafka_settings.topic,
                        value=msg.value,
                        key=msg.key,
                    )
            log.debug(f'Mirroring done: {len(data)} messages')

        await asyncio.sleep(5)

    log.info('Mirroring stopped')
