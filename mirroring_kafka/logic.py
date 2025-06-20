import asyncio


from mirroring_kafka.logger import log
from mirroring_kafka.utils import (
    KafkaSettings, get_consumer, get_producer
)

PAST_LAG = 30*60


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
        ) as src_consumer:
            data = await src_consumer.getmany(timeout_ms=10000, max_records=1000)
            count = {'read': 0, 'sent': 0, 'skipped': 0}
            async with get_producer(dest_kafka_settings) as dest_producer:
                offsets = {}
                for pt, messages in data.items():
                    for msg in messages:
                        count['read'] += 1

                        await dest_producer.send_and_wait(
                            topic=dest_kafka_settings.topic,
                            value=msg.value,
                            key=msg.key,
                            timestamp_ms=msg.timestamp,
                        )
                        count['sent'] += 1
                        offsets[pt] = msg.offset + 1

            log.debug({
                "message": 'Mirroring done',
                "read": count["read"],
                "sent": count["sent"],
                "skipped": count["skipped"],
                "src_topic": src_kafka_settings.topic,
                "dest_topic": dest_kafka_settings.topic,
            })
            if offsets:
                await src_consumer.commit(offsets)

        await asyncio.sleep(5)

    log.info('Mirroring stopped')
