import asyncio
from datetime import datetime

from mirroring_kafka.logger import log
from mirroring_kafka.utils import (
    KafkaSettings, get_consumer, get_producer, Past
)

PAST_LAG = 30*60


async def run_mirroring(
    src_kafka_settings: KafkaSettings,
    dest_kafka_settings: KafkaSettings,
    stopping: asyncio.Event,
):
    log.info('Mirroring started')
    past_data = await get_last(dest_kafka_settings)
    past_ids = {x for _, x in past_data}
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
                        if past_data and past_data[-1][0] >= msg.timestamp:
                            if msg.key in past_ids:
                                offsets[pt] = msg.offset + 1
                                count['skipped'] += 1
                                log.debug({
                                    "message": 'Skip message',
                                    "offset": msg.offset,
                                    "src_topic": src_kafka_settings.topic,
                                })
                                continue

                        await dest_producer.send_and_wait(
                            topic=dest_kafka_settings.topic,
                            value=msg.value,
                            key=msg.key,
                            timestamp_ms=msg.timestamp,
                        )
                        count['sent'] += 1
                        offsets[pt] = msg.offset + 1

            if offsets:
                # say only if we have offset something
                log.info(
                    {
                        "message": 'Mirroring done',
                        "read": count["read"],
                        "sent": count["sent"],
                        "skipped": count["skipped"],
                        "src_topic": src_kafka_settings.topic,
                        "dest_topic": dest_kafka_settings.topic,
                    }
                )
                await src_consumer.commit(offsets)

        await asyncio.sleep(5)

    log.info('Mirroring stopped')


async def get_last(dest_kafka_settings: KafkaSettings) -> Past:
    async with get_consumer(
        dest_kafka_settings,
        [dest_kafka_settings.topic],
    ) as dest_consumer:
        offsets = await dest_consumer.end_offsets(dest_consumer.assignment())
        past: Past = []
        earlier = datetime.utcnow().timestamp()
        for tp, offset in offsets.items():
            if offset > 0:
                dest_consumer.seek(tp, offset - 1)
                msg = await dest_consumer.getone()
                earlier = min(earlier, msg.timestamp)
                past.append((msg.timestamp, msg.key))

        _ts = earlier - PAST_LAG
        new_offsets = await dest_consumer.offsets_for_times(
            {tp: _ts for tp in offsets}
        )
        for tp, ot in new_offsets.items():
            if ot is not None:
                dest_consumer.seek(tp, ot.offset)

        data = await dest_consumer.getmany(timeout_ms=10000, max_records=1000)

        for pt, messages in data.items():
            for msg in messages:
                past.append((msg.timestamp, msg.key))

        log.info({
            "message": 'Got last messages',
            "dest_topic": dest_kafka_settings.topic,
            "count": len(past),
            "earlier": f"{datetime.fromtimestamp(earlier)}",
        })
        past.sort()
        return past
