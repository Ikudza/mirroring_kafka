"""
This is the main file for the project. It will be used to run the project.
Usage:
    mirroring-kafka run --src=<source> --dest=<destination> [options]

Options:
    --verbose                   Debug log

Environment:
    <source>_KAFKA_SERVER                  Kafka server source.
    <source>_KAFKA_TOPIC                   Kafka topic source.
    <source>_KAFKA_USERNAME                Kafka username.
    <source>_KAFKA_PASSWORD                Kafka password.
    <source>_KAFKA_SASL_MECHANISM          Kafka SASL mechanism (SASL_PLAINTEXT or SASL_SSL).
    <source>_KAFKA_SECURITY_PROTOCOL       Kafka security protocol.
    <source>_KAFKA_CA_FILE                 Kafka CA file path.
    <source>_KAFKA_CERT_FILE               Kafka file patch for auth
    <source>_KAFKA_KEY_FILE                Kafka file patch for auth

    <destination>_KAFKA_SERVER             Kafka server destination.
    <destination>_KAFKA_TOPIC              Kafka topic destination.
    <destination>_KAFKA_USERNAME           Kafka username.
    <destination>_KAFKA_PASSWORD           Kafka password.
    <destination>_KAFKA_SASL_MECHANISM     Kafka SASL mechanism (SASL_PLAINTEXT or SASL_SSL).
    <destination>_KAFKA_SECURITY_PROTOCOL  Kafka security protocol.
    <destination>_KAFKA_CA_FILE            Kafka CA file path.
    <destination>_KAFKA_CERT_FILE          Kafka file patch for auth
    <destination>_KAFKA_KEY_FILE           Kafka file patch for auth

"""
import asyncio
import logging
import os

import sentry_sdk
from docopt import docopt
from sentry_sdk.integrations.logging import LoggingIntegration

from mirroring_kafka import utils, logic
from mirroring_kafka.logger import init_logs


def main():
    args = docopt(__doc__)
    init_logs(bool(args.get('--verbose')))
    env = os.environ
    sentry_logging = LoggingIntegration(
        level=logging.INFO,
        event_level=logging.ERROR,
    )
    dsn = args.get('--sentry') or env.get('SENTRY_DSN', '')
    if dsn:
        sentry_sdk.init(
            dsn=dsn,
            environment=os.environ.get('SENTRY_ENVIRONMENT', 'local'),
            release='mirroring_kafka:0.0.1',
            integrations=[sentry_logging],
        )
    src_kafka_settings = utils.KafkaSettings(
        servers=os.environ.get(f"{args['--src'].upper()}_KAFKA_SERVER"),
        topic=os.environ.get(f"{args['--src'].upper()}_KAFKA_TOPIC"),
        username=os.environ.get(f"{args['--src'].upper()}_KAFKA_USERNAME"),
        password=os.environ.get(f"{args['--src'].upper()}_KAFKA_PASSWORD"),
        sasl_mechanism=os.environ.get(f"{args['--src'].upper()}_KAFKA_SASL_MECHANISM"),
        security_protocol=os.environ.get(f"{args['--src'].upper()}_KAFKA_SECURITY_PROTOCOL"),
        ca_file=os.environ.get(f"{args['--src'].upper()}_KAFKA_CA_FILE"),
        cert_file=os.environ.get(f"{args['--src'].upper()}_KAFKA_CERT_FILE"),
        key_file=os.environ.get(f"{args['--src'].upper()}_KAFKA_KEY_FILE"),
    )
    dest_kafka_settings = utils.KafkaSettings(
        servers=os.environ.get(f"{args['--dest'].upper()}_KAFKA_SERVER"),
        topic=os.environ.get(f"{args['--dest'].upper()}_KAFKA_TOPIC"),
        username=os.environ.get(f"{args['--dest'].upper()}_KAFKA_USERNAME"),
        password=os.environ.get(f"{args['--dest'].upper()}_KAFKA_PASSWORD"),
        sasl_mechanism=os.environ.get(f"{args['--dest'].upper()}_KAFKA_SASL_MECHANISM"),
        security_protocol=os.environ.get(f"{args['--dest'].upper()}_KAFKA_SECURITY_PROTOCOL"),
        ca_file=os.environ.get(f"{args['--dest'].upper()}_KAFKA_CA_FILE"),
        cert_file=os.environ.get(f"{args['--dest'].upper()}_KAFKA_CERT_FILE"),
        key_file=os.environ.get(f"{args['--dest'].upper()}_KAFKA_KEY_FILE"),
    )
    if args.get('run'):
        stopping = asyncio.Event()
        asyncio.run(
            logic.run_mirroring(
                src_kafka_settings, dest_kafka_settings, stopping
            )
        )


if '__main__' == __name__:
    main()
