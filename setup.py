from setuptools import setup, find_packages

setup(
    name='mirroring_kafka',
    description='Service mirroring kafka topic',
    author='Ivan Kudashin',
    author_email='kudashin-ivan@ya.ru',
    platforms='any',
    install_requires=[
        'aiokafka[lz4]',
        'asynch',
        'docopt',
        'sentry_sdk',
        'python-json-logger==0.1.*',
    ],
    packages=find_packages(),
    include_package_data=True,
    entry_points={
        'console_scripts': [
            'mirroring-kafka = mirroring_kafka.__main__:main',
        ],
    }
)
