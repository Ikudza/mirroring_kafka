FROM python:3
WORKDIR /app
COPY . .
RUN pip install --upgrade pip &&  pip install .
CMD ["python", "-m", "mirroring_kafka", "run", "--verbose"]