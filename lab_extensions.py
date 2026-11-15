"""Required service integrations; failures propagate instead of being marked complete."""
import json
import os
import time
import uuid
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import pandas as pd
import requests


def mongo_integration(records, unified):
    from pymongo import MongoClient, UpdateOne
    with MongoClient(os.getenv('MONGO_URI', 'mongodb://localhost:27017'),
                     serverSelectionTimeoutMS=15000) as client:
        client.admin.command('ping')
        coll = client.alaska_lab.specifications
        for start in range(0, len(records), 1000):
            coll.bulk_write([UpdateOne({'_id': r['spec_id']}, {'$set': {
                'source': r['source'], 'raw': r['raw']}}, upsert=True)
                for r in records[start:start + 1000]])
        docs = []
        ids = unified.spec_id.tolist()
        for start in range(0, len(ids), 1000):
            docs.extend(coll.find({'_id': {'$in': ids[start:start + 1000]}}, {'raw': 1}))
        docs = pd.DataFrame(docs).rename(columns={'_id': 'spec_id', 'raw': 'mongo_raw'})
        result = unified.merge(docs, on='spec_id', how='left', validate='one_to_one')
        assert len(result) == len(unified) and result.mongo_raw.notna().all()
        return result


def spark_session():
    from pyspark.sql import SparkSession
    master = os.getenv('SPARK_MASTER', 'local[2]')
    builder = (SparkSession.builder.master(master).appName('AlaskaIntegration')
               .config('spark.sql.shuffle.partitions', '4')
               .config('spark.cores.max', '2')
               .config('spark.executor.cores', '1')
               .config('spark.executor.memory', '1g'))
    if os.getenv('SPARK_DRIVER_HOST'):
        builder = builder.config('spark.driver.host', os.environ['SPARK_DRIVER_HOST'])
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    topology = {'master': master, 'physical_multi_node': False}
    if master.startswith('spark://'):
        host = master.split('://')[1].split(':')[0]
        deadline = time.monotonic() + 120
        while True:
            response = requests.get(f'http://{host}:8080/json/', timeout=10)
            response.raise_for_status()
            workers = [w for w in response.json()['workers'] if w['state'] == 'ALIVE']
            if len(workers) >= 2:
                break
            if time.monotonic() > deadline:
                raise RuntimeError('Two live Spark workers are required for this cluster run')
            time.sleep(2)
        topology['workers'] = [{k: w[k] for k in ['id', 'host', 'cores']} for w in workers]
    # Force Python work onto executors and retain the actual execution hosts.
    def worker_host(_):
        import socket
        import time
        time.sleep(0.2)
        yield socket.gethostname()
    hosts = sorted(set(spark.sparkContext.parallelize(range(16), 16)
                       .mapPartitions(worker_host).collect()))
    topology['observed_python_hosts'] = hosts
    if master.startswith('spark://'):
        assert len(hosts) >= 2, f'Expected work on both workers, observed {hosts}'
    return spark, topology


def stream_replay(spark, clean, results):
    """Poll a real HTTP service, publish Kafka events, verify stream output and recovery."""
    from kafka import KafkaProducer
    from kafka.admin import KafkaAdminClient, NewTopic
    from pyspark.sql import functions as F
    run_id = uuid.uuid4().hex[:12]
    topic = 'alaska-' + run_id
    broker = os.getenv('KAFKA_BOOTSTRAP', 'localhost:9092')
    admin = KafkaAdminClient(bootstrap_servers=broker)
    try:
        admin.create_topics([NewTopic(topic, num_partitions=2, replication_factor=1)])
    finally:
        admin.close()
    samples = clean[['spec_id', 'title']].head(120).to_dict('records')
    stamp = datetime.now(timezone.utc).isoformat()
    events = [dict(r, event_id=f'{run_id}-{i}', event_time=stamp)
              for i, r in enumerate(samples)]
    # Deliberate quality probes: one duplicate and one invalid record.
    batches = [events[:40] + [events[0]], events[40:80],
               events[80:] + [dict(events[0], event_id=None)]]

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            from urllib.parse import urlparse, parse_qs
            parsed = urlparse(self.path)
            if parsed.path != '/events':
                self.send_error(404)
                return
            try:
                index = int(parse_qs(parsed.query).get('batch', ['0'])[0])
            except ValueError:
                self.send_error(400)
                return
            payload = json.dumps(batches[index] if 0 <= index < len(batches) else []).encode()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *_):
            pass

    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    endpoint = f'http://127.0.0.1:{server.server_port}/events'
    out = (Path(results) / 'stream_runs' / run_id).resolve()
    checkpoint = str(out / 'checkpoint')
    destination = str(out / 'events')
    schema = 'event_id STRING, spec_id STRING, event_time TIMESTAMP, title STRING'
    raw = (spark.readStream.format('kafka').option('kafka.bootstrap.servers', broker)
           .option('subscribe', topic).option('startingOffsets', 'earliest').load())
    parsed = raw.select(F.from_json(F.col('value').cast('string'), schema).alias('e')).select('e.*')
    valid = parsed.filter('event_id IS NOT NULL AND spec_id IS NOT NULL AND event_time IS NOT NULL AND title IS NOT NULL')
    unique = valid.withWatermark('event_time', '10 minutes').dropDuplicatesWithinWatermark(['event_id'])
    dimension = spark.createDataFrame(clean[['spec_id', 'source']])
    enriched = unique.join(F.broadcast(dimension), 'spec_id', 'left')

    def start_query():
        return (enriched.writeStream.format('parquet').outputMode('append')
                .option('path', destination).option('checkpointLocation', checkpoint)
                .trigger(processingTime='2 seconds').start())

    query = None
    progress = []
    started = time.perf_counter()
    try:
        query = start_query()
        producer = KafkaProducer(bootstrap_servers=broker, acks='all', retries=3,
                                 value_serializer=lambda v: json.dumps(v).encode())
        try:
            for batch in range(3):
                response = requests.get(endpoint, params={'batch': batch}, timeout=10)
                response.raise_for_status()
                for event in response.json():
                    producer.send(topic, value=event).get(timeout=30)
                producer.flush()
                query.processAllAvailable()
                progress.extend(query.recentProgress)
        finally:
            producer.close()
        query.stop()
        query = None
        stored = spark.read.parquet(destination)
        assert stored.count() == len(events)
        assert stored.select('event_id').distinct().count() == len(events)
        assert stored.filter('source IS NULL').count() == 0
        # Restart using the same checkpoint and confirm already committed events do not repeat.
        query = start_query()
        query.processAllAvailable()
        query.stop()
        query = None
        assert spark.read.parquet(destination).count() == len(events)
        metrics = {'mode': 'controlled HTTP replay of static Alaska records', 'topic': topic,
                   'published': len(events) + 2, 'expected_valid_unique': len(events),
                   'output_rows': len(events), 'restart_preserved_count': True,
                   'elapsed_seconds': time.perf_counter() - started,
                   'progress': progress}
        (out / 'metrics.json').write_text(json.dumps(metrics, indent=2), encoding='utf-8')
        return metrics
    finally:
        if query is not None:
            query.stop()
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
