# Alaska integration lab

## Complete execution

Install Docker Desktop with Linux containers and Compose v2. Allow roughly 8 GB of memory for the lab. Run from this project directory:

```powershell
docker compose up --build --abort-on-container-exit --exit-code-from lab lab
```

The first build downloads Python dependencies, Java runtime image, MongoDB, Kafka, and the Spark Kafka connector. Internet access is required. MongoDB/Kafka stay internal to the Compose network. Spark has one master and two workers. All workers and the driver mount this directory at /workspace so they share inputs, streaming output, and checkpoints. Keep the directory in place while running.

The lab exits with an error if a required stage fails. Inspect logs with `docker compose logs lab` and `docker compose logs spark-master spark-worker-1 spark-worker-2`. Rerun the command after resolving the error. Clean up containers with `docker compose down`; generated files remain in this folder.

## Completed run and submission files

The full Docker Compose run exited with code 0. Pandas, Dask, SQLite through
SQLAlchemy, PySpark, MongoDB through PyMongo, and Kafka with Spark Structured
Streaming all completed. Spark ran tasks on two worker containers on one
physical machine. The controlled HTTP replay published 122 Kafka messages and
produced 120 unique output rows. Restarting from the same checkpoint preserved
the 120-row count.

- [Executed notebook](alaska_integration.executed.ipynb) contains the code and
  outputs from the successful run.
- [Run evidence](lab_results/evidence.json) records stage completion, data
  quality, entity-resolution scores, runtime comparisons, Spark worker
  execution, and streaming results.
- [Source notebook](alaska_integration.ipynb) is the notebook used to rerun
  the lab.
- The final report, `DDS8530_Perez_Alaska_Lab_Report.docx`, is submitted
  separately with the executed notebook. It includes the measured results
  from the completed run.
- `alaska_report.pdf`, `alaska_lab.zip`, and `baseline_submission/` contain
  earlier versions of the project.
  
## Requirement coverage

The notebook reads official Alaska JSON, retains unstructured titles, maps heterogeneous attributes, handles missing values/units/conflicts, applies Dask partition cleaning and listing deduplication, and tests a deliberate duplicate-input probe. Pandas and Dask aggregates must agree. SQLAlchemy reads a SQLite dimension, PyMongo retrieves raw MongoDB documents, and both are merged with canonical CSV data. Entity resolution is evaluated using curated labels, and a separate entity table preserves listing membership. Matching remains a baseline with limited recall.

Spark DataFrame and RDD transformations compute equivalent count/mean aggregates and each perform a join. Each engine has one warm-up and three timed trials. Spark startup and first execution are separate. The runner verifies two registered workers and observes Python tasks on two executor hosts. Both workers run on one physical host: this demonstrates distributed processes, not physical multi-machine scalability. The in-memory canonicalization stage also limits scale. Deploy the same image and shared paths on external workers if the instructor requires distinct physical nodes.

The default dataset is the complete monitor domain (16,662 records). `ALASKA_DOMAINS` accepts camera, monitor, and notebook; set it in the lab service environment to expand the input. IDs and ground-truth layout must remain unique across the selected archives, and assertions detect collisions. The assignment does not explicitly require every domain; confirm subset scope with the instructor if necessary.

## Streaming experiment

A local HTTP server replays 120 real Alaska listing IDs and titles in three batches. The producer polls it and publishes to a real Apache Kafka broker while Spark Structured Streaming consumes events. The pipeline parses an explicit schema, rejects invalid events, deduplicates with a watermark, joins static metadata, and writes checkpointed Parquet. A deliberate duplicate and an invalid event verify quality handling. A stop/restart check confirms committed output counts do not increase. Per-run topics/checkpoints avoid contamination from earlier runs. Topics remain until the disposable broker is removed.

Replay timestamps are current ingestion times; no prices, commercial transactions, or live market activity are fabricated. This is a real running streaming pipeline using a controlled API, not an independently operated live commercial feed. If the instructor requires an external live API, that additional source must be selected and mapped rather than representing this replay as such.

## Native execution and local QA

For native execution use Python 3.10–3.12, Java 17, and `pip install -r requirements.txt`. Configure running MongoDB/Kafka, MONGO_URI, KAFKA_BOOTSTRAP and optionally SPARK_MASTER. Before starting Python, set PYSPARK_SUBMIT_ARGS to `--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 pyspark-shell`. Run `python run_lab.py`. Without SPARK_MASTER this uses local[2] and does not satisfy the two-worker execution check.

`python validate_lab.py` validates Python/notebook syntax and runs Pandas, Dask, SQL, and entity evaluation in qa_results, leaving the submission evidence untouched. It does not test the external services. 