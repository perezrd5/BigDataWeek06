FROM eclipse-temurin:17-jdk-jammy
RUN apt-get update && apt-get install -y python3 python3-pip tini && rm -rf /var/lib/apt/lists/*
WORKDIR /workspace
COPY requirements.txt /tmp/requirements.txt
RUN python3 -m pip install --no-cache-dir -r /tmp/requirements.txt
ENV SPARK_HOME=/usr/local/lib/python3.10/dist-packages/pyspark PYSPARK_PYTHON=python3 PYSPARK_DRIVER_PYTHON=python3
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python3", "run_lab.py"]
