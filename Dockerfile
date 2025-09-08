FROM openjdk:11-slim

# ==== Versões ====
ENV SPARK_VERSION=4.0.1
ENV HADOOP_VERSION=hadoop3
ENV SPARK_HOME=/opt/spark
ENV PATH="$PATH:$SPARK_HOME/bin"
ENV PYSPARK_PYTHON=python3

# ==== Dependências ====
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        curl \
        bash \
        python3 \
        python3-pip \
        ca-certificates && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# ==== Spark ====
RUN curl -fsSL https://dlcdn.apache.org/spark/spark-4.0.1/spark-4.0.1-bin-hadoop3.tgz \
    | tar -xz -C /opt && \
    mv /opt/spark-4.0.1-bin-hadoop3 $SPARK_HOME

# ==== Python packages ====
RUN pip3 install --no-cache-dir \
    notebook \
    pandas \
    pyspark \
    duckdb \
    tqdm

# ==== Usuário não-root ====
RUN useradd -ms /bin/bash jupyter && \
    mkdir -p /workspace && \
    chown -R jupyter:jupyter /workspace
USER jupyter
WORKDIR /workspace

# ==== Jupyter ====
EXPOSE 8888
CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--NotebookApp.token=", "--NotebookApp.password="]
