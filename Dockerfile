FROM jupyter/all-spark-notebook:python-3.10.9

# Set working directory
WORKDIR /container

# Install packages using pip
RUN pip install duckdb boto3 xlwings pyspark duckdb-engine xgboost python-dotenv==0.21.1 findspark

ENV SPARK_HOME=/usr/local/spark
ENV PATH="$SPARK_HOME/bin:$PATH"

# connect Docker container to the host network
CMD ["--network", "bridge"]