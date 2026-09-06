#!/bin/bash
set -euo pipefail
# Credentials are supplied at runtime, never baked into the image.
# Guard against duplicate appends on container restart.
if ! grep -q '# RUNTIME_APPENDED' /opt/spark/conf/spark-defaults.conf 2>/dev/null; then
cat >> /opt/spark/conf/spark-defaults.conf <<CONFIG
# RUNTIME_APPENDED
spark.hadoop.fs.s3a.access.key ${MINIO_ROOT_USER:-tikblog}
spark.hadoop.fs.s3a.secret.key ${MINIO_ROOT_PASSWORD:-tikblog-local}
spark.hadoop.fs.s3a.endpoint ${ANALYTICS_MINIO_ENDPOINT:-http://minio:9000}
spark.executor.instances ${ANALYTICS_EXECUTOR_INSTANCES:-1}
spark.executor.cores ${ANALYTICS_EXECUTOR_CORES:-1}
spark.executor.memory ${ANALYTICS_EXECUTOR_MEMORY:-512m}
spark.cores.max ${ANALYTICS_EXECUTOR_INSTANCES:-1}
CONFIG
fi
case "$1" in
  master) exec /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master --host spark-master ;;
  worker) exec /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker \
    --cores "${SPARK_WORKER_CORES:-2}" --memory "${SPARK_WORKER_MEMORY:-1g}" spark://spark-master:7077 ;;
  thrift)
    # Wait for ivy jars (populated by driver via shared volume on first start).
    for _i in $(seq 1 60); do
      [ -f /opt/spark/.ivy2/jars/io.delta_delta-spark_2.12-3.3.0.jar ] && break
      sleep 5
    done
    # HiveThriftServer2 needs Delta on the driver classpath at init time.
    IVY_JARS=$(find /opt/spark/.ivy2/jars -name '*.jar' 2>/dev/null | paste -sd ',' -)
    exec /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
    --class org.apache.spark.sql.hive.thriftserver.HiveThriftServer2 \
    --jars "${IVY_JARS}" \
    --conf "spark.driver.extraClassPath=$(echo "${IVY_JARS}" | tr ',' ':')" \
    --conf spark.ui.port=4040 \
    --conf hive.server2.thrift.port=10000 \
    --conf hive.server2.thrift.bind.host=0.0.0.0 \
    --conf hive.server2.authentication=NOSASL ;;
  *) exec /opt/spark/bin/spark-submit --master "${ANALYTICS_SPARK_MASTER:-spark://spark-master:7077}" \
    /app/analytics/driver.py "$@" ;;
esac
