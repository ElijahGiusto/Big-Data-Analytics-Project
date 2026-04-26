#!/usr/bin/env bash
set -euo pipefail

export HADOOP_HOME="${HADOOP_HOME:-/opt/hadoop}"
export HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-$HADOOP_HOME/etc/hadoop}"

if [ ! -d /hadoop/dfs/name/current ]; then
  echo "Formatting HDFS NameNode..."
  hdfs namenode -format -force -nonInteractive
fi

hdfs --daemon start namenode
hdfs --daemon start datanode

echo "Waiting for HDFS..."
for attempt in $(seq 1 60); do
  if hdfs dfs -fs hdfs://localhost:9000 -mkdir -p /artist-popularity/processed; then
    hdfs dfs -fs hdfs://localhost:9000 -chmod -R 777 /artist-popularity
    echo "HDFS is ready at hdfs://localhost:9000"
    break
  fi
  if [ "$attempt" -eq 60 ]; then
    echo "HDFS did not become ready in time" >&2
    exit 1
  fi
  sleep 2
done

tail -F "$HADOOP_HOME"/logs/*.log
