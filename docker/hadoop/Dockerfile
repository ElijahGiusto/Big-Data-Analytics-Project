FROM apache/hadoop:3.4.1

USER root

ENV HADOOP_HOME=/opt/hadoop \
    HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
    HDFS_NAMENODE_USER=root \
    HDFS_DATANODE_USER=root \
    HDFS_SECONDARYNAMENODE_USER=root

RUN mkdir -p /hadoop/dfs/name /hadoop/dfs/data

COPY core-site.xml /opt/hadoop/etc/hadoop/core-site.xml
COPY hdfs-site.xml /opt/hadoop/etc/hadoop/hdfs-site.xml
COPY start-hdfs.sh /opt/hadoop/bin/start-hdfs.sh

RUN chmod +x /opt/hadoop/bin/start-hdfs.sh

EXPOSE 9000 9870 9864 9866

CMD ["/bin/bash", "/opt/hadoop/bin/start-hdfs.sh"]
