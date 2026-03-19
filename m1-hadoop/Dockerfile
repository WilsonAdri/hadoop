FROM arm64v8/openjdk:8-jdk-slim

RUN apt-get update && apt-get install -y wget tar ssh rsync nano

RUN wget https://archive.apache.org/dist/hadoop/common/hadoop-2.7.3/hadoop-2.7.3.tar.gz && \
    tar -xzf hadoop-2.7.3.tar.gz && \
    mv hadoop-2.7.3 /usr/local/hadoop && \
    rm hadoop-2.7.3.tar.gz

ENV HADOOP_HOME=/usr/local/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
ENV JAVA_HOME=/usr/local/openjdk-8
RUN echo "export JAVA_HOME=/usr/local/openjdk-8" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh

# Configuraciones de HDFS
RUN echo '<?xml version="1.0"?><configuration><property><name>fs.defaultFS</name><value>hdfs://namenode:9000</value></property></configuration>' > $HADOOP_HOME/etc/hadoop/core-site.xml
RUN echo '<?xml version="1.0"?><configuration><property><name>dfs.replication</name><value>1</value></property></configuration>' > $HADOOP_HOME/etc/hadoop/hdfs-site.xml

# ¡NUEVO! Configuraciones de YARN y MapReduce (Ajustadas para Docker)
RUN echo '<?xml version="1.0"?><configuration><property><name>mapreduce.framework.name</name><value>yarn</value></property><property><name>mapreduce.map.memory.mb</name><value>256</value></property><property><name>mapreduce.reduce.memory.mb</name><value>256</value></property></configuration>' > $HADOOP_HOME/etc/hadoop/mapred-site.xml
RUN echo '<?xml version="1.0"?><configuration><property><name>yarn.resourcemanager.hostname</name><value>namenode</value></property><property><name>yarn.nodemanager.resource.memory-mb</name><value>2048</value></property><property><name>yarn.scheduler.minimum-allocation-mb</name><value>256</value></property><property><name>yarn.nodemanager.vmem-check-enabled</name><value>false</value></property></configuration>' > $HADOOP_HOME/etc/hadoop/yarn-site.xml

CMD ["tail", "-f", "/dev/null"]