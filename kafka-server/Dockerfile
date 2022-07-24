FROM ubuntu:18.04

ENV KAFKA_HOME /usr/local/kafka
ADD ./start-kafka.sh /scripts/

RUN apt-get update && apt-get install -y \
  wget \
  openjdk-8-jdk


RUN wget https://archive.apache.org/dist/kafka/0.10.2.0/kafka_2.11-0.10.2.0.tgz && \
  tar -xzf kafka_2.11-0.10.2.0.tgz && \
  mv kafka_2.11-0.10.2.0 $KAFKA_HOME


RUN chmod +x /scripts/start-kafka.sh