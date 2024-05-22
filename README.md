# Overview

This is the source code repository for the HORSE project. HORSE is a **holistic and reliable task scheduling framework for distributed key-value stores**. It is designed to provide a unified and reliable framework for high-performance distributed key-value stores.

## Code Structure

- Server: The source code for the storage server. It is implemented on top of [Apache Cassandra 5.0-beta1](https://github.com/apache/cassandra/releases/tag/cassandra-5.0-beta1).
- Client: The source code for the client driver ([DataStax Java Driver for Apache Cassandra, v3.0.0](https://github.com/apache/cassandra-java-driver/releases/tag/3.0.0)) and benchmark tool ([YCSB-0.17.0](https://github.com/brianfrankcooper/YCSB/releases/download/0.17.0/ycsb-0.17.0.tar.gz)).
- Scripts: The scripts for running the experiments.