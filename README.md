# DS-SimpleDynamo
Replicated Key-Value Storage based on Simplified Dynamo

A Replicated distributed Key-Value Storage based on Dynamo. This project is about implementing a simplified version of Dynamo. There are three main goal acheived through this project: 1) Partitioning, 2) Replication, and 3) Failure handling.

The main goal is to provide both availability and linearizability at the same time. In other words, the app always perform read and write operations successfully even under failures.

