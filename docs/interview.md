# Interview notes

## 30-second version

I simulated 4G/5G cell metrics with correlated physics, streamed them through Kafka keyed by cell, wrote a consumer that validates and stores them in Postgres, and used Grafana for both network KPIs and pipeline health. Isolation Forest flags cells that deviate from their own baseline. Injected outages and congestion sit in a side table, not on the Kafka message, so I can measure precision and recall.

## Why Kafka

So the generator and the database are decoupled. I can replay from offset 0, inspect lag, and keep per-cell order with the message key.

## Why not Spark

About 10 events per second. Spark would be theatre. Postgres + a Python consumer is the honest size.

## At-least-once vs exactly-once

I commit Kafka offsets after a successful batch insert. A crash can resend a batch; the clean table unique key drops duplicates.

## Why Isolation Forest, not an LSTM

Unsupervised, small, and I engineered cell-level z-scores so “high latency” is relative to that cell. Sequence models would dominate the story and fight the project identity (data engineering). Thresholds still catch hard outages; the forest is for multivariate congestion.

## Why labels are off the wire

In a real network, probes do not ship a field called `CONGESTION`. Hiding labels is how I evaluate without cheating.

## What I would add at an operator

Alert routing into ServiceNow, Avro schemas, an orchestrator for the batch job, and horizontal consumer scale. Not a chatbot.
