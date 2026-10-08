# Scala Spark Day 24 – Stateful vs Stateless Streaming

## Objective

Understand the difference between stateless and stateful processing in Spark Streaming using DStreams.

This project demonstrates:

- Stateless transformations
- Stateful transformations
- Current-batch transaction counts
- Running transaction counts
- Running transaction amounts
- State maintenance across multiple micro-batches
- Checkpointing for stateful processing

## Technologies Used

- Scala 2.13.16
- Apache Spark 3.5.6
- Spark Streaming / DStreams
- SBT 1.10.11
- Java 17
- Netcat
- Ubuntu / WSL

## Problem Statement

Build a Spark Streaming application that processes bank transaction events and compares stateless processing with stateful processing.

The application should:

1. Read transaction events from a TCP socket.
2. Process data using 5-second micro-batches.
3. Calculate transaction counts for the current batch.
4. Maintain running transaction counts by bank account.
5. Maintain running transaction amounts by bank account.
6. Compare current-batch results with accumulated state.
7. Use checkpointing for stateful processing.

## Input Format

Transactions are sent through the TCP socket using:

```text
ACCOUNT_ID,AMOUNT
