# Decoding Stackoverflow
Insight Data Engineering project for demystifying StackOverflow and GitHub.

## Project Idea
The idea of this project is to build a pipeline to discover dense communities of users on StackOverflow and analyze the clusters.

## Use Case
1. Find the most active and influencial users on StackOverflow for the top trending tags.
2. Companies and potential employers can analyze the graph to target future employees.
3. Data scientists can leverage the graph to extract useful information about each tag.

## Technologies
1. Amazon S3 - StackOverflow data dump
2. Apache Spark - Graph analysis on batches of data
3. PostgreSQL - Database
4. Flask - UI

## Proposed architecture
StackOverflow -> S3 -> Spark -> PostgreSQL -> Flask UI

## Engineering challenges
1. Creating the pipeline to efficiently handle the data and process it.
2. Implementing k-core graph algorithm that is not available in GraphX library yet.
3. k-core in distributed systems.

## Constraints
* The data dump is not that huge (~150GB)
* Solution: Simulate streaming data for StackOverflow
