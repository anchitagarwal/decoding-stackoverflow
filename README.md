# Decoding Stackoverflow
> Bringing you the posts that matter.

## Project Idea
The idea is to build a platform that recommends open Stackoverflow questions to developers, where they are most likely to get an accepted answer.

## Use Case
* Only 8% of the users answers more than 5 times on Stackoverflow
* How can we leverage the data to motivate more users to actively participate
* Recommend each user a personalized list of questions that do not have an accepted answer yet
* The recommended posts will be in user's expert area, which is determined by their interactions and behavior

## Architecture
![data pipeline]( decoding-stackoverflow/util/data_pipeline.png  "Data Pipeline")

## Engineering challenges
1. Creating the pipeline to efficiently handle the data and process it.
2. Finding the 1-circle network for a user efficiently.
