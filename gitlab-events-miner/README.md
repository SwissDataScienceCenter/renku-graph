# gitlab-events-miner

Reads events from the gitlab events database (currently on testing.datascience) and selects the push events. The events are then pushed towards the KG.

Open issues:
 - commit_count = 0 (deleted branches) is ignored
 - merge request: not all commits are properly returned

To run: 
sbt run
