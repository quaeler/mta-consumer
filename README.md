# mta-consumer
A technology soup project consuming MTA updates which are transformed and published to Kafka, to then be consumed by a Spark bundle which both does a map-reduce count based on train statuses, and writes elements of the train events into a JanusGraph instance.
