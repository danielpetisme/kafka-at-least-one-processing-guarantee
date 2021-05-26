# Kafka at least one processing guarantee
Simple demo of how to implement a Kafka consumer to achieve at least one processing.

# Objective
- Understand what "at-least one processing" is and what are the pros/cons.
- Implement different commit strategies and learn how each impact the "at-least one processing" guarantee.
- Understand how the commit strategies resists to failures
- Discuss how you learn impact your use cases

# Prerequisites
You need:
* Docker and Docker Compose installed (it's recommended to increase the Docker Engine's memory limits).

# Start the environment

Run the following command to startup the platform:
```shell
docker compose up
```

You can the start the consumer by running:
```shell
cd consumer
./mvnw
```

# Destroy the environment

Run the following command to stop and destroy the platform:
```shell
docker compose down -v
```

# What does *at least once processing guarantee* means?

From a system point, a least one processing guarantee at the consumer level means we want to want to give an assurance on the fact
that **every single message has been delivered AND processed**.
As consequence, the consumer guarantee nones of the message will be lost.
The side-effect of "at least one processing" implies that the consumer may read multiple times the same Kafka record thus generates duplicates in the downstream system.

So the next question would be: What is a **processed message** actually means?
The straightforward answer would be to acknowledge a message as processed when the original intent is fulfilled.
If your system aims to persist a record into a database, then the record is processed when the database acknowledges that the record has persisted.

As always, the happy path looks obvious and easy.
Things get more tricky when we focus on errors.

# Which questions should we be answering? 
## Step1: How do we manage commits?
Compare auto-commit vs manual commit

Questions: 
- When does the commit happen with auto-commit?
- If you are doing manual commit, where should it be? Per batch, per record? Start of loop, end of loop?
- Parameterless commit vs map of commit?
- You have the control of the commit. Test what happens when the consumer dies in different parts of the processing... Are there any duplicates (i.e, same event processed more than once)? Is there any data missing? 
- Let's take a step back, based on the benefits/limits/tradeoffs you discovered in the above experiences which configurations would be best fits for your usecases. 

## Step2: How do we manage per message exceptions?
We are processing orders, which come as JSON objects. Some bug is introduced in the producers, and sometimes they just sent a stack trace of an exception instead of JSON... 
- Create a poison pill so the consumer raises an exception (hint: deserialize with KafkaJsonDeserializer and send something that is JSON) to force an infinite reprocessing
- Skip the full failing batch
- Skip just the poison pill, and identify it. 


## Step3: How do we manage external failures?
- Simulate a target failure (http 500/JDBC exception/File permission).
- The consumer should pause to retries the error
- Resume once the message is considered as “processed”
- Run multiple instance of the consumer and generate a rebalance (by killing one instance). How do you manage the commits. Where should the processing resume from.

