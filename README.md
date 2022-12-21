# worker-message-prioritization

The project attempts to _fairly_ distribute work to workers built with the 
[Document Worker Framework](https://github.com/CAFDataProcessing/worker-document).
In this context the definition of fair is:-

_Each worker will receive a roughly equal number of message from each staging 
queue, since the processing of each message is dependent on the complexity of the message the actual amount of 
processing time allocated to each staging queue may be unfair._

In FAS the [Workflow Worker](https://github.houston.softwaregrp.net/Verity/darwin-worker-workflow) attaches a script 
to DocumentWorkerTask which uses the AfterProcessTaskEvent to route the output message to the next appropriate worker 
in the workflow. The MessageRouter modifies this behaviour, choosing to reroute the message to a staging queue instead 
of the original target queue.

These **staging queue** names follow a naming convention which incorporates the original queue name followed by » and 
additional discriminators. For example a target queue **worker-input** could result in a staging queue with the tenant 
id and workflow name added to it resulting **worker-input»tenant1/workflow1**. 
This mutation of the original target queue is performed by QueueNameMutator implementations and is intended to be 
extensible with different mutators for different worker types. 

The **MessageDistributor** uses the RabbitMq managememnt API to retrieve a list of queue matching the naming convention 
of the above staging queues. It obtains information about the target of these staging queues such as the current queue
size and what (if any) capacity it has for new messages. If a target queue has capacity it is divided by the number of 
staging queues and that number of messages is attempted to be moved from the staging queue to the worker input queue.

At present there are two implementations of a MessageDistributor, one is using the low-level RabbitMQ client to consume 
messages from a staging queue and publish them to the worker input queue. The second implementation uses RabbitMQ 
shovel plugin to achieve the same goal. Ultimately a much simpler approach but with the requirement of an additional 
RabbitMq plugin. **During the POC phase some instability was observed with this plugin which should be revisted.**

