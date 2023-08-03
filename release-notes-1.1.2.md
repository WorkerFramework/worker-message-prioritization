#### Version Number
${version-number}

#### New Features
- US691054: Dynamically set srcPrefetchCount shovel property based on number of messages
- US692066: Manage shovel creation and deletion effectively to try to avoid rapid creation and deletion 
- US692066: Add a threshold for moving messages to target queue to better manage shovel creation or deletion

#### Bug Fixes
- 714101: Switched implementation of the message distributor from the ShovelDistributor to the LowLevelDistributor.
  Occasionally, when running in a RabbitMQ cluster, RabbitMQ shovels would get stuck, and the only way to fix them was to restart the
  `rabbit-server` service on each node. Due to this, we have stopped using shovels to move messages from staging queues to target queues,
  and have instead switched to a low-level implementation that uses the RabbitMQ Java client to move messages.

#### Known Issues
- None