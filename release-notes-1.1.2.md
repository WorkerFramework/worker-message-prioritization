#### Version Number
${version-number}

#### New Features
- None

#### Bug Fixes
- 714101: Switched implementation of the message distributor from the ShovelDistributor to the LowLevelDistributor.
  Occasionally, when running in a RabbitMQ cluster, RabbitMQ shovels would get stuck, and the only way to fix them was to restart the
  `rabbit-server` service on each node. Due to this, we have stopped using shovels to move messages from staging queues to target queues,
  and have instead switched to a low-level implementation that uses the RabbitMQ Java client to move messages.

#### Known Issues
- None