#### Version Number
${version-number}

#### New Features
- **US857114:** Introduced `CAF_RABBITMQ_PROTOCOL` environment variable so that RabbitMQ URL protocol is customisable.
        This allows for TLS-enabled connections to be made to RabbitMQ if desired.
        By default, this variable is set to "amqp" so there is no change in behaviour unless specified.
- **US749035**: Classic queues and priority queues are deprecated to prepare for the move to quorum queues.
        Priority queues can still be created however it is no longer possible to publish messages with priority.
        The type of queue created by workers can be controlled by the ENV CAF_RABBITMQ_QUEUE_TYPE. This will be removed in a future release.

#### Breaking Changes
- **US361030:** Java 8 and Java 11 support dropped  
  Java 17 is now the minimum supported version.

- **US361030:** Jakarta EE version update  
  The version of Jakarta EE used for validation and other purposes has been updated.

#### Bug Fixes
- **US870109:** Replaced okhttp with jersey to make RabbitMQ Management API calls.

#### Known Issues
- None
