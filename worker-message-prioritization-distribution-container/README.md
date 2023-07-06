# worker-message-prioritization-distribution-container

This repository consists of the source to build a container that includes the 
[Worker Message Prioritization Distributor](https://github.com/WorkerFramework/worker-message-prioritization/tree/main/worker-message-prioritization-distribution) application.

## Configuration

### Environment Variables

* `CAF_RABBITMQ_VHOST`  
    **Default**: `/`  
    **Description**: The RabbitMQ virtual host.

* `CAF_RABBITMQ_HOST`  
    **Default**: `rabbitmq`  
    **Description**: The RabbitMQ host.

* `CAF_RABBITMQ_PORT`  
    **Default**: `5672`  
    **Description**: The RabbitMQ port.

* `CAF_RABBITMQ_USERNAME`  
    **Default**: `guest`  
    **Description**: The RabbitMQ username.

* `CAF_RABBITMQ_PASSWORD`  
    **Default**: `guest`  
    **Description**: The RabbitMQ password.

* `CAF_RABBITMQ_MGMT_URL`  
    **Default**: `http://rabbitmq:15672`  
    **Description**: The RabbitMQ management API endpoint.

* `CAF_RABBITMQ_MGMT_USERNAME`  
    **Default**: `guest`  
    **Description**: The RabbitMQ management API username.

* `CAF_RABBITMQ_MGMT_PASSWORD`  
    **Default**: `guest`  
    **Description**: The RabbitMQ management API password.

* `CAF_WMP_DISTRIBUTOR_RUN_INTERVAL_MILLISECONDS`  
    **Default**: `10000`  
    **Description**: How often the distributor runs.

* `CAF_WMP_NON_RUNNING_SHOVEL_TIMEOUT_MILLISECONDS`  
    **Default**: `120000`  
    **Description**: The timeout in milliseconds after which to delete RabbitMQ shovels that are in a bad state (i.e. not 'running'). The
    timeout begins from the time this application first observed the shovel in a bad state, which will depend on how often the shovel
    state check runs (the `CAF_WMP_NON_RUNNING_SHOVEL_CHECK_INTERVAL_MILLISECONDS` environment variable).

* `CAF_WMP_NON_RUNNING_SHOVEL_CHECK_INTERVAL_MILLISECONDS`  
    **Default**: `120000`  
    **Description**: How often to check for non-running RabbitMQ shovels.

* `CAF_WMP_SHOVEL_RUNNING_TOO_LONG_TIMEOUT_MILLISECONDS`  
    **Default**: `1800000`  
    **Description**: The timeout in milliseconds after which to delete RabbitMQ shovels that have been running too long. The
    timeout begins from the time this application first observed the shovel in a running state, which will depend on how often the 
    check runs (the `CAF_WMP_SHOVEL_RUNNING_TOO_LONG_CHECK_INTERVAL_MILLISECONDS` environment variable).

* `CAF_WMP_SHOVEL_RUNNING_TOO_LONG_CHECK_INTERVAL_MILLISECONDS`  
    **Default**: `120000`  
    **Description**: How often to check for RabbitMQ shovels that have been running too long.

* `CAF_WMP_CORRUPTED_SHOVEL_TIMEOUT_MILLISECONDS`  
    **Default**: `600000`  
    **Description**: The timeout in milliseconds after which to delete corrupted RabbitMQ shovels. A corrupted shovel is defined as a 
    shovel that is returned by /api/parameters/shovel but is NOT returned by /api/shovels/. The timeout begins from the time this
    application first observed the corrupted shovel, which will depend on how often the check runs, which is configured by the 
   `CAF_WMP_CORRUPTED_SHOVEL_CHECK_INTERVAL_MILLISECONDS` environment variable.

* `CAF_WMP_CORRUPTED_SHOVEL_CHECK_INTERVAL_MILLISECONDS`  
    **Default**: `120000`  
    **Description**: How often to check for corrupted RabbitMQ shovels.

* `CAF_WMP_KUBERNETES_NAMESPACES`  
    **Default**: None.  
    **Description**: Used to specify the Kubernetes namespaces, comma separated, in which to search for a worker's labels. These
    labels contain information about each worker's target queue, such as its name and maximum length. A non-null and non-empty value must be provided for this environment variable.

* `CAF_WMP_KUBERNETES_LABEL_CACHE_EXPIRY_MINUTES`  
    **Default**: 60.  
    **Description**: Used to specify the 'expire after write' minutes after which a Kubernetes label that has been added to the cache
    should be removed. Set this to 0 to disable caching.

* `CAF_NOOP_MODE`  
  **Default**: `true`  
  **Description**: Used to toggle the TunedTargetQueueLength functionality on and off. While NoOp mode is true, any recommended tuning 
  of the target queue length will not be implemented. Target queue length will only be altered if this is false. 

* `CAF_ROUNDING_MULTIPLE`  
  **Default**: `100`  
  **Description**: Used to round the recommended tuned target queue length to the nearest rounding multiple value. The default value 
  is 100, therefore the recommended tuned target queue length will be rounded to the nearest 100.

* `CAF_MAX_CONSUMPTION_RATE_HISTORY_SIZE`  
  **Default**: `100`  
  **Description**: Used to set the maximum amount of tuned target queue length history that will be stored. This will be used to 
  recommend an average tuned target queue length, based on up to 100 values. 

* `CAF_MIN_CONSUMPTION_RATE_HISTORY_SIZE`  
  **Default**: `10`  
  **Description**: Used to set the minimum tuned target queue length history that is required before an actual change to the target 
  queue length can be made. When noOp mode is false and the history rate size is above the minimum set value, then the target queue 
  length will be altered. 

* `CAF_QUEUE_PROCESSING_TIME_GOAL_SECONDS`  
  **Default**: `300`  
  **Description**: Used to set the amount of time in which we want a target queue length to be processed. This time will be used to 
  compute how long the tuned target queue length should be to process the queue in this set amount of time. 
