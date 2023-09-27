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

* `CAF_WMP_CONSUMER_PUBLISHER_PAIR_LAST_DONE_WORK_TIMEOUT_MILLISECONDS`  
    **Default**: `600000`  
    **Description**: The timeout in milliseconds since it has last done work after which to close a RabbitMQ staging queue consumer/target
    queue publisher pair. This is used to clean up staging queue consumer/target queue publisher pairs that may have become stuck.
    Set to 0 to disable this feature.

* `CAF_WMP_KUBERNETES_NAMESPACES`  
    **Default**: None.  
    **Description**: Used to specify the Kubernetes namespaces, comma separated, in which to search for a worker's labels. These
    labels contain information about each worker's target queue, such as its name and maximum length. A non-null and non-empty value must be provided for this environment variable.

* `CAF_WMP_KUBERNETES_LABEL_CACHE_EXPIRY_MINUTES`  
    **Default**: 60.  
    **Description**: Used to specify the 'expire after write' minutes after which a Kubernetes label that has been added to the cache
    should be removed. Set this to 0 to disable caching.

* `CAF_ENABLE_TARGET_QUEUE_LENGTH_TUNING`  
  **Default**: `false`  
  **Description**: Used to toggle the TunedTargetQueueLength functionality on and off. While this variable is set to is true, 
  any recommended tuning of the target queue length will be implemented when there is sufficient consumption rate history to do so. 
  Target queue length tuning will be turned off if this value is set to false, and the default target queue lengths used.

* `CAF_MIN_TARGET_QUEUE_LENGTH`  
  **Default**: `100`  
  **Description**: Used to determine the minimum length the tunedTargetQueueLength can be reduced to. Will not go below this length. 

* `CAF_MAX_TARGET_QUEUE_LENGTH`  
  **Default**: `10000000`  
  **Description**: Used to determine the maximum length the tunedTargetQueueLength can be increased to. Will not go above this length.
* 
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
  queue length can be made. When tuning is enabled and the history rate size is above the minimum set value, then the target queue
  length will be altered. 

* `CAF_QUEUE_PROCESSING_TIME_GOAL_SECONDS`  
  **Default**: `300`  
  **Description**: Used to set the amount of time in which we want a target queue length to be processed. This time will be used to 
  compute how long the tuned target queue length should be to process the queue in this set amount of time. 

* `CAF_CONSUMPTION_TARGET_CALCULATOR_MODE`  
  **Default**: `EqualConsumptionTargetCalculator`  
  **Description**: Used to set the method of moving messages from their staging queues on to their target queues. The default is 
  EqualConsumptionTargetCalculator meaning that messages will be moved equally and fairly on to the target queue. Currently, the other 
  option is FastLaneConsumptionTargetCalculator. When this is set, the ability exists to alter weighting for different workers to get 
  messages processed faster or slower than other workers.

* `CAF_ADJUST_WORKER_WEIGHT`  
  **Default**: None.   
  **Description**: Used when CAF_CONSUMPTION_TARGET_CALCULATOR_MODE is set to FastLaneConsumptionTargetCalculator. This controls the 
  workers that are to be weighted to increase or decrease processing. This should be set using a string of a regex followed by a number. 
  The regex will match the worker, and the number will indicate the weighting to be added to the worker. For example to allow 
  enrichment-workflow to get processed at 10 times the rate of other workers this variable should be set to "enrichment\-workflow$,10".
  Multiple of these can be added at any one time, just add an incrementing value to the variable name, eg: CAF_ADJUST_WORKER_WEIGHT_1. 
  Do decrease weighting the integer should be less than one. Note it is possible to set the weight to 0 in order to stop processing of 
  the worker entirely. 
