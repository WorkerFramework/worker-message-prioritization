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
