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

* `CAF_WMP_KUBERNETES_NAMESPACES`  
    **Default**: None. A value must be provided for this environment variable. Must not be null and must not be empty.
    **Description**: Used to specify the Kubernetes namespaces, comma separated, in which to search for a worker's labels. These
    labels contain information about each worker's target queue, such as its name and maximum length.
