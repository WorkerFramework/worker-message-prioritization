# worker-message-prioritization-distribution-container

This repository consists of the source to build a container that includes the 
[Worker Message Prioritization Distributor](https://github.com/WorkerFramework/worker-message-prioritization/tree/main/worker-message-prioritization-distribution) application.

## Configuration

### Environment Variables

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