# worker-message-prioritization-rerouting

## Configuration

### Environment Variables

* `CAF_WMP_ENABLED`  
    **Default**: false  
    **Description**: Determines whether a worker should reroute a message or not. If true, a message will attempt to be rerouted.
    If false, a message will not be rerouted and will be sent to the target queue rather than to a staging queue. This environment 
    variable should be set on whichever component is performing the rerouting. For example, if Worker A routes a message to Worker B, and
    you want Worker A to reroute the message to one of Worker B's staging queues instead of to Worker B's target queue, then you would
    set this environment variable on **Worker A**.

* `CAF_WMP_STAGING_QUEUE_HTTP_CACHE_MAX_AGE_MILLISECONDS`  
    **Default**: 60000  
    **Description**: A HTTP cache is used to store staging queues returned by the RabbitMQ API. The cache is used during the
    `MessageRouterSingleton::route` call to determine whether a staging queue needs to be created or not. It is possible that a 
    staging queue is deleted after this cache has been checked, but before it has been refreshed. In this case, a staging queue would
    not be created when it should be, and routing a message to that staging queue would fail. However, the staging queue will 
    eventually be created if `MessageRouterSingleton::route` is called again after the cache has expired.
