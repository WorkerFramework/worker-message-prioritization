# worker-message-prioritization-rerouting

## Configuration

### Environment Variables

These environment variables should be set on whichever worker is performing the rerouting.

For example, if Worker A routes a message to Worker B, and you want Worker A to reroute the message to one of Worker B's staging queues
instead of to Worker B's target queue, then you would set these environment variables on **Worker A**.

* `CAF_WMP_ENABLED`  
    **Default**: false  
    **Description**: Determines whether a worker should reroute a message or not. If true, a message will attempt to be rerouted.
    If false, a message will not be rerouted and will be sent to the target queue rather than to a staging queue.

* `CAF_WMP_USE_TARGET_QUEUE_CAPACITY_TO_REROUTE`  
    **Default**: false  
    **Description**: Only applies when `CAF_WMP_ENABLED` is true. Determines whether a worker should use the target queue's capacity when
    making a decision on whether to reroute a message. If true, a message will only be rerouted to a staging queue if the target queue
    does not have capacity for it. If false, a message will **always** be rerouted to a staging queue, irregardless of the target queue's
    capacity.

