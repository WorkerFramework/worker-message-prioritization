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
