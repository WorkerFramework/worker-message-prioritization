package com.microfocus.apollo.worker.prioritization.redistribution;

import com.google.gson.Gson;
import com.microfocus.apollo.worker.prioritization.rabbitmq.QueuesApi;
import com.microfocus.apollo.worker.prioritization.rabbitmq.RabbitManagementApi;
import com.microfocus.apollo.worker.prioritization.rabbitmq.ShovelsApi;
import com.rabbitmq.client.ConnectionFactory;

public class DistributorTestBase {

    protected static final String T1_STAGING_QUEUE_NAME = "target»tenant1";
    protected static final String T2_STAGING_QUEUE_NAME = "target»tenant2";
    protected static final String TARGET_QUEUE_NAME = "target";
    
    protected final Gson gson = new Gson();
    protected ConnectionFactory connectionFactory;
    protected int managementPort;
    protected RabbitManagementApi<QueuesApi> queuesApi;
    protected RabbitManagementApi<ShovelsApi> shovelsApi;

    public DistributorTestBase() {
        final var connectionFactory = new ConnectionFactory();
        connectionFactory.setHost(System.getProperty("rabbitmq.node.address", "localhost"));
        connectionFactory.setUsername(System.getProperty("rabbitmq.username", "guest"));
        connectionFactory.setPassword(System.getProperty("rabbitmq.password", "guest"));
        connectionFactory.setPort(Integer.parseInt(System.getProperty("rabbitmq.node.port", "25672")));
        connectionFactory.setVirtualHost("/");
        this.connectionFactory = connectionFactory;

        managementPort = Integer.parseInt(System.getProperty("rabbitmq.ctrl.port", "25673"));

        queuesApi =
                new RabbitManagementApi<>(QueuesApi.class,
                        "http://" + connectionFactory.getHost() + ":" + managementPort + "/",
                        connectionFactory.getUsername(), connectionFactory.getPassword());
        
        shovelsApi = new RabbitManagementApi<>(ShovelsApi.class, 
                "http://" + connectionFactory.getHost() + ":" + managementPort + "/",
                connectionFactory.getUsername(), connectionFactory.getPassword());

    }
    
}
