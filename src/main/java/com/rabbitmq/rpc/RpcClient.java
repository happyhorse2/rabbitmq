package com.rabbitmq.rpc;


import com.rabbitmq.client.*;
import com.rabbitmq.client.AMQP.BasicProperties;

import java.io.IOException;
import java.util.ConcurrentModificationException;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

public class RpcClient {
    private Connection connection;
    private Channel channel;
    private String requestQueueName = "rpc_queue";
    private String replyQueueName;
    private int response= 7;


    public RpcClient() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("10.12.24.104");
        factory.setPort(5672);
        factory.setUsername("kreditplus_dev");
        factory.setPassword("kreditplus_dev");
        connection = factory.newConnection();
        channel = connection.createChannel();
        replyQueueName = channel.queueDeclare().getQueue();

    }


    public int call(String message) throws IOException, ShutdownSignalException, ConcurrentModificationException, InterruptedException{
        String corrId = UUID.randomUUID().toString();
        BasicProperties props = new BasicProperties.Builder()
                .correlationId(corrId)
                .replyTo(replyQueueName)
                .build();
        channel.basicPublish("", requestQueueName, props, message.getBytes());
        channel.basicConsume(replyQueueName, false, "clientconsumer", new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag,
                                       Envelope envelope,
                                       AMQP.BasicProperties properties,
                                       byte[] body)
                    throws IOException
            {
                String routingKey = envelope.getRoutingKey();
                String contentType = properties.getContentType();
                long deliveryTag = envelope.getDeliveryTag();
                // (process the message components here ...)
                System.out.println("aaaaaavdadfadsa"+deliveryTag);
                System.out.println(RpcClient.this.response+ "55555555555555");
                RpcClient.this.response = 8;
                System.out.println("client get server   "+ RpcClient.this.response );
                try {
                    Thread.sleep(1000*6);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                channel.basicAck(deliveryTag, false);
            }
        });
        System.out.println("what's wrong  "+ RpcClient.this.response);
        Thread.sleep(10);
        return RpcClient.this.response;
    }
    public void close() throws Exception{
        connection.close();
    }

    public static void main(String[] args) {
        try {
            RpcClient fibRpc = new RpcClient();
            System.out.println(" [x] Requesting fib(30)");
            int response = fibRpc.call("70");
            System.out.println(" [.] Got '"+response+"'");
            //fibRpc.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
