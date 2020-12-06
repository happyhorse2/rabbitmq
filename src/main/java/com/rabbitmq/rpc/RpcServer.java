package com.rabbitmq.rpc;

import com.rabbitmq.client.*;
import com.rabbitmq.client.AMQP.BasicProperties;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class RpcServer {
    private Connection connection;
    private Channel channel;
    private String requestQueueName = "rpc_queue";
    private String replyQueueName;
    private String response;
    private static final String RPC_QUEUE_NAME = "rpc_queue";

    public RpcServer() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("10.12.24.104");
        factory.setPort(5672);
        factory.setUsername("kreditplus_dev");
        factory.setPassword("kreditplus_dev");
        connection = factory.newConnection();
        channel = connection.createChannel();
    }

    public void process() throws IOException, TimeoutException{
        channel.queueDeclare(RPC_QUEUE_NAME,false,false,false,null);
        channel.basicQos(1);
        System.out.println("waiting for message");
        channel.basicConsume(RPC_QUEUE_NAME, false, "serverconsumer", new DefaultConsumer(channel) {
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
                System.out.println(deliveryTag);
                // (process the message components here ...)
                //System.out.print("aaaaaavdadfadsa"+new String(body));
                response = new String(body);
                System.out.println("server get message  "+response);
                BasicProperties replyProps = new BasicProperties.Builder().correlationId(properties.getCorrelationId()).build();
                int n = Integer.parseInt(response);
                System.out.println(" [.] fib("+response+")");
                String conResponse = ""+fib(n);
                channel.basicPublish("", properties.getReplyTo(), replyProps, conResponse.getBytes());
                try {
                    Thread.sleep(1000*6);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                channel.basicAck(deliveryTag, false);
            }
        });
    }

    public int fib(int n){
        return n*2;
    }

    public static void main(String[] args) {
        try {
            RpcServer server = new RpcServer();
            server.process();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
