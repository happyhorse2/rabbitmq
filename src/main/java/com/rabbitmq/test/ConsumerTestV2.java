package com.rabbitmq.test;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class ConsumerTestV2 {
    static String QUEUE_NAME = "queue_name";

    public static void main(String[] args) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("10.12.24.104");
        factory.setPort(5672);
        factory.setUsername("kreditplus_dev");
        factory.setPassword("kreditplus_dev");
        //factory.setVirtualHost("/");
        Connection connection = null;

        try {
            connection = factory.newConnection();
            final Channel channel = connection.createChannel();
            //设置客户端最多接收未被ack的消息的个数
            channel.basicQos(3);
            System.out.println("aaaaaa");
            boolean autoAck = false;
            channel.basicConsume(QUEUE_NAME, autoAck, "myConsumerTagV2",
                    new DefaultConsumer(channel) {
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
                            System.out.print("aaaaaavdadfadsa"+new String(body)+deliveryTag);
                            try {
                                Thread.sleep(1000);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                            channel.basicAck(deliveryTag, false);
                        }
                    });
            System.out.println("fuck");
//            hannel.close();
//            connection.close();

       } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }finally {

        }
    }
}
