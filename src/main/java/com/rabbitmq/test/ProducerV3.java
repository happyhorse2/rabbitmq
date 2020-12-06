package com.rabbitmq.test;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.TreeMap;
import java.util.concurrent.TimeoutException;

public class ProducerV3 {
    static String EXCHANGE_NAME = "exchange_name";
    static String QUEUE_NAME = "queue_name";
    static String ROUTING_KEY = "root_key";
    public static void main(String[] args) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUsername("kreditplus_dev");
        factory.setPassword("kreditplus_dev");
        factory.setVirtualHost("/");
        factory.setHost("10.12.24.104");
        factory.setPort(5672);
        Connection conn = null;
        final Channel channel;
        final TreeMap<Long, String> map = new TreeMap<Long, String>();
        try {
            conn = factory.newConnection();
            channel = conn.createChannel();
            channel.exchangeDeclare(EXCHANGE_NAME,"direct", true, false, null);
            channel.exchangeDeclare(EXCHANGE_NAME+"dddd","direct", true, false, null);

            channel.queueDeclare(QUEUE_NAME, true, false, false, null);
            channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, ROUTING_KEY);
            channel.confirmSelect();
            channel.addReturnListener(new ReturnListener() {
                public void handleReturn(int replyCode, String replyText, String exchange, String routingKey, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    System.out.println("replyCode:"+replyCode);
                    System.out.println("replyText:"+replyText);
                    System.out.println("exchange:"+exchange);
                    System.out.println("routingKey:"+routingKey);
                    String string = new String(body);
                    System.out.println("basic return"+string);

                }
            });
            channel.addConfirmListener(new ConfirmListener() {
                public void handleAck(long deliveryTag, boolean multiple) throws IOException {
                   System.out.println("handleAck"+deliveryTag);
                   map.remove(deliveryTag);
                }
                public void handleNack(long deliveryTag, boolean multiple) throws IOException {
                    System.out.println("handleNAck"+deliveryTag);
                    map.remove(deliveryTag);
                    // string need has primary key,
                    // repeat send
                    String string = map.get(deliveryTag);
                    channel.basicPublish(EXCHANGE_NAME,  ROUTING_KEY, true, MessageProperties.PERSISTENT_TEXT_PLAIN,
                            string.getBytes());
                }
            });
            int i=1;
            while (i<20){
                String string = "hello world"+i;
                long nextNo = channel.getNextPublishSeqNo();
                channel.basicPublish(EXCHANGE_NAME,  ROUTING_KEY, false, MessageProperties.PERSISTENT_TEXT_PLAIN,
                        string.getBytes());
                i++;
                map.put(nextNo, string);
            }
            System.out.println("dddfdafdf");
            //channel.addReturnListener();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }
}
