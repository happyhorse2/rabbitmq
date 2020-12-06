package com.rabbitmq.test;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.TreeSet;
import java.util.concurrent.TimeoutException;

public class ProducerV2 {
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
        Channel channel = null;
        try {
            conn = factory.newConnection();
            channel = conn.createChannel();
            channel.exchangeDeclare(EXCHANGE_NAME,"direct", true, false, null);
            //channel.exchangeDeclare(EXCHANGE_NAME+"dddd","direct", true, false, null);

            channel.queueDeclare(QUEUE_NAME, true, false, false, null);
            channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, ROUTING_KEY);
            channel.confirmSelect();
            ArrayList<String> arraylist = new ArrayList<String>();
            channel.addReturnListener(new ReturnListener() {
                public void handleReturn(int replyCode, String replyText, String exchange, String routingKey, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    System.out.println("replyCode:" + replyCode);
                    System.out.println("replyText:" + replyText);
                    System.out.println("exchange:" + exchange);
                    System.out.println("routingKey:" + routingKey);
                    String string = new String(body);
                    System.out.println("basic return" + string);
                }
            });
            int i=1;
            int mod = 0;
            while (i<40) {
                String string = "hello world" + i;
                long nextNo = channel.getNextPublishSeqNo();
                System.out.println(nextNo + "addddddddd");
                channel.basicPublish(EXCHANGE_NAME, ROUTING_KEY, true, MessageProperties.PERSISTENT_TEXT_PLAIN,
                        string.getBytes());
                i++;
                arraylist.add(string);
                if (++mod >= 20) {
                    mod = 0;
                    try {
                        boolean value = channel.waitForConfirms();
                        System.out.println(value);
                        if (!value) {
                            //repeat send
                            for (String string_v2 : arraylist) {
                                channel.basicPublish(EXCHANGE_NAME, "dadfa", true, MessageProperties.PERSISTENT_TEXT_PLAIN,
                                        string_v2.getBytes());
                            }
                            System.out.println();
                        } else {
                            arraylist.clear();
                            System.out.println("success" + string);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
            //channel.addReturnListener();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
        System.out.println("aaaaaa");
    }
}
