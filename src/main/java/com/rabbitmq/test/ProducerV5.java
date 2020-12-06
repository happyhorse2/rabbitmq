package com.rabbitmq.test;
import com.rabbitmq.client.*;
import org.omg.CosNaming.NamingContextExtPackage.StringNameHelper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;


public class ProducerV5 {
    static String EXCHANGE_NAME = "exchange_name_v2";
    static String BEI_FEN_EXCHANGE_NAME = "bei_fen_exchange_name";
    static String UN_ROUTED_QUEUE_NAME = "un_routed_exchange_name";
    static String QUEUE_NAME = "queue_name";
    static String ROUTING_KEY = "root_key";
    public static void main(String[] args) throws Exception{
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUsername("kreditplus_dev");
        factory.setPassword("kreditplus_dev");
        factory.setVirtualHost("/");
        factory.setHost("10.12.24.104");
        factory.setPort(5672);
        Connection conn = null;
        final Channel channel;
        try {
            conn = factory.newConnection();
            channel = conn.createChannel();
            Map<String,Object> argsProperties = new HashMap<String, Object>();
            argsProperties.put("alternate-exchange", BEI_FEN_EXCHANGE_NAME);
            channel.exchangeDeclare(EXCHANGE_NAME,"direct", true, false, argsProperties);
            channel.exchangeDeclare(BEI_FEN_EXCHANGE_NAME,"fanout", true, false, null);
            channel.queueDeclare(QUEUE_NAME, true, false, false, null);
            channel.queueDeclare(UN_ROUTED_QUEUE_NAME, true, false, false, null);
            channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, ROUTING_KEY);
            channel.queueBind(UN_ROUTED_QUEUE_NAME, BEI_FEN_EXCHANGE_NAME, "");
            int i=1;
            while (i<20){
                String string = "hello world"+i;
                channel.basicPublish(EXCHANGE_NAME,  ROUTING_KEY+"uuuufff", true, MessageProperties.PERSISTENT_TEXT_PLAIN,
                        string.getBytes());
                i++;
                System.out.println(i+"ddd");
            }
            System.out.println("dddfdafdf");
            //channel.addReturnListener();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }finally {
            conn.close();
        }
        System.out.println("end");
    }
}
