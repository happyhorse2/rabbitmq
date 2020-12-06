ckage com.rabbitmq.test;
import com.rabbitmq.client.*;
import com.rabbitmq.client.AMQP.BasicProperties;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class Producer {
    static String EXCHANGE_NAME = "exchange_name";
    static String QUEUE_NAME = "queue_name_v2";
    static String ROUTING_KEY = "root_key";
    static String DEAD_DLX_EXCHANGE = "dlx_exchange";
    static String DEAD_QUEUE = "dlx_queue";
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
            System.out.println("11111");
            conn = factory.newConnection();
            System.out.println("222222");
            channel = conn.createChannel();
            channel.exchangeDeclare(EXCHANGE_NAME,"direct", true, false, null);
            //dead exchange
            channel.exchangeDeclare(DEAD_DLX_EXCHANGE,"direct", true, false, null);
            Map<String,Object> ars= new HashMap<String,Object>();
            ars.put("x-dead-letter-exchange", DEAD_DLX_EXCHANGE);
            //也可以为这个DLX指定路由键，如果没有特殊指定，则使用原队列的路由键
            ars.put("x-dead-letter-routing-key","dlx-routing-key");  
            channel.queueDeclare(QUEUE_NAME, true, false, false, ars);
            channel.queueDeclare(DEAD_QUEUE, true, false, false, null);
            channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, ROUTING_KEY);
            channel.queueBind(DEAD_QUEUE, DEAD_DLX_EXCHANGE, "dlx-routing-key");
            int i=10;
            while (i<20){
                String string = "hello world"+i;
                channel.basicPublish(EXCHANGE_NAME,  ROUTING_KEY, new BasicProperties.Builder().deliveryMode(2).build(),  //设置为持久化
                        string.getBytes());
                i++;
            }

            channel.close();
            conn.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
        System.out.println("aaaaaa");
    }

}
