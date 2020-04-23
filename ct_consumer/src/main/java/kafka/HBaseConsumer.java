package kafka;

import hbase.HBaseDao;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import utils.PropertiesUtil;

import java.util.Arrays;

/*
 * @author: sunxiaoxiong
 * @date  : Created in 2020/4/15 17:31
 */
//kafka消费类
public class HBaseConsumer {
    public static void main(String[] args) {
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(PropertiesUtil.properties);
        kafkaConsumer.subscribe(Arrays.asList(PropertiesUtil.getProperty("kafka.topics")));
        HBaseDao hd = new HBaseDao();
        while (true) {
            //从kafka的主题中获取数据
            ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                String value = record.value();
                String key = record.key();
                System.out.println(value);
                System.out.println(key);
                //将数据保存到hbase中
                hd.put(value);
            }
        }
    }
}
