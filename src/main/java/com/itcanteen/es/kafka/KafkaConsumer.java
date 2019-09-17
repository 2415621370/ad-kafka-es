package com.itcanteen.es.kafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.Properties;

/**
 * @author baimugudu
 * @email 2415621370@qq.com
 * @date 2019/9/17 10:20
 */

@Component
@Slf4j
public class KafkaConsumer implements CommandLineRunner {

/*    private static KafkaConsumer<String,String> consumer;
    private static Properties properties;
    static {
        properties = new Properties();
        properties.put("bootstrap.servers","10.211.55.8:9092");
        properties.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id","test-consumer-group");
    }*/

    @Override
    public void run(String... strings) throws Exception {

    while(true){

    }

    }
}
