package com.itcanteen.es.kafka;

import com.alibaba.fastjson.JSON;
import com.itcanteen.es.vo.BinLogKafkaData;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

/**
 * @author baimugudu
 * @email 2415621370@qq.com
 * @date 2019/9/16 9:50
 */

@Component
@Slf4j
public class BinlogConsumer {

    @Autowired
    private EsSyncData esSyncData;


    @KafkaListener(topics={"ad-search-mysql-data"})
    public  void consumerMysqlBinLogData(ConsumerRecord<?, ?> record) throws IOException {
        log.info("开始监听消息");
      //  System.out.println(record.value());
        Optional<?> kafkaMessage = Optional.ofNullable(record.value());
        log.info("开始监听消息-》{}",record.value());
        if(kafkaMessage.isPresent()){
            Object o = kafkaMessage.get();
            log.info("消息的内容是-》{}",o.toString());
            //反序列化：将json字符串转化成对象
            BinLogKafkaData binLogKafkaData =
                    JSON.parseObject(o.toString(), BinLogKafkaData.class);

            String eventType = binLogKafkaData.getEventType();

            String tableName =  binLogKafkaData.getTableName();


            //添加数据
            if(eventType.equals("WriteRowsEventData")){
                esSyncData.add(tableName,binLogKafkaData);

            }
            //更新数据
            if(eventType.equals("UpdateRowsEventData")){
                esSyncData.upd(tableName,binLogKafkaData);
            }

            //删除数据
            if(eventType.equals("DeleteRowsEventData")){
                esSyncData.del(tableName,binLogKafkaData);
            }


        }


    }

}
