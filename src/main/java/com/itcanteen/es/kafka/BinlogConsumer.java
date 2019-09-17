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
    private TransportClient client;


    @KafkaListener(topics={"ad-search-mysql-data"})
    public  void consumerMysqlBinLogData(ConsumerRecord<?, ?> record) throws IOException {
        log.info("开始监听消息");
        System.out.println(record.value());
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

//{"after":[{"user_status":"0","update_time":"Thu Jan 01 08:00:00 IRKT 1970",
// "create_time":"Thu Jan 01 08:00:00 IRKT 1970",
// "id":"30","username":"21ww","token":"2122323232"}],"eventType":"UpdateRowsEventData","tableName":"ad_user"}
            //添加数据
            if(eventType.equals("WriteRowsEventData")){

                if(tableName.equals("ad_plan")){
                    for(int i=0;i<binLogKafkaData.getAfter().size();i++){
                        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject()
                                .field("user_status", binLogKafkaData.getAfter().get(i).get("user_status"))
                                .field("update_time", binLogKafkaData.getAfter().get(i).get("update_time"))
                                .field("create_time", binLogKafkaData.getAfter().get(i).get("create_time"))
                                .field("username", binLogKafkaData.getAfter().get(i).get("username"))
                                .field("token",binLogKafkaData.getAfter().get(i).get("token"))
                                .field("id", binLogKafkaData.getAfter().get(i).get("id"))
                                .endObject();

                        IndexResponse indexResponse = this.client.prepareIndex(
                                "ad", "ad_user",binLogKafkaData.getAfter().get(i).get("id")
                        ).setSource(xContentBuilder).get();

                        log.info("添加数据->{}",indexResponse.getResult().toString());
                    }
                }


               // if()
                for(int i=0;i<binLogKafkaData.getAfter().size();i++){
                    XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject()
                            .field("user_status", binLogKafkaData.getAfter().get(i).get("user_status"))
                            .field("update_time", binLogKafkaData.getAfter().get(i).get("update_time"))
                            .field("create_time", binLogKafkaData.getAfter().get(i).get("create_time"))
                            .field("username", binLogKafkaData.getAfter().get(i).get("username"))
                            .field("token",binLogKafkaData.getAfter().get(i).get("token"))
                            .field("id", binLogKafkaData.getAfter().get(i).get("id"))
                            .endObject();

                    IndexResponse indexResponse = this.client.prepareIndex(
                            "ad", "ad_user",binLogKafkaData.getAfter().get(i).get("id")
                    ).setSource(xContentBuilder).get();

                    log.info("添加数据->{}",indexResponse.getResult().toString());
                }
            }
            //更新数据
            if(eventType.equals("UpdateRowsEventData")){
                binLogKafkaData.getAfter().forEach(i->{
                    UpdateRequest updateRequest = new UpdateRequest("ad", "ad_user", i.get("id"));

                    //构建请求字段
                    try {
                        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject();
                        if(null!=i.get("user_status")){
                            xContentBuilder.field("user_status",i.get("user_status"));
                        }

                        if(null!=i.get("update_time")){
                            xContentBuilder.field("update_time",i.get("update_time"));
                        }

                        if(null!=i.get("create_time")){
                            xContentBuilder.field("create_time",i.get("create_time"));
                        }

                        if(null!=i.get("username")){
                            xContentBuilder.field("username",i.get("username"));
                        }

                        if(null!=i.get("token")){
                            xContentBuilder.field("token",i.get("token"));
                        }
                        xContentBuilder.endObject();
                        updateRequest.doc(xContentBuilder);

                        UpdateResponse updateResponse = this.client.update(updateRequest).get();
                        log.info("更新数据-》{}",updateResponse.getResult().toString());

                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    }

                });

            }

            //删除数据
            if(eventType.equals("DeleteRowsEventData")){
                binLogKafkaData.getAfter().forEach(i->{
                    DeleteRequestBuilder deleteRequestBuilder = this.client.prepareDelete("ad", "ad_user", i.get("id"));
                    DeleteResponse deleteResponse = deleteRequestBuilder.get();
                    log.info("删除数据-》{}",deleteResponse.getResult().toString());
                });
            }


        }


    }

}
